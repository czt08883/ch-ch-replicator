# Architecture

_Last updated: 2026-04-01_

## Summary

ch-ch-replicator is a single-binary Rust async CLI that replicates a ClickHouse database to another ClickHouse instance over HTTP. It runs as a sequential 5-phase pipeline: connectivity check → schema discovery/setup → parallel initial full-table sync → perpetual CDC polling loop. All data transfer uses JSONEachRow format; there is no native TCP protocol dependency.

## Pattern Overview

**Overall:** Sequential phase pipeline with async parallelism inside the initial-sync phase.

**Key Characteristics:**
- Tokio-based async runtime; all I/O is non-blocking
- Shared state passed via `Arc<T>` and `Arc<Mutex<T>>`; no global state
- Graceful shutdown driven by a `CancellationToken` checked at every phase boundary and loop iteration
- Append-only replication: no UPDATE/DELETE detection; suitable for immutable event streams and versioned tables

## Phases (executed in `main.rs::run()`)

**Phase 1 — Connectivity:**
- Purpose: Verify TCP reachability and auth to both ClickHouse instances before any work begins
- Entry: `check_connectivity()` in `src/main.rs`
- Mechanism: `ClickHouseClient::ping()` runs `SELECT 1` via HTTP GET

**Phase 2 — Schema Discovery & Setup:**
- Purpose: Enumerate source tables; create target database and tables idempotently
- Entry: `setup_target_schema()` in `src/main.rs`
- Key functions: `schema::list_tables()`, `schema::get_create_ddl()`, `schema::ensure_database()`, `schema::apply_ddl()`
- DDL is adapted: source DB name replaced with destination, `CREATE TABLE IF NOT EXISTS` enforced, deprecated MergeTree engine syntax rewritten to modern form

**Phase 3 — Initial Sync:**
- Purpose: Full-table copy from source to destination with resumability
- Entry: `InitialSync::run()` in `src/sync.rs`
- Mechanism: Per-table, spawns `config.threads` Tokio tasks; each claims batches of 50,000 rows via a shared `AtomicU64` offset counter; SELECT with `LIMIT/OFFSET` → INSERT as JSONEachRow; per-batch checkpointing tracks the contiguous frontier via `BatchTracker`
- Retry: up to 10 attempts per batch with 10-second delays; respects cancellation between retries

**Phase 4 — CDC Loop:**
- Purpose: Perpetual polling for new rows; runs until SIGINT/SIGTERM
- Entry: `CdcEngine::run()` in `src/cdc.rs`
- Mechanism: Polls each table sequentially every 5 seconds (configurable); two strategies:
  1. **Watermark-based** (preferred): `WHERE wm_col > last_wm_val ORDER BY wm_col ASC LIMIT batch_size`; updates watermark to MAX value in returned batch
  2. **Row-count fallback**: compares `COUNT()` between source and destination; fetches the delta rows by offset if count increased
- Watermark seeding: on first run, seeds from source `MAX(wm_col)` after initial sync to avoid re-replicating already-synced rows

**Phase 5 — Graceful Shutdown:**
- Mechanism: A background Tokio task listens for SIGINT/SIGTERM and calls `cancel.cancel()`; all phases and batch loops check `cancel.is_cancelled()` at their tops and between retries; the `Cancelled` error variant propagates up and is treated as a clean exit

## Data Flow

**Initial Sync Batch (per worker):**

1. Worker atomically claims next offset via `AtomicU64::fetch_add`
2. `ClickHouseClient::select_batch_raw()` → HTTP GET to source: `SELECT * FROM db.table ORDER BY sorting_key LIMIT N OFFSET M FORMAT JSONEachRow`
3. Raw JSONEachRow string returned (no deserialization)
4. `ClickHouseClient::insert_jsonl()` → HTTP POST to destination: `INSERT INTO db.table FORMAT JSONEachRow` with body bytes
5. `BatchTracker::mark_complete()` advances the contiguous frontier
6. `Checkpoint::update_synced_rows()` writes `checkpoint.json` atomically

**CDC Watermark Poll (per table):**

1. `ClickHouseClient::select_delta_raw()` → HTTP GET to source: `SELECT * FROM db.table WHERE wm_col > 'last_val' ORDER BY wm_col ASC LIMIT N FORMAT JSONEachRow`
2. `max_watermark_in_jsonl()` scans raw body to find new high watermark
3. `ClickHouseClient::insert_jsonl()` → HTTP POST to destination
4. `Checkpoint::update_watermark()` saves new watermark atomically

**State Management:**
- `Arc<Mutex<Checkpoint>>` is the single shared mutable state object, accessed under a lock only briefly per checkpoint write
- `Arc<ClickHouseClient>` is shared read-only across all tasks (reqwest Client is internally thread-safe)
- Per-table CDC state (`TableState`) is owned by the CDC loop and not shared

## Key Abstractions

**`ClickHouseClient` (`src/client.rs`):**
- Purpose: Thin async HTTP wrapper over the ClickHouse HTTP API
- All SELECT queries use HTTP GET with `?query=...`; all INSERT/DDL use HTTP POST with explicit `Content-Length` header (required by ClickHouse 24.x to avoid HTTP 411)
- Key methods: `ping()`, `query_json_rows()`, `query_scalar()`, `query_typed<T>()`, `select_batch_raw()`, `select_delta_raw()`, `insert_jsonl()`, `execute_no_db()`

**`Checkpoint` (`src/checkpoint.rs`):**
- Purpose: Durable per-table sync state stored as JSON on disk
- Atomic writes: write to `checkpoint.json.tmp` then `rename()` to final path
- Tracks: `synced_rows` (initial sync progress), `initial_sync_complete` flag, `cdc_watermark` value, `watermark_column` name

**`WatermarkKind` (`src/schema.rs`):**
- Purpose: Discriminated union encoding the CDC strategy for a table
- Variants: `DateTime(col_name)`, `UInt64(col_name)`, `None` (falls back to row-count)
- Selection logic in `pick_watermark()`: 4-tier priority with Nullable awareness; MATERIALIZED/ALIAS/EPHEMERAL columns excluded

**`BatchTracker` (`src/sync.rs`):**
- Purpose: Computes the contiguous checkpoint frontier across out-of-order parallel batch completions
- Uses a `BTreeSet` of ahead-of-frontier offsets; frontier advances only when the next expected batch completes

**`Config` / `ClickHouseConfig` (`src/config.rs`):**
- Purpose: Parsed and validated connection + tuning parameters
- Constants embedded in `Config::new()`: `batch_size = 50_000`, `cdc_poll_secs = 5`, `checkpoint_path = "checkpoint.json"`

## Entry Points

**`main()` (`src/main.rs`):**
- Parses CLI args via `clap` derive
- Masks passwords in process title via `proctitle` crate
- Initializes `tracing_subscriber` with `RUST_LOG` env-filter
- Delegates to `run()` and handles `ReplicatorError::Cancelled` as clean exit vs. fatal error

**`run()` (`src/main.rs`):**
- Orchestrates all 5 phases sequentially
- Constructs `Arc<Config>`, `Arc<ClickHouseClient>` x2, `Arc<Mutex<Checkpoint>>`
- Spawns signal handler task with cloned `CancellationToken`

## Error Handling

**Strategy:** `thiserror`-based error enum `ReplicatorError` with a `Result<T>` type alias. Errors propagate via `?` up to `run()`. The `Cancelled` variant is the only non-fatal exit path.

**Patterns:**
- `#[from]` auto-conversions for `serde_json::Error`, `std::io::Error`
- Manual `From<reqwest::Error>` implementation that masks passwords in error messages before logging
- CDC loop catches per-table errors and logs them without aborting; initial sync collects worker errors and fails the phase
- Schema and connectivity errors are immediately fatal

## Cross-Cutting Concerns

**Logging:** `tracing` crate with `tracing_subscriber`; level controlled by `RUST_LOG` (defaults to `info`). Structured spans not used — all logging is flat `info!/warn!/error!/debug!` macros.

**Validation:** DSN validation in `ClickHouseConfig::from_dsn()` (scheme, host, port, user, password, database all required). No runtime config hot-reload.

**Authentication:** ClickHouse credentials passed as query parameters (`user=`, `password=`) on every HTTP request. Passwords masked in logs and process title.

## Gaps / Unknowns

- No UPDATE or DELETE replication. Rows deleted or updated at source are silently ignored.
- CDC poll is sequential across tables — a slow/large table delays polling of subsequent tables.
- `batch_size`, `cdc_poll_secs`, and `checkpoint_path` are hardcoded in `Config::new()`; they are not exposed as CLI flags.
- No backpressure or adaptive throttling — inserts go as fast as the destination accepts.
- The row-count CDC fallback (`poll_by_count`) uses `LIMIT N OFFSET M` which can miss rows if source rows are not stable in insertion order.
