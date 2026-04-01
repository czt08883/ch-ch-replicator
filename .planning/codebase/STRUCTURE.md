# Codebase Structure

_Last updated: 2026-04-01_

## Summary

ch-ch-replicator is a flat single-crate Rust project. All application logic lives directly under `src/` as 7 modules; there are no subdirectories within `src/`. The binary entry point is `src/main.rs`. Tests are co-located inside each module under `#[cfg(test)]`.

## Directory Layout

```
ch-ch-replicator/
├── src/
│   ├── main.rs          # CLI entry point, phase orchestration, signal handling
│   ├── config.rs        # DSN parsing, Config and ClickHouseConfig structs
│   ├── client.rs        # ClickHouse HTTP API client (reqwest wrapper)
│   ├── schema.rs        # Table discovery, DDL adaptation, watermark selection
│   ├── sync.rs          # Initial full-table sync with parallel workers
│   ├── cdc.rs           # CDC polling loop (watermark-based and count-based)
│   ├── checkpoint.rs    # JSON checkpoint persistence (atomic writes)
│   └── error.rs         # ReplicatorError enum, Result type alias
├── Cargo.toml           # Package manifest and dependencies
├── Cargo.lock           # Locked dependency versions
├── Dockerfile           # Container build (release binary)
├── CLAUDE.md            # AI assistant instructions for this repo
├── README.md            # Project documentation
├── docs/
│   └── plans/           # Planning documents (not consumed at runtime)
├── .planning/
│   └── codebase/        # GSD codebase analysis documents (this directory)
└── target/              # Cargo build artifacts (not committed)
```

## Module Responsibilities

**`src/main.rs`:**
- Purpose: Program entry point and pipeline orchestrator
- Contains: `Cli` struct (clap derive), `main()`, `run()`, `check_connectivity()`, `setup_target_schema()`, `wait_for_shutdown_signal()`, `sanitize_dsn()`
- Owns: construction of all `Arc`-wrapped shared state; `CancellationToken` lifecycle
- Dependencies: all other modules

**`src/config.rs`:**
- Purpose: DSN parsing and application configuration
- Contains: `ClickHouseConfig` (per-endpoint: user, password, host, port, database, http_url), `Config` (both endpoints + batch_size, cdc_poll_secs, checkpoint_path, threads)
- Key detail: `batch_size = 50_000`, `cdc_poll_secs = 5`, `checkpoint_path = "checkpoint.json"` are hardcoded constants
- No dependencies on other internal modules

**`src/client.rs`:**
- Purpose: All HTTP communication with ClickHouse
- Contains: `ClickHouseClient` struct wrapping `reqwest::Client`
- SELECT queries use HTTP GET; INSERT/DDL use HTTP POST with explicit `Content-Length`
- Key methods: `ping()`, `query_json_rows()`, `query_scalar()`, `query_typed<T>()`, `select_batch_raw()`, `select_delta_raw()`, `insert_jsonl()`, `execute_no_db()`
- Dependencies: `config`, `error`

**`src/schema.rs`:**
- Purpose: Source schema introspection and DDL transformation
- Contains: `TableInfo`, `ColumnInfo`, `WatermarkKind` types; functions for table listing, DDL retrieval/adaptation, watermark column selection
- Key functions: `list_tables()`, `get_create_ddl()`, `adapt_ddl()`, `ensure_database()`, `apply_ddl()`, `get_columns()`, `pick_watermark()`, `fetch_max_watermark()`
- Internal helpers: `rewrite_deprecated_engine_syntax()`, `find_matching_close_paren()`, `split_engine_args()`, `escape_string()`
- Dependencies: `client`, `error`

**`src/sync.rs`:**
- Purpose: Initial full-table copy with parallel workers and resumable checkpointing
- Contains: `InitialSync` struct (public), `BatchTracker` struct (private), `count_jsonl_lines()` helper
- Parallelism: `tokio::spawn` per worker thread; batch offset allocation via `Arc<AtomicU64>`; checkpoint frontier via `Arc<Mutex<BatchTracker>>`
- Retry: `MAX_RETRIES = 10`, `RETRY_DELAY = 10s`
- Dependencies: `checkpoint`, `client`, `config`, `error`, `schema::TableInfo`

**`src/cdc.rs`:**
- Purpose: Continuous change detection and replication after initial sync
- Contains: `CdcEngine` struct (public), `TableState` struct (private), helper functions `watermark_initial_value()`, `count_jsonl_lines()`, `max_watermark_in_jsonl()`
- Two polling strategies: watermark-based (preferred) and row-count comparison (fallback)
- Poll cycle: 5-second sleep between full table sweeps; tables polled sequentially within each cycle
- Dependencies: `checkpoint`, `client`, `config`, `error`, `schema`

**`src/checkpoint.rs`:**
- Purpose: Durable sync state persistence
- Contains: `TableCheckpoint` (per-table fields: `synced_rows`, `initial_sync_complete`, `cdc_watermark`, `watermark_column`), `Checkpoint` (wraps `HashMap<String, TableCheckpoint>`)
- Atomic write pattern: serialize to `checkpoint.json.tmp`, then `fs::rename()` to `checkpoint.json`
- Dependencies: `error`

**`src/error.rs`:**
- Purpose: Unified error type for the entire application
- Contains: `ReplicatorError` enum with variants: `DsnParse`, `Http`, `ClickHouse`, `Json`, `Io`, `Checkpoint`, `Schema`, `Sync`, `Cdc`, `Cancelled`; `Result<T>` type alias
- `From<reqwest::Error>` masks `password=` values in error messages before they surface in logs
- No internal dependencies

## Key File Locations

**Entry Points:**
- `src/main.rs`: Binary entry, `#[tokio::main]`, `run()` orchestrator

**Configuration:**
- `Cargo.toml`: Package metadata, dependencies, binary target definition
- `src/config.rs`: Runtime configuration structs and DSN parsing

**Core Data Flow:**
- `src/client.rs`: All network I/O
- `src/sync.rs`: `InitialSync::run()` and `InitialSync::sync_table()`
- `src/cdc.rs`: `CdcEngine::run()`, `poll_by_watermark()`, `poll_by_count()`

**Shared State:**
- `src/checkpoint.rs`: `Checkpoint::save()` and `Checkpoint::load()` — the only disk I/O path

**Schema Logic:**
- `src/schema.rs`: `pick_watermark()` — watermark column selection algorithm; `adapt_ddl()` — DDL transformation

## Naming Conventions

**Files:** `snake_case.rs` matching the module name (standard Rust convention)

**Structs:** `PascalCase` — e.g., `ClickHouseClient`, `InitialSync`, `CdcEngine`, `BatchTracker`, `TableCheckpoint`

**Functions/methods:** `snake_case` — e.g., `run()`, `sync_table()`, `poll_by_watermark()`, `mark_complete()`

**Error variants:** `PascalCase` — e.g., `ReplicatorError::DsnParse`, `ReplicatorError::Cancelled`

**Constants:** `SCREAMING_SNAKE_CASE` — e.g., `MAX_RETRIES`, `RETRY_DELAY`

## Where to Add New Code

**New replication strategy or sink:**
- Implementation: `src/` as a new module (e.g., `src/kafka_sink.rs`), declared in `src/main.rs` with `mod kafka_sink;`
- Wire up in: `src/main.rs::run()`

**New CDC detection method:**
- Add a variant to `WatermarkKind` in `src/schema.rs`
- Update `pick_watermark()` in `src/schema.rs`
- Add a `poll_by_*` method in `src/cdc.rs` and dispatch from `CdcEngine::poll_table()`

**New CLI flag:**
- Add field to `Cli` struct in `src/main.rs`
- Propagate to `Config` in `src/config.rs`

**New checkpoint field:**
- Add to `TableCheckpoint` in `src/checkpoint.rs`
- Add accessor/mutator method on `Checkpoint`
- Backward-compatible due to `#[serde(default)]` on struct fields

**New error variant:**
- Add to `ReplicatorError` in `src/error.rs`

**Tests:**
- Co-locate in the same `.rs` file under `#[cfg(test)]` at the bottom of the file
- No separate test files or `tests/` directory

## Special Directories

**`target/`:**
- Purpose: Cargo build artifacts (debug and release binaries, incremental compilation cache)
- Generated: Yes
- Committed: No (in `.gitignore`)

**`.planning/codebase/`:**
- Purpose: GSD codebase analysis documents consumed by planning and execution agents
- Generated: Yes (by `gsd:map-codebase`)
- Committed: No (project-specific, not checked in by default)

**`docs/plans/`:**
- Purpose: Human-readable planning documents for feature development
- Generated: No
- Committed: Yes

## Gaps / Unknowns

- No `tests/` directory — all tests are unit tests inside modules; no integration tests exist
- No `benches/` directory — no benchmarks
- No `examples/` directory
- The `Dockerfile` build target is not documented in CLAUDE.md
