# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ch-ch-replicator is a Rust CLI tool that replicates a ClickHouse database to another via the HTTP API. It performs a parallel initial full-table copy, then switches to continuous CDC (Change Data Capture) polling to keep the target in sync.

## Build & Test Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build → target/release/ch-ch-replicator
cargo test                     # Run all ~83 unit tests
cargo test --lib config::tests # Run tests for a specific module
cargo test -- --nocapture      # Run tests with stdout/logging visible
```

No integration tests exist — unit tests are embedded in each source module under `#[cfg(test)]`.

## Running

```bash
ch-ch-replicator \
  --src="clickhouse://user:pass@source:8123/mydb" \
  --dest="clickhouse://user:pass@dest:8123/mydb" \
  --threads=4
```

Log level controlled via `RUST_LOG` env var (default: `info`).

## Architecture

The application is a **5-phase async pipeline** orchestrated in `main.rs::run()`:

1. **Connectivity** — Ping source and destination ClickHouse instances
2. **Schema discovery** — List tables (excluding views), retrieve DDL, create target database/tables
3. **Initial sync** — Parallel batched SELECT→INSERT (50K rows/batch) with per-batch checkpointing
4. **CDC loop** — Poll every 5 seconds per table using watermark-based or row-count-fallback detection
5. **Graceful shutdown** — SIGINT/SIGTERM triggers CancellationToken; current batch finishes and checkpoint saves

### Module responsibilities

| Module | Role |
|---|---|
| `main.rs` | CLI parsing (clap), logging init, orchestrates the 5 phases, signal handling |
| `config.rs` | DSN parsing (`clickhouse://` URLs), app-wide config struct (batch size, poll interval, threads) |
| `client.rs` | Thin reqwest wrapper for ClickHouse HTTP API; GET for SELECT, POST for INSERT/DDL; explicit Content-Length headers (required by ClickHouse 24.x) |
| `schema.rs` | Table discovery via `system.tables`/`system.columns`, DDL retrieval and adaptation, watermark column selection for CDC |
| `sync.rs` | Initial full-table copy with tokio semaphore-bounded parallelism, resumable via checkpoint offsets |
| `cdc.rs` | Watermark-based delta polling (preferred) or row-count comparison (fallback), runs indefinitely |
| `checkpoint.rs` | JSON file persistence with atomic writes (write `.tmp` then rename), tracks per-table sync progress and CDC watermark position |
| `error.rs` | `thiserror`-based error enum with `#[from]` auto-conversions |

### Key design details

- **All data transfer uses JSONEachRow format** over HTTP (port 8123). No native TCP protocol.
- **Watermark priority for CDC**: preferred DateTime column names (`updated_at`, `modified_at`, `event_time`, `created_at`, `timestamp`) → any DateTime → preferred UInt names (`_version`, `version`, `id`) → any UInt64/UInt32/Int64 → fallback to row-count comparison. MATERIALIZED/ALIAS/EPHEMERAL columns are excluded.
- **CDC seeds watermarks from source MAX** after initial sync to avoid re-replicating existing data.
- **CDC is append-only** — no UPDATE/DELETE detection. Works for immutable event streams and versioned tables.
- **Shared state** uses `Arc<Mutex<Checkpoint>>` and `Arc<ClickHouseClient>` across async tasks. Graceful shutdown via `tokio_util::sync::CancellationToken`.
- **Checkpoint file** is `./checkpoint.json` in the working directory.
