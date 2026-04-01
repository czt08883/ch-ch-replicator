# External Integrations
_Last updated: 2026-04-01_

## Summary

The tool integrates with exactly one external service: ClickHouse (twice — source and destination). All communication uses the ClickHouse HTTP API on port 8123. No third-party cloud services, message queues, or monitoring platforms are used.

## APIs & External Services

**ClickHouse HTTP API (source and destination):**
- Protocol: HTTP/1.1 (plain text, no TLS in transport layer — connections are `http://host:port`)
- Endpoint: `http://<host>:<port>/` — single endpoint for all queries and inserts
- Auth: `user` and `password` passed as URL query parameters on every request
- SELECT queries use HTTP GET with `?query=...`
- INSERT and DDL use HTTP POST with explicit `Content-Length` header (required by ClickHouse 24.x to avoid HTTP 411 errors)
- Client implementation: `src/client.rs` (`ClickHouseClient` struct wrapping `reqwest::Client`)
- Connection config: 300-second timeout per request; no connection pooling configuration beyond reqwest defaults

**ClickHouse system tables queried:**
- `system.tables` — table discovery, engine type, sorting key (`src/schema.rs:list_tables`)
- `system.columns` — column metadata including `default_kind` for MATERIALIZED/ALIAS detection (`src/schema.rs:get_columns`)

**ClickHouse DDL commands issued:**
- `SHOW CREATE TABLE` — retrieve table DDL from source
- `CREATE DATABASE IF NOT EXISTS` — idempotent target database creation
- `CREATE TABLE IF NOT EXISTS` — idempotent target table creation with adapted DDL

## Data Formats

**Wire format:**
- `FORMAT JSONEachRow` — one JSON object per line (NDJSON) for all SELECT and INSERT operations
- `FORMAT JSON` — used only by the currently unused `query_json_meta` method in `src/client.rs`

**Checkpoint format:**
- JSON file at `./checkpoint.json` (path configurable in `src/config.rs`)
- Schema: `{ "tables": { "<table_name>": { "synced_rows": u64, "initial_sync_complete": bool, "cdc_watermark": string, "watermark_column": string } } }`
- Written atomically: data written to `checkpoint.json.tmp` then renamed to `checkpoint.json`
- Implementation: `src/checkpoint.rs`

## Data Storage

**Databases:**
- Source ClickHouse — read-only (SELECT, SHOW CREATE TABLE, system table queries)
- Destination ClickHouse — write-only (CREATE DATABASE, CREATE TABLE, INSERT)
- Connection info parsed from `clickhouse://user:password@host:port/database` DSN format

**File Storage (local):**
- `./checkpoint.json` — persists per-table sync progress and CDC watermark position
- Written to the process working directory; no configurable path override exposed via CLI

**Caching:**
- None

## Authentication & Identity

**Auth Provider:**
- No external auth provider. Credentials are embedded directly in the DSN CLI arguments (`--src`, `--dest`).
- Credentials are passed as HTTP query parameters (`?user=...&password=...`) on every request.
- Password masking: `proctitle` crate rewrites `/proc/self/cmdline` to hide passwords from `ps aux`. `sanitize_dsn()` in `src/main.rs` masks passwords in log output. `mask_password()` in `src/error.rs` scrubs passwords from reqwest error messages.

## Signal Handling

**OS Signals:**
- `SIGINT` (Ctrl-C) and `SIGTERM` trigger graceful shutdown via `tokio_util::sync::CancellationToken`
- Current batch finishes before exit; checkpoint is saved
- Unix-only: registered via `tokio::signal::unix`; non-Unix falls back to `ctrl_c()` only
- Implementation: `src/main.rs:wait_for_shutdown_signal`

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, Datadog, etc.)

**Logs:**
- Structured text logs via `tracing` + `tracing-subscriber` fmt output to stdout
- Level controlled by `RUST_LOG` env var; defaults to `info`
- All modules emit `info` progress, `warn` for non-fatal issues, `error` for fatal failures, `debug` for per-query SQL

## CI/CD & Deployment

**Hosting:**
- Docker container (see `Dockerfile`); no Kubernetes manifests or Compose files present in repo
- Binary can also run directly on the host

**CI Pipeline:**
- None detected (no `.github/workflows/`, `.gitlab-ci.yml`, or similar)

## CDC Polling Mechanism

**Approach:**
- Watermark-based: polls `SELECT * FROM table WHERE <col> > <last_value>` every 5 seconds per table
- Watermark column selection priority defined in `src/schema.rs:pick_watermark`: preferred DateTime names → any DateTime → preferred UInt names → any UInt64/UInt32/Int64 → row-count fallback
- Append-only: no UPDATE or DELETE detection

**Watermark seeding:**
- After initial sync, seeds `MAX(<watermark_col>)` from source to avoid re-replicating existing rows
- Implementation: `src/cdc.rs`

## Gaps / Unknowns

- No TLS between the replicator and ClickHouse. All traffic is plain HTTP. This is an operational concern if source or destination are not on a private network.
- No retry configuration exposed to the user; initial sync has 10 retries with 10-second delays (hardcoded in `src/sync.rs`); CDC has no explicit retry loop beyond the 5-second poll cycle.
- No webhook, event bus, or notification mechanism for sync completion or errors.
