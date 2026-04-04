# ch-ch-replicator

A CLI tool for replicating one standalone ClickHouse database into another. It performs an initial full-table copy using parallel worker threads, then switches to continuous CDC (Change Data Capture) mode and keeps the target in sync indefinitely.

## Features

- **Full initial sync** — copies all rows from every table in the source database to the target, in configurable-size batches with parallel workers
- **CDC mode** — after the initial sync, polls for new rows using watermark columns (`DateTime`, `UInt64`) or row-count comparison as a fallback; transient errors are retried with exponential backoff
- **Schema setup** — creates the target database and tables automatically if they do not exist; DDL is copied from the source with `CREATE TABLE IF NOT EXISTS`
- **Table filtering** — replicate only a subset of tables with `--include`, or skip specific tables with `--exclude`
- **Column exclusion** — omit specific columns from schema creation, initial sync, and CDC with `--exclude-columns`
- **Skips views** — `View`, `MaterializedView`, `LiveView`, and `WindowView` objects are ignored
- **Idempotent / resumable** — progress is saved to `checkpoint.json` after every batch; if the process is stopped it resumes from where it left off on the next run
- **Graceful shutdown** — `SIGINT` (Ctrl-C) and `SIGTERM` both trigger a clean stop: the current batch finishes before the process exits

## DSN format

```
clickhouse://<user>:<password>@<host>:<port>/<database>[?options]
```

All parts except `?options` are mandatory. Special characters in the password must be percent-encoded (e.g. `@` → `%40`).

## Usage

### Binary

```bash
ch-ch-replicator \
  --src="clickhouse://user:password@source-host:8123/mydb" \
  --dest="clickhouse://user:password@dest-host:8123/mydb" \
  --threads=4
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--src` | ✓ | — | Source ClickHouse DSN |
| `--dest` | ✓ | — | Destination ClickHouse DSN |
| `--threads` | | `3` | Number of parallel workers for initial sync |
| `--batch` | | `300000` | Batch size (rows) for SELECT/INSERT during initial sync |
| `--include` | | — | Comma-separated list of tables to replicate (whitelist); takes priority over `--exclude` |
| `--exclude` | | — | Comma-separated list of tables to skip (blacklist) |
| `--exclude-columns` | | — | Comma-separated `table.column` pairs to omit from schema, initial sync, and CDC (e.g. `orders.secret,users.password_hash`) |

Logging verbosity is controlled via the `RUST_LOG` environment variable (e.g. `RUST_LOG=debug`).

### Docker

```bash
docker run --rm \
  -v /path/to/data:/data \
  czt08883/ch-ch-replicator:0.2.15 \
  --src="clickhouse://user:password@source-host:8123/mydb" \
  --dest="clickhouse://user:password@dest-host:8123/mydb" \
  --threads=4
```

The container writes `checkpoint.json` to its working directory (`/data`). Mount a host directory to preserve the checkpoint across container restarts:

```bash
docker run --rm \
  -v /path/to/data:/data \
  czt08883/ch-ch-replicator:0.2.15 \
  --src="clickhouse://user:password@source-host:8123/mydb" \
  --dest="clickhouse://user:password@dest-host:8123/mydb"
```

With Docker Compose:

```yaml
services:
  replicator:
    image: czt08883/ch-ch-replicator:0.2.15
    volumes:
      - ./replicator-data:/data
    command:
      - --src=clickhouse://user:password@source:8123/mydb
      - --dest=clickhouse://user:password@dest:8123/mydb
      - --threads=4
    restart: unless-stopped
```

## Checkpoint file

Progress is tracked in **`checkpoint.json`** in the working directory. The file is written atomically (via a `.tmp` rename) and records per-table state:

```json
{
  "tables": {
    "events": {
      "synced_rows": 1000000,
      "initial_sync_complete": true,
      "watermark_column": "updated_at",
      "cdc_watermark": "2025-03-31 18:00:00"
    }
  }
}
```

Map this file (or its parent directory) as a volume to survive container restarts without re-syncing data that has already been copied.

## CDC strategy

For each table the replicator picks the best available watermark column in priority order:

1. `DateTime` / `DateTime64` column with a preferred name (`updated_at`, `modified_at`, `event_time`, `created_at`, `timestamp`)
2. Any `DateTime` / `DateTime64` column
3. `UInt64` / `UInt32` / `Int64` column with a preferred name (`_version`, `version`, `id`)
4. Any `UInt64` / `UInt32` / `Int64` column
5. **Row-count fallback** — if no suitable column exists, the replicator compares `count()` on source and target and fetches any new rows by offset

`MATERIALIZED` and `ALIAS` columns are always excluded from watermark selection.

The CDC poll interval is 5 seconds. On a poll error the table is retried up to **5 times** with exponential backoff (1 s → 2 s → 4 s → 8 s → 16 s, capped at 60 s). After all retries are exhausted the error is logged and the table is skipped until the next poll cycle — CDC never aborts due to a single table failure.

## Building from source

```bash
cargo build --release
# binary is at target/release/ch-ch-replicator
```

Requires Rust 1.70+. No C dependencies — TLS is handled by rustls.

## Building the Docker image

```bash
docker build -t czt08883/ch-ch-replicator:0.2.15 .
```
