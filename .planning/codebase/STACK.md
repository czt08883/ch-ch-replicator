# Technology Stack
_Last updated: 2026-04-01_

## Summary

ch-ch-replicator is a single-binary Rust CLI tool. It uses async Rust (tokio) throughout, communicates with ClickHouse exclusively over HTTP using reqwest, and serializes all checkpoint state as JSON. The build produces a statically-linked binary distributed as a minimal Docker image.

## Languages

**Primary:**
- Rust (edition 2021) — all application code in `src/`

**No secondary languages.** No shell scripts, Python helpers, or build scripts beyond `Cargo.toml`.

## Runtime

**Environment:**
- Rust stable — pinned to `1.93` in `Dockerfile` (builder stage uses `rust:1.93-slim`)
- Tokio async runtime (`#[tokio::main]` in `src/main.rs`)

**Package Manager:**
- Cargo
- Lockfile: `Cargo.lock` present and committed

## Frameworks

**Async Runtime:**
- `tokio` 1.50.0 — full feature set; drives all I/O, timers, signal handling, and task spawning

**CLI Parsing:**
- `clap` 4.6.0 with derive macros — `--src`, `--dest`, `--threads` args defined in `src/main.rs`

**Logging / Tracing:**
- `tracing` 0.1.44 — structured, leveled spans and events used in all modules
- `tracing-subscriber` 0.3.23 — `fmt` subscriber with `EnvFilter`; log level via `RUST_LOG` env var

**Error Handling:**
- `thiserror` 1.0.69 — `ReplicatorError` enum with `#[from]` auto-conversions in `src/error.rs`
- `anyhow` 1.0.102 — present as a dependency but not used in application code; pulled in transitively

**Build/Container:**
- Multi-stage `Dockerfile`: builder on `rust:1.93-slim`, runtime on `debian:bookworm-slim`
- `ca-certificates` installed in runtime image to support rustls TLS verification

## Key Dependencies

**HTTP Client:**
- `reqwest` 0.12.28 — `default-features = false`, only `json` and `rustls-tls` features enabled; no OpenSSL dependency

**TLS:**
- `rustls` 0.23.37 — used via reqwest's rustls-tls feature; no native system TLS

**Serialization:**
- `serde` 1.0.228 with derive — all structs that cross JSON boundaries use `#[derive(Serialize, Deserialize)]`
- `serde_json` 1.0.149 — JSON encoding/decoding for HTTP payloads and checkpoint file

**URL Parsing:**
- `url` 2.5.8 — parses `clickhouse://` DSNs in `src/config.rs`

**Bytes:**
- `bytes` 1.11.1 — `Bytes` type used for zero-copy HTTP POST bodies in `src/client.rs`

**Concurrency:**
- `futures` 0.3.32 — `join_all` for parallel async tasks in `src/sync.rs`
- `tokio-util` 0.7.18 — `CancellationToken` for graceful shutdown coordination

**Unique IDs:**
- `uuid` 1.23.0 — v4 UUIDs used in test helpers within `src/checkpoint.rs`

**Process Title:**
- `proctitle` 0.1.1 — rewrites `/proc/self/cmdline` to mask passwords from `ps aux`

## Configuration

**Environment:**
- `RUST_LOG` — controls log verbosity (e.g. `info`, `debug`). Default: `info`.
- No `.env` file or secrets management library. Credentials are passed as CLI DSN arguments.

**Build:**
- `Cargo.toml` — package manifest and dependency declarations
- `Cargo.lock` — pinned resolved versions
- `Dockerfile` — multi-stage container build

**Runtime config (hardcoded defaults in `src/config.rs`):**
- Batch size: 50,000 rows per SELECT/INSERT
- CDC poll interval: 5 seconds
- Checkpoint file: `./checkpoint.json`
- HTTP client timeout: 300 seconds

## Platform Requirements

**Development:**
- Rust toolchain (edition 2021, stable channel)
- `cargo build` / `cargo test`

**Production:**
- Docker image recommended (see `Dockerfile`)
- Binary requires `ca-certificates` on the host for TLS; the Docker image installs them
- No database driver installation required — all communication is HTTP

## Gaps / Unknowns

- Rust toolchain version used in local development is not pinned (only the Docker builder specifies `1.93`). A `.rust-version` file or `rust-toolchain.toml` is absent.
- `anyhow` appears in `Cargo.toml` but is not actively used in application source; it may be a leftover or unused transitive re-export.
