# Coding Conventions

_Last updated: 2026-04-01_

## Summary

ch-ch-replicator is a Rust async CLI tool using Tokio. Code follows standard Rust idioms: snake_case everywhere, `thiserror` for typed errors, `tracing` for structured logging, and Arc/Mutex for shared async state. All public functions are documented with `///` doc comments.

---

## Naming Patterns

**Files:**
- `snake_case.rs` — all source files follow standard Rust module naming: `cdc.rs`, `checkpoint.rs`, `client.rs`, `config.rs`, `error.rs`, `schema.rs`, `sync.rs`

**Functions and methods:**
- `snake_case` throughout — `pick_watermark`, `select_batch_raw`, `mark_initial_sync_complete`
- Private helpers use underscore-prefixed names only when semantically needed (e.g., `pick_watermark_inner`)
- Async functions are not specially named; async is indicated by `async fn`

**Types and structs:**
- `PascalCase` — `ClickHouseClient`, `InitialSync`, `CdcEngine`, `BatchTracker`, `WatermarkKind`, `TableState`
- Error variants match domain terminology: `DsnParse`, `ClickHouse`, `Sync`, `Cdc`, `Checkpoint`

**Variables:**
- Short, descriptive: `src`, `dst`, `cp`, `wm_val`, `wm_col`, `wm_kind`
- Atomic types explicitly named: `next_offset`, `rows_inserted`

**Constants:**
- `SCREAMING_SNAKE_CASE` — `MAX_RETRIES`, `RETRY_DELAY`, `ENGINE_KW`

---

## Module Design

**Structure:** All modules declared in `src/main.rs` with `mod` statements. No separate `lib.rs`.

**Visibility:**
- `pub` for types and functions consumed across module boundaries
- `pub(crate)` for helpers shared within the crate but not part of public API (e.g., `watermark_initial_value`, `count_jsonl_lines`, `max_watermark_in_jsonl` in `cdc.rs`; `adapt_ddl` in `schema.rs`)
- Private (no annotation) for all internal helpers

**Exports:** No barrel pattern — callers import specific items: `use crate::schema::{apply_ddl, ensure_database, get_create_ddl, list_tables}`

**`#[allow(dead_code)]`:** Used sparingly for fields/methods that are API surface but not yet called in the current binary (e.g., `execute`, `execute_on_db`, `query_json_meta` in `client.rs`; `Cdc` error variant in `error.rs`)

---

## Error Handling

**Error type:** Single crate-wide `ReplicatorError` enum in `src/error.rs` using `thiserror`.

**Type alias:** `pub type Result<T> = std::result::Result<T, ReplicatorError>` — used in all modules.

**Error variant pattern:**
```rust
#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("DSN parse error: {0}")]
    DsnParse(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Cancelled")]
    Cancelled,
    // ...
}
```

**`#[from]` conversions:** Only for standard library types (`std::io::Error`, `serde_json::Error`). `reqwest::Error` gets a manual `From` impl to mask passwords.

**`?` operator:** Used throughout for propagation. Callers match on specific variants only when handling `Cancelled` or logging:
```rust
Err(ReplicatorError::Cancelled) => {
    info!("Replicator shut down cleanly");
}
_ => {
    error!("Fatal error: {}", e);
    std::process::exit(1);
}
```

**Non-fatal errors in CDC:** Table poll errors are logged and the loop continues rather than aborting:
```rust
Err(e) => {
    error!("CDC: table '{}' poll error: {}", state.name, e);
    // continue polling other tables
}
```

**Soft failures:** Operations that should degrade gracefully return `Option<T>` (e.g., `fetch_max_watermark`) or use `unwrap_or`/`unwrap_or_default`.

---

## Async Patterns

**Runtime:** `#[tokio::main]` on `main()`. Worker tasks use `tokio::spawn`.

**Shared state:**
- `Arc<ClickHouseClient>` — cloned cheaply across tasks (client is `Clone`)
- `Arc<Mutex<Checkpoint>>` — guards the single mutable checkpoint; locked briefly for read/write, never held across await points with business logic
- `Arc<AtomicU64>` — lock-free progress counters in parallel sync workers (`next_offset`, `rows_inserted`)

**Cancellation:** `tokio_util::sync::CancellationToken` cloned into every long-running path. Poll pattern:
```rust
tokio::select! {
    _ = cancel.cancelled() => return Ok(()),
    _ = tokio::time::sleep(poll_interval) => {}
}
```
Also checked synchronously with `cancel.is_cancelled()` before batch operations.

**Parallel workers:** `tokio::spawn` inside a loop, handles collected in `Vec<JoinHandle<_>>`, joined sequentially with error aggregation (not `join_all`).

**No channels:** State is shared via `Arc`; no `mpsc` or broadcast channels.

**Retry loop pattern (in `sync.rs`):**
```rust
for attempt in 1..=MAX_RETRIES {
    match async { /* batch op */ }.await {
        Ok(v) => { last_err = None; break; }
        Err(e) => {
            if attempt < MAX_RETRIES {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(ReplicatorError::Cancelled),
                    _ = tokio::time::sleep(RETRY_DELAY) => {}
                }
            }
            last_err = Some(e);
        }
    }
}
```

---

## Logging

**Framework:** `tracing` crate with `tracing_subscriber` fmt layer.

**Log level usage:**
- `info!` — phase transitions, table counts, sync progress, completion
- `warn!` — non-fatal anomalies (nullable watermark column, row count mismatch)
- `error!` — worker failures, connectivity errors, fatal errors before exit
- `debug!` — individual SQL statements before execution

**Format:** Plain format strings with `{}` — no structured fields (`tracing` key-value pairs not used).

**Password safety:** DSNs are sanitized before logging via `sanitize_dsn()` (replaces password with `*****`). HTTP error messages go through `mask_password()` before being stored in `ReplicatorError::Http`.

---

## Comments and Documentation

**`///` doc comments:** On all public structs, enums, and functions. Private helpers use `//` inline comments.

**Section separators:** Long files use `// ---...--- //` banners to group methods (e.g., `// --- Parameter helpers ---`).

**Inline rationale comments:** Used liberally for non-obvious decisions:
```rust
// ClickHouse 24.x rejects POST requests that use chunked transfer encoding...
// Wrap datetime values in single quotes for the WHERE clause
```

---

## SQL Construction

**Pattern:** `format!()` strings with backtick-quoted identifiers to handle names with special characters:
```rust
format!("SELECT * FROM `{}`.`{}`...", self.config.database, table)
```

**String escaping:** `schema::escape_string()` used for values interpolated into `WHERE database = '{}'` clauses (replaces `'` with `''`).

**No query builder / ORM:** All SQL is hand-written strings.

---

## Import Organization

**Order (per file):**
1. `std::` imports
2. External crate imports (`tokio`, `serde`, `tracing`, etc.)
3. `crate::` imports

No blank-line separation enforced between groups within a file; grouping is visual only.

**No path aliases** (`use ... as ...` not used except implicitly via re-exports).

---

## Gaps / Unknowns

- No `rustfmt.toml` or `clippy.toml` found — formatting and lint configuration rely on Rust defaults.
- No `Makefile` or CI script visible; lint/format discipline is manual.
- `#[allow(dead_code)]` on multiple items suggests some API surface was written speculatively and may be cleaned up later.
