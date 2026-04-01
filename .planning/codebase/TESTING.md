# Testing Patterns

_Last updated: 2026-04-01_

## Summary

All tests are inline unit tests embedded in each source module under `#[cfg(test)]`. There are approximately 83 tests total with no integration or end-to-end tests. Tests are pure (no mocks, no HTTP stubs) — they test only pure functions and file-system-backed logic. Networking, ClickHouse queries, and async orchestration are entirely untested.

---

## Test Framework

**Runner:** Rust built-in `cargo test`

**No external test crate** — no `assert_matches`, `proptest`, `mockall`, `wiremock`, or similar dependencies in `Cargo.toml`.

**Assertion library:** Standard `assert_eq!`, `assert!`, `assert!(matches!(...))`, `panic!`

**Run commands:**
```bash
cargo test                          # Run all ~83 unit tests
cargo test --lib config::tests      # Run tests for a specific module
cargo test -- --nocapture           # Run with stdout/tracing output visible
cargo test -- <test_name>           # Run a single test by name substring
```

---

## Test File Organization

**Location:** Co-located in each source file. Each module ends with:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    // ...
}
```

**Files with test modules:**
- `src/config.rs` — 11 tests
- `src/schema.rs` — ~40 tests
- `src/checkpoint.rs` — ~16 tests
- `src/sync.rs` — 6 tests
- `src/cdc.rs` — ~13 tests

**Files with no tests:**
- `src/main.rs` — orchestration only; no unit-testable pure logic
- `src/client.rs` — HTTP client; requires live ClickHouse
- `src/error.rs` — thin `thiserror` enum; no logic to test

---

## Test Structure

**Suite organization:** Tests are grouped with comment banners inside the `mod tests` block:
```rust
// -----------------------------------------------------------------------
// DSN parsing — happy paths
// -----------------------------------------------------------------------
#[test]
fn test_dsn_minimal() { ... }

// -----------------------------------------------------------------------
// DSN parsing — error paths
// -----------------------------------------------------------------------
#[test]
fn test_dsn_wrong_scheme() { ... }
```

**Naming convention:** `snake_case` function names that describe the scenario, not the function under test:
- `test_dsn_minimal`, `test_dsn_missing_port` (config)
- `pick_watermark_prefers_updated_at_over_non_preferred_datetime` (schema)
- `watermark_uint_numeric_not_lexicographic` (cdc)
- `save_is_atomic_temp_file_cleaned_up` (checkpoint)

**Setup:** Inline setup in each test body. No `#[test]` fixtures or shared setup functions — except for two test-local helpers in `checkpoint.rs`:
```rust
fn tmp_path() -> String {
    let dir = std::env::temp_dir();
    let name = format!("ch_replicator_test_{}.json", uuid_simple());
    dir.join(name).to_string_lossy().into_owned()
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // timestamp-based unique name without uuid crate dependency
}
```

**Teardown:** Manual cleanup for file-system tests — `std::fs::remove_file(&path).ok()` at end of test body (best-effort, not in a drop guard).

---

## Mocking

**None.** The codebase has no mocking infrastructure. `ClickHouseClient` and async functions are not tested — all tested functions are pure (no I/O) or file-system-only.

**Consequence:** Testing any code that calls `ClickHouseClient` requires a live ClickHouse instance.

---

## What IS Tested

### `src/config.rs` — DSN parsing
- Happy-path DSN parsing: user, password, host, port, database, http_url extraction
- URL-encoded characters in password (`p%40ss%21` → `p@ss!`)
- Error paths: wrong scheme, missing port, missing password, missing database, missing username, invalid URL
- `Config::new()` default field values (batch_size=50000, cdc_poll_secs=5, checkpoint_path)
- Error propagation when either DSN is invalid

### `src/schema.rs` — DDL adaptation and watermark selection
- `escape_string`: no quotes, single quote, multiple quotes, empty, only quotes
- `adapt_ddl`: backtick-qualified name replacement, unquoted name replacement, `IF NOT EXISTS` insertion, no duplication of `IF NOT EXISTS`
- `rewrite_deprecated_engine_syntax`: ReplacingMergeTree, MergeTree with tuple sort key, CollapsingMergeTree, ReplacingMergeTree with ver_col, modern syntax left untouched, empty engine args untouched, same src/dst db
- `pick_watermark` (via `pick_watermark_inner`): empty columns → None, preferred datetime name wins, fallback to any datetime, preferred uint name wins, datetime beats uint, fallback to any uint, None when only strings, DateTime64 recognized, UInt32/Int64 recognized, MATERIALIZED excluded, ALIAS excluded, EPHEMERAL excluded, Nullable types recognized, non-Nullable preferred over Nullable within same tier, preferred-name Nullable beats arbitrary non-Nullable
- `unwrap_nullable` / `is_nullable`: Nullable wrapper stripping, passthrough for non-Nullable

### `src/checkpoint.rs` — file persistence
- Load returns empty when file missing
- Save then load round-trip preserves all fields
- Atomic save: `.tmp` file does not persist after successful save
- Load rejects corrupt JSON with `Checkpoint` error variant
- `is_initial_sync_complete`: false for unknown table, false before marking, true after marking
- `synced_rows`: zero for unknown table, reflects update, overwrite works
- `update_watermark`: stored and retrieved, overwritten on second call
- Multiple tables are independent
- Reload preserves watermark and sync state

### `src/sync.rs` — `BatchTracker`
- In-order completion advances frontier correctly
- Out-of-order completion: gaps are tracked, frontier advances when gap filled
- Resume from non-zero start offset
- Duplicate completion is silently ignored
- Single batch
- Reverse-order completion eventually fills frontier

### `src/cdc.rs` — pure helper functions
- `count_jsonl_lines`: empty string, whitespace-only, single row, multiple rows, trailing blank lines, leading blank lines
- `max_watermark_in_jsonl` (datetime): None for empty, None when column absent, single row, picks maximum, identical values
- `max_watermark_in_jsonl` (uint): single row, picks maximum, numeric (not lexicographic) comparison, large u64 values, skips invalid JSON lines
- `watermark_initial_value`: DateTime sentinel, UInt64 sentinel, None returns empty strings

---

## What Is NOT Tested

**All HTTP/network paths:**
- `ClickHouseClient` methods: `ping`, `query_json_rows`, `query_scalar`, `query_typed`, `select_batch_raw`, `select_delta_raw`, `insert_jsonl`, `execute`, `execute_no_db`
- Any error that can only be triggered by HTTP failures

**Async orchestration:**
- `InitialSync::run` and `InitialSync::sync_table`
- `CdcEngine::run`, `CdcEngine::poll_table`, `CdcEngine::poll_by_watermark`, `CdcEngine::poll_by_count`
- Cancellation behavior during sync/CDC
- Retry logic in `sync_table` (10-retry loop with 10s delays)
- Parallel worker coordination

**Schema discovery functions:**
- `list_tables` — requires live ClickHouse `system.tables`
- `get_create_ddl`, `apply_ddl`, `ensure_database` — all require HTTP
- `fetch_max_watermark`, `get_columns` — require live HTTP

**CLI / main:**
- Signal handling
- `sanitize_dsn` (trivially tested manually but no test)
- `mask_password` in `error.rs` (no test)
- Full pipeline `run()` function

**Edge cases not covered:**
- `BatchTracker` with very large offsets / overflow
- `count_jsonl_lines` with Windows-style `\r\n` line endings
- `max_watermark_in_jsonl` with numeric values as JSON strings vs numbers
- Checkpoint concurrent save from multiple threads

---

## Coverage Characteristics

**High coverage (pure logic):** DSN parsing, DDL adaptation, watermark column selection, checkpoint serialization, batch tracker, JSONL helpers.

**Zero coverage (requires I/O or live service):** All HTTP client operations, all async orchestration, all schema discovery, retry/cancellation behavior.

**No coverage tool** configured in `Cargo.toml`. To generate coverage manually:
```bash
cargo install cargo-tarpaulin
cargo tarpaulin --lib
```

---

## Gaps / Unknowns

- No integration test harness or Docker Compose setup for running against real ClickHouse.
- `mask_password` in `src/error.rs` has no test, despite being security-relevant.
- `sanitize_dsn` in `src/main.rs` has no test.
- Retry delay of 10 seconds in sync workers makes any future async test of that path slow without dependency injection.
- File cleanup in checkpoint tests uses `.ok()` — test failures can leave temp files behind.
