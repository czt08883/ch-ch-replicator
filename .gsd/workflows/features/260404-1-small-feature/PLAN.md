# Plan: Table Filtering

## Tasks

### T1 — Add filter fields to `Config` and parse CLI flags

**Files:** `src/config.rs`, `src/main.rs`

**Changes:**
- Add `include_tables: Vec<String>` and `exclude_tables: Vec<String>` to `Config`
- Update `Config::new()` signature to accept both vecs
- Add `--include` and `--exclude` args to `Cli` struct in `main.rs` (each `Option<String>`, comma-split into vecs)
- Pass them through to `Config::new()`

**Verify:** `cargo build` compiles; `cargo test --lib config::tests` passes

---

### T2 — Implement `filter_tables()` helper and apply it in `run()`

**Files:** `src/main.rs`

**Changes:**
- Add `fn filter_tables(tables: Vec<TableInfo>, include: &[String], exclude: &[String]) -> Vec<TableInfo>`
  - If `include` is non-empty: keep only tables whose name is in the include list
  - Then remove any table whose name is in the exclude list
  - Log which tables were kept/dropped at info level
- Call `filter_tables()` immediately after `list_tables()` in `run()`
- Update proctitle line to include filter flags

**Verify:** `cargo build` compiles; tests pass

---

### T3 — Add unit tests for `filter_tables()`

**Files:** `src/main.rs` (or new `src/filter.rs`)

**Changes:**
- Test: no flags → all tables returned
- Test: `--include` → only matching tables
- Test: `--exclude` → all except excluded
- Test: both flags → include first, then exclude
- Test: unknown name in `--include` → empty result (not an error)
- Test: `--include` and `--exclude` overlap → excluded wins

**Verify:** `cargo test` — all tests pass

---

## Acceptance Criteria

- `--include` with known table names replicates only those tables
- `--exclude` with known table names replicates all others
- No flags → unchanged behavior
- Clear log messages show which tables are included/skipped
- All existing tests continue to pass
