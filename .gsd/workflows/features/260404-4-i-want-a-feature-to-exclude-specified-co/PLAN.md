# Plan: Column Exclusion

## Tasks

### T1 — Config: parse --exclude-columns into HashMap

**Files:** `src/config.rs`, `src/main.rs`

**Changes:**
- Add `exclude_columns: HashMap<String, Vec<String>>` to `Config`
- Update `Config::new()` to accept it
- Add `--exclude-columns` `Option<Vec<String>>` arg to `Cli` (value_delimiter=',')
- Parse each element as `"table.col"` → split on first `.` → insert into HashMap
- Warn and skip any entry that doesn't contain a dot
- Pass map to `Config::new()`; log at startup

**Verify:** `cargo build`; `cargo test --lib config::tests`

---

### T2 — Schema: strip excluded columns from DDL

**Files:** `src/schema.rs`

**Changes:**
- Add `pub fn strip_excluded_columns(ddl: &str, excluded: &[String]) -> String`
  - Splits DDL on newlines
  - For each line in the column block (between `(` and `)ENGINE`), check if
    the line defines a column whose name (backtick-quoted or unquoted) is in `excluded`
  - Remove matching lines (and their trailing comma on the preceding line if needed)
  - Return reassembled DDL
- Update `get_create_ddl()` to accept `excluded_cols: &[String]` and call `strip_excluded_columns()` after `adapt_ddl()`
- Update call site in `main.rs::setup_target_schema()` to look up excluded cols from config

**Verify:** `cargo build`; unit tests for `strip_excluded_columns()`

---

### T3 — Schema: build explicit column SELECT list

**Files:** `src/schema.rs`, `src/client.rs`

**Changes in schema.rs:**
- Add `pub fn excluded_col_set<'a>(config: &'a Config, table: &str) -> &'a [String]`
  helper that returns the excluded column vec for a table (empty slice if none)
- Add `pub fn build_select_cols(columns: &[ColumnInfo], excluded: &[String]) -> String`
  - Filters out excluded names AND virtual columns (MATERIALIZED/ALIAS/EPHEMERAL)
  - Returns backtick-quoted comma-separated list, e.g. `` `id`, `name`, `ts` ``

**Changes in client.rs:**
- Update `select_batch_raw()` signature: add `col_list: &str`
  - If `col_list` is empty, use `*`; otherwise use the provided list
- Update `select_delta_raw()` signature: add `col_list: &str`
  - Same logic

**Verify:** `cargo build`; unit tests for `build_select_cols()`

---

### T4 — Wire up initial sync and CDC

**Files:** `src/sync.rs`, `src/cdc.rs`

**Changes in sync.rs:**
- In `sync_table()`, before the batch loop:
  - Call `get_columns(&self.src, &table.name)` to get the column list
  - Call `build_select_cols(&cols, excluded)` to get the col list string
  - Pass `col_list` into `select_batch_raw()`

**Changes in cdc.rs:**
- In `CdcEngine::run()`, when building `TableState`:
  - Resolve excluded cols for this table from config
  - Build col list string via `build_select_cols(&cols, excluded)`
  - If the watermark column is in the excluded list → override `wm_kind` to `WatermarkKind::None` + warn
  - Store `col_list: String` in `TableState`
- Pass `col_list` into `select_delta_raw()` and `select_batch_raw()`

**Verify:** `cargo build`; `cargo test`

---

### T5 — Unit tests

**Files:** `src/schema.rs` (tests module), `src/main.rs` (filter_tests or new mod)

**Tests to add:**
- `strip_excluded_columns`: removes column line + cleans trailing comma
- `strip_excluded_columns`: leaves DDL unchanged when no match
- `strip_excluded_columns`: handles backtick-quoted and unquoted column names
- `build_select_cols`: excludes named columns
- `build_select_cols`: excludes virtual columns even if not in exclusion list
- `build_select_cols`: empty exclusion → all real columns
- Config parsing: valid `table.col` pairs → correct HashMap
- Config parsing: entry without dot → skipped with no panic

**Verify:** `cargo test` — all tests pass

---

## Acceptance Criteria

- `--exclude-columns=orders.internal_id,users.password` drops those columns from target DDL, initial sync SELECT, and CDC SELECT
- No excluded columns are inserted into the target
- If the watermark column is excluded, CDC falls back to row-count mode with a warning
- All existing 104 tests continue to pass
