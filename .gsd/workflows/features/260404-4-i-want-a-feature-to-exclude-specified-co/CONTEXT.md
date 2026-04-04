# Feature: Column Exclusion (--exclude-columns)

## Description

Add `--exclude-columns=table1.col1,table2.col2` to let users drop specific columns
from replication. The excluded columns are omitted in three places:
1. **Schema creation** — the DDL applied to the target strips those column definitions
2. **Initial sync** — `SELECT` uses an explicit column list (all columns except excluded ones)
3. **CDC polling** — same explicit column list in the delta `SELECT`

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Flag syntax | `--exclude-columns=table.col,...` | `table.col` pairs allow per-table precision; a bare `col` would be ambiguous across tables |
| Storage in Config | `HashMap<String, Vec<String>>` keyed by table name | O(1) lookup per table; clean API for callers |
| DDL stripping | Regex-free line-by-line column removal from SHOW CREATE TABLE output | Safer than regex on complex DDL; ClickHouse DDL is line-per-column |
| SELECT column list | Query `system.columns` (already done in CDC via `get_columns`), filter excluded cols, build explicit `SELECT col1, col2, ...` | Re-uses existing `get_columns()`; avoids `SELECT *` |
| Watermark column excluded | If the CDC watermark column is in the exclude list → fall back to row-count CDC for that table | Preserves correctness; warn loudly |
| Sorting key column excluded | Warn but do not block — ClickHouse INSERT with explicit col list still works | User's responsibility |
| Error on unknown table/col in --exclude-columns | Warn at startup, do not abort | Defensive; typos shouldn't kill the replicator |

## Implementation touch-points

- `src/config.rs` — add `exclude_columns: HashMap<String, Vec<String>>`, parse from CLI arg
- `src/main.rs` — add `--exclude-columns` CLI arg, pass to `Config`
- `src/schema.rs` — add `strip_excluded_columns()` to rewrite DDL; add helper to build explicit column list
- `src/client.rs` — add `select_batch_cols_raw()` and `select_delta_cols_raw()` variants that take an explicit column list; OR extend existing methods with an optional param
- `src/sync.rs` — resolve excluded cols per table before syncing, use explicit col SELECT
- `src/cdc.rs` — resolve excluded cols per table, use explicit col SELECT; handle watermark-column-excluded case

## Scope

### In scope
- `--exclude-columns=table.col,...` flag
- DDL stripping of excluded column definitions
- Explicit SELECT column list in initial sync
- Explicit SELECT column list in CDC (watermark-based and count-based)
- Warning if excluded column is the watermark column (falls back to count-based CDC)
- Unit tests for DDL stripping and column-list building

### Out of scope
- Wildcard/glob column names
- Excluding columns from all tables at once (must specify table name)
- ALTER TABLE on already-created target to drop columns (schema is set up fresh)
- Excluding columns that are part of the sorting key (warn only)
