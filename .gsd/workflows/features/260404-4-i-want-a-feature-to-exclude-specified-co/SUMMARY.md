# Summary: Column Exclusion (--exclude-columns)

## What was built

A `--exclude-columns` CLI flag that lets users drop specific columns from replication.
Columns are specified as `table.column` pairs and are omitted in all three phases:
schema creation, initial sync, and CDC polling.

## Usage

```bash
# Exclude a sensitive column from users, and an internal ID from orders
ch-ch-replicator \
  --src="clickhouse://user:pass@source:8123/mydb" \
  --dest="clickhouse://user:pass@dest:8123/mydb" \
  --exclude-columns=users.password_hash,orders.internal_id

# Multiple tables, multiple columns
ch-ch-replicator ... --exclude-columns=t1.col1,t1.col2,t2.col3
```

**Notes:**
- Each entry must be `table.column` format (dot required)
- Matching is exact and case-sensitive
- If the watermark column for CDC is excluded, that table falls back to row-count CDC (with a warning)
- Invalid entries (no dot, empty table/column) are warned and skipped — they don't abort startup

## Files changed

| File | Change |
|---|---|
| `src/config.rs` | Added `exclude_columns: HashMap<String, Vec<String>>` field; `Config::new()` accepts it; `excluded_columns_for()` lookup helper; 2 new tests |
| `src/main.rs` | Added `--exclude-columns` CLI arg; `parse_exclude_columns()` helper; startup log lines; 5 new tests |
| `src/schema.rs` | `strip_excluded_columns()` — removes column definitions from DDL; `build_select_cols()` — builds explicit SELECT list; `get_create_ddl()` accepts `excluded_cols`; `WatermarkKind` derives `PartialEq`; 13 new tests |
| `src/client.rs` | `select_batch_raw()` and `select_delta_raw()` extended with `col_list` param |
| `src/sync.rs` | Resolves col list before batch loop; passes it to `select_batch_raw()` |
| `src/cdc.rs` | Resolves col list per table; detects excluded watermark columns (falls back to count CDC); passes col list to both SELECT methods |

## Commits

```
feat(config): add --exclude-columns flag and Config::exclude_columns HashMap
feat(schema): add strip_excluded_columns() and wire into get_create_ddl()
feat(schema): add build_select_cols(); extend select_batch_raw/select_delta_raw with col_list param
feat(sync,cdc): wire build_select_cols() into initial sync and CDC; handle excluded watermark column
test(schema,config): add 18 unit tests for strip_excluded_columns, build_select_cols, parse_exclude_columns
fix(schema): use strip_prefix() in extract_col_name per clippy::manual_strip
```

## Test results

122 tests passed, 0 failed. Release build successful. Clippy clean (`-D warnings`).
18 new tests added (13 in schema, 5 in main).
