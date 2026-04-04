# Summary: Table Filtering (--include / --exclude)

## What was built

Two new CLI flags for `ch-ch-replicator` that control which tables are replicated:

- `--include table1,table2` — whitelist: only these tables are replicated
- `--exclude table1,table2` — blacklist: replicate all tables except these
- Both flags can be combined: `--include` is applied first, then `--exclude`
- Matching is exact and case-sensitive (ClickHouse table names are case-sensitive)
- Tables filtered out are logged at INFO level with the reason

## Files changed

| File | Change |
|---|---|
| `src/config.rs` | Added `include_tables` / `exclude_tables` fields to `Config`; updated `Config::new()` signature; updated config tests |
| `src/main.rs` | Added `--include` / `--exclude` args to `Cli`; wired them into `Config::new()`; added `filter_tables()` helper; apply filter after `list_tables()`; 8 unit tests |
| `src/error.rs` | Fixed pre-existing clippy warning (char array in `.find()`) |

## Commits

```
feat(config): add --include/--exclude table filter flags and Config fields
feat(filter): implement filter_tables() and apply after list_tables() in run()
test(filter): add 8 unit tests for filter_tables() covering all combinations
fix(error): use char array in find() per clippy::manual_pattern_char_comparison
```

## How to use

```bash
# Replicate only the 'events' and 'users' tables
ch-ch-replicator \
  --src="clickhouse://user:pass@source:8123/mydb" \
  --dest="clickhouse://user:pass@dest:8123/mydb" \
  --include=events,users

# Replicate everything except temporary/scratch tables
ch-ch-replicator \
  --src="clickhouse://user:pass@source:8123/mydb" \
  --dest="clickhouse://user:pass@dest:8123/mydb" \
  --exclude=tmp_events,scratch

# Combine: from {a,b,c,d} include {a,b,c} then exclude {b} → replicates a,c
ch-ch-replicator ... --include=a,b,c --exclude=b
```

## Test results

104 tests passed, 0 failed. Clippy clean (`-D warnings`).
