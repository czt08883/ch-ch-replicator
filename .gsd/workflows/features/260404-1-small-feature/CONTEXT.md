# Feature: Table Filtering (--include / --exclude flags)

## Description

Add `--include` and `--exclude` CLI flags that let users control which tables are
replicated. By default all tables are replicated; with these flags the user can
whitelist or blacklist specific tables.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Flag names | `--include` / `--exclude` | Consistent with common CLI conventions (rsync, pg_dump, etc.) |
| Value syntax | Comma-separated list of names | Simple to parse; no shell glob complexity |
| Mutual exclusivity | Allow both flags simultaneously, with `--include` taking priority | More flexible; `--include` already narrows the set, `--exclude` further trims it |
| Where to apply | At the `list_tables` call site in `main.rs::run()` | Filters happen after schema discovery, before schema setup and sync — keeps schema/sync code unchanged |
| Config propagation | Add `include_tables` / `exclude_tables` `Vec<String>` fields to `Config` | Consistent with how `threads`, `batch_size` etc. are carried around |
| Glob patterns | Out of scope — exact table names only | Keeps implementation simple; can be added later |
| Case sensitivity | Case-sensitive match | ClickHouse table names are case-sensitive |

## Scope

### In scope
- `--include table1,table2` whitelist flag
- `--exclude table1,table2` blacklist flag
- Filter applied at the point of table list in `main.rs` (before schema setup, initial sync, and CDC)
- Info-level log messages showing which tables were included/excluded
- Unit tests for the filtering logic

### Out of scope
- Glob / regex pattern matching
- Per-phase filtering (same filter applies to all phases)
- Persistent filter in checkpoint (filter is always re-applied from CLI on restart)
