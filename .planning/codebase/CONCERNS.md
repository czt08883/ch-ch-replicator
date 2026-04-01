# Concerns
_Last updated: 2026-04-01_

## Summary

ch-ch-replicator is a focused, well-structured tool for append-only ClickHouse replication, but it has significant architectural limitations: it cannot detect updates or deletes, relies on heuristic watermark column selection, and provides no mechanism for schema drift after initial setup. Several operational concerns around memory pressure, CDC data loss edge cases, and the hardcoded checkpoint file path are also worth tracking.

---

## Technical Debt

**Hardcoded batch size and poll interval — no CLI or env override:**
- Severity: Medium
- `Config::batch_size` is hardcoded to `50_000` and `cdc_poll_secs` to `5` in `src/config.rs:133-135`. There is no way to tune these without recompiling. Tables with very wide rows will hit memory pressure at 50K rows/batch.
- Fix approach: Add `--batch-size` and `--cdc-poll-secs` CLI flags in `src/main.rs` and thread them into `Config::new()`.

**Checkpoint path hardcoded to working directory:**
- Severity: Medium
- `checkpoint_path` is always `"checkpoint.json"` (relative, in `src/config.rs:136`). If the process is run from different working directories on restart, it will not find the previous checkpoint and will restart from scratch silently.
- Fix approach: Add a `--checkpoint` CLI flag; alternatively resolve to an absolute path at startup.

**`uuid` crate imported but not used in production code:**
- Severity: Low
- `uuid = { version = "1", features = ["v4"] }` is in `Cargo.toml` but the only usage is a comment-only UUID workaround in test code in `src/checkpoint.rs:119-125` (which uses `SystemTime` instead). The dependency is dead weight.
- Fix approach: Remove the `uuid` dependency.

**`anyhow` imported but unused:**
- Severity: Low
- `anyhow = "1"` appears in `Cargo.toml` but is not imported in any source file. The codebase uses a custom `thiserror`-based error type in `src/error.rs`.
- Fix approach: Remove from `Cargo.toml`.

**`query_json_meta` and `execute` / `execute_on_db` methods are dead code:**
- Severity: Low
- `src/client.rs:131`, `249`, `277` carry `#[allow(dead_code)]` annotations. These methods exist but are not called from anywhere in the application.
- Fix approach: Remove unused methods or document their intended future use.

**`wm_col` field in `TableState` is dead (allowed via attribute):**
- Severity: Low
- `src/cdc.rs:256` stores `wm_col` but marks it `#[allow(dead_code)]`. The value is computed and stored but never read after initialization.
- Fix approach: Remove the field or use it for logging/checkpointing.

**`is_datetime` parameter in `fetch_max_watermark` is unused:**
- Severity: Low
- `src/schema.rs:281`: `let _ = is_datetime;` — the parameter is accepted but does nothing. The function signature implies different behaviour for DateTime vs UInt columns that is not implemented.
- Fix approach: Remove the parameter from the signature and all call sites, or implement the intended differentiation.

**Datetime watermark comparison is purely lexicographic:**
- Severity: Medium
- `src/cdc.rs:299-304`: max watermark for DateTime columns is determined by string comparison (`s > prev`). This works correctly for `YYYY-MM-DD HH:MM:SS` ISO-style strings but will produce wrong results for `DateTime64` values that include sub-second precision or timezone offsets (e.g. `2024-03-01T12:00:00.123456+00:00`).
- Fix approach: Normalize to a canonical sortable format before comparison, or parse as `chrono::NaiveDateTime`.

---

## Known Limitations

**No UPDATE or DELETE replication (append-only only):**
- The tool is documented as append-only (`src/cdc.rs` header comment). Any row deleted or updated at the source is not propagated to the target. Tables using `ReplacingMergeTree` or `CollapsingMergeTree` engines will silently diverge over time if rows are updated.

**No schema drift handling after initial setup:**
- `src/main.rs:185-200` applies DDL once at startup using `CREATE TABLE IF NOT EXISTS`. Any `ALTER TABLE` on the source after initial setup is not detected or applied to the destination. The replication will silently start failing or producing incomplete rows if columns are added/removed.

**CDC batch size is capped — high-velocity tables can fall behind permanently:**
- `src/cdc.rs:175`: `let limit = self.config.batch_size;` — each CDC poll fetches at most `batch_size` rows (50K). If a table produces more than 50K rows per 5-second poll interval, CDC will lag indefinitely with no backpressure signalling or catchup mechanism.

**Row-count CDC fallback is unreliable with concurrent deletes:**
- `src/cdc.rs:208-245`: the fallback `poll_by_count()` compares `COUNT(*)` on source vs destination. If rows are deleted from the source between polls, `src_count` may be less than or equal to `dst_count` even though new rows exist, causing them to be silently skipped.

**Initial sync is not linearizable — parallel workers may observe moving source:**
- `src/sync.rs:108`: source row count is sampled once before spawning workers. If rows are inserted during the initial sync, the sync terminates when it reaches the original `src_count` but the checkpoint marks the table complete, causing those new rows to be caught by CDC seeding from the post-sync MAX watermark. This is usually correct for append-only tables, but could miss rows if the watermark column is not monotonically increasing.

**Tables without a usable watermark column fall back to row-count CDC only:**
- `src/schema.rs:332-399` + `src/cdc.rs:152-155`: tables with only `String`, `Float`, `Date`, or other non-DateTime/non-UInt columns fall to `WatermarkKind::None`, and row-count comparison is the only CDC strategy. This is fragile (see above) and emits no warning to the operator.

**No TLS support for ClickHouse connections:**
- `src/client.rs:29`: the HTTP URL is always constructed as `http://` in `src/config.rs:64`. The `rustls-tls` feature is enabled in `Cargo.toml`, but there is no mechanism to pass `https://` or configure TLS certificates.

**Single-checkpoint-file design does not support multi-instance runs:**
- Running two instances of the replicator against the same source/destination with the same working directory will cause checkpoint file corruption due to concurrent atomic writes from separate processes.

**No filtering — all non-view tables are always replicated:**
- `src/schema.rs:36-62`: `list_tables()` returns all non-view tables. There is no `--include-tables` or `--exclude-tables` flag. Operators cannot selectively replicate a subset of tables.

---

## Risks

**CDC watermark seed race condition:**
- `src/cdc.rs:79`: after initial sync, the watermark is seeded by querying `MAX(watermark_col)` from the source. Between the end of initial sync and this MAX query, rows inserted at the source could be missed if they have a watermark value equal to or less than the MAX at seed time. This window is small but non-zero.

**Memory exhaustion on large batches:**
- `src/client.rs:93` and `src/client.rs:199`: full HTTP response bodies are buffered in memory as `String`. A 50K-row batch with wide rows (e.g. JSON columns, large strings) can easily consume hundreds of MB. No streaming or backpressure exists.

**Checkpoint file loss causes full re-sync:**
- If `checkpoint.json` is deleted or corrupted, the tool restarts from scratch. For large databases this means hours or days of re-sync. No backup or versioned checkpoint exists.

**SIGTERM handler uses `expect()` — panics if signal registration fails:**
- `src/main.rs:209`: `.expect("failed to register SIGTERM handler")` will panic if called in an environment where `SIGTERM` cannot be registered (unusual but possible in some container runtimes).

**SQL injection via table and column names in client queries:**
- `src/client.rs:183-184`, `219-220`, `167-169`: table names are interpolated into SQL using backtick quoting, which prevents keyword conflicts but does not prevent injection via backtick characters in table names. ClickHouse table names with backticks in them (edge case, but legal) could produce malformed queries. `src/schema.rs:435-437`: `escape_string()` only escapes single quotes, not backticks.

**`select_delta_raw` silently returns empty string on HTTP failure:**
- `src/client.rs:237-239`: unlike other query methods that return an error, `select_delta_raw` logs a warning and returns `Ok("")` on a non-2xx HTTP status. This means transient CDC failures are silently swallowed and the watermark is not advanced, causing data loss for that poll cycle to go undetected until manually checked in logs.

---

## Gaps / Unknowns

**No integration tests exist:**
- All tests are unit tests embedded in source modules (`#[cfg(test)]`). There are no end-to-end tests exercising the actual ClickHouse HTTP API. Regressions in query formatting, DDL adaptation, or CDC logic that depend on real ClickHouse behaviour cannot be caught by the current test suite.

**Behaviour under ClickHouse MergeTree deduplication is untested:**
- ClickHouse's `ReplacingMergeTree` and `CollapsingMergeTree` engines deduplicate/collapse rows asynchronously. The `sync.rs:266-272` warning about `dst_count < total` acknowledges this, but the actual impact on CDC correctness (e.g. watermark advancing past rows that were later deduplicated away) is not analysed.

**No observability beyond structured logs:**
- There are no metrics endpoints, no Prometheus exposition, and no alerting hooks. Operators have no way to monitor replication lag, throughput, or error rates without parsing log output.

**Table engine compatibility is undocumented:**
- Non-MergeTree engines (e.g. `Memory`, `Log`, `Distributed`, `Buffer`) are replicated structurally by copying DDL. Whether data replication is correct for these engines is unknown and untested.

**`futures` crate listed as a dependency but appears unused:**
- `futures = "0.3"` is in `Cargo.toml` but no `use futures::` appears in any source file. This may be an unused transitive-aid or leftover from a refactor.
