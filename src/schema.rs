use crate::client::ClickHouseClient;
use crate::error::{ReplicatorError, Result};
use serde::Deserialize;
use tracing::{info, warn};

/// Metadata about a single ClickHouse table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub engine: String,
    /// The table's sorting key expression (from `system.tables.sorting_key`).
    /// Empty string for non-MergeTree engines that have no sorting key.
    pub sorting_key: String,
}

/// Column metadata for a table.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    /// ClickHouse default_kind: '', 'DEFAULT', 'MATERIALIZED', 'ALIAS', 'EPHEMERAL'
    #[serde(default)]
    pub default_kind: String,
}

/// The kind of column usable as a CDC watermark.
#[derive(Debug, Clone)]
pub enum WatermarkKind {
    DateTime(String),   // column name
    UInt64(String),     // column name (e.g. version, id)
    None,
}

/// Returns the list of ordinary tables in the source database, skipping views/MViews.
pub async fn list_tables(client: &ClickHouseClient) -> Result<Vec<TableInfo>> {
    let sql = format!(
        "SELECT name, engine, sorting_key \
         FROM system.tables \
         WHERE database = '{}' \
           AND engine NOT IN ('View','MaterializedView','LiveView','WindowView','Dictionary') \
         ORDER BY name",
        escape_string(&client.config.database)
    );

    #[derive(Deserialize)]
    struct Row {
        name: String,
        engine: String,
        sorting_key: String,
    }

    let rows: Vec<Row> = client.query_typed(&sql).await?;
    Ok(rows
        .into_iter()
        .map(|r| TableInfo {
            name: r.name,
            engine: r.engine,
            sorting_key: r.sorting_key,
        })
        .collect())
}

/// Returns the names of all dictionaries in the source database.
pub async fn list_dictionaries(client: &ClickHouseClient) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT name FROM system.tables \
         WHERE database = '{}' AND engine = 'Dictionary' \
         ORDER BY name",
        escape_string(&client.config.database)
    );

    #[derive(Deserialize)]
    struct Row {
        name: String,
    }

    let rows: Vec<Row> = client.query_typed(&sql).await?;
    Ok(rows.into_iter().map(|r| r.name).collect())
}

/// Adapt a DDL string from the source database to the destination database.
/// - Replaces `src_db`.`table` with `dest_db`.`table`
/// - Ensures the statement uses `CREATE TABLE IF NOT EXISTS` or `CREATE DICTIONARY IF NOT EXISTS`
/// - Rewrites deprecated MergeTree engine syntax to the modern ORDER BY / PARTITION BY form
pub(crate) fn adapt_ddl(ddl: &str, src_db: &str, table: &str, dest_db: &str) -> String {
    let ddl = ddl.replace(
        &format!("`{}`.`{}`", src_db, table),
        &format!("`{}`.`{}`", dest_db, table),
    );
    // Also handle unquoted form just in case
    let ddl = ddl.replace(
        &format!("{}.{}", src_db, table),
        &format!("`{}`.`{}`", dest_db, table),
    );
    let ddl = if ddl.contains("CREATE TABLE IF NOT EXISTS") {
        ddl
    } else {
        ddl.replacen("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
    };
    let ddl = if ddl.contains("CREATE DICTIONARY IF NOT EXISTS") {
        ddl
    } else {
        ddl.replacen("CREATE DICTIONARY ", "CREATE DICTIONARY IF NOT EXISTS ", 1)
    };
    rewrite_deprecated_engine_syntax(&ddl)
}

/// Convert deprecated MergeTree engine syntax to the modern ORDER BY / PARTITION BY form.
///
/// Old (pre-1.1.54382):
///   `ENGINE = ReplacingMergeTree(date_col, sort_key, granularity[, extra...])`
///
/// Modern equivalent:
///   `ENGINE = ReplacingMergeTree([extra...])`
///   `ORDER BY sort_key`
///   `PARTITION BY toYYYYMM(date_col)`
///   `SETTINGS index_granularity = granularity`
///
/// Detection heuristic: the ENGINE(...) call has ≥ 3 top-level comma-separated
/// arguments where the third is a plain integer (the index_granularity).
fn rewrite_deprecated_engine_syntax(ddl: &str) -> String {
    const ENGINE_KW: &str = "ENGINE = ";
    let engine_pos = match ddl.find(ENGINE_KW) {
        Some(p) => p,
        None => return ddl.to_string(),
    };

    let after = &ddl[engine_pos + ENGINE_KW.len()..];

    let paren_offset = match after.find('(') {
        Some(p) => p,
        None => return ddl.to_string(),
    };

    let engine_name = after[..paren_offset].trim();
    if !engine_name.ends_with("MergeTree") {
        return ddl.to_string();
    }

    // Locate the content inside ENGINE(...)
    let args_start = engine_pos + ENGINE_KW.len() + paren_offset + 1;
    let rest = &ddl[args_start..];
    let close_offset = match find_matching_close_paren(rest) {
        Some(p) => p,
        None => return ddl.to_string(),
    };

    let args_str = &rest[..close_offset];
    let args = split_engine_args(args_str);

    // Deprecated form has ≥ 3 args; the 3rd must be a plain integer (index_granularity).
    if args.len() < 3 {
        return ddl.to_string();
    }
    let granularity_str = args[2].trim();
    if granularity_str.is_empty() || !granularity_str.chars().all(|c| c.is_ascii_digit()) {
        return ddl.to_string();
    }

    let date_col = args[0].trim();
    let sort_key = args[1].trim();
    let granularity: u32 = granularity_str.parse().unwrap_or(8192);
    let engine_inner = build_engine_inner(engine_name, &args[3..]);

    let modern = format!(
        "ENGINE = {}({})\nORDER BY {}\nPARTITION BY toYYYYMM({})\nSETTINGS index_granularity = {}",
        engine_name, engine_inner, sort_key, date_col, granularity
    );

    let close_in_ddl = args_start + close_offset;
    format!("{}{}{}", &ddl[..engine_pos], modern, &ddl[close_in_ddl + 1..])
}

/// Find the byte offset of the `)` that closes the current nesting level.
/// `s` must start immediately after the matching `(`.
fn find_matching_close_paren(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Split a comma-separated argument list, respecting nested parentheses.
fn split_engine_args(s: &str) -> Vec<&str> {
    let mut args: Vec<&str> = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                args.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        args.push(&s[start..]);
    }
    args
}

/// Return the arguments to place inside `EngineName(...)` in the modern syntax.
/// Extra args beyond (date, sort_key, granularity) that belong to the engine itself.
fn build_engine_inner(engine: &str, extra: &[&str]) -> String {
    match engine {
        // ReplacingMergeTree([ver_col])
        "ReplacingMergeTree" if !extra.is_empty() => extra[0].trim().to_string(),
        // CollapsingMergeTree(sign_col)
        "CollapsingMergeTree" if !extra.is_empty() => extra[0].trim().to_string(),
        // VersionedCollapsingMergeTree(sign_col, ver_col)
        "VersionedCollapsingMergeTree" if extra.len() >= 2 => {
            format!("{}, {}", extra[0].trim(), extra[1].trim())
        }
        // SummingMergeTree([(columns)])
        "SummingMergeTree" if !extra.is_empty() => extra[0].trim().to_string(),
        // GraphiteMergeTree(config_section)
        "GraphiteMergeTree" if !extra.is_empty() => extra[0].trim().to_string(),
        _ => String::new(),
    }
}

/// Remove excluded column definitions from a `CREATE TABLE` DDL string.
///
/// ClickHouse `SHOW CREATE TABLE` produces one column definition per line inside
/// the `(...)` block.  Each line looks like:
///   `    \`col_name\` Type [modifiers][,]`
///
/// The function:
/// 1. Removes any line whose column name (backtick-quoted or bare) matches an entry in `excluded`.
/// 2. Ensures the remaining column list has correct comma placement (the last column has no
///    trailing comma; all others do).
///
/// Lines outside the column block (ENGINE, ORDER BY, …) are never modified.
pub fn strip_excluded_columns(ddl: &str, excluded: &[String]) -> String {
    if excluded.is_empty() {
        return ddl.to_string();
    }

    let lines: Vec<&str> = ddl.lines().collect();
    let mut in_columns = false;
    let mut col_lines: Vec<String> = Vec::new();   // column definition lines to keep
    let mut before: Vec<&str> = Vec::new();        // lines before the column block
    let mut after: Vec<&str> = Vec::new();         // lines from ENGINE onwards

    for line in &lines {
        let trimmed = line.trim();
        if !in_columns {
            // The column block starts on the line that is just "(" (or the opening
            // paren of CREATE TABLE ... (\n).
            if trimmed == "(" {
                in_columns = true;
                before.push(line);
                continue;
            }
            before.push(line);
        } else {
            // The column block ends when we hit a line that starts ENGINE or ")" alone.
            // In modern ClickHouse DDL the closing ")" is on its own line before ENGINE.
            if trimmed == ")" || trimmed.starts_with(')') {
                in_columns = false;
                after.push(line);
                continue;
            }
            // Check if this line defines one of the excluded columns
            if let Some(col_name) = extract_col_name(trimmed) {
                if excluded.iter().any(|ex| ex == col_name) {
                    // Skip this column line
                    continue;
                }
            }
            // Strip trailing comma, we'll add them back correctly below
            let stripped = trimmed.trim_end_matches(',').to_string();
            col_lines.push(stripped);
        }
    }

    if col_lines.is_empty() {
        // Nothing was in the column block (DDL format not recognised) — return unchanged
        return ddl.to_string();
    }

    // Re-add commas: every line except the last gets a comma
    let last = col_lines.len() - 1;
    let formatted_cols: Vec<String> = col_lines
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let indent = "    ";
            if i < last {
                format!("{}{},", indent, l)
            } else {
                format!("{}{}", indent, l)
            }
        })
        .collect();

    let mut result = Vec::new();
    result.extend(before.iter().map(|s| s.to_string()));
    result.extend(formatted_cols);
    result.extend(after.iter().map(|s| s.to_string()));
    result.join("\n")
}

/// Extract the column name from a DDL column definition line.
/// Handles both backtick-quoted (`` `col` ``) and bare (`col`) names.
/// Returns `None` if the line doesn't look like a column definition.
fn extract_col_name(line: &str) -> Option<&str> {
    let line = line.trim_start_matches(' ').trim_end_matches(',');
    if line.starts_with('`') {
        // backtick-quoted: `name` Type ...
        let rest = &line[1..];
        let end = rest.find('`')?;
        Some(&rest[..end])
    } else if line.starts_with('"') {
        // double-quoted: "name" Type ...
        let rest = &line[1..];
        let end = rest.find('"')?;
        Some(&rest[..end])
    } else {
        // bare: name Type ...
        let end = line.find(char::is_whitespace)?;
        Some(&line[..end])
    }
}

/// Returns the CREATE TABLE DDL for a given table, adapted for use on the target.
///
/// Steps:
/// 1. `SHOW CREATE TABLE src_db.table`
/// 2. Replace the source database name with the destination database name
/// 3. Turn `CREATE TABLE` into `CREATE TABLE IF NOT EXISTS`
pub async fn get_create_ddl(
    client: &ClickHouseClient,
    table: &str,
    dest_database: &str,
    excluded_cols: &[String],
) -> Result<String> {
    let sql = format!(
        "SHOW CREATE TABLE `{}`.`{}`",
        client.config.database, table
    );

    let rows = client.query_json_rows(&sql).await?;
    if rows.is_empty() {
        return Err(ReplicatorError::Schema(format!(
            "no DDL returned for table '{}'",
            table
        )));
    }

    // SHOW CREATE TABLE returns a single column called "statement"
    let ddl = rows[0]
        .get("statement")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ReplicatorError::Schema(format!("unexpected DDL response for '{}'", table))
        })?
        .to_string();

    let adapted = adapt_ddl(&ddl, &client.config.database, table, dest_database);
    Ok(strip_excluded_columns(&adapted, excluded_cols))
}

/// Ensure the target database exists.
pub async fn ensure_database(client: &ClickHouseClient, database: &str) -> Result<()> {
    let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", database);
    info!("Ensuring target database '{}' exists", database);
    client.execute_no_db(&sql).await
}

/// Apply a DDL statement on the target ClickHouse.
pub async fn apply_ddl(client: &ClickHouseClient, ddl: &str) -> Result<()> {
    info!("Applying DDL: {:.120}…", ddl);
    client.execute_no_db(ddl).await
}

/// Build an explicit SELECT column list for a table, excluding virtual and user-excluded columns.
///
/// Returns a comma-separated, backtick-quoted list of column names, e.g.:
///   `` `id`, `name`, `ts` ``
///
/// If the resulting list would be empty (all columns excluded), returns `"*"` as a safe fallback.
pub fn build_select_cols(columns: &[ColumnInfo], excluded: &[String]) -> String {
    let cols: Vec<String> = columns
        .iter()
        .filter(|c| !is_virtual(&c.default_kind) && !excluded.contains(&c.name))
        .map(|c| format!("`{}`", c.name))
        .collect();
    if cols.is_empty() {
        "*".to_string()
    } else {
        cols.join(", ")
    }
}

/// Fetch the current MAX value of a watermark column from the source table.
/// Returns None if the table is empty or the query fails.
pub async fn fetch_max_watermark(
    client: &ClickHouseClient,
    table: &str,
    col: &str,
    is_datetime: bool,
) -> Option<String> {
    let sql = format!(
        "SELECT max(`{}`) AS m FROM `{}`.`{}`",
        col,
        client.config.database,
        table
    );
    let val = client.query_scalar(&sql).await.ok()?;
    if val.is_empty() || val == "0" || val == "1970-01-01 00:00:00" {
        return None;
    }
    // DateTime values come back without quotes; keep as-is for the watermark store
    let _ = is_datetime; // no transformation needed
    Some(val)
}

/// Retrieve the column list for a table, including default_kind to detect MATERIALIZED columns.
pub async fn get_columns(
    client: &ClickHouseClient,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let sql = format!(
        "SELECT name, type, default_kind \
         FROM system.columns \
         WHERE database = '{}' AND table = '{}' \
         ORDER BY position",
        escape_string(&client.config.database),
        escape_string(table)
    );

    #[derive(Deserialize)]
    struct Row {
        name: String,
        #[serde(rename = "type")]
        col_type: String,
        default_kind: String,
    }

    let rows: Vec<Row> = client.query_typed(&sql).await?;
    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            name: r.name,
            col_type: r.col_type,
            default_kind: r.default_kind,
        })
        .collect())
}

/// Pick the best watermark column for CDC polling.
///
/// Priority (within each tier, non-Nullable columns are preferred over Nullable):
/// 1. DateTime / DateTime64 column with a preferred name (`updated_at`, `modified_at`, …)
/// 2. Any DateTime / DateTime64 column
/// 3. UInt64/UInt32/Int64 column with a preferred name (`_version`, `version`, `id`)
/// 4. Any UInt64/UInt32/Int64 column
/// 5. Nothing
///
/// MATERIALIZED and ALIAS columns are always excluded — they are computed
/// constants (e.g. `_version UInt64 MATERIALIZED 1`) and cannot track real changes.
///
/// Nullable columns *are* considered (e.g. `Nullable(DateTime)`), but a warning
/// is emitted because rows with NULL watermark values will not be detected by CDC.
pub fn pick_watermark(columns: &[ColumnInfo]) -> WatermarkKind {
    let result = pick_watermark_inner(columns);

    // Warn once if the chosen column is Nullable
    if let WatermarkKind::DateTime(ref name) | WatermarkKind::UInt64(ref name) = &result {
        if let Some(col) = columns.iter().find(|c| c.name == *name) {
            if is_nullable(&col.col_type) {
                warn!(
                    "Selected Nullable watermark column '{}' (type '{}'). \
                     Rows with NULL values in this column will not be detected by CDC polling.",
                    col.name, col.col_type
                );
            }
        }
    }

    result
}

fn pick_watermark_inner(columns: &[ColumnInfo]) -> WatermarkKind {
    let preferred_datetime = ["updated_at", "modified_at", "event_time", "created_at", "timestamp"];
    let preferred_uint = ["_version", "version", "id"];

    // Only consider real stored columns
    let real: Vec<&ColumnInfo> = columns
        .iter()
        .filter(|c| !is_virtual(&c.default_kind))
        .collect();

    // 1. Preferred datetime name (non-Nullable first, then Nullable)
    for nullable in [false, true] {
        for col in &real {
            if is_datetime(&col.col_type) && is_nullable(&col.col_type) == nullable {
                let lower = col.name.to_lowercase();
                if preferred_datetime.iter().any(|&p| lower == p) {
                    return WatermarkKind::DateTime(col.name.clone());
                }
            }
        }
    }
    // 2. Any datetime (non-Nullable first, then Nullable)
    for nullable in [false, true] {
        for col in &real {
            if is_datetime(&col.col_type) && is_nullable(&col.col_type) == nullable {
                return WatermarkKind::DateTime(col.name.clone());
            }
        }
    }
    // 3. Preferred uint name (non-Nullable first, then Nullable)
    for nullable in [false, true] {
        for col in &real {
            if is_uint(&col.col_type) && is_nullable(&col.col_type) == nullable {
                let lower = col.name.to_lowercase();
                if preferred_uint.iter().any(|&p| lower == p) {
                    return WatermarkKind::UInt64(col.name.clone());
                }
            }
        }
    }
    // 4. Any uint (non-Nullable first, then Nullable)
    for nullable in [false, true] {
        for col in &real {
            if is_uint(&col.col_type) && is_nullable(&col.col_type) == nullable {
                return WatermarkKind::UInt64(col.name.clone());
            }
        }
    }
    WatermarkKind::None
}

/// Returns true for MATERIALIZED and ALIAS columns — they are computed, not stored.
fn is_virtual(default_kind: &str) -> bool {
    matches!(default_kind, "MATERIALIZED" | "ALIAS" | "EPHEMERAL")
}

/// Strip the `Nullable(...)` wrapper from a ClickHouse type string, returning the inner type.
/// If the type is not Nullable, returns it unchanged.
fn unwrap_nullable(type_name: &str) -> &str {
    let t = type_name.trim();
    if let Some(inner) = t.strip_prefix("Nullable(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return inner;
        }
    }
    t
}

/// Returns true if the column type is Nullable.
fn is_nullable(type_name: &str) -> bool {
    type_name.trim().to_lowercase().starts_with("nullable(")
}

fn is_datetime(type_name: &str) -> bool {
    let t = unwrap_nullable(type_name).to_lowercase();
    t.starts_with("datetime") || t.starts_with("date32") || t.starts_with("date")
}

fn is_uint(type_name: &str) -> bool {
    let t = unwrap_nullable(type_name).to_lowercase();
    t.starts_with("uint64") || t.starts_with("uint32") || t.starts_with("int64")
}

/// Minimal SQL string escaping (replace single quotes with two single quotes).
pub fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // escape_string
    // -----------------------------------------------------------------------

    #[test]
    fn escape_no_quotes() {
        assert_eq!(escape_string("hello"), "hello");
    }

    #[test]
    fn escape_single_quote() {
        assert_eq!(escape_string("it's"), "it''s");
    }

    #[test]
    fn escape_multiple_quotes() {
        assert_eq!(escape_string("a'b'c"), "a''b''c");
    }

    #[test]
    fn escape_empty_string() {
        assert_eq!(escape_string(""), "");
    }

    #[test]
    fn escape_only_quotes() {
        assert_eq!(escape_string("'''"), "''''''");
    }

    // -----------------------------------------------------------------------
    // adapt_ddl — database name replacement
    // -----------------------------------------------------------------------

    #[test]
    fn adapt_ddl_replaces_backtick_qualified_name() {
        let ddl = "CREATE TABLE `src`.`events` (id UInt64) ENGINE = MergeTree()";
        let out = adapt_ddl(ddl, "src", "events", "dst");
        assert!(out.contains("`dst`.`events`"), "got: {}", out);
        assert!(!out.contains("`src`"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_replaces_unquoted_qualified_name() {
        let ddl = "CREATE TABLE src.events (id UInt64) ENGINE = MergeTree()";
        let out = adapt_ddl(ddl, "src", "events", "dst");
        assert!(out.contains("`dst`.`events`"), "got: {}", out);
        assert!(!out.contains("src.events"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_adds_if_not_exists() {
        let ddl = "CREATE TABLE `src`.`t` (id UInt64) ENGINE = MergeTree()";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("CREATE TABLE IF NOT EXISTS"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_does_not_duplicate_if_not_exists() {
        let ddl = "CREATE TABLE IF NOT EXISTS `src`.`t` (id UInt64) ENGINE = MergeTree()";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        let count = out.matches("IF NOT EXISTS").count();
        assert_eq!(count, 1, "expected exactly one IF NOT EXISTS, got: {}", out);
    }

    #[test]
    fn adapt_ddl_dictionary_adds_if_not_exists() {
        let ddl = "CREATE DICTIONARY `src`.`browsers` (`id` Int32, `name` String) PRIMARY KEY id SOURCE(FILE(PATH '/var/lib/clickhouse/user_files/browsers.csv' FORMAT 'CSVWithNames')) LIFETIME(MIN 0 MAX 1000) LAYOUT(HASHED())";
        let out = adapt_ddl(ddl, "src", "browsers", "dst");
        assert!(out.contains("CREATE DICTIONARY IF NOT EXISTS"), "got: {}", out);
        assert!(out.contains("`dst`.`browsers`"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_dictionary_does_not_duplicate_if_not_exists() {
        let ddl = "CREATE DICTIONARY IF NOT EXISTS `src`.`browsers` (`id` Int32) PRIMARY KEY id SOURCE(FILE(PATH '/tmp/f')) LIFETIME(0) LAYOUT(HASHED())";
        let out = adapt_ddl(ddl, "src", "browsers", "dst");
        let count = out.matches("IF NOT EXISTS").count();
        assert_eq!(count, 1, "expected exactly one IF NOT EXISTS, got: {}", out);
    }

    // -----------------------------------------------------------------------
    // adapt_ddl — deprecated engine syntax rewriting
    // -----------------------------------------------------------------------

    #[test]
    fn adapt_ddl_rewrites_replacing_mergetree() {
        let ddl = "CREATE TABLE `src`.`t` (\n    `date` Date,\n    `version` String\n)\nENGINE = ReplacingMergeTree(date, version, 8192)";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("ENGINE = ReplacingMergeTree()"), "got: {}", out);
        assert!(out.contains("ORDER BY version"), "got: {}", out);
        assert!(out.contains("PARTITION BY toYYYYMM(date)"), "got: {}", out);
        assert!(out.contains("SETTINGS index_granularity = 8192"), "got: {}", out);
        assert!(!out.contains("ReplacingMergeTree(date,"), "should not have old args: {}", out);
    }

    #[test]
    fn adapt_ddl_rewrites_mergetree_with_tuple_sort_key() {
        let ddl = "CREATE TABLE `src`.`t` (x UInt64)\nENGINE = MergeTree(date, (col1, col2), 8192)";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("ENGINE = MergeTree()"), "got: {}", out);
        assert!(out.contains("ORDER BY (col1, col2)"), "got: {}", out);
        assert!(out.contains("PARTITION BY toYYYYMM(date)"), "got: {}", out);
        assert!(out.contains("SETTINGS index_granularity = 8192"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_rewrites_collapsing_mergetree() {
        let ddl = "CREATE TABLE `src`.`t` (x UInt64)\nENGINE = CollapsingMergeTree(date, id, 8192, sign)";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("ENGINE = CollapsingMergeTree(sign)"), "got: {}", out);
        assert!(out.contains("ORDER BY id"), "got: {}", out);
        assert!(out.contains("PARTITION BY toYYYYMM(date)"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_rewrites_replacing_mergetree_with_ver_col() {
        let ddl = "CREATE TABLE `src`.`t` (x UInt64)\nENGINE = ReplacingMergeTree(date, id, 8192, ver)";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("ENGINE = ReplacingMergeTree(ver)"), "got: {}", out);
        assert!(out.contains("ORDER BY id"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_modern_syntax_untouched() {
        let ddl = "CREATE TABLE `src`.`t` (x UInt64)\nENGINE = ReplacingMergeTree()\nORDER BY id\nSETTINGS index_granularity = 8192";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        // Must remain unchanged (modulo db rename and IF NOT EXISTS)
        assert!(out.contains("ENGINE = ReplacingMergeTree()"), "got: {}", out);
        assert!(out.contains("ORDER BY id"), "got: {}", out);
        // Should not have gained a duplicate ORDER BY
        assert_eq!(out.matches("ORDER BY").count(), 1, "got: {}", out);
    }

    #[test]
    fn adapt_ddl_empty_engine_args_untouched() {
        let ddl = "CREATE TABLE `src`.`t` (x UInt64) ENGINE = MergeTree() ORDER BY x";
        let out = adapt_ddl(ddl, "src", "t", "dst");
        assert!(out.contains("ENGINE = MergeTree()"), "got: {}", out);
        assert!(!out.contains("PARTITION BY"), "got: {}", out);
    }

    #[test]
    fn adapt_ddl_same_src_and_dst_db() {
        // If src == dst the table reference is rewritten to backtick form but DB name unchanged
        let ddl = "CREATE TABLE `mydb`.`orders` (id UInt64) ENGINE = MergeTree()";
        let out = adapt_ddl(ddl, "mydb", "orders", "mydb");
        assert!(out.contains("`mydb`.`orders`"), "got: {}", out);
        assert!(out.contains("IF NOT EXISTS"), "got: {}", out);
    }

    // -----------------------------------------------------------------------
    // pick_watermark — priority rules
    // -----------------------------------------------------------------------

    fn col(name: &str, col_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: col_type.to_string(),
            default_kind: String::new(),
        }
    }

    fn materialized_col(name: &str, col_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: col_type.to_string(),
            default_kind: "MATERIALIZED".to_string(),
        }
    }

    #[test]
    fn pick_watermark_empty_columns() {
        assert!(matches!(pick_watermark(&[]), WatermarkKind::None));
    }

    #[test]
    fn pick_watermark_prefers_updated_at_over_non_preferred_datetime() {
        // Only one preferred and one non-preferred datetime column
        let cols = vec![
            col("id", "UInt64"),
            col("happened_on", "DateTime"),  // non-preferred name
            col("updated_at", "DateTime"),   // preferred name → must win
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "updated_at"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_falls_back_to_any_datetime() {
        let cols = vec![
            col("id", "UInt64"),
            col("happened_on", "DateTime"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "happened_on"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_prefers_version_uint_over_arbitrary_uint() {
        let cols = vec![
            col("some_count", "UInt64"),
            col("version", "UInt64"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "version"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_prefers_datetime_over_uint() {
        let cols = vec![
            col("version", "UInt64"),
            col("ts", "DateTime"),
        ];
        assert!(matches!(pick_watermark(&cols), WatermarkKind::DateTime(_)));
    }

    #[test]
    fn pick_watermark_falls_back_to_any_uint() {
        let cols = vec![
            col("some_num", "UInt64"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "some_num"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_none_when_only_strings() {
        let cols = vec![
            col("name", "String"),
            col("payload", "String"),
        ];
        assert!(matches!(pick_watermark(&cols), WatermarkKind::None));
    }

    #[test]
    fn pick_watermark_datetime64_recognized() {
        let cols = vec![col("event_time", "DateTime64(3)")];
        assert!(matches!(pick_watermark(&cols), WatermarkKind::DateTime(_)));
    }

    #[test]
    fn pick_watermark_uint32_recognized() {
        let cols = vec![col("counter", "UInt32")];
        assert!(matches!(pick_watermark(&cols), WatermarkKind::UInt64(_)));
    }

    #[test]
    fn pick_watermark_int64_recognized() {
        let cols = vec![col("ts_ms", "Int64")];
        assert!(matches!(pick_watermark(&cols), WatermarkKind::UInt64(_)));
    }

    #[test]
    fn pick_watermark_prefers_event_time_preferred_name() {
        let cols = vec![
            col("whatever_dt", "DateTime"),
            col("event_time", "DateTime"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "event_time"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_prefers_id_uint_preferred_name() {
        let cols = vec![
            col("foo_count", "UInt64"),
            col("id", "UInt64"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "id"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_skips_materialized_version() {
        // _version is MATERIALIZED — must not be chosen even though it matches preferred name
        let cols = vec![
            col("id", "Int32"),
            materialized_col("_version", "UInt64"),
        ];
        // Only non-preferred, non-uint real column is id (Int32 not uint) → None
        assert!(matches!(pick_watermark(&cols), WatermarkKind::None));
    }

    #[test]
    fn pick_watermark_skips_materialized_falls_back_to_real_uint() {
        let cols = vec![
            materialized_col("_version", "UInt64"),
            col("sequence_num", "UInt64"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "sequence_num"),
            other => panic!("expected real uint, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_skips_materialized_datetime() {
        let cols = vec![
            materialized_col("event_time", "DateTime"),
            col("id", "UInt64"),
        ];
        // event_time is MATERIALIZED → skip; id is real UInt64 → fallback
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "id"),
            other => panic!("expected uint fallback, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_alias_also_excluded() {
        let cols = vec![
            ColumnInfo {
                name: "updated_at".to_string(),
                col_type: "DateTime".to_string(),
                default_kind: "ALIAS".to_string(),
            },
            col("real_ts", "DateTime"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "real_ts"),
            other => panic!("expected real datetime, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // unwrap_nullable / is_nullable
    // -----------------------------------------------------------------------

    #[test]
    fn unwrap_nullable_strips_wrapper() {
        assert_eq!(unwrap_nullable("Nullable(DateTime)"), "DateTime");
        assert_eq!(unwrap_nullable("Nullable(DateTime64(3))"), "DateTime64(3)");
        assert_eq!(unwrap_nullable("Nullable(UInt64)"), "UInt64");
    }

    #[test]
    fn unwrap_nullable_passthrough_non_nullable() {
        assert_eq!(unwrap_nullable("DateTime"), "DateTime");
        assert_eq!(unwrap_nullable("UInt64"), "UInt64");
        assert_eq!(unwrap_nullable("String"), "String");
    }

    #[test]
    fn is_nullable_detects_correctly() {
        assert!(is_nullable("Nullable(DateTime)"));
        assert!(is_nullable("Nullable(UInt64)"));
        assert!(!is_nullable("DateTime"));
        assert!(!is_nullable("UInt64"));
    }

    // -----------------------------------------------------------------------
    // pick_watermark — Nullable types
    // -----------------------------------------------------------------------

    #[test]
    fn pick_watermark_nullable_datetime_recognized() {
        let cols = vec![col("updated_at", "Nullable(DateTime)")];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "updated_at"),
            other => panic!("expected DateTime, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_nullable_datetime64_recognized() {
        let cols = vec![col("event_time", "Nullable(DateTime64(3))")];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "event_time"),
            other => panic!("expected DateTime, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_nullable_uint_recognized() {
        let cols = vec![col("version", "Nullable(UInt64)")];
        match pick_watermark(&cols) {
            WatermarkKind::UInt64(c) => assert_eq!(c, "version"),
            other => panic!("expected UInt64, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_prefers_non_nullable_over_nullable_same_tier() {
        // Both are preferred DateTime names; non-Nullable should win
        let cols = vec![
            col("updated_at", "Nullable(DateTime)"),
            col("modified_at", "DateTime"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "modified_at"),
            other => panic!("expected non-nullable DateTime, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_nullable_preferred_beats_non_nullable_any() {
        // Preferred-name Nullable should beat arbitrary non-Nullable (higher tier wins)
        let cols = vec![
            col("random_ts", "DateTime"),
            col("updated_at", "Nullable(DateTime)"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "updated_at"),
            other => panic!("expected preferred Nullable, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_non_nullable_any_beats_nullable_any() {
        let cols = vec![
            col("some_ts", "Nullable(DateTime)"),
            col("other_ts", "DateTime"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "other_ts"),
            other => panic!("expected non-nullable any DateTime, got: {:?}", other),
        }
    }

    #[test]
    fn pick_watermark_nullable_with_materialized_version_scenario() {
        // The motivating bug: updated_at is Nullable(DateTime), _version is MATERIALIZED
        // Both created_at and updated_at are preferred names; first match in column order wins.
        let cols = vec![
            col("id", "String"),
            col("player_id", "Int32"),
            col("created_at", "Nullable(DateTime)"),
            col("updated_at", "Nullable(DateTime)"),
            materialized_col("_sign", "Int8"),
            materialized_col("_version", "UInt64"),
        ];
        match pick_watermark(&cols) {
            WatermarkKind::DateTime(c) => assert_eq!(c, "created_at"),
            other => panic!("expected Nullable DateTime watermark, got: {:?}", other),
        }
    }
}
