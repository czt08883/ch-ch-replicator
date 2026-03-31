use crate::client::ClickHouseClient;
use crate::error::{ReplicatorError, Result};
use serde::Deserialize;
use tracing::info;

/// Metadata about a single ClickHouse table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub engine: String,
}

/// Column metadata for a table.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
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
        "SELECT name, engine \
         FROM system.tables \
         WHERE database = '{}' \
           AND engine NOT IN ('View','MaterializedView','LiveView','WindowView') \
         ORDER BY name",
        escape_string(&client.config.database)
    );

    #[derive(Deserialize)]
    struct Row {
        name: String,
        engine: String,
    }

    let rows: Vec<Row> = client.query_typed(&sql).await?;
    Ok(rows
        .into_iter()
        .map(|r| TableInfo {
            name: r.name,
            engine: r.engine,
        })
        .collect())
}

/// Adapt a DDL string from the source database to the destination database.
/// - Replaces `src_db`.`table` with `dest_db`.`table`
/// - Ensures the statement uses `CREATE TABLE IF NOT EXISTS`
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
    if ddl.contains("CREATE TABLE IF NOT EXISTS") {
        ddl
    } else {
        ddl.replacen("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
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

    Ok(adapt_ddl(&ddl, &client.config.database, table, dest_database))
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

/// Retrieve the column list for a table.
pub async fn get_columns(
    client: &ClickHouseClient,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let sql = format!(
        "SELECT name, type \
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
    }

    let rows: Vec<Row> = client.query_typed(&sql).await?;
    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            name: r.name,
            col_type: r.col_type,
        })
        .collect())
}

/// Pick the best watermark column for CDC polling.
///
/// Priority:
/// 1. DateTime / DateTime64 column named something like `updated_at`, `modified_at`, `event_time`
/// 2. Any DateTime / DateTime64 column
/// 3. UInt64 column named `version`, `_version`, `id`
/// 4. Any UInt64 column
/// 5. Nothing
pub fn pick_watermark(columns: &[ColumnInfo]) -> WatermarkKind {
    let preferred_datetime = ["updated_at", "modified_at", "event_time", "created_at", "timestamp"];
    let preferred_uint = ["_version", "version", "id"];

    // 1. Preferred datetime name
    for col in columns {
        if is_datetime(&col.col_type) {
            let lower = col.name.to_lowercase();
            if preferred_datetime.iter().any(|&p| lower == p) {
                return WatermarkKind::DateTime(col.name.clone());
            }
        }
    }
    // 2. Any datetime
    for col in columns {
        if is_datetime(&col.col_type) {
            return WatermarkKind::DateTime(col.name.clone());
        }
    }
    // 3. Preferred uint name
    for col in columns {
        if is_uint(&col.col_type) {
            let lower = col.name.to_lowercase();
            if preferred_uint.iter().any(|&p| lower == p) {
                return WatermarkKind::UInt64(col.name.clone());
            }
        }
    }
    // 4. Any UInt64
    for col in columns {
        if is_uint(&col.col_type) {
            return WatermarkKind::UInt64(col.name.clone());
        }
    }
    WatermarkKind::None
}

fn is_datetime(t: &str) -> bool {
    let t = t.to_lowercase();
    t.starts_with("datetime") || t.starts_with("date32") || t.starts_with("date")
}

fn is_uint(t: &str) -> bool {
    let t = t.to_lowercase();
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
}
