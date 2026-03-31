use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::checkpoint::Checkpoint;
use crate::client::ClickHouseClient;
use crate::config::Config;
use crate::error::{ReplicatorError, Result};
use crate::schema::{get_columns, pick_watermark, TableInfo, WatermarkKind};

/// CDC engine: polls each table for changes and replicates them to the target.
pub struct CdcEngine {
    config: Arc<Config>,
    src: Arc<ClickHouseClient>,
    dst: Arc<ClickHouseClient>,
    checkpoint: Arc<Mutex<Checkpoint>>,
    cancel: CancellationToken,
}

impl CdcEngine {
    pub fn new(
        config: Arc<Config>,
        src: Arc<ClickHouseClient>,
        dst: Arc<ClickHouseClient>,
        checkpoint: Arc<Mutex<Checkpoint>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            src,
            dst,
            checkpoint,
            cancel,
        }
    }

    /// Run the CDC loop until cancelled.
    pub async fn run(&self, tables: Vec<TableInfo>) -> Result<()> {
        info!(
            "Entering CDC mode for {} table(s), polling every {}s",
            tables.len(),
            self.config.cdc_poll_secs
        );

        // Resolve watermark strategy per table once
        let mut table_states: Vec<TableState> = Vec::with_capacity(tables.len());
        for t in &tables {
            let cols = get_columns(&self.src, &t.name).await.unwrap_or_default();
            let wm_kind = pick_watermark(&cols);

            // Seed watermark from checkpoint if available
            let (wm_col, wm_val) = {
                let cp = self.checkpoint.lock().await;
                if let Some(tc) = cp.table(&t.name) {
                    if !tc.watermark_column.is_empty() {
                        (tc.watermark_column.clone(), tc.cdc_watermark.clone())
                    } else {
                        // Derive initial watermark from the wm_kind
                        watermark_initial_value(&wm_kind)
                    }
                } else {
                    watermark_initial_value(&wm_kind)
                }
            };

            info!(
                "Table '{}': CDC watermark column='{}' value='{}'",
                t.name, wm_col, wm_val
            );

            table_states.push(TableState {
                name: t.name.clone(),
                wm_kind,
                wm_col,
                wm_val,
            });
        }

        let poll_interval = Duration::from_secs(self.config.cdc_poll_secs);

        loop {
            // Check cancellation at the top of each cycle
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    info!("CDC loop: cancellation received, stopping");
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }

            debug!("CDC poll cycle");

            for state in &mut table_states {
                if self.cancel.is_cancelled() {
                    return Ok(());
                }

                match self.poll_table(state).await {
                    Ok(rows_replicated) => {
                        if rows_replicated > 0 {
                            info!(
                                "CDC: table '{}' — replicated {} new row(s)",
                                state.name, rows_replicated
                            );
                        }
                    }
                    Err(ReplicatorError::Cancelled) => return Ok(()),
                    Err(e) => {
                        // Log and continue — don't abort CDC for a single table error
                        error!("CDC: table '{}' poll error: {}", state.name, e);
                    }
                }
            }
        }
    }

    /// Poll a single table for new rows and replicate them.
    /// Returns the number of rows replicated.
    async fn poll_table(&self, state: &mut TableState) -> Result<usize> {
        match &state.wm_kind {
            WatermarkKind::None => {
                // Fallback: row-count-based detection
                self.poll_by_count(state).await
            }
            WatermarkKind::DateTime(col) | WatermarkKind::UInt64(col) => {
                let col = col.clone();
                self.poll_by_watermark(state, &col).await
            }
        }
    }

    /// Watermark-based CDC: fetch rows where wm_col > last watermark.
    async fn poll_by_watermark(&self, state: &mut TableState, col: &str) -> Result<usize> {
        let wm_val = state.wm_val.clone();
        let is_datetime = matches!(&state.wm_kind, WatermarkKind::DateTime(_));

        // Wrap datetime values in single quotes for the WHERE clause
        let wm_quoted = if is_datetime && !wm_val.starts_with('\'') {
            format!("'{}'", wm_val)
        } else {
            wm_val.clone()
        };

        let limit = self.config.batch_size;
        let body = self
            .src
            .select_delta_raw(&state.name, col, &wm_quoted, limit)
            .await?;

        let line_count = count_jsonl_lines(&body);
        if line_count == 0 {
            return Ok(0);
        }

        // Find the max watermark value in the batch
        let new_wm = max_watermark_in_jsonl(&body, col, is_datetime).unwrap_or(wm_val.clone());

        // Insert into destination
        self.dst.insert_jsonl(&state.name, body).await?;

        // Update state + checkpoint
        state.wm_val = new_wm.clone();
        {
            let mut cp = self.checkpoint.lock().await;
            cp.update_watermark(
                &state.name,
                col,
                &new_wm,
                &self.config.checkpoint_path,
            )?;
        }

        Ok(line_count)
    }

    /// Row-count-based CDC fallback: re-sync if row count increased.
    async fn poll_by_count(&self, state: &mut TableState) -> Result<usize> {
        let src_count = self.src.count(&state.name).await?;
        let dst_count = self.dst.count(&state.name).await?;

        if src_count <= dst_count {
            return Ok(0);
        }

        let diff = src_count - dst_count;
        info!(
            "CDC (count): table '{}' has {} new rows (src={} dst={})",
            state.name, diff, src_count, dst_count
        );

        // Fetch exactly the new rows (LIMIT from the end of known range)
        let body = self
            .src
            .select_batch_raw(&state.name, dst_count, diff as usize + 1)
            .await?;

        let line_count = count_jsonl_lines(&body);
        if line_count == 0 {
            return Ok(0);
        }

        self.dst.insert_jsonl(&state.name, body).await?;

        // Save updated row count as checkpoint
        {
            let mut cp = self.checkpoint.lock().await;
            cp.update_synced_rows(
                &state.name,
                dst_count + line_count as u64,
                &self.config.checkpoint_path,
            )?;
        }

        Ok(line_count)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct TableState {
    name: String,
    wm_kind: WatermarkKind,
    #[allow(dead_code)]
    wm_col: String,
    wm_val: String,
}

/// Return the initial watermark placeholder value based on kind.
pub(crate) fn watermark_initial_value(kind: &WatermarkKind) -> (String, String) {
    match kind {
        WatermarkKind::DateTime(col) => (col.clone(), "1970-01-01 00:00:00".to_string()),
        WatermarkKind::UInt64(col) => (col.clone(), "0".to_string()),
        WatermarkKind::None => (String::new(), String::new()),
    }
}

/// Count non-empty lines in a JSONEachRow body.
pub(crate) fn count_jsonl_lines(body: &str) -> usize {
    body.lines().filter(|l| !l.trim().is_empty()).count()
}

/// Find the maximum value of `col` across all JSONL rows.
/// For datetime columns, values are strings; for uint they are numbers.
pub(crate) fn max_watermark_in_jsonl(body: &str, col: &str, is_datetime: bool) -> Option<String> {
    let mut max_val: Option<String> = None;

    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(field) = v.get(col) {
            let s = match field {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => continue,
            };
            max_val = Some(match max_val {
                None => s,
                Some(prev) => {
                    if is_datetime {
                        if s > prev {
                            s
                        } else {
                            prev
                        }
                    } else {
                        // Numeric comparison
                        let prev_n: u128 = prev.parse().unwrap_or(0);
                        let s_n: u128 = s.parse().unwrap_or(0);
                        if s_n > prev_n {
                            s
                        } else {
                            prev
                        }
                    }
                }
            });
        }
    }

    max_val
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // count_jsonl_lines
    // -----------------------------------------------------------------------

    #[test]
    fn count_empty_string() {
        assert_eq!(count_jsonl_lines(""), 0);
    }

    #[test]
    fn count_only_whitespace() {
        assert_eq!(count_jsonl_lines("   \n  \n\t\n"), 0);
    }

    #[test]
    fn count_single_row() {
        assert_eq!(count_jsonl_lines(r#"{"id":1}"#), 1);
    }

    #[test]
    fn count_multiple_rows() {
        let body = "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n";
        assert_eq!(count_jsonl_lines(body), 3);
    }

    #[test]
    fn count_rows_with_trailing_blank_lines() {
        let body = "{\"id\":1}\n{\"id\":2}\n\n\n";
        assert_eq!(count_jsonl_lines(body), 2);
    }

    #[test]
    fn count_rows_with_leading_blank_lines() {
        let body = "\n\n{\"id\":1}\n{\"id\":2}";
        assert_eq!(count_jsonl_lines(body), 2);
    }

    // -----------------------------------------------------------------------
    // max_watermark_in_jsonl — datetime (string lexicographic comparison)
    // -----------------------------------------------------------------------

    #[test]
    fn watermark_datetime_returns_none_for_empty() {
        assert!(max_watermark_in_jsonl("", "updated_at", true).is_none());
    }

    #[test]
    fn watermark_datetime_returns_none_when_column_absent() {
        let body = "{\"id\":1}\n{\"id\":2}\n";
        assert!(max_watermark_in_jsonl(body, "updated_at", true).is_none());
    }

    #[test]
    fn watermark_datetime_single_row() {
        let body = r#"{"id":1,"updated_at":"2024-01-15 10:00:00"}"#;
        assert_eq!(
            max_watermark_in_jsonl(body, "updated_at", true),
            Some("2024-01-15 10:00:00".to_string())
        );
    }

    #[test]
    fn watermark_datetime_picks_maximum() {
        let body = [
            r#"{"id":1,"updated_at":"2024-01-10 00:00:00"}"#,
            r#"{"id":2,"updated_at":"2024-03-01 12:00:00"}"#,
            r#"{"id":3,"updated_at":"2024-02-20 08:30:00"}"#,
        ]
        .join("\n");
        assert_eq!(
            max_watermark_in_jsonl(&body, "updated_at", true),
            Some("2024-03-01 12:00:00".to_string())
        );
    }

    #[test]
    fn watermark_datetime_identical_values() {
        let body = [
            r#"{"id":1,"ts":"2024-01-01 00:00:00"}"#,
            r#"{"id":2,"ts":"2024-01-01 00:00:00"}"#,
        ]
        .join("\n");
        assert_eq!(
            max_watermark_in_jsonl(&body, "ts", true),
            Some("2024-01-01 00:00:00".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // max_watermark_in_jsonl — uint (numeric comparison)
    // -----------------------------------------------------------------------

    #[test]
    fn watermark_uint_single_row() {
        let body = r#"{"id":42,"version":7}"#;
        assert_eq!(
            max_watermark_in_jsonl(body, "version", false),
            Some("7".to_string())
        );
    }

    #[test]
    fn watermark_uint_picks_maximum() {
        let body = [
            r#"{"id":1,"version":100}"#,
            r#"{"id":2,"version":999}"#,
            r#"{"id":3,"version":500}"#,
        ]
        .join("\n");
        assert_eq!(
            max_watermark_in_jsonl(&body, "version", false),
            Some("999".to_string())
        );
    }

    #[test]
    fn watermark_uint_numeric_not_lexicographic() {
        // "9" > "10" lexicographically but 10 > 9 numerically
        let body = [
            r#"{"v":9}"#,
            r#"{"v":10}"#,
        ]
        .join("\n");
        assert_eq!(
            max_watermark_in_jsonl(&body, "v", false),
            Some("10".to_string())
        );
    }

    #[test]
    fn watermark_uint_large_values() {
        let body = [
            r#"{"v":18446744073709551615}"#,
            r#"{"v":1}"#,
        ]
        .join("\n");
        assert_eq!(
            max_watermark_in_jsonl(&body, "v", false),
            Some("18446744073709551615".to_string())
        );
    }

    #[test]
    fn watermark_skips_invalid_json_lines() {
        let body = "not-json\n{\"v\":5}\n{\"v\":3}\n";
        assert_eq!(
            max_watermark_in_jsonl(body, "v", false),
            Some("5".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // watermark_initial_value
    // -----------------------------------------------------------------------

    #[test]
    fn initial_value_datetime() {
        let (col, val) =
            watermark_initial_value(&WatermarkKind::DateTime("updated_at".into()));
        assert_eq!(col, "updated_at");
        assert_eq!(val, "1970-01-01 00:00:00");
    }

    #[test]
    fn initial_value_uint() {
        let (col, val) =
            watermark_initial_value(&WatermarkKind::UInt64("version".into()));
        assert_eq!(col, "version");
        assert_eq!(val, "0");
    }

    #[test]
    fn initial_value_none() {
        let (col, val) = watermark_initial_value(&WatermarkKind::None);
        assert_eq!(col, "");
        assert_eq!(val, "");
    }
}
