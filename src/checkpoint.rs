use crate::error::{ReplicatorError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info};

/// Per-table synchronization state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableCheckpoint {
    /// Number of rows confirmed synced on the target (used for initial sync progress).
    pub synced_rows: u64,
    /// True once the initial full-table copy is complete.
    pub initial_sync_complete: bool,
    /// The last CDC watermark value (stringified). Empty = not yet set.
    pub cdc_watermark: String,
    /// Name of the watermark column (e.g. "updated_at"). Empty = none.
    pub watermark_column: String,
}

/// The full checkpoint file written to disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub tables: HashMap<String, TableCheckpoint>,
}

impl Checkpoint {
    /// Load checkpoint from disk. Returns an empty checkpoint if the file doesn't exist.
    pub fn load(path: &str) -> Result<Self> {
        let p = Path::new(path);
        if !p.exists() {
            info!("No checkpoint file found at '{}', starting fresh", path);
            return Ok(Self::default());
        }
        let data = std::fs::read_to_string(p)?;
        let cp: Self = serde_json::from_str(&data)
            .map_err(|e| ReplicatorError::Checkpoint(format!("parse error: {}", e)))?;
        info!("Loaded checkpoint from '{}' ({} tables)", path, cp.tables.len());
        Ok(cp)
    }

    /// Persist checkpoint to disk atomically (write-to-temp + rename).
    pub fn save(&self, path: &str) -> Result<()> {
        let tmp = format!("{}.tmp", path);
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, path)?;
        debug!("Checkpoint saved to '{}'", path);
        Ok(())
    }

    /// Get mutable access to a table's checkpoint, creating a default entry if absent.
    pub fn table_mut(&mut self, table: &str) -> &mut TableCheckpoint {
        self.tables.entry(table.to_string()).or_default()
    }

    /// Get a read-only snapshot of a table's checkpoint.
    pub fn table(&self, table: &str) -> Option<&TableCheckpoint> {
        self.tables.get(table)
    }

    /// Mark initial sync as complete for a table and update synced_rows.
    pub fn mark_initial_sync_complete(&mut self, table: &str, rows: u64, path: &str) -> Result<()> {
        let tc = self.table_mut(table);
        tc.synced_rows = rows;
        tc.initial_sync_complete = true;
        self.save(path)
    }

    /// Update synced_rows (partial progress) and save.
    pub fn update_synced_rows(&mut self, table: &str, rows: u64, path: &str) -> Result<()> {
        let tc = self.table_mut(table);
        tc.synced_rows = rows;
        self.save(path)
    }

    /// Update the CDC watermark for a table and save.
    pub fn update_watermark(
        &mut self,
        table: &str,
        column: &str,
        value: &str,
        path: &str,
    ) -> Result<()> {
        let tc = self.table_mut(table);
        tc.watermark_column = column.to_string();
        tc.cdc_watermark = value.to_string();
        self.save(path)
    }

    /// Returns true if the initial sync is already complete for this table.
    pub fn is_initial_sync_complete(&self, table: &str) -> bool {
        self.tables
            .get(table)
            .map(|t| t.initial_sync_complete)
            .unwrap_or(false)
    }

    /// Returns the last synced row offset for a table (for resuming initial sync).
    pub fn synced_rows(&self, table: &str) -> u64 {
        self.tables
            .get(table)
            .map(|t| t.synced_rows)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Write checkpoint to a temp file, return its path as a String.
    fn tmp_path() -> String {
        let dir = std::env::temp_dir();
        let name = format!("ch_replicator_test_{}.json", uuid_simple());
        dir.join(name).to_string_lossy().into_owned()
    }

    // Tiny deterministic "uuid" using timestamp nanos to avoid a dependency.
    fn uuid_simple() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{}{}", t.as_secs(), t.subsec_nanos())
    }

    // -----------------------------------------------------------------------
    // load / save round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn load_returns_empty_when_file_missing() {
        let cp = Checkpoint::load("/tmp/does_not_exist_xyzzy_12345.json").unwrap();
        assert!(cp.tables.is_empty());
    }

    #[test]
    fn save_then_load_round_trip() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.mark_initial_sync_complete("events", 1000, &path).unwrap();

        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.is_initial_sync_complete("events"));
        assert_eq!(loaded.synced_rows("events"), 1000);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn save_is_atomic_temp_file_cleaned_up() {
        let path = tmp_path();
        let tmp = format!("{}.tmp", path);
        let mut cp = Checkpoint::default();
        cp.update_synced_rows("t1", 42, &path).unwrap();

        // The .tmp file must not persist after a successful save
        assert!(!std::path::Path::new(&tmp).exists());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_rejects_corrupt_json() {
        let path = tmp_path();
        std::fs::write(&path, b"not json {{{").unwrap();
        let err = Checkpoint::load(&path).unwrap_err();
        assert!(matches!(err, crate::error::ReplicatorError::Checkpoint(_)));
        std::fs::remove_file(&path).ok();
    }

    // -----------------------------------------------------------------------
    // is_initial_sync_complete
    // -----------------------------------------------------------------------

    #[test]
    fn initial_sync_complete_false_for_unknown_table() {
        let cp = Checkpoint::default();
        assert!(!cp.is_initial_sync_complete("nonexistent"));
    }

    #[test]
    fn initial_sync_complete_false_before_marking() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_synced_rows("t", 500, &path).unwrap();
        assert!(!cp.is_initial_sync_complete("t"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn initial_sync_complete_true_after_marking() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.mark_initial_sync_complete("orders", 9999, &path).unwrap();
        assert!(cp.is_initial_sync_complete("orders"));
        std::fs::remove_file(&path).ok();
    }

    // -----------------------------------------------------------------------
    // synced_rows
    // -----------------------------------------------------------------------

    #[test]
    fn synced_rows_zero_for_unknown_table() {
        let cp = Checkpoint::default();
        assert_eq!(cp.synced_rows("no_such_table"), 0);
    }

    #[test]
    fn synced_rows_reflects_update() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_synced_rows("hits", 12_345, &path).unwrap();
        assert_eq!(cp.synced_rows("hits"), 12_345);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn synced_rows_can_be_overwritten() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_synced_rows("hits", 100, &path).unwrap();
        cp.update_synced_rows("hits", 200, &path).unwrap();
        assert_eq!(cp.synced_rows("hits"), 200);
        std::fs::remove_file(&path).ok();
    }

    // -----------------------------------------------------------------------
    // update_watermark
    // -----------------------------------------------------------------------

    #[test]
    fn watermark_stored_and_retrieved() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_watermark("events", "updated_at", "2024-06-01 00:00:00", &path)
            .unwrap();
        let tc = cp.table("events").unwrap();
        assert_eq!(tc.watermark_column, "updated_at");
        assert_eq!(tc.cdc_watermark, "2024-06-01 00:00:00");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn watermark_overwritten_on_second_call() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_watermark("t", "ts", "2024-01-01 00:00:00", &path).unwrap();
        cp.update_watermark("t", "ts", "2024-12-31 23:59:59", &path).unwrap();
        assert_eq!(cp.table("t").unwrap().cdc_watermark, "2024-12-31 23:59:59");
        std::fs::remove_file(&path).ok();
    }

    // -----------------------------------------------------------------------
    // Multiple tables are independent
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_tables_independent() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.update_synced_rows("a", 10, &path).unwrap();
        cp.update_synced_rows("b", 20, &path).unwrap();
        cp.mark_initial_sync_complete("a", 10, &path).unwrap();

        assert!(cp.is_initial_sync_complete("a"));
        assert!(!cp.is_initial_sync_complete("b"));
        assert_eq!(cp.synced_rows("a"), 10);
        assert_eq!(cp.synced_rows("b"), 20);
        std::fs::remove_file(&path).ok();
    }

    // -----------------------------------------------------------------------
    // Persistence: reload preserves all fields
    // -----------------------------------------------------------------------

    #[test]
    fn reload_preserves_watermark_and_sync_state() {
        let path = tmp_path();
        let mut cp = Checkpoint::default();
        cp.mark_initial_sync_complete("logs", 5000, &path).unwrap();
        cp.update_watermark("logs", "event_time", "2025-01-01 12:00:00", &path)
            .unwrap();

        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.is_initial_sync_complete("logs"));
        assert_eq!(loaded.synced_rows("logs"), 5000);
        let tc = loaded.table("logs").unwrap();
        assert_eq!(tc.watermark_column, "event_time");
        assert_eq!(tc.cdc_watermark, "2025-01-01 12:00:00");

        std::fs::remove_file(&path).ok();
    }
}
