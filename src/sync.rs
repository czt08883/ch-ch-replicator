use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::checkpoint::Checkpoint;
use crate::client::ClickHouseClient;
use crate::config::Config;
use crate::error::{ReplicatorError, Result};
use crate::schema::TableInfo;

const MAX_RETRIES: u32 = 10;
const RETRY_DELAY: Duration = Duration::from_secs(10);

/// Initial sync controller: copies all rows from source to target using parallel workers.
pub struct InitialSync {
    config: Arc<Config>,
    src: Arc<ClickHouseClient>,
    dst: Arc<ClickHouseClient>,
    checkpoint: Arc<Mutex<Checkpoint>>,
    cancel: CancellationToken,
}

impl InitialSync {
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

    /// Run initial sync for all tables, using up to `config.threads` workers per table.
    pub async fn run(&self, tables: Vec<TableInfo>) -> Result<()> {
        info!(
            "Starting initial sync for {} table(s) with {} thread(s)",
            tables.len(),
            self.config.threads
        );

        // Filter out tables already fully synced
        let pending: Vec<TableInfo> = {
            let cp = self.checkpoint.lock().await;
            tables
                .into_iter()
                .filter(|t| !cp.is_initial_sync_complete(&t.name))
                .collect()
        };

        if pending.is_empty() {
            info!("All tables already fully synced — skipping initial sync phase");
            return Ok(());
        }

        info!("{} table(s) need initial sync", pending.len());

        let mut had_error = false;
        for table in pending {
            if self.cancel.is_cancelled() {
                break;
            }
            match self.sync_table(&table).await {
                Ok(()) => info!("Table '{}' sync complete", table.name),
                Err(ReplicatorError::Cancelled) => {
                    info!("Table '{}' sync cancelled", table.name);
                    break;
                }
                Err(e) => {
                    error!("Table '{}' sync failed: {}", table.name, e);
                    had_error = true;
                }
            }
        }

        if had_error {
            return Err(ReplicatorError::Sync(
                "one or more tables failed during initial sync".into(),
            ));
        }

        Ok(())
    }

    /// Sync a single table using `config.threads` parallel workers.
    ///
    /// Workers claim batch offsets from a shared atomic counter and process them
    /// independently.  A [`BatchTracker`] records completed offsets and computes
    /// the contiguous frontier — the safe resume point for checkpointing.
    async fn sync_table(&self, table: &TableInfo) -> Result<()> {
        info!("Syncing table '{}'", table.name);

        let start_offset = {
            let cp = self.checkpoint.lock().await;
            cp.synced_rows(&table.name)
        };

        let src_count = self.src.count(&table.name).await.unwrap_or(0);
        info!(
            "Table '{}': source has {} rows, resuming from offset {}",
            table.name, src_count, start_offset
        );

        if src_count == 0 {
            let mut cp = self.checkpoint.lock().await;
            cp.mark_initial_sync_complete(&table.name, 0, &self.config.checkpoint_path)?;
            info!("Table '{}' is empty — marked as synced", table.name);
            return Ok(());
        }

        let batch_size = self.config.batch_size;
        let threads = self.config.threads;

        // Shared state across workers
        let next_offset = Arc::new(AtomicU64::new(start_offset));
        let tracker = Arc::new(Mutex::new(BatchTracker::new(start_offset, batch_size as u64)));
        let rows_inserted = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..threads {
            let next_offset = next_offset.clone();
            let tracker = tracker.clone();
            let rows_inserted = rows_inserted.clone();
            let src = self.src.clone();
            let dst = self.dst.clone();
            let checkpoint = self.checkpoint.clone();
            let cancel = self.cancel.clone();
            let config = self.config.clone();
            let table_name = table.name.clone();
            let sorting_key = table.sorting_key.clone();

            handles.push(tokio::spawn(async move {
                loop {
                    if cancel.is_cancelled() {
                        return Err(ReplicatorError::Cancelled);
                    }

                    let offset = next_offset.fetch_add(batch_size as u64, Ordering::SeqCst);
                    if offset >= src_count {
                        break;
                    }

                    let mut line_count = 0usize;
                    let mut last_err: Option<ReplicatorError> = None;
                    for attempt in 1..=MAX_RETRIES {
                        if cancel.is_cancelled() {
                            return Err(ReplicatorError::Cancelled);
                        }
                        match async {
                            let body = src
                                .select_batch_raw(&table_name, offset, batch_size, &sorting_key, "")
                                .await?;
                            let lines = count_jsonl_lines(&body);
                            if lines > 0 {
                                dst.insert_jsonl(&table_name, body).await?;
                            }
                            Ok::<usize, ReplicatorError>(lines)
                        }
                        .await
                        {
                            Ok(lines) => {
                                line_count = lines;
                                last_err = None;
                                break;
                            }
                            Err(e) => {
                                if attempt < MAX_RETRIES {
                                    warn!(
                                        "Table '{}' batch at offset {} failed (attempt {}/{}): {}. Retrying in {}s...",
                                        table_name, offset, attempt, MAX_RETRIES, e, RETRY_DELAY.as_secs()
                                    );
                                    tokio::select! {
                                        _ = cancel.cancelled() => return Err(ReplicatorError::Cancelled),
                                        _ = tokio::time::sleep(RETRY_DELAY) => {}
                                    }
                                } else {
                                    error!(
                                        "Table '{}' batch at offset {} failed after {} attempts: {}",
                                        table_name, offset, MAX_RETRIES, e
                                    );
                                }
                                last_err = Some(e);
                            }
                        }
                    }
                    if let Some(e) = last_err {
                        return Err(e);
                    }

                    if line_count == 0 {
                        break;
                    }

                    let inserted = rows_inserted.fetch_add(line_count as u64, Ordering::Relaxed)
                        + line_count as u64;

                    // Advance the checkpoint to the contiguous frontier
                    let frontier = {
                        let mut t = tracker.lock().await;
                        t.mark_complete(offset)
                    };
                    {
                        let mut cp = checkpoint.lock().await;
                        cp.update_synced_rows(
                            &table_name,
                            frontier,
                            &config.checkpoint_path,
                        )?;
                    }

                    info!(
                        "Table '{}': synced ~{} / {} rows",
                        table_name,
                        start_offset + inserted,
                        src_count
                    );

                    if line_count < batch_size {
                        break;
                    }
                }
                Ok(())
            }));
        }

        // Wait for all workers
        let mut had_error = false;
        let mut was_cancelled = false;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(ReplicatorError::Cancelled)) => was_cancelled = true,
                Ok(Err(e)) => {
                    error!("Table '{}' worker error: {}", table.name, e);
                    had_error = true;
                }
                Err(e) => {
                    error!("Table '{}' worker panicked: {}", table.name, e);
                    had_error = true;
                }
            }
        }

        if was_cancelled {
            return Err(ReplicatorError::Cancelled);
        }
        if had_error {
            return Err(ReplicatorError::Sync(format!(
                "one or more workers failed for table '{}'",
                table.name
            )));
        }

        // Verify row counts and mark complete
        let total = start_offset + rows_inserted.load(Ordering::Relaxed);
        let dst_count = self.dst.count(&table.name).await.unwrap_or(0);
        if dst_count < total {
            warn!(
                "Table '{}': target has {} rows but we synced {} — possible dedup on target",
                table.name, dst_count, total
            );
        }

        let mut cp = self.checkpoint.lock().await;
        cp.mark_initial_sync_complete(&table.name, total, &self.config.checkpoint_path)?;
        info!(
            "Table '{}': initial sync complete ({} rows)",
            table.name, total
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BatchTracker
// ---------------------------------------------------------------------------

/// Tracks completed batch offsets to compute the contiguous checkpoint frontier.
///
/// When multiple workers process batches in parallel, they may complete out of
/// order.  The *frontier* is the lowest offset where all preceding batches are
/// done — the safe point to resume from if the process restarts.
struct BatchTracker {
    /// Batch offsets completed ahead of the frontier (out-of-order).
    completed: BTreeSet<u64>,
    /// Everything below this offset has been synced contiguously.
    frontier: u64,
    batch_size: u64,
}

impl BatchTracker {
    fn new(start_offset: u64, batch_size: u64) -> Self {
        Self {
            completed: BTreeSet::new(),
            frontier: start_offset,
            batch_size,
        }
    }

    /// Mark the batch starting at `offset` as complete.
    /// Returns the new contiguous frontier (safe resume offset).
    fn mark_complete(&mut self, offset: u64) -> u64 {
        if offset == self.frontier {
            // Extends the frontier directly
            self.frontier += self.batch_size;
            // Drain any previously-completed batches that now form a contiguous run
            while self.completed.remove(&self.frontier) {
                self.frontier += self.batch_size;
            }
        } else if offset > self.frontier {
            // Out-of-order completion — remember for later
            self.completed.insert(offset);
        }
        // offset < frontier is a duplicate; ignore silently
        self.frontier
    }
}

/// Count non-empty lines in a JSONEachRow response body.
fn count_jsonl_lines(body: &str) -> usize {
    body.lines().filter(|l| !l.trim().is_empty()).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // BatchTracker
    // -----------------------------------------------------------------------

    #[test]
    fn tracker_in_order() {
        let mut t = BatchTracker::new(0, 100);
        assert_eq!(t.mark_complete(0), 100);
        assert_eq!(t.mark_complete(100), 200);
        assert_eq!(t.mark_complete(200), 300);
    }

    #[test]
    fn tracker_out_of_order_catches_up() {
        let mut t = BatchTracker::new(0, 100);
        assert_eq!(t.mark_complete(200), 0);   // gap at 0, 100
        assert_eq!(t.mark_complete(100), 0);   // gap still at 0
        assert_eq!(t.mark_complete(0), 300);   // fills gap → all contiguous
    }

    #[test]
    fn tracker_with_start_offset() {
        let mut t = BatchTracker::new(500, 100);
        assert_eq!(t.mark_complete(600), 500); // gap at 500
        assert_eq!(t.mark_complete(500), 700); // fills gap
    }

    #[test]
    fn tracker_duplicate_ignored() {
        let mut t = BatchTracker::new(0, 100);
        assert_eq!(t.mark_complete(0), 100);
        assert_eq!(t.mark_complete(0), 100);   // duplicate, no change
    }

    #[test]
    fn tracker_single_batch() {
        let mut t = BatchTracker::new(0, 50000);
        assert_eq!(t.mark_complete(0), 50000);
    }

    #[test]
    fn tracker_reverse_order() {
        let mut t = BatchTracker::new(0, 10);
        assert_eq!(t.mark_complete(40), 0);
        assert_eq!(t.mark_complete(30), 0);
        assert_eq!(t.mark_complete(20), 0);
        assert_eq!(t.mark_complete(10), 0);
        assert_eq!(t.mark_complete(0), 50);
    }
}
