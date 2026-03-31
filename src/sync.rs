use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::checkpoint::Checkpoint;
use crate::client::ClickHouseClient;
use crate::config::Config;
use crate::error::{ReplicatorError, Result};
use crate::schema::TableInfo;

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

    /// Run initial sync for all tables in `tables`, using up to `config.threads` workers.
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

        // Bounded semaphore for parallelism
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.threads));
        let mut handles = Vec::new();

        for table in pending {
            if self.cancel.is_cancelled() {
                break;
            }

            let sem = semaphore.clone();
            let config = self.config.clone();
            let src = self.src.clone();
            let dst = self.dst.clone();
            let checkpoint = self.checkpoint.clone();
            let cancel = self.cancel.clone();
            let table_name = table.name.clone();
            let table_name_for_task = table_name.clone();

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                if cancel.is_cancelled() {
                    return Ok(());
                }
                sync_table(config, src, dst, checkpoint, cancel, &table_name_for_task).await
            });
            handles.push((table_name, handle));
        }

        let mut had_error = false;
        for (name, handle) in handles {
            match handle.await {
                Ok(Ok(())) => info!("Table '{}' sync complete", name),
                Ok(Err(ReplicatorError::Cancelled)) => {
                    info!("Table '{}' sync cancelled", name);
                }
                Ok(Err(e)) => {
                    error!("Table '{}' sync failed: {}", name, e);
                    had_error = true;
                }
                Err(e) => {
                    error!("Table '{}' task panicked: {}", name, e);
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
}

/// Sync a single table from source to destination.
async fn sync_table(
    config: Arc<Config>,
    src: Arc<ClickHouseClient>,
    dst: Arc<ClickHouseClient>,
    checkpoint: Arc<Mutex<Checkpoint>>,
    cancel: CancellationToken,
    table: &str,
) -> Result<()> {
    info!("Syncing table '{}'", table);

    // Determine starting offset from checkpoint
    let start_offset = {
        let cp = checkpoint.lock().await;
        cp.synced_rows(table)
    };

    // Total rows on source
    let src_count = src.count(table).await.unwrap_or(0);
    info!(
        "Table '{}': source has {} rows, resuming from offset {}",
        table, src_count, start_offset
    );

    if src_count == 0 {
        let mut cp = checkpoint.lock().await;
        cp.mark_initial_sync_complete(table, 0, &config.checkpoint_path)?;
        info!("Table '{}' is empty — marked as synced", table);
        return Ok(());
    }

    let batch_size = config.batch_size;
    let mut offset = start_offset;

    loop {
        if cancel.is_cancelled() {
            return Err(ReplicatorError::Cancelled);
        }

        // Fetch batch from source
        let body = src.select_batch_raw(table, offset, batch_size).await?;
        let line_count = count_jsonl_lines(&body);

        if line_count == 0 {
            // No more rows
            break;
        }

        // Insert into destination
        dst.insert_jsonl(table, body).await?;
        offset += line_count as u64;

        // Checkpoint progress
        {
            let mut cp = checkpoint.lock().await;
            cp.update_synced_rows(table, offset, &config.checkpoint_path)?;
        }

        info!(
            "Table '{}': synced {} / {} rows",
            table, offset, src_count
        );

        if line_count < batch_size {
            // Last batch
            break;
        }
    }

    // Verify row counts match (best-effort)
    let dst_count = dst.count(table).await.unwrap_or(0);
    if dst_count < offset {
        warn!(
            "Table '{}': target has {} rows but we inserted up to {} — possible dedup on target",
            table, dst_count, offset
        );
    }

    let mut cp = checkpoint.lock().await;
    cp.mark_initial_sync_complete(table, offset, &config.checkpoint_path)?;
    info!("Table '{}': initial sync complete ({} rows)", table, offset);
    Ok(())
}

/// Count non-empty lines in a JSONEachRow response body.
fn count_jsonl_lines(body: &str) -> usize {
    body.lines().filter(|l| !l.trim().is_empty()).count()
}
