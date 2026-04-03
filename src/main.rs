mod cdc;
mod checkpoint;
mod client;
mod config;
mod error;
mod schema;
mod sync;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::cdc::CdcEngine;
use crate::checkpoint::Checkpoint;
use crate::client::ClickHouseClient;
use crate::config::Config;
use crate::error::ReplicatorError;
use crate::schema::{apply_ddl, ensure_database, get_create_ddl, list_dictionaries, list_tables};
use crate::sync::InitialSync;

use clap::Parser;

/// Replicate one standalone ClickHouse database into another.
#[derive(Parser, Debug)]
#[command(
    name = "ch-ch-replicator",
    version,
    about = "Replicate a ClickHouse database to another ClickHouse instance"
)]
struct Cli {
    /// Source ClickHouse DSN.
    /// Format: clickhouse://user:password@host:port/database[?options]
    #[arg(long)]
    src: String,

    /// Destination ClickHouse DSN.
    /// Format: clickhouse://user:password@host:port/database[?options]
    #[arg(long)]
    dest: String,

    /// Number of parallel worker threads for initial sync.
    #[arg(long, default_value = "3")]
    threads: usize,

    /// Batch size for SELECT/INSERT during initial sync.
    #[arg(long, default_value = "300000")]
    batch: usize,
}

#[tokio::main]
async fn main() {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Overwrite argv in /proc/self/cmdline to hide passwords from `ps aux`.
    proctitle::set_title(format!(
        "ch-ch-replicator --src={} --dest={} --threads={} --batch={}",
        sanitize_dsn(&cli.src),
        sanitize_dsn(&cli.dest),
        cli.threads,
        cli.batch,
    ));

    if let Err(e) = run(cli).await {
        match e {
            ReplicatorError::Cancelled => {
                info!("Replicator shut down cleanly");
            }
            _ => {
                error!("Fatal error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

async fn run(cli: Cli) -> Result<(), ReplicatorError> {
    info!("ch-ch-replicator starting");
    info!("  source:      {}", sanitize_dsn(&cli.src));
    info!("  destination: {}", sanitize_dsn(&cli.dest));
    info!("  threads:     {}", cli.threads);
    info!("  batch:       {}", cli.batch);

    // Validate threads > 0
    let threads = if cli.threads == 0 { 1 } else { cli.threads };

    // Parse configuration
    let config = Arc::new(Config::new(&cli.src, &cli.dest, threads, cli.batch)?);

    // Build HTTP clients
    let src_client = Arc::new(ClickHouseClient::new(config.source.clone())?);
    let dst_client = Arc::new(ClickHouseClient::new(config.destination.clone())?);

    // Load or create checkpoint
    let checkpoint = Arc::new(Mutex::new(
        Checkpoint::load(&config.checkpoint_path)?,
    ));

    // Set up cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Spawn signal handler task
    let cancel_signal = cancel.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("Shutdown signal received — initiating graceful shutdown");
        cancel_signal.cancel();
    });

    // Phase 1: Connectivity check
    info!("Verifying connectivity to source and destination…");
    check_connectivity(&src_client, "source").await?;
    check_connectivity(&dst_client, "destination").await?;

    // Phase 2: Schema discovery
    info!("Discovering tables in source database '{}'…", config.source.database);
    let tables = list_tables(&src_client).await?;

    if tables.is_empty() {
        warn!("No tables found in source database '{}' — nothing to replicate", config.source.database);
        return Ok(());
    }
    info!("Found {} table(s) to replicate", tables.len());
    for t in &tables {
        info!("  - {} ({})", t.name, t.engine);
    }

    // Phase 3: Ensure target database and tables exist
    if cancel.is_cancelled() {
        return Err(ReplicatorError::Cancelled);
    }
    info!("Setting up target database and schema…");
    setup_target_schema(&src_client, &dst_client, &config, &tables).await?;

    // Phase 4: Initial sync
    if cancel.is_cancelled() {
        return Err(ReplicatorError::Cancelled);
    }
    info!("--- Phase: Initial Sync ---");
    let initial_sync = InitialSync::new(
        config.clone(),
        src_client.clone(),
        dst_client.clone(),
        checkpoint.clone(),
        cancel.clone(),
    );
    initial_sync.run(tables.clone()).await?;

    if cancel.is_cancelled() {
        return Err(ReplicatorError::Cancelled);
    }

    // Phase 5: CDC (runs indefinitely until cancelled)
    info!("--- Phase: CDC (Change Data Capture) ---");
    let cdc = CdcEngine::new(
        config.clone(),
        src_client.clone(),
        dst_client.clone(),
        checkpoint.clone(),
        cancel.clone(),
    );
    cdc.run(tables).await?;

    info!("Replicator finished");
    Ok(())
}

/// Verify that we can connect to a ClickHouse instance.
async fn check_connectivity(client: &ClickHouseClient, label: &str) -> Result<(), ReplicatorError> {
    match client.ping().await {
        Ok(()) => {
            info!("{} ClickHouse is reachable", label);
            Ok(())
        }
        Err(e) => {
            error!("Cannot connect to {} ClickHouse: {}", label, e);
            Err(e)
        }
    }
}

/// Create the target database and apply DDL for all tables (idempotent).
async fn setup_target_schema(
    src: &ClickHouseClient,
    dst: &ClickHouseClient,
    config: &Config,
    tables: &[crate::schema::TableInfo],
) -> Result<(), ReplicatorError> {
    ensure_database(dst, &config.destination.database).await?;

    for table in tables {
        info!("Ensuring DDL for table '{}' on target", table.name);
        let ddl = get_create_ddl(src, &table.name, &config.destination.database).await?;
        apply_ddl(dst, &ddl).await?;
    }

    let dictionaries = list_dictionaries(src).await?;
    for dict in &dictionaries {
        info!("Ensuring DDL for dictionary '{}' on target", dict);
        let ddl = get_create_ddl(src, dict, &config.destination.database).await?;
        apply_ddl(dst, &ddl).await?;
    }
    if !dictionaries.is_empty() {
        info!("{} dictionary/dictionaries set up (data loaded from their own SOURCE)", dictionaries.len());
    }

    info!("Target schema setup complete");
    Ok(())
}

/// Wait for SIGINT (Ctrl-C) or SIGTERM.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT (Ctrl-C)");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
        info!("Received Ctrl-C");
    }
}

/// Replace password in DSN for logging.
fn sanitize_dsn(dsn: &str) -> String {
    // Replace everything between ':' and '@' in the authority section
    if let Ok(mut url) = url::Url::parse(dsn) {
        let _ = url.set_password(Some("*****"));
        url.to_string()
    } else {
        "<invalid DSN>".to_string()
    }
}
