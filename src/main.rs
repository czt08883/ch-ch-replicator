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

    /// Comma-separated list of tables to replicate (whitelist).
    /// If set, only these tables are replicated. Takes priority over --exclude.
    #[arg(long, value_delimiter = ',')]
    include: Option<Vec<String>>,

    /// Comma-separated list of tables to skip (blacklist).
    /// Applied after --include filtering.
    #[arg(long, value_delimiter = ',')]
    exclude: Option<Vec<String>>,
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
    // Note: --include/--exclude not included in proctitle (not sensitive, but keeps it short)

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
    let include_tables = cli.include.unwrap_or_default();
    let exclude_tables = cli.exclude.unwrap_or_default();
    let config = Arc::new(Config::new(&cli.src, &cli.dest, threads, cli.batch, include_tables, exclude_tables)?);

    if !config.include_tables.is_empty() {
        info!("  include:     {}", config.include_tables.join(", "));
    }
    if !config.exclude_tables.is_empty() {
        info!("  exclude:     {}", config.exclude_tables.join(", "));
    }

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
    let all_tables = list_tables(&src_client).await?;

    if all_tables.is_empty() {
        warn!("No tables found in source database '{}' — nothing to replicate", config.source.database);
        return Ok(());
    }

    // Apply --include / --exclude filters
    let tables = filter_tables(all_tables, &config.include_tables, &config.exclude_tables);

    if tables.is_empty() {
        warn!("All tables were filtered out — nothing to replicate");
        return Ok(());
    }
    info!("Replicating {} table(s)", tables.len());
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

/// Filter a list of tables according to include/exclude lists.
///
/// Algorithm:
/// 1. If `include` is non-empty, keep only tables whose name is in the list.
/// 2. Remove any table whose name is in `exclude`.
///
/// Both lists use exact, case-sensitive matching.
fn filter_tables(
    tables: Vec<crate::schema::TableInfo>,
    include: &[String],
    exclude: &[String],
) -> Vec<crate::schema::TableInfo> {
    let filtered: Vec<crate::schema::TableInfo> = tables
        .into_iter()
        .filter(|t| {
            // Step 1: include filter
            if !include.is_empty() && !include.contains(&t.name) {
                info!("  skipping table '{}' (not in --include list)", t.name);
                return false;
            }
            // Step 2: exclude filter
            if exclude.contains(&t.name) {
                info!("  skipping table '{}' (in --exclude list)", t.name);
                return false;
            }
            true
        })
        .collect();
    filtered
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

#[cfg(test)]
mod filter_tests {
    use super::*;
    use crate::schema::TableInfo;

    fn make_tables(names: &[&str]) -> Vec<TableInfo> {
        names.iter().map(|n| TableInfo {
            name: n.to_string(),
            engine: "MergeTree".to_string(),
            sorting_key: String::new(),
        }).collect()
    }

    fn names(tables: &[TableInfo]) -> Vec<&str> {
        tables.iter().map(|t| t.name.as_str()).collect()
    }

    #[test]
    fn no_filters_returns_all() {
        let tables = make_tables(&["a", "b", "c"]);
        let result = filter_tables(tables, &[], &[]);
        assert_eq!(names(&result), vec!["a", "b", "c"]);
    }

    #[test]
    fn include_keeps_only_listed() {
        let tables = make_tables(&["a", "b", "c"]);
        let include = vec!["a".to_string(), "c".to_string()];
        let result = filter_tables(tables, &include, &[]);
        assert_eq!(names(&result), vec!["a", "c"]);
    }

    #[test]
    fn exclude_removes_listed() {
        let tables = make_tables(&["a", "b", "c"]);
        let exclude = vec!["b".to_string()];
        let result = filter_tables(tables, &[], &exclude);
        assert_eq!(names(&result), vec!["a", "c"]);
    }

    #[test]
    fn include_then_exclude_applied_in_order() {
        // Include a,b,c then exclude b → result: a, c
        let tables = make_tables(&["a", "b", "c", "d"]);
        let include = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let exclude = vec!["b".to_string()];
        let result = filter_tables(tables, &include, &exclude);
        assert_eq!(names(&result), vec!["a", "c"]);
    }

    #[test]
    fn include_unknown_name_yields_empty() {
        let tables = make_tables(&["a", "b"]);
        let include = vec!["nonexistent".to_string()];
        let result = filter_tables(tables, &include, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn include_and_exclude_same_table_exclude_wins() {
        // Include a,b and also exclude a → only b remains
        let tables = make_tables(&["a", "b", "c"]);
        let include = vec!["a".to_string(), "b".to_string()];
        let exclude = vec!["a".to_string()];
        let result = filter_tables(tables, &include, &exclude);
        assert_eq!(names(&result), vec!["b"]);
    }

    #[test]
    fn matching_is_case_sensitive() {
        let tables = make_tables(&["Events", "events"]);
        let include = vec!["events".to_string()];
        let result = filter_tables(tables, &include, &[]);
        assert_eq!(names(&result), vec!["events"]);
    }

    #[test]
    fn exclude_all_yields_empty() {
        let tables = make_tables(&["a", "b"]);
        let exclude = vec!["a".to_string(), "b".to_string()];
        let result = filter_tables(tables, &[], &exclude);
        assert!(result.is_empty());
    }
}
