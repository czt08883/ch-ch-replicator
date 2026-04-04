use crate::error::{ReplicatorError, Result};
use url::Url;

/// Parsed ClickHouse connection configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub user: String,
    pub password: String,
    #[allow(dead_code)]
    pub host: String,
    #[allow(dead_code)]
    pub port: u16,
    pub database: String,
    /// Base HTTP URL, e.g. http://host:port
    pub http_url: String,
}

impl ClickHouseConfig {
    /// Parse a DSN of the form:
    ///   clickhouse://user:password@host:port/database[?options]
    pub fn from_dsn(dsn: &str) -> Result<Self> {
        let url = Url::parse(dsn).map_err(|e| {
            ReplicatorError::DsnParse(format!("invalid URL '{}': {}", dsn, e))
        })?;

        if url.scheme() != "clickhouse" {
            return Err(ReplicatorError::DsnParse(format!(
                "expected scheme 'clickhouse', got '{}'",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| ReplicatorError::DsnParse("missing host".into()))?
            .to_string();

        let port = url
            .port()
            .ok_or_else(|| ReplicatorError::DsnParse("missing port".into()))?;

        let user = if url.username().is_empty() {
            return Err(ReplicatorError::DsnParse("missing username".into()));
        } else {
            url.username().to_string()
        };

        let password = url
            .password()
            .ok_or_else(|| ReplicatorError::DsnParse("missing password".into()))?;
        // Percent-decode the password (e.g. p%40ss -> p@ss)
        let password = percent_decode(password);

        // Strip leading '/' from path to get database name
        let database = url
            .path()
            .trim_start_matches('/')
            .to_string();

        if database.is_empty() {
            return Err(ReplicatorError::DsnParse("missing database in path".into()));
        }

        let http_url = format!("http://{}:{}", host, port);

        Ok(Self {
            user,
            password,
            host,
            port,
            database,
            http_url,
        })
    }
}

/// Percent-decode a string (e.g. `p%40ss` → `p@ss`).
/// Silently returns the original on malformed sequences.
fn percent_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (
                hex_val(bytes[i + 1]),
                hex_val(bytes[i + 2]),
            ) {
                out.push(char::from(h << 4 | l));
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Top-level application configuration derived from CLI arguments.
#[derive(Debug, Clone)]
pub struct Config {
    pub source: ClickHouseConfig,
    pub destination: ClickHouseConfig,
    pub threads: usize,
    /// Batch size for SELECT/INSERT during initial sync.
    pub batch_size: usize,
    /// CDC poll interval in seconds.
    pub cdc_poll_secs: u64,
    /// Path to the checkpoint file.
    pub checkpoint_path: String,
    /// If non-empty, only replicate tables whose names are in this list.
    pub include_tables: Vec<String>,
    /// Tables to exclude from replication (applied after `include_tables`).
    pub exclude_tables: Vec<String>,
}

impl Config {
    pub fn new(
        src_dsn: &str,
        dest_dsn: &str,
        threads: usize,
        batch_size: usize,
        include_tables: Vec<String>,
        exclude_tables: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            source: ClickHouseConfig::from_dsn(src_dsn)?,
            destination: ClickHouseConfig::from_dsn(dest_dsn)?,
            threads,
            batch_size,
            cdc_poll_secs: 5,
            checkpoint_path: "checkpoint.json".to_string(),
            include_tables,
            exclude_tables,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // DSN parsing — happy paths
    // -----------------------------------------------------------------------

    #[test]
    fn test_dsn_minimal() {
        let cfg = ClickHouseConfig::from_dsn(
            "clickhouse://default:secret@localhost:9000/mydb",
        )
        .unwrap();
        assert_eq!(cfg.user, "default");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 9000);
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.http_url, "http://localhost:9000");
    }

    #[test]
    fn test_dsn_with_query_options() {
        let cfg = ClickHouseConfig::from_dsn(
            "clickhouse://admin:pass@db.example.com:8123/analytics?compress=1&send_timeout=30",
        )
        .unwrap();
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 8123);
        assert_eq!(cfg.database, "analytics");
        // Options are not part of http_url
        assert_eq!(cfg.http_url, "http://db.example.com:8123");
    }

    #[test]
    fn test_dsn_special_chars_in_password() {
        // URL-encoded '@' in password
        let cfg = ClickHouseConfig::from_dsn(
            "clickhouse://user:p%40ss%21@127.0.0.1:8123/db",
        )
        .unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "p@ss!");
        assert_eq!(cfg.database, "db");
    }

    #[test]
    fn test_dsn_http_url_constructed_correctly() {
        let cfg = ClickHouseConfig::from_dsn(
            "clickhouse://u:p@clickhouse.internal:9001/events",
        )
        .unwrap();
        assert_eq!(cfg.http_url, "http://clickhouse.internal:9001");
    }

    // -----------------------------------------------------------------------
    // DSN parsing — error paths
    // -----------------------------------------------------------------------

    #[test]
    fn test_dsn_wrong_scheme() {
        let err = ClickHouseConfig::from_dsn("http://u:p@host:8123/db").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
        assert!(err.to_string().contains("clickhouse"));
    }

    #[test]
    fn test_dsn_not_a_url() {
        let err = ClickHouseConfig::from_dsn("not-a-url-at-all").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
    }

    #[test]
    fn test_dsn_missing_port() {
        let err =
            ClickHouseConfig::from_dsn("clickhouse://u:p@localhost/db").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn test_dsn_missing_password() {
        // No colon after username → no password component
        let err =
            ClickHouseConfig::from_dsn("clickhouse://user@localhost:8123/db").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
    }

    #[test]
    fn test_dsn_missing_database() {
        let err =
            ClickHouseConfig::from_dsn("clickhouse://u:p@localhost:8123/").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn test_dsn_missing_username() {
        let err =
            ClickHouseConfig::from_dsn("clickhouse://:pass@localhost:8123/db").unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
        assert!(err.to_string().contains("username"));
    }

    // -----------------------------------------------------------------------
    // Config construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = Config::new(
            "clickhouse://u:p@src:8123/src_db",
            "clickhouse://u:p@dst:8123/dst_db",
            3,
            300_000,
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(cfg.threads, 3);
        assert_eq!(cfg.batch_size, 300_000);
        assert_eq!(cfg.cdc_poll_secs, 5);
        assert_eq!(cfg.checkpoint_path, "checkpoint.json");
        assert_eq!(cfg.source.database, "src_db");
        assert_eq!(cfg.destination.database, "dst_db");
        assert!(cfg.include_tables.is_empty());
        assert!(cfg.exclude_tables.is_empty());
    }

    #[test]
    fn test_config_bad_src_dsn_propagates_error() {
        let err = Config::new("not-valid", "clickhouse://u:p@h:8123/db", 1, 300_000, vec![], vec![]).unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
    }

    #[test]
    fn test_config_bad_dst_dsn_propagates_error() {
        let err = Config::new("clickhouse://u:p@h:8123/db", "not-valid", 1, 300_000, vec![], vec![]).unwrap_err();
        assert!(matches!(err, ReplicatorError::DsnParse(_)));
    }
}
