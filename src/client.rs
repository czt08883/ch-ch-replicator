use crate::config::ClickHouseConfig;
use crate::error::{ReplicatorError, Result};
use bytes::Bytes;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, warn};

/// A thin wrapper around `reqwest::Client` that speaks the ClickHouse HTTP API.
///
/// * SELECT queries use GET with `?query=…`
/// * INSERT/DDL queries use POST with an explicit `Content-Length` header.
///   ClickHouse 24.x rejects POST requests that use chunked transfer encoding
///   (Transfer-Encoding: chunked) without a Content-Length, responding with
///   HTTP 411 Length Required. Passing `Bytes` as the body makes reqwest set
///   Content-Length automatically; for empty bodies we set it to "0" explicitly.
///
/// All data queries use `FORMAT JSONEachRow`.
#[derive(Clone, Debug)]
pub struct ClickHouseClient {
    client: Client,
    pub config: ClickHouseConfig,
}

impl ClickHouseClient {
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()?;
        Ok(Self { client, config })
    }

    // -----------------------------------------------------------------------
    // Parameter helpers
    // -----------------------------------------------------------------------

    /// Auth params without a database — safe before the target DB exists.
    fn base_params_no_db(&self) -> Vec<(&'static str, String)> {
        vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
        ]
    }

    /// Auth params including the configured database.
    fn base_params(&self) -> Vec<(&'static str, String)> {
        vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("database", self.config.database.clone()),
        ]
    }

    // -----------------------------------------------------------------------
    // Connectivity
    // -----------------------------------------------------------------------

    /// Verify connectivity without referencing any specific database.
    /// Safe to call even when the target database does not yet exist.
    pub async fn ping(&self) -> Result<()> {
        let url = &self.config.http_url;
        let mut params = self.base_params_no_db();
        params.push(("query", "SELECT 1".to_string()));

        let response = self.client.get(url).query(&params).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await?;
            return Err(ReplicatorError::ClickHouse(format!(
                "ping failed ({}): {}",
                status, body
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SELECT helpers
    // -----------------------------------------------------------------------

    /// Execute a query that returns rows in `FORMAT JSONEachRow`.
    /// Returns a `Vec<Value>` (one object per row).
    pub async fn query_json_rows(&self, sql: &str) -> Result<Vec<Value>> {
        let query_with_format = format!("{} FORMAT JSONEachRow", sql);
        debug!("query_json_rows: {}", query_with_format);

        let url = &self.config.http_url;
        let mut params = self.base_params();
        params.push(("query", query_with_format));

        let response = self.client.get(url).query(&params).send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(ReplicatorError::ClickHouse(format!(
                "query failed ({}): {}",
                status, body
            )));
        }

        let mut rows = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(line)?;
            rows.push(v);
        }
        Ok(rows)
    }

    /// Execute a query returning a single scalar value from the first column of the first row.
    pub async fn query_scalar(&self, sql: &str) -> Result<String> {
        let rows = self.query_json_rows(sql).await?;
        if rows.is_empty() {
            return Ok(String::new());
        }
        let row = &rows[0];
        if let Some(obj) = row.as_object() {
            if let Some((_, v)) = obj.iter().next() {
                return Ok(value_to_string(v));
            }
        }
        Ok(String::new())
    }

    /// Execute a query returning rows in `FORMAT JSON` (with metadata).
    #[allow(dead_code)]
    pub async fn query_json_meta(&self, sql: &str) -> Result<Vec<Value>> {
        let query_with_format = format!("{} FORMAT JSON", sql);
        debug!("query_json_meta: {}", query_with_format);

        let url = &self.config.http_url;
        let mut params = self.base_params();
        params.push(("query", query_with_format));

        let response = self.client.get(url).query(&params).send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(ReplicatorError::ClickHouse(format!(
                "query failed ({}): {}",
                status, body
            )));
        }

        let v: Value = serde_json::from_str(&body)?;
        Ok(v["data"].as_array().cloned().unwrap_or_default())
    }

    /// Generic deserialized query.
    pub async fn query_typed<T: DeserializeOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let rows = self.query_json_rows(sql).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let item: T = serde_json::from_value(row)?;
            result.push(item);
        }
        Ok(result)
    }

    /// Query a u64 COUNT from the given table.
    pub async fn count(&self, table: &str) -> Result<u64> {
        let sql = format!(
            "SELECT count() AS c FROM `{}`.`{}`",
            self.config.database, table
        );
        let s = self.query_scalar(&sql).await?;
        Ok(s.parse::<u64>().unwrap_or(0))
    }

    /// Query a batch of rows as raw JSONEachRow text (for bulk transfer).
    /// If `order_by` is non-empty, adds an ORDER BY clause for deterministic pagination.
    /// If `col_list` is non-empty, uses that as the SELECT list; otherwise uses `*`.
    pub async fn select_batch_raw(&self, table: &str, offset: u64, limit: usize, order_by: &str, col_list: &str) -> Result<String> {
        let select = if col_list.is_empty() { "*".to_string() } else { col_list.to_string() };
        let order_clause = if order_by.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", order_by)
        };
        let sql = format!(
            "SELECT {} FROM `{}`.`{}`{} LIMIT {} OFFSET {} FORMAT JSONEachRow",
            select, self.config.database, table, order_clause, limit, offset
        );
        debug!("select_batch_raw: {}", sql);

        let url = &self.config.http_url;
        let params = vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("database", self.config.database.clone()),
            ("query", sql),
        ];

        let response = self.client.get(url).query(&params).send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(ReplicatorError::ClickHouse(format!(
                "select_batch_raw failed ({}): {}",
                status, body
            )));
        }
        Ok(body)
    }

    /// Query rows WHERE a DateTime/UInt column is > watermark, returning raw JSONEachRow.
    /// If `col_list` is non-empty, uses that as the SELECT list; otherwise uses `*`.
    pub async fn select_delta_raw(
        &self,
        table: &str,
        watermark_col: &str,
        watermark_val: &str,
        limit: usize,
        col_list: &str,
    ) -> Result<String> {
        let select = if col_list.is_empty() { "*".to_string() } else { col_list.to_string() };
        let sql = format!(
            "SELECT {} FROM `{}`.`{}` WHERE `{}` > {} ORDER BY `{}` ASC LIMIT {} FORMAT JSONEachRow",
            select, self.config.database, table, watermark_col, watermark_val, watermark_col, limit
        );
        debug!("select_delta_raw: {}", sql);

        let url = &self.config.http_url;
        let params = vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("database", self.config.database.clone()),
            ("query", sql),
        ];

        let response = self.client.get(url).query(&params).send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            warn!("select_delta_raw failed ({}): {}", status, body);
            return Ok(String::new());
        }
        Ok(body)
    }

    // -----------------------------------------------------------------------
    // POST / DDL helpers — all use Bytes body so reqwest sets Content-Length
    // -----------------------------------------------------------------------

    /// Execute a DDL / control statement within the configured database.
    #[allow(dead_code)]
    pub async fn execute(&self, sql: &str) -> Result<()> {
        debug!("execute: {}", sql);
        let url = &self.config.http_url;
        let mut params = self.base_params();
        params.push(("query", sql.to_string()));

        let response = self
            .client
            .post(url)
            .query(&params)
            .header("Content-Length", "0")
            .body(Bytes::new())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await?;
            return Err(ReplicatorError::ClickHouse(format!(
                "execute failed ({}): {}",
                status, body
            )));
        }
        Ok(())
    }

    /// Execute a DDL statement against an explicit database (ignores self.config.database).
    #[allow(dead_code)]
    pub async fn execute_on_db(&self, sql: &str, database: &str) -> Result<()> {
        debug!("execute_on_db [{}]: {}", database, sql);
        let url = &self.config.http_url;
        let params = vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("database", database.to_string()),
            ("query", sql.to_string()),
        ];

        let response = self
            .client
            .post(url)
            .query(&params)
            .header("Content-Length", "0")
            .body(Bytes::new())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await?;
            return Err(ReplicatorError::ClickHouse(format!(
                "execute_on_db failed ({}): {}",
                status, body
            )));
        }
        Ok(())
    }

    /// Execute a statement without any database context (used for CREATE DATABASE etc.).
    pub async fn execute_no_db(&self, sql: &str) -> Result<()> {
        debug!("execute_no_db: {}", sql);
        let url = &self.config.http_url;
        let params = vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("query", sql.to_string()),
        ];

        let response = self
            .client
            .post(url)
            .query(&params)
            .header("Content-Length", "0")
            .body(Bytes::new())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await?;
            return Err(ReplicatorError::ClickHouse(format!(
                "execute_no_db failed ({}): {}",
                status, body
            )));
        }
        Ok(())
    }

    /// INSERT rows (already serialized as JSONEachRow lines) into a table.
    pub async fn insert_jsonl(&self, table: &str, jsonl_body: String) -> Result<()> {
        if jsonl_body.is_empty() {
            return Ok(());
        }
        let query = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
            self.config.database, table
        );
        let body = Bytes::from(jsonl_body);
        debug!("insert_jsonl into {} ({} bytes)", table, body.len());

        let url = &self.config.http_url;
        let params = vec![
            ("user", self.config.user.clone()),
            ("password", self.config.password.clone()),
            ("database", self.config.database.clone()),
            ("query", query),
            // Don't let failing materialized views on the destination block
            // base-table inserts.  The MV errors are still logged server-side,
            // but the replicated row lands in the target table regardless.
            ("materialized_views_ignore_errors", "1".to_string()),
        ];

        let content_length = body.len().to_string();
        let response = self
            .client
            .post(url)
            .query(&params)
            .header("Content-Length", content_length)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await?;
            return Err(ReplicatorError::ClickHouse(format!(
                "insert failed ({}): {}",
                status, body
            )));
        }
        Ok(())
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
