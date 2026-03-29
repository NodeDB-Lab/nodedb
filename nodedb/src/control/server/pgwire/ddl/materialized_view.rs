//! DDL handlers for materialized views (HTAP bridge).
//!
//! - `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]`
//! - `DROP MATERIALIZED VIEW [IF EXISTS] <name>`
//! - `REFRESH MATERIALIZED VIEW <name>`
//! - `SHOW MATERIALIZED VIEWS [FOR <source>]`
//!
//! View definitions are stored in the system catalog (redb). CDC orchestration
//! (automatic replication from source → view on writes) uses the change stream
//! infrastructure: the Data Plane publishes `ChangeEvent` on every write, and
//! the view refresh loop applies them to the target columnar collection.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::StoredMaterializedView;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]
pub fn create_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (name, source, query_sql, refresh_mode) = parse_create_mv(sql)?;

    let tenant_id = identity.tenant_id;

    // Validate source collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &source) {
            Ok(Some(_)) => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("source collection '{source}' does not exist"),
                ));
            }
        }

        // Check view doesn't already exist.
        if let Ok(Some(_)) = catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            return Err(sqlstate_error(
                "42P07",
                &format!("materialized view '{name}' already exists"),
            ));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let view = StoredMaterializedView {
        tenant_id: tenant_id.as_u32(),
        name: name.clone(),
        source: source.clone(),
        query_sql,
        refresh_mode,
        owner: identity.username.clone(),
        created_at: now,
    };

    if let Some(catalog) = state.credentials.catalog() {
        catalog
            .put_materialized_view(&view)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    tracing::info!(
        view = name,
        source,
        tenant = tenant_id.as_u32(),
        "materialized view created"
    );

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "CREATE MATERIALIZED VIEW",
    ))])
}

/// DROP MATERIALIZED VIEW [IF EXISTS] <name>
pub fn drop_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // DROP MATERIALIZED VIEW [IF EXISTS] <name>
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP MATERIALIZED VIEW [IF EXISTS] <name>",
        ));
    }

    let tenant_id = identity.tenant_id;

    // Handle IF EXISTS.
    let (name, if_exists) = if parts.len() >= 6
        && parts[3].to_uppercase() == "IF"
        && parts[4].to_uppercase() == "EXISTS"
    {
        (parts[5].to_lowercase(), true)
    } else {
        (parts[3].to_lowercase(), false)
    };

    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            Ok(Some(_)) => {
                catalog
                    .delete_materialized_view(tenant_id.as_u32(), &name)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            }
            Ok(None) if if_exists => {
                // IF EXISTS — silently succeed.
            }
            Ok(None) => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("materialized view '{name}' does not exist"),
                ));
            }
            Err(e) => return Err(sqlstate_error("XX000", &e.to_string())),
        }
    }

    tracing::info!(view = name, "materialized view dropped");

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "DROP MATERIALIZED VIEW",
    ))])
}

/// REFRESH MATERIALIZED VIEW <name>
///
/// Scans all documents from the source collection and writes them to
/// the view's target collection. Truncates the target first (full refresh).
pub async fn refresh_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REFRESH MATERIALIZED VIEW <name>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Look up view definition to get source collection.
    let view = if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("materialized view '{name}' does not exist"),
                ));
            }
            Err(e) => return Err(sqlstate_error("XX000", &e.to_string())),
        }
    } else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };

    // Dispatch refresh to Data Plane: scan source → write to target.
    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::RefreshMaterializedView {
            view_name: name.clone(),
            source_collection: view.source.clone(),
        },
    );

    super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &view.source,
        plan,
        std::time::Duration::from_secs(30),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("refresh failed: {e}")))?;

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "REFRESH MATERIALIZED VIEW",
    ))])
}

/// SHOW MATERIALIZED VIEWS [FOR <source>]
pub fn show_materialized_views(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    // Optional source filter: SHOW MATERIALIZED VIEWS FOR <source>
    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("source"),
        text_field("refresh_mode"),
        text_field("owner"),
        text_field("query"),
    ]);

    let views = if let Some(catalog) = state.credentials.catalog() {
        catalog
            .list_materialized_views(tenant_id.as_u32())
            .map_err(|e| sqlstate_error("XX000", &format!("catalog read failed: {e}")))?
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for view in &views {
        if let Some(ref filter) = source_filter
            && view.source != *filter
        {
            continue;
        }

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&view.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.source)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.refresh_mode)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.query_sql)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

// ── SQL Parsing ──────────────────────────────────────────────────────────

const KW_MATERIALIZED_VIEW: &str = "MATERIALIZED VIEW ";
const KW_ON: &str = " ON ";
const KW_AS: &str = " AS ";

/// Parse CREATE MATERIALIZED VIEW SQL.
///
/// Syntax:
/// ```text
/// CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ...
///   [WITH (refresh = 'auto'|'manual')]
/// ```
///
/// Returns `(name, source, query_sql, refresh_mode)`.
fn parse_create_mv(sql: &str) -> PgWireResult<(String, String, String, String)> {
    let upper = sql.to_uppercase();

    // Extract name: word after "MATERIALIZED VIEW"
    let mv_pos = upper
        .find(KW_MATERIALIZED_VIEW)
        .ok_or_else(|| sqlstate_error("42601", "expected MATERIALIZED VIEW keyword"))?;
    let after_mv_start = mv_pos + KW_MATERIALIZED_VIEW.len();
    let after_mv = sql[after_mv_start..].trim_start();
    let name = after_mv
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing view name"))?
        .to_lowercase();

    // Extract source: word after "ON"
    let on_pos = upper[after_mv_start..]
        .find(KW_ON)
        .ok_or_else(|| sqlstate_error("42601", "expected ON <source> clause"))?;
    let after_on_start = after_mv_start + on_pos + KW_ON.len();
    let after_on = sql[after_on_start..].trim_start();
    let source = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing source collection name"))?
        .to_lowercase();

    // Extract query SQL: everything after "AS" up to "WITH" or end.
    let as_pos = upper[after_on_start..]
        .find(KW_AS)
        .ok_or_else(|| sqlstate_error("42601", "expected AS SELECT ... clause"))?;
    let query_start = after_on_start + as_pos + KW_AS.len();

    // Find end of query: WITH clause or end of string.
    let remaining = &upper[query_start..];
    let with_pos = remaining.find(" WITH").or_else(|| {
        // Handle case where WITH immediately follows AS (no query body).
        if remaining.trim_start().starts_with("WITH") {
            Some(0)
        } else {
            None
        }
    });
    let query_end = with_pos.map(|p| query_start + p).unwrap_or(sql.len());
    let query_sql = sql[query_start..query_end].trim().to_string();

    if query_sql.is_empty() {
        return Err(sqlstate_error("42601", "empty query after AS"));
    }

    // Extract refresh mode from WITH clause (default: "auto").
    let refresh_mode = extract_refresh_mode(&upper, sql);

    Ok((name, source, query_sql, refresh_mode))
}

/// Extract refresh mode from WITH clause.
fn extract_refresh_mode(upper: &str, sql: &str) -> String {
    let with_pos = match upper.rfind("WITH") {
        Some(p) => p,
        None => return "auto".into(),
    };
    let after_with = sql[with_pos + 4..].trim_start();
    let open = match after_with.find('(') {
        Some(p) => p,
        None => return "auto".into(),
    };
    let close = match after_with.rfind(')') {
        Some(p) => p,
        None => return "auto".into(),
    };
    if close <= open {
        return "auto".into();
    }

    let inner = &after_with[open + 1..close];
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_lowercase();
            if key == "refresh" || key == "refresh_mode" {
                return val;
            }
        }
    }
    "auto".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_basic() {
        let sql = "CREATE MATERIALIZED VIEW sales_daily ON orders \
                    AS SELECT date, SUM(amount) FROM orders GROUP BY date";
        let (name, source, query, refresh) = parse_create_mv(sql).unwrap();
        assert_eq!(name, "sales_daily");
        assert_eq!(source, "orders");
        assert!(query.contains("SUM(amount)"));
        assert_eq!(refresh, "auto");
    }

    #[test]
    fn parse_create_with_refresh() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src \
                    AS SELECT * FROM src \
                    WITH (refresh = 'manual')";
        let (name, source, query, refresh) = parse_create_mv(sql).unwrap();
        assert_eq!(name, "m1");
        assert_eq!(source, "src");
        assert_eq!(query, "SELECT * FROM src");
        assert_eq!(refresh, "manual");
    }

    #[test]
    fn parse_create_missing_as_errors() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src";
        assert!(parse_create_mv(sql).is_err());
    }

    #[test]
    fn parse_create_empty_query_errors() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src AS WITH (refresh = 'manual')";
        assert!(parse_create_mv(sql).is_err());
    }
}
