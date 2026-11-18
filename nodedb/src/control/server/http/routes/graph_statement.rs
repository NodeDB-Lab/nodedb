// SPDX-License-Identifier: BUSL-1.1

//! A lone `GRAPH` or `MATCH` statement on a route that plans SQL itself.
//!
//! The planner cannot parse these statements. The DDL router owns them, as it
//! does on HTTP `/v1/query` and native SQL. NDJSON and WebSocket RPC call
//! [`try_graph_rows`] before planning. It declines every other statement, so
//! no DDL, admin, `SHOW` or `UPSERT` statement takes the router on those routes.

use nodedb_sql::parser::preprocess::lex::{first_sql_word, second_sql_word};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DetachedTxnScope;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::result_shape::ddl_results_to_json;

/// Whether `sql` opens with `GRAPH`, `MATCH` or `OPTIONAL MATCH`, the leading
/// words the statement parser sends to the graph parser.
fn is_graph_statement(sql: &str) -> bool {
    match first_sql_word(sql) {
        Some(word) if word.eq_ignore_ascii_case("GRAPH") || word.eq_ignore_ascii_case("MATCH") => {
            true
        }
        Some(word) if word.eq_ignore_ascii_case("OPTIONAL") => {
            second_sql_word(sql).is_some_and(|next| next.eq_ignore_ascii_case("MATCH"))
        }
        _ => false,
    }
}

/// Run `sql` through the DDL router when it is a `GRAPH` or `MATCH` statement.
///
/// Returns `None` for every other statement, so the caller plans it. The route
/// has no session, so the statement takes the autocommit branch.
pub(super) async fn try_graph_rows(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Option<crate::Result<Vec<serde_json::Value>>> {
    if !is_graph_statement(sql) {
        return None;
    }
    let scope = DetachedTxnScope::new();
    let txn_ctx = scope.ctx();
    let result = crate::control::server::shared::ddl::dispatch(
        state,
        identity,
        sql.trim(),
        database_id,
        &txn_ctx,
    )
    .await?;
    Some(result.map(ddl_results_to_json).map_err(crate::Error::from))
}
