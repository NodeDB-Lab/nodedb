//! `SEARCH <collection> USING FUSION(...)` DSL.
//!
//! Parsing is delegated to `nodedb_sql::ddl_ast::parse_search_using_fusion`
//! so this SQL surface shares the same quote- and bracket-aware tokenizer
//! and the same typed [`FusionParams`] extractor as `GRAPH RAG FUSION`.
//! Both surfaces dispatch through
//! [`crate::control::server::pgwire::ddl::graph_ops::rag_fusion::rag_fusion`],
//! so defaults and caps cannot drift between them.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::parse_search_using_fusion;

use super::super::graph_ops::rag_fusion::rag_fusion;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;

pub async fn search_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, params) = parse_search_using_fusion(sql).ok_or_else(|| {
        sqlstate_error(
            "42601",
            "syntax: SEARCH <collection> USING FUSION(ARRAY[...] ...)",
        )
    })?;
    rag_fusion(state, identity, collection, params).await
}
