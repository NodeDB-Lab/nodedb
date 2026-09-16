// SPDX-License-Identifier: BUSL-1.1

//! `SEARCH <collection> USING FUSION(...)` DSL.
//!
//! Parsing is delegated to `nodedb_sql::ddl_ast::parse_search_using_fusion`
//! so this SQL surface shares the same quote- and bracket-aware tokenizer
//! and the same typed [`FusionParams`](nodedb_sql::ddl_ast::FusionParams)
//! extractor as `GRAPH RAG FUSION`. Both surfaces dispatch through the same
//! protocol-neutral [`rag_fusion`], so defaults and caps cannot drift
//! between them.

use nodedb_sql::ddl_ast::parse_search_using_fusion;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::graph_ops::rag_fusion::rag_fusion;
use super::support::ddl_err;

pub async fn search_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let parsed = parse_search_using_fusion(sql).map_err(|e| {
        ddl_err(
            "42601",
            format!("SEARCH <collection> USING FUSION(...): {e}"),
        )
    })?;
    let (collection, params) = parsed.ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: SEARCH <collection> USING FUSION(ARRAY[...] ...)",
        )
    })?;
    rag_fusion(state, identity, database_id, collection, params).await
}
