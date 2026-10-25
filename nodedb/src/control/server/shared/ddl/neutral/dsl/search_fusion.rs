// SPDX-License-Identifier: BUSL-1.1

//! `SEARCH <collection> USING FUSION(...)` DSL.
//!
//! The router parses the statement with
//! `nodedb_sql::ddl_ast::parse_search_using_fusion`, which shares the
//! tokenizer and the clause reader of `GRAPH RAG FUSION`. Both surfaces
//! dispatch through the same protocol-neutral [`rag_fusion`], so defaults and
//! caps cannot drift between them.

use nodedb_sql::ddl_ast::FusionParams;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::graph_ops::rag_fusion::{RagFusionRequest, rag_fusion};

/// Run a parsed `SEARCH <collection> USING FUSION(...)` statement.
pub async fn search_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    params: FusionParams,
    linearizable: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    rag_fusion(
        state,
        identity,
        database_id,
        RagFusionRequest {
            collection,
            params,
            linearizable,
            // `SEARCH ... USING FUSION` reads the current graph.
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        },
    )
    .await
}
