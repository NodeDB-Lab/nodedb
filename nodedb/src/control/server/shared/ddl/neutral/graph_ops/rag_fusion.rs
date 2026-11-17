// SPDX-License-Identifier: BUSL-1.1

//! Shared handler for all graph-vector fusion SQL surfaces.
//!
//! Both `GRAPH RAG FUSION ON <col> ...` and `SEARCH <col> USING FUSION(...)`
//! parse into the same [`FusionParams`] typed bag and dispatch through
//! this single function, so caps and defaults cannot drift between the
//! two surfaces.

use nodedb_sql::ddl_ast::FusionParams;
use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::compose::shape_graph_rag;
use crate::control::server::response_translate::translate_search_response;
use crate::control::server::shared::ddl::user_dispatch;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::{GraphTraversalOptions, MAX_GRAPH_TRAVERSAL_DEPTH};
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::GraphOp;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

const FUSION_VECTOR_TOP_K_CAP: usize = 10_000;
const FUSION_TOP_CAP: usize = 10_000;

/// One fusion statement.
pub struct RagFusionRequest {
    pub collection: String,
    pub params: FusionParams,
    pub linearizable: bool,
    /// `AS OF SYSTEM TIME` / `AS OF VALID TIME`: the graph expansion reads
    /// the edges at that cut. The vector and text legs read their indexes
    /// as they stand.
    pub temporal: nodedb_sql::temporal::TemporalScope,
}

pub async fn rag_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    request: RagFusionRequest,
) -> Result<Vec<DdlResult>, DdlError> {
    let RagFusionRequest {
        collection,
        params,
        linearizable,
        temporal,
    } = request;
    // Gate on catalog `is_active` (see `support::ensure_collection_active`):
    // a plain `DROP COLLECTION` only flips `is_active=false` without
    // reclaiming edges/CSR, so this read must independently hide the target
    // collection until UNDROP or a hard purge.
    super::support::ensure_collection_active(
        state,
        database_id,
        identity.tenant_id.as_u64(),
        &collection,
    )?;
    check_bm25_leg(
        state.credentials.catalog(),
        database_id,
        identity.tenant_id.as_u64(),
        &collection,
        &params,
    )?;

    let query_vector = params
        .query_vector
        .ok_or_else(|| ddl_err("42601", "fusion query requires ARRAY[…] vector payload"))?;
    if query_vector.is_empty() {
        return Err(ddl_err("42601", "query vector must not be empty"));
    }

    let vector_top_k = params.vector_top_k.unwrap_or(20);
    if vector_top_k > FUSION_VECTOR_TOP_K_CAP {
        return Err(ddl_err(
            "22023",
            format!(
                "VECTOR_TOP_K {vector_top_k} exceeds maximum allowed value \
                 {FUSION_VECTOR_TOP_K_CAP}"
            ),
        ));
    }

    let expansion_depth = params.expansion_depth.unwrap_or(2);
    if expansion_depth > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(ddl_err(
            "22023",
            format!(
                "EXPANSION_DEPTH {expansion_depth} exceeds maximum allowed value \
                 {MAX_GRAPH_TRAVERSAL_DEPTH}"
            ),
        ));
    }

    let final_top_k = params.final_top_k.unwrap_or(10);
    if final_top_k > FUSION_TOP_CAP {
        return Err(ddl_err(
            "22023",
            format!("FINAL_TOP_K {final_top_k} exceeds maximum allowed value {FUSION_TOP_CAP}"),
        ));
    }

    // Resolve RRF k constants. A three-value triple takes precedence.
    let rrf_k_triple = params.rrf_k_triple;
    let rrf_k = params.rrf_k.unwrap_or((60.0, 60.0));

    let engine_direction = match params.direction {
        Some(GraphDirection::In) => Direction::In,
        Some(GraphDirection::Both) => Direction::Both,
        _ => Direction::Out,
    };

    let options = match params.max_visited {
        Some(mv) => GraphTraversalOptions { max_visited: mv },
        None => GraphTraversalOptions::default(),
    };

    let history = crate::control::server::graph_dispatch::GraphHistory {
        database_id,
        tenant_id: identity.tenant_id,
        collection: Some(&collection),
    };
    let scope = crate::control::server::graph_dispatch::graph_read_scope(state, &temporal, history)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    let plan = PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        query_vector,
        vector_top_k,
        edge_label: params.edge_label,
        direction: engine_direction,
        expansion_depth,
        final_top_k,
        rrf_k,
        rrf_k_triple,
        vector_field: params.vector_field.unwrap_or_default(),
        options,
        bm25_query: params.bm25_query,
        bm25_field: params.bm25_field,
        stage: nodedb_physical::physical_plan::RagStage::Local,
        scope,
    });

    // Only reached through `shared::ddl::dispatch`, which native/pgwire/HTTP
    // each call after their own single per-request admission gate.
    let payload = user_dispatch::dispatch_for_identity(user_dispatch::DispatchRequest {
        state,
        identity,
        database_id,
        collection: &collection,
        // The response translation below reads the plan the fusion ran.
        plan: plan.clone(),
        deadline: StatementDeadline::of_statement(state),
        // Admitted at this request's own transport entry; this handler has no
        // session or peer information of its own.
        admission: user_dispatch::RequestAdmission::AlreadyAdmitted,
        linearizable,
    })
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    // Rows named by no graph node take their primary keys here, through the
    // resolver every search response uses.
    let named = translate_search_response(&payload, &plan, state, database_id, identity.tenant_id)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    let shaped = shape_graph_rag(
        &named,
        None,
        None,
        None,
        &crate::control::crs_context::crs_set_for(state),
    )
    .map_err(|e| DdlError::from_error(&e))?;

    Ok(vec![DdlResult::Rows(shaped)])
}

/// Refuse a BM25 leg whose field no ready full-text index serves, by the
/// rule `text_match` and `bm25_score` apply. A field a `Building` index
/// covers is SQLSTATE 55000. A field no index covers is 42703.
///
/// The leg runs only when the query, the field and the three RRF constants
/// are all given. A statement without them reads no text, so it passes.
fn check_bm25_leg(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    params: &FusionParams,
) -> Result<(), DdlError> {
    let (Some(_), Some(field), Some(_)) = (
        params.bm25_query.as_deref(),
        params.bm25_field.as_deref(),
        params.rrf_k_triple,
    ) else {
        return Ok(());
    };
    crate::control::planner::catalog_adapter::check_text_read(
        catalog,
        database_id.as_u64(),
        tenant_id,
        collection,
        Some(field),
    )
    .map_err(|e| DdlError::from_error(&e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::{IndexKind, IndexRecordState, StoredIndexRecord};
    use crate::control::security::credential::CredentialStore;

    const TENANT: u64 = 1;

    fn seed(catalog: &SystemCatalog, name: &str, field: &str, state: IndexRecordState) {
        catalog
            .put_index_record(&StoredIndexRecord {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: TENANT,
                name: name.to_string(),
                kind: IndexKind::FullText,
                collection: "docs".to_string(),
                fields: vec![field.to_string()],
                is_active: true,
                state,
                analyzer: None,
                fuzzy_default: None,
            })
            .expect("seed index record");
    }

    fn bm25_on(field: &str) -> FusionParams {
        FusionParams {
            bm25_query: Some("rust".to_string()),
            bm25_field: Some(field.to_string()),
            rrf_k_triple: Some((60.0, 60.0, 60.0)),
            ..FusionParams::default()
        }
    }

    fn check(catalog: &SystemCatalog, params: &FusionParams) -> Result<(), DdlError> {
        check_bm25_leg(catalog, DatabaseId::DEFAULT, TENANT, "docs", params)
    }

    /// The BM25 leg reads only a field a ready full-text index serves.
    #[test]
    fn the_bm25_leg_needs_a_ready_index_on_its_field() {
        let store = CredentialStore::new().expect("credential store");
        let catalog = store.catalog();
        seed(catalog, "ft_title", "title", IndexRecordState::Ready);
        seed(catalog, "ft_body", "body", IndexRecordState::Building);

        assert!(check(catalog, &bm25_on("title")).is_ok());
        let building = check(catalog, &bm25_on("body")).expect_err("a Building index");
        assert_eq!(building.sqlstate, "55000", "{}", building.message);
        let undeclared = check(catalog, &bm25_on("ghost")).expect_err("no index");
        assert_eq!(undeclared.sqlstate, "42703", "{}", undeclared.message);
    }

    /// A fusion without the three BM25 parts runs no text leg.
    #[test]
    fn a_fusion_without_a_bm25_leg_passes() {
        let store = CredentialStore::new().expect("credential store");
        let mut two_source = bm25_on("ghost");
        two_source.rrf_k_triple = None;
        assert!(check(store.catalog(), &two_source).is_ok());
        assert!(check(store.catalog(), &FusionParams::default()).is_ok());
    }
}
