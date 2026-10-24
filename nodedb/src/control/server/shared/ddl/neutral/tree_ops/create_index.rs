// SPDX-License-Identifier: BUSL-1.1

//! `CREATE GRAPH INDEX name ON collection (parent_col -> id_col)`
//!
//! Scans every document in the collection and materialises each
//! `parent → child` relation as a CSR edge under the graph-index name.
//!
//! Correctness contract:
//!
//! 1. **Atomic-ish**: the whole edge set is dispatched in a single
//!    `EdgePutBatch` per vshard. Any `Err` from the Data Plane causes
//!    the DDL to attempt a best-effort rollback via `EdgeDeleteBatch`
//!    on the shards that already succeeded, then surfaces the dispatch
//!    error with its own SQLSTATE to the client.
//! 2. **Loud on partial failure**: no `tracing::warn!` + continue. The
//!    reported `edges_created` count either matches the number of
//!    valid parent→child relations in the collection or the DDL fails.
//! 3. **schema_version gated**: `state.schema_version.bump()` runs only
//!    on success so consumers observing catalog version do not see a
//!    half-built index.
//! 4. **Replicated**: each batch is proposed through the destination
//!    shard's Raft group. The entry's apply appends the batch's WAL records
//!    on every replica, this node included.
//! 5. **Owner scan**: the scan reads the collection's home group, here when
//!    this node replicates it and on its leader otherwise, then partitions
//!    the resulting edges by destination-shard for batched dispatch.
//! 6. **Linearizable scan**: the index serves every reader, so the scan
//!    always confirms the collection's home group first, whatever the
//!    session's read consistency. A stale replica will leave rows out of
//!    the index for good.

use std::collections::HashMap;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::broadcast::broadcast_to_all_cores;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use super::super::super::result::{DdlError, DdlResult};
use super::parse::parse_edge_columns;
use super::support::ddl_err;

pub async fn create_graph_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // CREATE GRAPH INDEX <name> ON <collection> (<parent_col> -> <id_col>)
    let index_name = parse_ident_token(
        parts
            .get(3)
            .ok_or_else(|| ddl_err("42601", "missing graph index name"))?,
    )?;

    let on_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| ddl_err("42601", "CREATE GRAPH INDEX requires ON <collection>"))?;

    let collection = parts
        .get(on_idx + 1)
        .ok_or_else(|| ddl_err("42601", "missing collection name after ON"))?
        .to_lowercase();

    let (parent_col, id_col) = parse_edge_columns(sql)?;

    // The index is built from a document scan, which reads the sparse store
    // only, so a KV or columnar-family collection is refused rather than
    // indexed from zero rows.
    let catalog = state.credentials.catalog();
    let stored = catalog
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| ddl_err("42P01", format!("collection '{collection}' not found")))?;
    if !stored.collection_type.is_document() {
        return Err(ddl_err(
            "0A000",
            format!(
                "CREATE GRAPH INDEX reads document collections; '{collection}' is a {} collection",
                stored.collection_type.as_str()
            ),
        ));
    }

    // Building the index reads every row of the collection, so the caller must
    // be allowed to read it.
    let audit =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    crate::control::server::shared::authorization::authorize_collection(
        identity,
        database_id,
        &collection,
        crate::control::security::identity::Permission::Read,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| ddl_err("42501", format!("permission denied: {}", error.resource())))?;

    // A read policy on the collection makes the index unbuildable rather than
    // partially built: the index is shared by every reader, so deriving it from
    // one principal's filtered view will answer other principals' queries from
    // rows that were never indexed. Refuse instead of silently indexing a
    // subset.
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id)?;
    if state
        .rls
        .combined_read_predicate_with_auth(tenant_id.as_u64(), &collection, scope.auth())
        .map_err(|e| DdlError::from_error_in_context("rls compile", &e))?
        .is_none_or(|filters| !filters.is_empty())
    {
        return Err(ddl_err(
            "0A000",
            format!(
                "cannot build a graph index on '{collection}': it carries a row-level security                  read policy, and an index derived from one principal's visible rows would be                  incomplete for every other principal"
            ),
        ));
    }

    // ── Broadcast scan: collect documents from every vshard ──────────
    let scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    // The collection's rows live on its home vShard (`types/record_home.rs`).
    // The scan runs here when this node replicates that group, and on the
    // group's leader otherwise. It is always linearizable (point 6).
    let home = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();
    let scope = crate::control::server::dispatch_utils::OwnedReadScope {
        tenant_id,
        database_id,
        vshard_id: home,
        trace_id: TraceId::ZERO,
        txn_id: None,
        linearizable: true,
    };
    let scan_resp =
        match crate::control::server::dispatch_utils::route_owned_read(state, scope, scan_plan)
            .await
            .map_err(|e| DdlError::from_error_in_context("scan failed", &e))?
        {
            crate::control::server::dispatch_utils::OwnedRead::Served(resp) => resp,
            crate::control::server::dispatch_utils::OwnedRead::Local(scan_plan) => {
                broadcast_to_all_cores(state, tenant_id, database_id, *scan_plan, TraceId::ZERO)
                    .await
                    .map_err(|e| DdlError::from_error_in_context("scan failed", &e))?
            }
        };
    crate::control::local_dispatch::reject_data_plane_error(&scan_resp)
        .map_err(|e| DdlError::from_error_in_context("scan failed", &e))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    // An empty payload is a scan that found no rows.
    let docs: Vec<serde_json::Value> = if payload_json.is_empty() {
        Vec::new()
    } else {
        sonic_rs::from_str(&payload_json)
            .map_err(|e| ddl_err("22P02", format!("invalid JSON in scan response: {e}")))?
    };

    // ── Build edge list partitioned by destination vshard ────────────
    //
    // Edges to the same vshard are batched into one `EdgePutBatch`. A
    // mixed-type `parent` field (e.g. an integer) surfaces SQLSTATE
    // `22P02` loudly; the earlier `.and_then(as_str).drop` behaviour
    // silently omitted the edge, leaving the index incomplete.
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for doc in &docs {
        // `DocumentOp::Scan` emits `{id, data: {...}}` per
        // `encode_raw_document_rows`. Anything else is a protocol bug,
        // not a shape to silently accommodate.
        let Some(obj_outer) = doc.as_object() else {
            continue;
        };
        let Some(obj) = obj_outer.get("data").and_then(|v| v.as_object()) else {
            return Err(DdlError::internal(format!(
                "CREATE GRAPH INDEX: document scan returned a row without a `data` field: {doc}"
            )));
        };

        let doc_id = obj
            .get("id")
            .or_else(|| obj.get("_id"))
            .and_then(|v| v.as_str())
            .or_else(|| obj.get(&id_col).and_then(|v| v.as_str()));

        let parent_raw = obj.get(&parent_col);

        match (doc_id, parent_raw) {
            // Missing parent — legitimate root; skip.
            (Some(_), None) | (Some(_), Some(serde_json::Value::Null)) => {}
            (Some(child), Some(parent_v)) => {
                let parent = match parent_v.as_str() {
                    Some(s) => s,
                    None => {
                        return Err(ddl_err(
                            "22P02",
                            format!(
                                "collection '{collection}' doc '{child}': parent field '{parent_col}' \
                                 must be a string, got {parent_v:?}"
                            ),
                        ));
                    }
                };
                if parent.is_empty() || parent == child {
                    continue;
                }
                pairs.push((parent, child));
            }
            _ => {}
        }
    }

    // Every endpoint's identity, in one batch at the collection's home:
    // `[parent, child]` per edge, in edge order.
    let endpoint_keys: Vec<&[u8]> = pairs
        .iter()
        .flat_map(|(parent, child)| [parent.as_bytes(), child.as_bytes()])
        .collect();
    let endpoint_surrogates = crate::control::server::surrogate_exchange::assign_surrogates_routed(
        state,
        nodedb_types::CollectionKey::from_bare(database_id, &collection),
        tenant_id,
        &endpoint_keys,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;
    if endpoint_surrogates.len() != endpoint_keys.len() {
        return Err(DdlError::internal(format!(
            "CREATE GRAPH INDEX: the home answered {} surrogates for {} edge endpoints",
            endpoint_surrogates.len(),
            endpoint_keys.len()
        )));
    }

    let mut edges_by_shard: HashMap<VShardId, Vec<BatchEdge>> = HashMap::new();
    let mut total_edges = 0u64;
    let (endpoint_pairs, _) = endpoint_surrogates.as_chunks::<2>();
    for ((parent, child), [src_surrogate, dst_surrogate]) in pairs.iter().zip(endpoint_pairs) {
        let shard = VShardId::from_key(parent.as_bytes());
        edges_by_shard.entry(shard).or_default().push(BatchEdge {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            src_id: parent.to_string(),
            label: index_name.clone(),
            dst_id: child.to_string(),
            src_surrogate: *src_surrogate,
            dst_surrogate: *dst_surrogate,
            write_gate: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        total_edges += 1;
    }

    // ── Dispatch batches, WAL + rollback discipline ──────────────────
    let mut committed_shards: Vec<(VShardId, Vec<BatchEdge>)> = Vec::new();
    for (shard, edges) in edges_by_shard {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: edges.clone(),
        });
        // The shard's Raft entry carries the batch, and its apply appends
        // the records on every replica.
        match dispatch_edge_batch(state, tenant_id, database_id, shard, plan).await {
            Ok(_) => committed_shards.push((shard, edges)),
            Err(e) => {
                return surface_failure(
                    state,
                    tenant_id,
                    database_id,
                    &committed_shards,
                    &format!("edge-insert dispatch failed on shard {shard:?}"),
                    &e,
                )
                .await;
            }
        }
    }

    // Only now is the index atomically complete.
    state.schema_version.bump();

    let mut row = Map::new();
    row.insert(
        "edges_created".to_string(),
        JsonValue::String(total_edges.to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["edges_created".to_string()],
        vec![row],
    ))])
}

/// Propose an edge batch through `shard`'s Raft group and wait until it
/// applied on this node.
async fn dispatch_edge_batch(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    shard: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
        state,
        tenant_id,
        database_id,
        shard,
        plan,
        crate::event::EventSource::User,
    )
    .await
}

/// Surface a build-time failure.
///
/// Runs rollback in parallel across all committed shards. If **every**
/// shard's `EdgeDeleteBatch` succeeds, returns `cause` with its own SQLSTATE,
/// prefixed `CREATE GRAPH INDEX failed: <context>; reverted N shards`.
///
/// If **any** shard's rollback itself fails, the CSR is now in an
/// inconsistent state across shards — some have the partial index,
/// others don't. This is surfaced loudly as
/// `XX001 GRAPH INDEX LEFT IN INCONSISTENT STATE` with the list of
/// shards that failed to revert. Clients MUST treat this as a hard
/// error requiring operator intervention; silently warn-logging the
/// failure hides the error that needs the operator.
async fn surface_failure(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    committed: &[(VShardId, Vec<BatchEdge>)],
    context: &str,
    cause: &crate::Error,
) -> Result<Vec<DdlResult>, DdlError> {
    let committed_count = committed.len();
    let rollback_futures = committed.iter().map(|(shard, edges)| {
        let plan = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
            edges: edges.clone(),
        });
        let shard = *shard;
        async move {
            (
                shard,
                dispatch_edge_batch(state, tenant_id, database_id, shard, plan).await,
            )
        }
    });

    let rollback_results = futures::future::join_all(rollback_futures).await;
    let failed: Vec<(VShardId, String)> = rollback_results
        .into_iter()
        .filter_map(|(shard, res)| res.err().map(|e| (shard, e.to_string())))
        .collect();

    if failed.is_empty() {
        Err(DdlError::from_error_in_context(
            &format!(
                "CREATE GRAPH INDEX failed: {context}; reverted {committed_count} committed shards"
            ),
            cause,
        ))
    } else {
        // Distinct SQLSTATE so clients / operators can distinguish
        // "failed cleanly" from "failed and left the graph broken".
        Err(ddl_err(
            "XX001",
            format!(
                "CREATE GRAPH INDEX failed: {context}: {cause}; rollback also failed on {}/{} shards \
                 ({:?}); GRAPH INDEX LEFT IN INCONSISTENT STATE — operator intervention required",
                failed.len(),
                committed_count,
                failed
            ),
        ))
    }
}
