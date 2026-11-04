// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH INSERT EDGE` and the native `EdgePutBatch`: one or more edges into
//! one collection, written as one unit (see `edge_unit`).
//!
//! The collection's write policy decides every edge on its own `PROPERTIES`
//! image, exactly as for a single edge. One refused edge refuses the whole
//! write before anything is written.

use nodedb_physical::physical_plan::GraphOp;
use nodedb_sql::ddl_ast::GraphEdgeSpec;
use nodedb_types::protocol::EdgeWrite;
use nodedb_types::{CollectionKey, QualifiedCollection, Surrogate};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes};

use super::super::super::result::{DdlError, DdlResult};
use super::edge_parse::{properties_to_msgpack, validate_edge_label};
use super::edge_unit::{
    EdgeWriteKind, PreparedEdge, admit_edge_write, bound_surrogate, endpoint_surrogates,
    in_transaction_block, stage_edges, write_edge_unit,
};
use super::support::ddl_err;

/// One edge to insert, its `PROPERTIES` already encoded as the plain
/// MessagePack map the edge store holds. Empty bytes store no properties.
pub struct EdgeInsert {
    pub src: String,
    pub dst: String,
    pub label: String,
    pub properties: Vec<u8>,
    pub valid_from_ms: Option<i64>,
    pub valid_until_ms: Option<i64>,
}

/// The edges of a parsed `GRAPH INSERT EDGE`, their `PROPERTIES` encoded.
pub fn edge_inserts_from_specs(specs: Vec<GraphEdgeSpec>) -> Result<Vec<EdgeInsert>, DdlError> {
    specs
        .into_iter()
        .map(|spec| {
            Ok(EdgeInsert {
                src: spec.src,
                dst: spec.dst,
                label: spec.label,
                properties: properties_to_msgpack(spec.properties)?,
                valid_from_ms: spec.valid_from_ms,
                valid_until_ms: spec.valid_until_ms,
            })
        })
        .collect()
}

/// The edges of a native `EdgePutBatch`, their properties encoded. An empty
/// property map stores no properties, as an absent `PROPERTIES` clause does.
pub fn edge_inserts_from_writes(writes: Vec<EdgeWrite>) -> Result<Vec<EdgeInsert>, DdlError> {
    writes
        .into_iter()
        .map(|write| {
            let properties = if write.properties.is_empty() {
                Vec::new()
            } else {
                nodedb_types::json_msgpack::value_to_msgpack(&nodedb_types::Value::Object(
                    write.properties,
                ))
                .map_err(|e| DdlError::internal(format!("edge properties encode error: {e}")))?
            };
            Ok(EdgeInsert {
                src: write.src.as_str().to_owned(),
                dst: write.dst.as_str().to_owned(),
                label: write.label,
                properties,
                valid_from_ms: write.valid_from_ms,
                valid_until_ms: write.valid_until_ms,
            })
        })
        .collect()
}

/// Insert `edges` into `collection` as one unit. The command tag counts the
/// edges written.
pub async fn insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<EdgeInsert>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    admit_edge_write(
        state,
        identity,
        database_id,
        EdgeWriteKind::Insert,
        &collection,
        edges.len(),
    )?;
    for edge in &edges {
        if edge.src.is_empty() || edge.dst.is_empty() {
            return Err(ddl_err("42601", "GRAPH INSERT EDGE requires FROM and TO"));
        }
        validate_edge_label(&edge.label)?;
        check_valid_interval(edge)?;
    }
    let tenant_id = identity.tenant_id;

    // Flags the collection edge-bearing so a later predicate DELETE routes through
    // OLLP instead of the fast path. Idempotent.
    crate::control::planner::implicit_edges::mark_collection_edge_bearing(
        state,
        CollectionKey::from_bare(database_id, &collection),
        tenant_id,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    let surrogates = endpoint_surrogates(
        state,
        database_id,
        tenant_id,
        &collection,
        &edges
            .iter()
            .flat_map(|edge| [edge.src.as_str(), edge.dst.as_str()])
            .collect::<Vec<_>>(),
    )
    .await?;
    let prepared = edges
        .into_iter()
        .map(|edge| prepare_edge(state, identity, database_id, &collection, edge, &surrogates))
        .collect::<Result<Vec<_>, _>>()?;

    // In a transaction, an insert stages into `GraphTxnOverlay` instead of applying now
    // (COMMIT replays, ROLLBACK discards); a cross-shard edge stages into both endpoints.
    let affected = if in_transaction_block(txn_ctx) {
        stage_edges(state, tenant_id, database_id, prepared, txn_ctx).await?
    } else {
        write_edge_unit(
            state,
            tenant_id,
            database_id,
            EdgeWriteKind::Insert,
            prepared,
        )
        .await?
    };

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGE".to_string(),
        rows_affected: Some(affected),
    }])
}

/// `UNTIL` needs `VALID_TIME`, and the interval must not be empty. The SQL
/// parser refuses both. A native batch reaches here unparsed.
fn check_valid_interval(edge: &EdgeInsert) -> Result<(), DdlError> {
    match (edge.valid_from_ms, edge.valid_until_ms) {
        (None, Some(_)) => Err(ddl_err(
            "42601",
            "GRAPH INSERT EDGE: UNTIL needs VALID_TIME <from>",
        )),
        (Some(from), Some(until)) if until <= from => Err(ddl_err(
            "42601",
            format!(
                "GRAPH INSERT EDGE: VALID_TIME {from} UNTIL {until} is empty; UNTIL must be \
                 after VALID_TIME"
            ),
        )),
        _ => Ok(()),
    }
}

/// Build one edge's `EdgePut` and run it through the collection's write
/// policy. This handler dispatches as trusted internal work, so nothing
/// downstream resolves a policy: the policy decides the `PROPERTIES` image
/// here, per edge, before staging.
fn prepare_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edge: EdgeInsert,
    surrogates: &std::collections::HashMap<String, Surrogate>,
) -> Result<PreparedEdge, DdlError> {
    let EdgeInsert {
        src,
        dst,
        label,
        properties,
        valid_from_ms,
        valid_until_ms,
    } = edge;
    // Dual-home: a cross-shard edge must be written on the home vShard of
    // both src and dst, or reverse/IN traversal never finds it.
    let homes = RecordHomes::edge(&src, &dst);
    let src_surrogate = bound_surrogate(surrogates, &src)?;
    let dst_surrogate = bound_surrogate(surrogates, &dst)?;
    let op = super::edge_rls::resolve_edge_write_rls(
        state,
        identity,
        database_id,
        GraphOp::EdgePut {
            collection: QualifiedCollection::new(database_id, collection),
            src_id: src,
            label,
            dst_id: dst,
            properties,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
        },
    )?;
    Ok(PreparedEdge { op, homes })
}
