// SPDX-License-Identifier: BUSL-1.1

//! `SHOW GRAPH STATS` handler.
//!
//! Reads persistent graph-stats counters from every Data-Plane core via
//! `broadcast_to_all_cores`, aggregates the per-core
//! [`CollectionStats`](crate::engine::graph::edge_store::stats::CollectionStats)
//! payloads, and emits a protocol-neutral result row set.
//!
//! Aggregation rules:
//! - `edge_count`: summed across cores (each core holds a disjoint partition).
//! - `distinct_node_count`: summed across cores. Per-core CSR partitions are
//!   hash-disjoint by node id, so the cross-core sum equals the global distinct
//!   count — no double-count.
//! - `distinct_label_count`: re-derived from the merged `labels` vec rather than
//!   summed (labels are NOT partition-disjoint — the same label name can appear
//!   in multiple cores).
//! - `labels`: merged by name; counts summed; output is sorted ascending by name.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::DatabaseId;
use nodedb_types::diagnostic::DiagnosticLayer;
use serde_json::{Map, Value as JsonValue};
use tracing::info_span;

/// Total number of `SHOW GRAPH STATS` calls served since process start.
/// Read by the metrics endpoint via [`graph_stats_calls_total`].
static GRAPH_STATS_CALLS: AtomicU64 = AtomicU64::new(0);

/// Counter for observability. Mirrors the `broadcast_call_count()` style
/// used elsewhere in the Control Plane. Exposed for metrics endpoints
/// and test harnesses to assert call counts.
#[allow(dead_code)]
pub fn graph_stats_calls_total() -> u64 {
    GRAPH_STATS_CALLS.load(Ordering::Relaxed)
}

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::broadcast::broadcast_to_all_cores;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::stats::CollectionStats;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::support::ddl_err;

/// Names the collection-scoped stats read in the refusal a read policy raises.
const STATS_WHAT: &str = "graph statistics, which are counters over the collection's edges";

/// …and the tenant-wide form, which cannot narrow itself to one collection.
const STATS_TENANT_WIDE_WHAT: &str =
    "graph statistics, which report counters for every collection holding edges";

/// `SHOW GRAPH STATS ['<collection>'] [VERBOSE] [AS OF SYSTEM TIME <ms>]`.
pub async fn show_graph_stats(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: Option<String>,
    verbose: bool,
    as_of: Option<i64>,
) -> Result<Vec<DdlResult>, DdlError> {
    GRAPH_STATS_CALLS.fetch_add(1, Ordering::Relaxed);
    let scope = if collection.is_some() {
        "collection"
    } else {
        "tenant"
    };
    let _span = info_span!(
        "graph.stats",
        layer = DiagnosticLayer::WritePath.as_str(),
        tenant_id = identity.tenant_id.as_u64(),
        scope = scope,
        collection = ?collection,
        verbose,
        as_of = ?as_of,
    );

    // The counters reach the Data Plane through `broadcast_to_all_cores`, which
    // never runs the planner's authorization or RLS passes, so both are
    // resolved here. A counter carries no row for a filter to apply to, and it
    // counts the edges of rows a policy hides, so a read policy refuses. The
    // tenant-wide form names no collection to ask the narrow question about, so
    // it asks the tenant-wide one — and narrows its rows to the collections the
    // caller may actually read, below.
    let gate = RefusingReadGate::for_request(state, identity, database_id);
    match collection.as_deref() {
        Some(name) => gate.gate_collection(name, STATS_WHAT)?,
        None => gate.refuse_if_any_read_policy(STATS_TENANT_WIDE_WHAT)?,
    }

    // Validate the collection exists if a name was supplied. We resolve
    // through the same catalog path used by SHOW COLLECTIONS / DESCRIBE,
    // so the same not-found / inactive semantics apply.
    if let Some(ref name) = collection {
        super::support::ensure_collection_active(
            state,
            database_id,
            identity.tenant_id.as_u64(),
            name,
        )?;
    }

    // Exact logical-edge scans are required even for current-time reads:
    // source-owned summaries cannot deduplicate a destination shared by edges
    // from different source vShards, and mixed legacy/current collections may
    // have no summary row at all. Identity union below is the correctness path.
    let plan = PhysicalPlan::Graph(GraphOp::Stats {
        collection: collection
            .as_deref()
            .map(|name| nodedb_types::QualifiedCollection::new(database_id, name)),
        as_of: as_of.or(Some(i64::MAX)),
    });

    let resp = broadcast_to_all_cores(state, identity.tenant_id, database_id, plan, TraceId::ZERO)
        .await
        .map_err(|e| ddl_err("58000", format!("graph stats dispatch failed: {e}")))?;

    let merged: Vec<CollectionStats> = decode_merged_stats(resp.payload.as_bytes())
        .map_err(|e| ddl_err("XX000", format!("graph stats decode failed: {e}")))?;

    let aggregated = aggregate_by_collection(merged);

    // Tenant-wide (no collection name given): drop rows for collections that
    // have been soft-deactivated (plain `DROP COLLECTION` without PURGE).
    // Their edges/CSR/stats are still physically present until a hard purge,
    // but they must not surface in the merged tenant-wide result. Drop the
    // rows the caller holds no `Read` grant on for the same reason: an edge
    // count and a label name describe a collection this identity was never
    // authorized to read, and the tenant-wide form has no single collection to
    // refuse the whole request on.
    let aggregated = if collection.is_none() {
        let active =
            filter_active_collections(state, database_id, identity.tenant_id.as_u64(), aggregated);
        filter_readable_collections(&gate, active)
    } else {
        aggregated
    };

    if verbose {
        Ok(encode_verbose_response(aggregated))
    } else {
        Ok(encode_compact_response(aggregated))
    }
}

/// Decode the merged msgpack array produced by `broadcast_to_all_cores`.
fn decode_merged_stats(payload: &[u8]) -> crate::Result<Vec<CollectionStats>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: e.to_string(),
    })
}

/// Filter out `CollectionStats` rows for collections that are not currently
/// active in the catalog (soft-deleted via plain `DROP COLLECTION`). If the
/// catalog is unavailable, or a given collection has no catalog entry at
/// all (shouldn't normally happen for a row with live stats), the row is
/// dropped conservatively rather than leaked into the tenant-wide view.
fn filter_active_collections(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    rows: Vec<CollectionStats>,
) -> Vec<CollectionStats> {
    let catalog = state.credentials.catalog();
    let all_collections = catalog
        .load_collections_for_tenant(database_id, tenant_id)
        .unwrap_or_default();
    let active: std::collections::BTreeSet<&str> = all_collections
        .iter()
        .filter(|c| c.is_active)
        .map(|c| c.name.as_str())
        .collect();
    rows.into_iter()
        .filter(|r| active.contains(r.collection.as_str()))
        .collect()
}

/// Drop `CollectionStats` rows for collections the caller holds no `Read`
/// grant on.
///
/// Only the tenant-wide form needs this: the collection-scoped form already
/// failed closed on the one collection it names. A caller granted every
/// collection sees the identical row set it saw before.
fn filter_readable_collections(
    gate: &RefusingReadGate<'_>,
    rows: Vec<CollectionStats>,
) -> Vec<CollectionStats> {
    rows.into_iter()
        .filter(|row| gate.may_read(&row.collection))
        .collect()
}

/// Aggregate per-core `CollectionStats` entries by `collection` name, merging
/// label counts and re-deriving `distinct_label_count` from the merged set.
fn aggregate_by_collection(entries: Vec<CollectionStats>) -> Vec<CollectionStats> {
    let mut label_acc: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut historical_edges: BTreeMap<
        String,
        std::collections::BTreeSet<(String, String, String)>,
    > = BTreeMap::new();
    let mut by_name: BTreeMap<String, CollectionStats> = BTreeMap::new();

    for e in entries {
        let acc = by_name
            .entry(e.collection.clone())
            .or_insert_with(|| CollectionStats::zero(e.collection.clone()));
        acc.edge_count = acc.edge_count.saturating_add(e.edge_count);
        acc.distinct_node_count = acc
            .distinct_node_count
            .saturating_add(e.distinct_node_count);

        let labels = label_acc.entry(e.collection.clone()).or_default();
        for (label, count) in e.labels {
            let slot = labels.entry(label).or_insert(0);
            *slot = slot.saturating_add(count);
        }
        historical_edges
            .entry(e.collection)
            .or_default()
            .extend(e.logical_edges);
    }

    let mut result: Vec<CollectionStats> = Vec::with_capacity(by_name.len());
    for (collection, mut acc) in by_name {
        let exact_edges = historical_edges.remove(&collection).unwrap_or_default();
        if exact_edges.is_empty() {
            let labels_map = label_acc.remove(&collection).unwrap_or_default();
            acc.labels = labels_map.into_iter().collect();
        } else {
            let mut nodes = std::collections::BTreeSet::new();
            let mut labels = BTreeMap::<String, u64>::new();
            for (src, label, dst) in &exact_edges {
                nodes.insert(src);
                nodes.insert(dst);
                *labels.entry(label.clone()).or_default() += 1;
            }
            acc.edge_count = exact_edges.len() as u64;
            acc.distinct_node_count = nodes.len() as u64;
            acc.labels = labels.into_iter().collect();
            acc.logical_edges = exact_edges.into_iter().collect();
        }
        acc.distinct_label_count = acc.labels.len() as u64;
        result.push(acc);
    }
    result
}

fn encode_compact_response(rows: Vec<CollectionStats>) -> Vec<DdlResult> {
    let columns = vec![
        "collection".to_string(),
        "node_count".to_string(),
        "edge_count".to_string(),
        "distinct_label_count".to_string(),
        "labels".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let mut data_rows = Vec::with_capacity(rows.len());
    for r in rows {
        let labels_json = serde_json::Value::Array(
            r.labels
                .iter()
                .map(|(name, count)| {
                    let mut m = serde_json::Map::new();
                    m.insert("label".into(), serde_json::Value::String(name.clone()));
                    m.insert("count".into(), serde_json::Value::Number((*count).into()));
                    serde_json::Value::Object(m)
                })
                .collect(),
        )
        .to_string();

        let mut row = Map::new();
        row.insert("collection".to_string(), JsonValue::String(r.collection));
        row.insert(
            "node_count".to_string(),
            JsonValue::String((r.distinct_node_count as i64).to_string()),
        );
        row.insert(
            "edge_count".to_string(),
            JsonValue::String((r.edge_count as i64).to_string()),
        );
        row.insert(
            "distinct_label_count".to_string(),
            JsonValue::String((r.distinct_label_count as i64).to_string()),
        );
        row.insert("labels".to_string(), JsonValue::String(labels_json));
        data_rows.push(row);
    }

    vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        data_rows,
    ))]
}

fn encode_verbose_response(rows: Vec<CollectionStats>) -> Vec<DdlResult> {
    let columns = vec![
        "collection".to_string(),
        "label".to_string(),
        "edge_count".to_string(),
    ];
    let column_types = vec![DdlColType::Text, DdlColType::Text, DdlColType::Int8];

    let mut data_rows = Vec::new();
    for r in &rows {
        for (label, count) in &r.labels {
            let mut row = Map::new();
            row.insert(
                "collection".to_string(),
                JsonValue::String(r.collection.clone()),
            );
            row.insert("label".to_string(), JsonValue::String(label.clone()));
            row.insert(
                "edge_count".to_string(),
                JsonValue::String((*count as i64).to_string()),
            );
            data_rows.push(row);
        }
    }

    vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        data_rows,
    ))]
}
