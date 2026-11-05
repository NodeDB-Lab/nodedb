// SPDX-License-Identifier: BUSL-1.1

//! Optimistic pre-execution scan for OLLP dependent-read transactions.
//!
//! Before submitting a `BulkUpdate` or `BulkDelete` via the Calvin
//! dependent-read path, the Control Plane runs this scan to collect the set of
//! document surrogates that currently match the predicate. That set is passed
//! as `initial_predicted` to `run_dependent_with_retry` and embedded as
//! `ollp_predicted_surrogates` in the `BulkUpdate`/`BulkDelete` plan via the
//! `submit` closure. The active executor verifies the set at admission time and
//! returns `ErrorCode::OllpRetryRequired` on mismatch — without writing.
//!
//! # Determinism
//!
//! This function runs on the Control Plane (Tokio) and does not touch WAL
//! bytes. The returned surrogate list is sorted before returning so the
//! comparison in the executor is order-independent. No `SystemTime::now()`,
//! no unseeded RNG, no `HashMap` iteration order dependency.

use nodedb_types::{
    DEFAULT_IDENTITY_COLUMN, ROWID_COLUMN, RowIdentity, StorageKey, Surrogate, TenantId, Value,
};

use super::dependent_recon_node_edges::NodeIncidentEdges;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, TxnId};
use nodedb_physical::physical_plan::{DocumentOp, OllpPredictedEdge, PhysicalPlan};

/// One implicit graph edge surfaced from the pre-execution reconnaissance scan.
///
/// When a schemaless document carrying `_from`/`_to` is matched by a predicate
/// `DELETE`, the implicit edge auto-created for it on INSERT must be deleted in
/// the SAME Calvin transaction. The recon scan surfaces the OLD `_from`/`_to`
/// (and raw `_type`) of every matched edge document so the delete-side helper
/// can emit the symmetric `GraphOp::EdgeDelete`.
///
/// `label` carries the raw `_type` exactly as stored (or `None` when absent);
/// the default-label substitution is applied by the delete helper so it matches
/// the label the matching INSERT used.
#[derive(Clone)]
pub struct ScannedEdge {
    /// Surrogate of the edge DOCUMENT (the `_from`/`_to`-carrying schemaless
    /// doc), parsed from the row's `id`. Carried so the data plane can
    /// validate edge CONTENT (not only the matched surrogate set) against the
    /// actual stored docs at execution time, closing the recon→execute TOCTOU
    /// on `_from`/`_to`/`_type`.
    pub surrogate: u32,
    pub from: String,
    pub to: String,
    pub label: Option<String>,
    /// The document's `weight` as stored, when present and finite. Carried so an
    /// UPDATE that moves or relabels the edge re-creates it with the SAME weight
    /// the matching INSERT mirrored — otherwise the re-created edge will silently
    /// revert to the default unit weight. `None` when absent or non-finite.
    pub weight: Option<f64>,
}

/// The plan-carried prediction of a scanned edge: the edge document's
/// surrogate and its mirrored `_from`/`_to`/`_type`.
impl From<&ScannedEdge> for OllpPredictedEdge {
    fn from(edge: &ScannedEdge) -> Self {
        Self {
            surrogate: edge.surrogate,
            from: edge.from.clone(),
            to: edge.to.clone(),
            label: edge.label.clone(),
        }
    }
}

/// Whether the recon scan reads each matched row's identity, and which
/// column holds it.
#[derive(Clone, Copy)]
pub enum RowIdentityRead<'a> {
    /// The caller needs no row identity.
    Skip,
    /// Read the identity a delete keys the row's graph node by: the declared
    /// primary key column, else `id`, else `_rowid`, else the surrogate.
    Read {
        declared_primary_key: Option<&'a str>,
    },
}

/// Result of the OLLP pre-execution reconnaissance scan.
///
/// `surrogates` is the sorted set of matched document surrogates used for OLLP
/// write-set verification. `edges` carries the implicit edges of any matched
/// edge documents so their auto-created graph edges can be cleaned up
/// atomically in the same Calvin transaction. `identities` holds each matched
/// row's identity, in surrogate order, when the scan read it. `node_edges`
/// holds the live edges incident on each matched row's node, when the caller
/// read them. Edge order is irrelevant; surrogates remain sorted.
#[derive(Default)]
pub struct PreexecScan {
    pub surrogates: Vec<u32>,
    pub edges: Vec<ScannedEdge>,
    pub identities: Vec<String>,
    pub node_edges: Vec<NodeIncidentEdges>,
}

/// What a pre-execution scan reads.
pub struct PreexecRequest<'a> {
    /// The database-qualified collection name.
    pub collection: &'a str,
    /// Serialized filter predicates. Empty matches every row.
    pub filters: Vec<u8>,
    /// When `Some`, only rows whose surrogate the bitmap holds.
    pub prefilter: Option<nodedb_types::SurrogateBitmap>,
    pub identity: RowIdentityRead<'a>,
    /// The session transaction whose staged rows the scan also sees. `None`
    /// outside a transaction block.
    pub txn_id: Option<TxnId>,
    /// The scope the scan reads at: `Current` outside a block, the block's
    /// `TxnSnapshot` for a write the block stages.
    pub system_time: nodedb_types::SystemTimeScope,
}

/// Dispatch a pre-execution scan for `request`. Returns the sorted list of
/// matching surrogate u32 values plus the implicit edges of any matched edge
/// documents, and each row's identity when `request.identity` asks for it.
///
/// The scan is routed through the gateway so it reaches the owning vshard
/// leader — a bare local data-plane dispatch on a coordinator that does not
/// host the shard will return an empty result, causing OLLP convergence to
/// fail.
///
/// Returns `Err` on dispatch failure (SPSC timeout, serialization error, etc.).
/// Returns an empty `PreexecScan` if no documents match.
pub async fn run_preexec_scan(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    request: PreexecRequest<'_>,
) -> crate::Result<PreexecScan> {
    let PreexecRequest {
        collection,
        filters: filter_bytes,
        prefilter,
        identity,
        txn_id,
        system_time,
    } = request;
    // `id` (the hex surrogate) is always included regardless of projection.
    // `_from`/`_to`/`_type`/`weight` surface the implicit edge of any matched
    // edge document — its auto-created graph edge must be kept consistent in
    // the same Calvin txn, including its `weight` when the edge is moved or
    // relabeled. A delete also projects the columns the row's identity is
    // read from, so the Data Plane returns their stored values.
    let mut projection = vec![
        "_from".to_string(),
        "_to".to_string(),
        "_type".to_string(),
        "weight".to_string(),
    ];
    if let RowIdentityRead::Read {
        declared_primary_key,
    } = identity
    {
        projection.push(
            declared_primary_key
                .unwrap_or(DEFAULT_IDENTITY_COLUMN)
                .to_string(),
        );
        projection.push(ROWID_COLUMN.to_string());
    }
    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        // `collection` is the dependent plan's database-qualified name.
        collection: nodedb_types::QualifiedCollection::from_stored(
            database_id,
            collection.to_string(),
        )?,
        filters: filter_bytes,
        limit: usize::MAX,
        offset: 0,
        sort_keys: vec![],
        distinct: false,
        projection,
        computed_columns: vec![],
        window_functions: vec![],
        system_time,
        valid_at_ms: None,
        prefilter,
    });

    // Route the recon scan to the vshard's OWNER (leader/replica), not the
    // coordinator's local data plane. A bare `dispatch_to_data_plane` submits
    // to the LOCAL core and returns an empty result on any coordinator that
    // does not host the target shard — which silently breaks OLLP cross-node
    // (the predicted set comes back empty, so the dependent write never
    // converges). The gateway routes the read to the owning node exactly like
    // a normal `SELECT` and returns the same msgpack payload shape, so
    // `decode_scan` applies unchanged.
    let gateway = shared.installed_gateway()?;
    let gw_ctx = crate::control::gateway::core::QueryContext {
        tenant_id,
        trace_id: TraceId::ZERO,
        database_id,
        txn_id,
        linearizable: true,
    };
    // A shard verdict keeps its own typed error.
    let payloads = gateway.execute_internal(&gw_ctx, scan_plan).await?;
    // A single-collection scan routes to one vshard → one payload. An
    // absent payload means zero matching rows.
    let payload = payloads.into_iter().next().unwrap_or_default();
    decode_scan(&payload, identity)
}

/// Decode the msgpack scan response payload into a sorted list of surrogate u32
/// values plus the implicit edges of any matched edge documents.
///
/// Each row in the response is a msgpack map `{"id": .., "data": {..}}`
/// (`encode_raw_document_rows`). `id` is an 8-character lowercase hex string
/// encoding the document's u32 surrogate (e.g. `"0000002a"` → `42u32`). Rows
/// whose `id` is no storage key carry no surrogate. They are skipped and do
/// not participate in OLLP verification.
///
/// Additionally, for any row carrying BOTH `_from` and `_to` as strings, an
/// implicit [`ScannedEdge`] is recorded (with the raw `_type` as `label`, or
/// `None`). Rows without both fields are not edges — their surrogate is still
/// extracted, but no edge is recorded.
///
/// The surrogate output is sorted ascending so the comparison with
/// `ollp_predicted_surrogates` in the executor is a simple equality check on
/// sorted slices. Identities follow the same order. Edge order is irrelevant.
fn decode_scan(payload: &[u8], identity: RowIdentityRead<'_>) -> crate::Result<PreexecScan> {
    if payload.is_empty() {
        return Ok(PreexecScan::default());
    }
    let rows =
        nodedb_types::value_from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("OLLP reconnaissance scan payload: {e}"),
        })?;
    Ok(decode_rows(&rows, identity))
}

/// Decode the rows of a scan response. Split from [`decode_scan`] so tests
/// build rows as values.
fn decode_rows(rows: &Value, identity: RowIdentityRead<'_>) -> PreexecScan {
    let mut matched: Vec<(u32, Option<String>)> = Vec::new();
    let mut edges = Vec::new();
    let Value::Array(rows) = rows else {
        return PreexecScan::default();
    };
    for row in rows {
        let Value::Object(row) = row else {
            continue;
        };
        // A row whose `id` is not a parseable 8-hex storage key carries no
        // surrogate: it is excluded from the surrogate set AND cannot be a
        // surrogate-keyed edge doc, so any edge it carries is skipped too.
        let Some(surrogate) = row
            .get("id")
            .and_then(Value::as_str)
            .and_then(StorageKey::parse)
            .map(|key| key.surrogate().as_u32())
        else {
            continue;
        };
        let data = row.get("data");
        let field = |name: &str| match data {
            Some(Value::Object(fields)) => fields.get(name),
            _ => None,
        };
        let row_identity = match identity {
            RowIdentityRead::Skip => None,
            RowIdentityRead::Read {
                declared_primary_key,
            } => Some(
                RowIdentity::of_row_value(
                    data.unwrap_or(&Value::Null),
                    declared_primary_key,
                    StorageKey::for_surrogate(Surrogate::new(surrogate)),
                )
                .into_string(),
            ),
        };
        matched.push((surrogate, row_identity));

        // An edge document carries BOTH `_from` and `_to` as strings inside
        // `data`.
        let from = field("_from").and_then(Value::as_str);
        let to = field("_to").and_then(Value::as_str);
        if let (Some(from), Some(to)) = (from, to) {
            let label = field("_type").and_then(Value::as_str).map(str::to_string);
            // A finite numeric `weight` mirrors the doc's mirrored edge
            // weight; non-finite / absent / non-numeric → `None` (unit
            // weight).
            let weight = field("weight")
                .and_then(|v| match v {
                    Value::Float(w) => Some(*w),
                    Value::Integer(w) => Some(*w as f64),
                    _ => None,
                })
                .filter(|w| w.is_finite());
            edges.push(ScannedEdge {
                surrogate,
                from: from.to_string(),
                to: to.to_string(),
                label,
                weight,
            });
        }
    }

    matched.sort_unstable_by_key(|(surrogate, _)| *surrogate);
    let surrogates = matched.iter().map(|(surrogate, _)| *surrogate).collect();
    let identities = matched
        .into_iter()
        .filter_map(|(_, identity)| identity)
        .collect();
    PreexecScan {
        surrogates,
        edges,
        identities,
        node_edges: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn row(id: &str, fields: &[(&str, Value)]) -> Value {
        let data: HashMap<String, Value> = fields
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect();
        Value::Object(HashMap::from([
            ("id".to_string(), Value::String(id.to_string())),
            ("data".to_string(), Value::Object(data)),
        ]))
    }

    fn text(s: &str) -> Value {
        Value::String(s.to_string())
    }

    #[test]
    fn decode_empty_payload_returns_empty() {
        let scan = decode_scan(&[], RowIdentityRead::Skip).expect("decode");
        assert!(scan.surrogates.is_empty());
        assert!(scan.edges.is_empty());
        assert!(scan.identities.is_empty());
    }

    #[test]
    fn decode_extracts_surrogates_and_edges() {
        // Two edge rows + one non-edge row. `id` is the 8-hex surrogate; an edge
        // row additionally carries `_from`/`_to` (and optionally `_type`).
        let rows = Value::Array(vec![
            row(
                "0000002a",
                &[
                    ("_from", text("a")),
                    ("_to", text("b")),
                    ("_type", text("ROAD")),
                    ("weight", Value::Float(5.0)),
                ],
            ),
            row("0000000b", &[("_from", text("c")), ("_to", text("d"))]),
            row("00000001", &[("name", text("alice"))]),
        ]);
        let scan = decode_rows(&rows, RowIdentityRead::Skip);

        // Surrogates: all three rows parse; sorted ascending.
        assert_eq!(scan.surrogates, vec![1, 11, 42]);
        assert!(scan.identities.is_empty());

        // Edges: only the two rows with BOTH _from and _to.
        assert_eq!(scan.edges.len(), 2);
        let road = scan
            .edges
            .iter()
            .find(|e| e.from == "a")
            .expect("edge a->b present");
        assert_eq!(road.to, "b");
        assert_eq!(road.label.as_deref(), Some("ROAD"));
        assert_eq!(road.surrogate, 42);
        assert_eq!(road.weight, Some(5.0));
        let untyped = scan
            .edges
            .iter()
            .find(|e| e.from == "c")
            .expect("edge c->d present");
        assert_eq!(untyped.to, "d");
        assert_eq!(untyped.label, None);
        assert_eq!(untyped.surrogate, 11);
        assert_eq!(untyped.weight, None);
    }

    #[test]
    fn decode_row_without_both_endpoints_is_not_an_edge() {
        let rows = Value::Array(vec![row("00000005", &[("_from", text("x"))])]);
        let scan = decode_rows(&rows, RowIdentityRead::Skip);
        assert_eq!(scan.surrogates, vec![5]);
        assert!(scan.edges.is_empty());
    }

    #[test]
    fn decode_reads_the_identity_a_delete_keys_the_node_by() {
        let rows = Value::Array(vec![
            row(
                "00000009",
                &[("sku", Value::Integer(42)), ("id", text("x"))],
            ),
            row("00000003", &[("_rowid", Value::Integer(3))]),
            row("00000007", &[]),
        ]);
        let declared = decode_rows(
            &rows,
            RowIdentityRead::Read {
                declared_primary_key: Some("sku"),
            },
        );
        assert_eq!(declared.surrogates, vec![3, 7, 9]);
        // Sorted with the surrogates: `_rowid`, then the decimal surrogate,
        // then the declared key.
        assert_eq!(declared.identities, vec!["3", "7", "42"]);

        let by_id = decode_rows(
            &rows,
            RowIdentityRead::Read {
                declared_primary_key: None,
            },
        );
        assert_eq!(by_id.identities, vec!["3", "7", "x"]);
    }

    #[test]
    fn a_payload_that_is_not_msgpack_is_an_error() {
        assert!(decode_scan(&[0xc1], RowIdentityRead::Skip).is_err());
    }

    // Format-coupled coverage of the surrogate decode lives in
    // `tests/executor_tests/test_ollp_verification.rs`, which exercises the
    // decoder against real scan-response payloads emitted by the Data Plane.
}
