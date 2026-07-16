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

use nodedb_types::TenantId;

use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

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
    /// validate edge CONTENT (not just the matched surrogate set) against the
    /// actual stored docs at execution time, closing the recon→execute TOCTOU
    /// on `_from`/`_to`/`_type`.
    pub surrogate: u32,
    pub from: String,
    pub to: String,
    pub label: Option<String>,
    /// The document's `weight` as stored, when present and finite. Carried so an
    /// UPDATE that moves or relabels the edge re-creates it with the SAME weight
    /// the matching INSERT mirrored — otherwise the re-created edge would silently
    /// revert to the default unit weight. `None` when absent or non-finite.
    pub weight: Option<f64>,
}

/// Result of the OLLP pre-execution reconnaissance scan.
///
/// `surrogates` is the sorted set of matched document surrogates used for OLLP
/// write-set verification (unchanged from the prior `Vec<u32>` return).
/// `edges` carries the implicit edges of any matched edge documents so their
/// auto-created graph edges can be cleaned up atomically in the same Calvin
/// transaction. Edge order is irrelevant; surrogates remain sorted.
pub struct PreexecScan {
    pub surrogates: Vec<u32>,
    pub edges: Vec<ScannedEdge>,
}

/// Dispatch a pre-execution scan for the given collection and serialized
/// filter bytes. Returns the sorted list of matching surrogate u32 values plus
/// the implicit edges of any matched edge documents.
///
/// When a gateway is wired (cluster mode), the scan is routed through
/// `gateway.execute` so it reaches the owning vshard leader — a bare local
/// data-plane dispatch on a coordinator that does not host the shard would
/// return an empty result, causing OLLP convergence to fail. In single-node
/// deployments (no gateway) the scan falls back to `dispatch_to_data_plane`.
///
/// Returns `Err` on dispatch failure (SPSC timeout, serialization error, etc.).
/// Returns an empty `PreexecScan` if no documents match.
pub async fn run_preexec_scan(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    filter_bytes: Vec<u8>,
) -> crate::Result<PreexecScan> {
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);

    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: collection.to_owned(),
        filters: filter_bytes,
        limit: usize::MAX,
        offset: 0,
        sort_keys: vec![],
        distinct: false,
        // `id` (the hex surrogate) is always included regardless of
        // projection. We additionally request `_from`/`_to`/`_type`/`weight` so
        // the recon scan can surface the implicit edge of any matched edge
        // document — its auto-created graph edge must be kept consistent in the
        // same Calvin txn, including its `weight` when the edge is moved or
        // relabeled.
        projection: vec![
            "_from".to_string(),
            "_to".to_string(),
            "_type".to_string(),
            "weight".to_string(),
        ],
        computed_columns: vec![],
        window_functions: vec![],
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });

    // Route the recon scan to the vshard's OWNER (leader/replica), not the
    // coordinator's local data plane. A bare `dispatch_to_data_plane` submits
    // to the LOCAL core and returns an empty result on any coordinator that
    // does not host the target shard — which silently breaks OLLP cross-node
    // (the predicted set comes back empty, so the dependent write never
    // converges). The gateway routes the read to the owning node exactly like
    // a normal `SELECT` and returns the same msgpack payload shape, so
    // `decode_scan` applies unchanged. In single-node deployments without a
    // gateway, fall back to the local data-plane dispatch.
    if let Some(gateway) = &shared.gateway {
        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: TraceId::ZERO,
            database_id,
            txn_id: None,
        };
        let payloads =
            gateway
                .execute(&gw_ctx, scan_plan)
                .await
                .map_err(|e| crate::Error::Storage {
                    engine: "preexec-scan".into(),
                    detail: format!("pre-execution scan failed: {e}"),
                })?;
        // A single-collection scan routes to one vshard → one payload. An
        // absent payload means zero matching rows.
        let payload = payloads.into_iter().next().unwrap_or_default();
        return Ok(decode_scan(&payload));
    }

    let response = dispatch_to_data_plane(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        scan_plan,
        TraceId::ZERO,
    )
    .await?;

    if response.status != crate::bridge::envelope::Status::Ok {
        return Err(crate::Error::Storage {
            engine: "preexec-scan".into(),
            detail: format!("pre-execution scan failed: {:?}", response.error_code),
        });
    }

    Ok(decode_scan(&response.payload))
}

/// Decode the msgpack scan response payload into a sorted list of surrogate u32
/// values plus the implicit edges of any matched edge documents.
///
/// Each row in the response is a msgpack map with an `id` field whose value is
/// an 8-character lowercase hex string encoding the document's u32 surrogate
/// (e.g. `"0000002a"` → `42u32`). Rows whose `id` cannot be parsed are silently
/// skipped for the surrogate set — they are legacy non-surrogate documents that
/// predate the surrogate-keyed storage format and do not participate in OLLP
/// verification.
///
/// Additionally, for any row carrying BOTH `_from` and `_to` as strings, an
/// implicit [`ScannedEdge`] is recorded (with the raw `_type` as `label`, or
/// `None`). Rows without both fields are not edges — their surrogate is still
/// extracted, but no edge is recorded.
///
/// The surrogate output is sorted ascending so the comparison with
/// `ollp_predicted_surrogates` in the executor is a simple equality check on
/// sorted slices. Edge order is irrelevant.
fn decode_scan(payload: &[u8]) -> PreexecScan {
    if payload.is_empty() {
        return PreexecScan {
            surrogates: vec![],
            edges: vec![],
        };
    }

    // Transcode msgpack → JSON string and parse the fields.
    // This avoids introducing a zerompk-level partial decode dependency
    // into the Control Plane layer: we let the existing transcoder convert
    // the payload to a JSON array, then pull the fields out per row.
    let json_str = nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned());

    decode_scan_json(&json_str)
}

/// Pure decode of the transcoded JSON-array scan payload. Split from
/// [`decode_scan`] so it is unit-testable without hand-rolling msgpack.
fn decode_scan_json(json_str: &str) -> PreexecScan {
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};

    let mut surrogates = Vec::new();
    let mut edges = Vec::new();

    if let Ok(rows) = sonic_rs::from_str::<sonic_rs::Value>(json_str)
        && rows.is_array()
    {
        for row in rows.as_array().into_iter().flatten() {
            // Parse the row's surrogate ONCE and reuse it for both the
            // surrogate set and the edge record. A row whose `id` is not a
            // parseable 8-hex surrogate is a legacy non-surrogate document: it
            // is excluded from the surrogate set AND cannot be a surrogate-keyed
            // edge doc, so any edge it carries is skipped too.
            let surrogate = row
                .get("id")
                .and_then(|id_val| id_val.as_str())
                .filter(|id_str| id_str.len() == 8)
                .and_then(|id_str| u32::from_str_radix(id_str, 16).ok());

            if let Some(surrogate) = surrogate {
                surrogates.push(surrogate);
            }

            // The raw-document scan encoder nests the document's user fields
            // under a `data` object alongside the top-level `id`
            // (`encode_raw_document_rows`): `{"id": "..", "data": {..fields..}}`.
            // An edge document carries BOTH `_from` and `_to` as strings inside
            // `data`.
            let data = row.get("data");
            let from = data
                .as_ref()
                .and_then(|d| d.get("_from"))
                .and_then(|v| v.as_str());
            let to = data
                .as_ref()
                .and_then(|d| d.get("_to"))
                .and_then(|v| v.as_str());
            // Only record an edge when the row is BOTH a surrogate-keyed doc
            // AND carries both endpoints — content-drift validation keys edges
            // by surrogate, so an unparseable-id edge row is not a real edge.
            if let (Some(surrogate), Some(from), Some(to)) = (surrogate, from, to) {
                let label = data
                    .as_ref()
                    .and_then(|d| d.get("_type"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                // A finite numeric `weight` mirrors the doc's mirrored edge
                // weight; non-finite / absent / non-numeric → `None` (unit
                // weight). `as_f64` covers both integer and float JSON numbers.
                let weight = data
                    .as_ref()
                    .and_then(|d| d.get("weight"))
                    .and_then(|v| v.as_f64())
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
    }

    surrogates.sort_unstable();
    PreexecScan { surrogates, edges }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_empty_payload_returns_empty() {
        let scan = decode_scan(&[]);
        assert!(scan.surrogates.is_empty());
        assert!(scan.edges.is_empty());
    }

    #[test]
    fn decode_json_extracts_surrogates_and_edges() {
        // Two edge rows + one non-edge row. `id` is the 8-hex surrogate; an edge
        // row additionally carries `_from`/`_to` (and optionally `_type`).
        let json = r#"[
            {"id":"0000002a","data":{"_from":"a","_to":"b","_type":"ROAD","weight":5.0}},
            {"id":"0000000b","data":{"_from":"c","_to":"d"}},
            {"id":"00000001","data":{"name":"alice"}}
        ]"#;
        let scan = decode_scan_json(json);

        // Surrogates: all three rows parse; sorted ascending.
        assert_eq!(scan.surrogates, vec![1, 11, 42]);

        // Edges: only the two rows with BOTH _from and _to.
        assert_eq!(scan.edges.len(), 2);
        let road = scan
            .edges
            .iter()
            .find(|e| e.from == "a")
            .expect("edge a->b present");
        assert_eq!(road.to, "b");
        assert_eq!(road.label.as_deref(), Some("ROAD"));
        // The edge carries the document's surrogate (id "0000002a" → 42).
        assert_eq!(road.surrogate, 42);
        // A finite numeric `weight` is surfaced so a moved/relabeled edge keeps
        // it.
        assert_eq!(road.weight, Some(5.0));
        let untyped = scan
            .edges
            .iter()
            .find(|e| e.from == "c")
            .expect("edge c->d present");
        assert_eq!(untyped.to, "d");
        assert_eq!(untyped.label, None);
        // id "0000000b" → 11.
        assert_eq!(untyped.surrogate, 11);
        // No `weight` field → `None` (unit weight).
        assert_eq!(untyped.weight, None);
    }

    #[test]
    fn decode_json_row_without_both_endpoints_is_not_an_edge() {
        // `_from` present but no `_to` → surrogate extracted, no edge recorded.
        // The fixture must use the wire shape emitted by `encode_raw_document_rows`:
        // `{"id": "..", "data": {..fields..}}`.  A top-level `_from` would test
        // the "no `data` field" branch instead of the intended one.
        let json = r#"[{"id":"00000005","data":{"_from":"x"}}]"#;
        let scan = decode_scan_json(json);
        assert_eq!(scan.surrogates, vec![5]);
        assert!(scan.edges.is_empty());
    }

    // Format-coupled coverage of the surrogate decode lives in
    // `tests/executor_tests/test_ollp_verification.rs`, which exercises the
    // decoder against real scan-response payloads emitted by the Data Plane.
    // Keeping a hand-rolled msgpack mock in this unit test would duplicate
    // the wire format and break on every response_codec change.
}
