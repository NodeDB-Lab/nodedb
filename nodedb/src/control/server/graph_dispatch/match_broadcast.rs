// SPDX-License-Identifier: BUSL-1.1

//! MATCH-specific cross-core broadcast that unwraps the DP→CP `{rows, frontier}`
//! envelope.
//!
//! The Data Plane MATCH handlers (`execute_graph_match` /
//! `execute_graph_match_continuation`) encode each core's result as a 2-field
//! msgpack map:
//!
//! ```text
//! { "rows": <rows msgpack array>, "frontier": <frontier msgpack array> }
//! ```
//!
//! The generic `gather_all_cores` / `broadcast_to_all_cores` primitives treat
//! the whole payload as a BARE msgpack array of row elements, which will
//! mis-merge this map. This module mirrors `gather_all_cores`'s per-core SPSC
//! fan-out (eager dispatch → `join_all`, NotFound-tolerant) but, for each core,
//! it DECODES the envelope and:
//!
//! - merges the per-core `rows` subfields into a single bare msgpack array
//!   (the SAME shape `match_payload_to_response` already expects), and
//! - UNIONs every core's `frontier` entries into one `Vec<UnresolvedExpansion>`
//!   for cross-shard continuation dispatch (consumed in B2).
//!
//! On a fully-local CSR every core's frontier is empty, so the returned rows
//! payload is byte-identical to the prior bare-array gather and single-node
//! client behaviour is unchanged.
//!
//! This is MATCH-only: the generic gather primitives are left untouched for all
//! other plan types.

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response};
use crate::control::server::exchange::core_outcome::{
    classify_core_response, reported_versions, require_every_core,
};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::data::executor::handlers::graph_match::{
    MATCH_ENVELOPE_FRONTIER_KEY, MATCH_ENVELOPE_RESUME_KEY, MATCH_ENVELOPE_ROWS_KEY,
};
use crate::engine::graph::pattern::executor::{UnresolvedExpansion, VarLenResume};
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};
use nodedb_query::msgpack_scan::reader::{map_header, read_str_advance, skip_value};

/// How a graph read runs.
#[derive(Debug, Clone, Copy)]
pub struct GraphRead {
    /// The session transaction whose staged edge overlay the read merges.
    /// `None` for autocommit.
    pub txn_id: Option<TxnId>,
    /// The read observes every write committed before it began: each node
    /// that serves part of it confirms its groups first.
    pub linearizable: bool,
}

/// Result of a MATCH cross-core broadcast after envelope unwrapping.
pub struct MatchBroadcastOutcome {
    /// Merged binding rows as a single BARE msgpack array — the exact shape
    /// `match_payload_to_response` decodes (byte-identical to the prior
    /// bare-array gather for single-node / empty-frontier results).
    pub rows_payload: Payload,
    /// Union of every core's cross-shard frontier entries. Empty on a
    /// fully-local CSR. Consumed by B2 cross-shard continuation dispatch.
    pub frontier: Vec<UnresolvedExpansion>,
    /// Union of every core's variable-length truncation resume cursors. Empty
    /// when nothing truncated. Each capping core contributes its own cursor, so
    /// this is a `Vec` — a single node fanned across N cores can truncate on
    /// several cores at once and ALL their cursors must survive (the round loop
    /// re-dispatches each independently). Carried onto the cross-node wire by
    /// `encode_match_envelope_raw` so remote truncation survives.
    pub resume: Vec<VarLenResume>,
    /// `true` if any core returned a partial (truncated) result.
    pub partial: bool,
    /// The highest watermark any core served the MATCH at.
    pub watermark_lsn: crate::types::Lsn,
    /// The versions every core reported, one per vShard it holds.
    pub read_versions: crate::types::ReadVersions,
}

/// Locate a top-level map value by key in a msgpack map payload.
///
/// Returns the raw msgpack bytes of the value (a complete, self-contained
/// msgpack value) for the first matching key, or `None` if the payload is not
/// a map or the key is absent.
fn map_value_raw<'a>(payload: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let (count, mut pos) = map_header(payload, 0)?;
    for _ in 0..count {
        let k = read_str_advance(payload, &mut pos)?;
        let val_start = pos;
        let val_end = skip_value(payload, pos)?;
        if k == key {
            return Some(&payload[val_start..val_end]);
        }
        pos = val_end;
    }
    None
}

/// Decoded fields of a single Data-Plane MATCH `{rows, frontier, resume}` envelope.
///
/// Returned by [`decode_match_envelope`] and [`unwrap_match_envelope`] so callers
/// can address fields by name instead of destructuring a positional 3-tuple.
pub(crate) struct DecodedMatchEnvelope {
    /// Raw msgpack bytes of each binding row (one element per row), ready to be
    /// merged via [`encode_msgpack_array`].
    pub(crate) row_elements: Vec<Vec<u8>>,
    /// Cross-shard frontier entries decoded from the `frontier` map value.
    pub(crate) frontier: Vec<UnresolvedExpansion>,
    /// Variable-length truncation resume cursors decoded from the optional
    /// `resume` map value. Empty when the key is absent (backward-compat with
    /// pre-resume 2-key envelopes from older peers).
    pub(crate) resume: Vec<VarLenResume>,
}

/// Decode one core's `{rows, frontier, resume}` envelope.
///
/// The `resume` key is OPTIONAL on decode: an older-version peer (or the
/// historic 2-key envelope) that omits it decodes to an empty cursor vec, so a
/// mixed-version cluster never breaks. `rows` and `frontier` remain mandatory.
///
/// Malformed bytes (not a map, missing mandatory keys, undecodable frontier or
/// resume) surface as a typed [`crate::Error`] rather than a panic.
fn decode_match_envelope(payload: &[u8]) -> crate::Result<DecodedMatchEnvelope> {
    let rows_bytes =
        map_value_raw(payload, MATCH_ENVELOPE_ROWS_KEY).ok_or_else(|| crate::Error::Codec {
            detail: "match envelope: missing or malformed 'rows' field".into(),
        })?;
    let frontier_bytes =
        map_value_raw(payload, MATCH_ENVELOPE_FRONTIER_KEY).ok_or_else(|| crate::Error::Codec {
            detail: "match envelope: missing or malformed 'frontier' field".into(),
        })?;

    let row_elements = extract_msgpack_elements(rows_bytes);
    let frontier: Vec<UnresolvedExpansion> =
        zerompk::from_msgpack(frontier_bytes).map_err(|e| crate::Error::Codec {
            detail: format!("match envelope: invalid frontier: {e}"),
        })?;
    // Backward-compat: a missing `resume` key (older peer / pre-resume 2-key
    // envelope) decodes to an empty cursor vec, never an error.
    let resume: Vec<VarLenResume> = match map_value_raw(payload, MATCH_ENVELOPE_RESUME_KEY) {
        Some(resume_bytes) => {
            zerompk::from_msgpack(resume_bytes).map_err(|e| crate::Error::Codec {
                detail: format!("match envelope: invalid resume: {e}"),
            })?
        }
        None => Vec::new(),
    };
    Ok(DecodedMatchEnvelope {
        row_elements,
        frontier,
        resume,
    })
}

/// Decoded fields from [`unwrap_match_envelope`], with `rows` already merged
/// into a single bare msgpack array ready for the MATCH consumer.
pub struct UnwrappedMatchEnvelope {
    /// Merged binding rows as a single BARE msgpack array — the exact shape
    /// `match_payload_to_response` decodes (byte-identical to the prior
    /// bare-array response for single-node / empty-frontier results).
    pub(crate) rows_payload: Payload,
    /// Cross-shard frontier entries decoded from the envelope.
    pub(crate) frontier: Vec<UnresolvedExpansion>,
    /// Variable-length truncation resume cursors. Empty when nothing truncated
    /// or when the envelope is from a pre-resume peer (backward-compat).
    pub(crate) resume: Vec<VarLenResume>,
}

/// Unwrap a SINGLE Data-Plane MATCH `{rows, frontier, resume}` envelope payload
/// into a bare rows msgpack array plus its frontier entries and resume cursors.
///
/// Used by surfaces that dispatch a MATCH plan to one shard rather than
/// fanning out to all cores (e.g. the native protocol's direct-op path), so
/// they can recover the same bare rows array shape every MATCH consumer
/// expected before the envelope existed. On a single-node / empty-frontier
/// MATCH the returned rows payload is byte-identical to the prior bare-array
/// response. The cross-node scatter reads the returned `resume` cursors so a
/// remote shard's truncation lands in the coordinator instead of being dropped.
///
/// Malformed bytes surface a typed [`crate::Error`], never a panic. An empty
/// payload (e.g. a successful op with no result) passes through unchanged.
pub fn unwrap_match_envelope(payload: &Payload) -> crate::Result<UnwrappedMatchEnvelope> {
    if payload.is_empty() {
        return Ok(UnwrappedMatchEnvelope {
            rows_payload: payload.clone(),
            frontier: Vec::new(),
            resume: Vec::new(),
        });
    }
    let decoded = decode_match_envelope(payload.as_ref())?;
    let merged_rows = encode_msgpack_array(&decoded.row_elements);
    Ok(UnwrappedMatchEnvelope {
        rows_payload: Payload::from_vec(merged_rows),
        frontier: decoded.frontier,
        resume: decoded.resume,
    })
}

/// Fan a MATCH plan to every Data-Plane core, unwrap each core's
/// `{rows, frontier}` envelope, and merge the results.
///
/// `read.linearizable` is false when the read accepts this replica as it is,
/// or when the caller has already confirmed it (a leg received from another
/// node, confirmed by `exec_receiver::read_leg`).
///
/// Mirrors `exchange::gather::gather_all_cores`'s eager per-core dispatch +
/// `join_all` collection and its strict per-core merge, but unwraps the MATCH
/// envelope per core instead of treating the payload as a bare row array.
pub async fn broadcast_match_to_all_cores(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    read: GraphRead,
) -> crate::Result<MatchBroadcastOutcome> {
    let GraphRead {
        txn_id,
        linearizable,
    } = read;
    // A linearizable read confirms the groups the plan reads first. A MATCH
    // walks the local CSR, so that is every group this node replicates. A
    // no-op without a cluster.
    if linearizable {
        super::read_groups::confirm_graph_read(state, database_id, &plan).await?;
    }

    // Shared broadcast call counter (parity with the generic gather path).
    crate::control::server::broadcast::broadcast_call_count_increment();

    // The running statement's deadline — the same instant
    // `eager_dispatch_to_all_cores` stamps on every per-core envelope below, so
    // the Control-Plane wait and the Data-Plane execution expire together.
    let deadline = crate::control::server::shared::session::statement_deadline(
        state.tuning.network.default_deadline_secs,
    );

    // Eager dispatch: register a tracker receiver and dispatch to each core
    // BEFORE awaiting any response, matching gather_all_cores' true-parallelism
    // prologue. `txn_id` (the caller's active session transaction, if any) is
    // stamped on each core's request so the Data-Plane MATCH handler resolves
    // the transaction's `GraphTxnOverlay` for read-your-own-writes; `None` is
    // the autocommit path (committed-CSR-only), byte-identical to before.
    let receivers =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, txn_id, |_| {
            plan.clone()
        })?;

    // Await all cores in parallel, draining the full bounded response per core
    // (a core's result can stream as several Partial frames before its terminal
    // frame).
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let response_futures = receivers
        .into_iter()
        .map(|(core_id, request_id, mut rx)| async move {
            let context = format!("match gather on core {core_id}");
            crate::control::local_dispatch::collect_under_deadline(
                &mut rx,
                crate::control::local_dispatch::DeadlineCollect {
                    request_id,
                    deadline,
                    max_result_bytes,
                    context: &context,
                },
            )
            .await
        });

    let results: Vec<crate::Result<Response>> = join_all(response_futures).await;
    let read_versions = reported_versions(&results);
    // `NotFound` is an empty CSR slice on that core. Any other core error fails
    // the MATCH: rows from the surviving cores are not the full answer.
    let answered = require_every_core(results.into_iter().map(classify_core_response))?;

    let mut all_row_elements: Vec<Vec<u8>> = Vec::new();
    let mut frontier: Vec<UnresolvedExpansion> = Vec::new();
    let mut resume: Vec<VarLenResume> = Vec::new();
    let mut partial = false;
    let mut watermark_lsn = crate::types::Lsn::ZERO;

    for resp in answered {
        if resp.partial {
            partial = true;
        }
        watermark_lsn = watermark_lsn.max(resp.watermark_lsn);

        if resp.payload.is_empty() {
            continue;
        }

        let mut decoded = decode_match_envelope(resp.payload.as_ref())?;
        all_row_elements.append(&mut decoded.row_elements);
        frontier.append(&mut decoded.frontier);
        // Union every capping core's resume cursor — a node fanned across cores
        // can truncate on more than one, and each cursor must survive for the
        // round loop to re-dispatch independently. Never keep only one.
        resume.append(&mut decoded.resume);
    }

    let merged_rows = encode_msgpack_array(&all_row_elements);

    Ok(MatchBroadcastOutcome {
        rows_payload: Payload::from_vec(merged_rows),
        frontier,
        resume,
        partial,
        watermark_lsn,
        read_versions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::graph_match::{
        encode_match_envelope, encode_match_envelope_raw,
    };
    use crate::engine::graph::pattern::executor::{BindingRow, UnresolvedExpansion, VarLenResume};
    use nodedb_query::msgpack_scan::writer::{write_kv_raw, write_map_header};

    fn row(pairs: &[(&str, &str)]) -> BindingRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn resume_cursor() -> VarLenResume {
        VarLenResume {
            triple_idx: 2,
            source_row: row(&[("a", "alice"), ("b", "bob")]),
            frontier: vec![
                ("bob".into(), "alice->bob".into()),
                ("carol".into(), "alice->carol".into()),
            ],
            depth: 3,
        }
    }

    /// Round-trip the `{rows, frontier}` envelope: encode in the handler shape,
    /// decode in the broadcast unwrap path, assert rows AND frontier survive.
    #[test]
    fn envelope_round_trips_rows_and_frontier() {
        let rows = vec![row(&[("a", "alice"), ("b", "bob")]), row(&[("a", "carol")])];
        let frontier = vec![UnresolvedExpansion {
            binding_var: "b".into(),
            node_name: "bob".into(),
            triple_idx: 1,
            partial_row: row(&[("a", "alice"), ("b", "bob")]),
        }];

        let payload = encode_match_envelope(&rows, &frontier, &[]).unwrap();
        let decoded = decode_match_envelope(&payload).unwrap();
        assert!(decoded.resume.is_empty());

        // Rows preserved: 2 elements. Merging them back into a bare array
        // reproduces the exact `rows` map values embedded in the envelope —
        // compare against the SAME bytes the envelope carries (a second
        // independent `rows_to_msgpack` call can differ only in HashMap key
        // order, so we reconstruct the expected bare array from the envelope's
        // own `rows` field rather than re-serializing).
        assert_eq!(decoded.row_elements.len(), 2);
        let merged = encode_msgpack_array(&decoded.row_elements);
        let envelope_rows = map_value_raw(&payload, MATCH_ENVELOPE_ROWS_KEY).unwrap();
        assert_eq!(
            merged, envelope_rows,
            "merged rows must equal the envelope's bare rows array byte-for-byte"
        );
        // And each decoded row's bindings are intact regardless of key order.
        let decoded_json = nodedb_types::json_from_msgpack(&merged).unwrap();
        let arr = decoded_json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["a"], "alice");
        assert_eq!(arr[0]["b"], "bob");
        assert_eq!(arr[1]["a"], "carol");

        // Frontier preserved.
        assert_eq!(decoded.frontier.len(), 1);
        assert_eq!(decoded.frontier[0].node_name, "bob");
        assert_eq!(decoded.frontier[0].binding_var, "b");
        assert_eq!(decoded.frontier[0].triple_idx, 1);
        assert_eq!(
            decoded.frontier[0].partial_row.get("a").map(String::as_str),
            Some("alice")
        );
    }

    /// Empty rows + empty frontier (the empty-partition / single-node case):
    /// the merged rows payload is an empty bare msgpack array and the frontier
    /// is empty.
    #[test]
    fn envelope_round_trips_empty() {
        let payload = encode_match_envelope(&[], &[], &[]).unwrap();
        let decoded = decode_match_envelope(&payload).unwrap();
        assert!(decoded.row_elements.is_empty());
        assert!(decoded.frontier.is_empty());
        assert!(decoded.resume.is_empty());
        let merged = encode_msgpack_array(&decoded.row_elements);
        let expected = crate::engine::graph::pattern::executor::rows_to_msgpack(&[]).unwrap();
        assert_eq!(merged, expected);
    }

    /// The truncation resume cursor survives the envelope round-trip with every
    /// field intact: triple_idx, source_row, the `(local_id, path)` frontier
    /// pairs, and depth. Exercised via BOTH the `&[BindingRow]` encoder and the
    /// pre-merged-bytes `_raw` encoder (the cross-node path) so they stay in
    /// lockstep.
    #[test]
    fn envelope_round_trips_resume_cursor() {
        let rows = vec![row(&[("a", "alice"), ("b", "bob")])];
        let cursor = resume_cursor();

        // `&[BindingRow]` encoder (DP handler shape).
        let payload = encode_match_envelope(&rows, &[], std::slice::from_ref(&cursor)).unwrap();
        let decoded = decode_match_envelope(&payload).unwrap();
        assert_eq!(decoded.resume.len(), 1);
        assert_eq!(decoded.resume[0], cursor);

        // `_raw` encoder (node-level re-wrap onto the cross-node wire). Two
        // cursors (two capping cores) must BOTH survive — never collapsed to one.
        let rows_array = crate::engine::graph::pattern::executor::rows_to_msgpack(&rows).unwrap();
        let two = vec![cursor.clone(), resume_cursor()];
        let raw = encode_match_envelope_raw(&rows_array, &[], &two).unwrap();
        let decoded_raw = decode_match_envelope(&raw).unwrap();
        assert_eq!(decoded_raw.resume.len(), 2);
        assert_eq!(decoded_raw.resume[0], cursor);
        assert_eq!(decoded_raw.resume[0].frontier, cursor.frontier);
        assert_eq!(decoded_raw.resume[0].depth, cursor.depth);
        assert_eq!(decoded_raw.resume[0].triple_idx, cursor.triple_idx);
        assert_eq!(decoded_raw.resume[0].source_row, cursor.source_row);
    }

    /// Backward-compat: an OLD-style 2-key `{rows, frontier}` envelope (no
    /// `resume` key) still decodes cleanly, with the resume cursors defaulting
    /// to empty — a mixed-version cluster never breaks.
    #[test]
    fn legacy_two_key_envelope_decodes_resume_as_empty() {
        let rows_array =
            crate::engine::graph::pattern::executor::rows_to_msgpack(&[row(&[("a", "x")])])
                .unwrap();
        let frontier_bytes = zerompk::to_msgpack_vec(&Vec::<UnresolvedExpansion>::new()).unwrap();

        // Hand-roll the legacy 2-key map exactly as the pre-resume encoder did.
        let mut buf = Vec::new();
        write_map_header(&mut buf, 2);
        write_kv_raw(&mut buf, MATCH_ENVELOPE_ROWS_KEY, &rows_array);
        write_kv_raw(&mut buf, MATCH_ENVELOPE_FRONTIER_KEY, &frontier_bytes);

        let decoded = decode_match_envelope(&buf).unwrap();
        assert_eq!(decoded.row_elements.len(), 1);
        assert!(decoded.frontier.is_empty());
        assert!(decoded.resume.is_empty());
    }

    /// Malformed bytes (not a map) surface a typed error, never a panic.
    #[test]
    fn malformed_envelope_is_typed_error() {
        // A bare msgpack array (the OLD pre-envelope shape) is not a map.
        let bogus = crate::engine::graph::pattern::executor::rows_to_msgpack(&[row(&[("a", "x")])])
            .unwrap();
        let err = decode_match_envelope(&bogus);
        assert!(
            err.is_err(),
            "bare array must not decode as an envelope map"
        );
    }
}
