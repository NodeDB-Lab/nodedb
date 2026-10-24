// SPDX-License-Identifier: BUSL-1.1

//! The DP→CP MATCH envelope: `{rows, frontier, resume}`.

use crate::engine::graph::pattern::executor::{
    BindingRow, UnresolvedExpansion, VarLenResume, rows_to_msgpack,
};

/// Map key carrying the binding-rows msgpack array in the MATCH envelope.
pub(crate) const MATCH_ENVELOPE_ROWS_KEY: &str = "rows";
/// Map key carrying the cross-shard frontier msgpack array in the MATCH envelope.
pub(crate) const MATCH_ENVELOPE_FRONTIER_KEY: &str = "frontier";
/// Map key carrying the variable-length truncation resume cursors in the MATCH
/// envelope. A msgpack array of [`VarLenResume`] (empty when nothing truncated).
///
/// A shard truncates a `[*min..max]` expansion independently, so a node fanned
/// across multiple cores can produce several resume cursors in one envelope —
/// the value is therefore an ARRAY, never a single optional cursor. The array
/// rides the SAME envelope bytes that already carry `rows`/`frontier` across the
/// SPSC bridge AND the cross-node round-trip; nothing widens `ExecuteResponse`.
pub(crate) const MATCH_ENVELOPE_RESUME_KEY: &str = "resume";

/// Zerompk-encode a `VarLenResume` slice into the `resume` map value (a msgpack
/// array, empty when nothing truncated). Modeled exactly on the `frontier`
/// value encoding so encode/decode/empty-case stay symmetric.
fn encode_resume_value(resume: &[VarLenResume]) -> Result<Vec<u8>, crate::Error> {
    zerompk::to_msgpack_vec(&resume.to_vec()).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("match resume serialization: {e}"),
    })
}

/// Encode a MATCH result into the DP→CP `{rows, frontier, resume}` msgpack
/// envelope.
///
/// `rows` are serialized exactly as [`rows_to_msgpack`] produces (an unchanged
/// bare msgpack array) and embedded as the `rows` map value. The
/// `unresolved_frontier` is zerompk-encoded (a msgpack array of
/// [`UnresolvedExpansion`]) and embedded as the `frontier` map value. The
/// variable-length truncation `resume` cursors are zerompk-encoded the same way
/// (a msgpack array of [`VarLenResume`], empty when nothing truncated). All are
/// already-valid msgpack values, so they are spliced in via `write_kv_raw`
/// without re-encoding. The map ALWAYS carries 3 keys so decode is uniform.
pub(crate) fn encode_match_envelope(
    rows: &[BindingRow],
    frontier: &[UnresolvedExpansion],
    resume: &[VarLenResume],
) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::writer::{write_kv_raw, write_map_header};

    let rows_bytes = rows_to_msgpack(rows)?;
    let frontier_bytes =
        zerompk::to_msgpack_vec(&frontier.to_vec()).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("match frontier serialization: {e}"),
        })?;
    let resume_bytes = encode_resume_value(resume)?;

    let mut buf =
        Vec::with_capacity(rows_bytes.len() + frontier_bytes.len() + resume_bytes.len() + 24);
    write_map_header(&mut buf, 3);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_ROWS_KEY, &rows_bytes);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_FRONTIER_KEY, &frontier_bytes);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_RESUME_KEY, &resume_bytes);
    Ok(buf)
}

/// Build the `{rows, frontier, resume}` envelope from an ALREADY-encoded bare
/// rows msgpack array plus frontier entries and resume cursors.
///
/// Mirrors [`encode_match_envelope`] but accepts pre-merged rows bytes (as
/// produced by `broadcast_match_to_all_cores`) instead of `&[BindingRow]`,
/// avoiding a redundant decode+re-encode round-trip. For the same inputs
/// the output is byte-identical to the output of `encode_match_envelope`.
///
/// Called by `execute_plan_all_local_cores` in the MATCH branch to reconstruct
/// the single-shard envelope shape from a node-level `MatchBroadcastOutcome`,
/// carrying the merged-across-cores `resume` cursors onto the cross-node wire.
pub(crate) fn encode_match_envelope_raw(
    rows_array: &[u8],
    frontier: &[UnresolvedExpansion],
    resume: &[VarLenResume],
) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::writer::{write_kv_raw, write_map_header};

    let frontier_bytes =
        zerompk::to_msgpack_vec(&frontier.to_vec()).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("match frontier serialization: {e}"),
        })?;
    let resume_bytes = encode_resume_value(resume)?;

    let mut buf =
        Vec::with_capacity(rows_array.len() + frontier_bytes.len() + resume_bytes.len() + 24);
    write_map_header(&mut buf, 3);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_ROWS_KEY, rows_array);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_FRONTIER_KEY, &frontier_bytes);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_RESUME_KEY, &resume_bytes);
    Ok(buf)
}
