// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base columnar scan result, so
//! an in-transaction columnar `SELECT` observes the transaction's own
//! uncommitted `ColumnarOp::Insert` rows (read-your-own-writes).
//!
//! Row identity here is the surrogate carried alongside each scanned row
//! (`Option<Surrogate>`, from `scan_memtable_rows_with_surrogates` / the
//! flushed-segment surrogate sidecar) rather than the hex-doc-id keying
//! [`super::merge::merge_overlay_into_scan`] uses — a columnar row has no
//! separate document id, so [`super::super::stage_write::stage_columnar`]
//! stages puts keyed by surrogate with `StorageKey::for_surrogate` used only
//! for the overlay's doc-id side-map. A base row with no recorded surrogate
//! (legacy segments predating the surrogate sidecar) cannot be resolved
//! against the overlay and is left untouched.
//!
//! Columnar has no delete/rollback/aggregate hazard for this merge (unlike
//! Timeseries, which is out of scope for this unit): a staged put/tombstone
//! either replaces or removes exactly one row, and the WHERE predicate is
//! re-evaluated on the staged body exactly like
//! [`super::merge::merge_overlay_into_scan`] does for Document.

use std::collections::HashSet;

use nodedb_types::Surrogate;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;

use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::convert::row_to_projected_json;
use crate::data::executor::handlers::columnar_read::filter::row_matches_filters;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::types::{DatabaseId, TenantId, TxnId};

/// One matched columnar row: its cross-engine surrogate (when known), the
/// decoded schema-ordered column values, and the already-projected response
/// JSON. Shared shape between the base scan (`execute_columnar_scan`) and
/// this overlay merge.
pub(in crate::data::executor) type ColumnarMatchedRow =
    (Option<Surrogate>, Vec<Value>, serde_json::Value);

/// Inputs for [`CoreLoop::merge_overlay_into_columnar_scan`].
pub(in crate::data::executor) struct ColumnarOverlayMergeParams<'a> {
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    pub schema: &'a ColumnarSchema,
    pub projection: &'a [String],
    pub filter_predicates: &'a [ScanFilter],
    pub computed_cols: &'a [ComputedColumn],
    pub all_versions: bool,
}

/// Decode a staged columnar row body (a `Value::Array` encoded by
/// `stage_columnar_insert` via `nodedb_types::value_to_msgpack`) back into
/// its schema-ordered `Vec<Value>`. Returns `None` for a body that fails to
/// decode or is not an array — defensively treated as "does not match" by
/// callers rather than surfacing a panic.
pub(in crate::data::executor) fn decode_staged_row(body: &[u8]) -> Option<Vec<Value>> {
    match nodedb_types::value_from_msgpack(body) {
        Ok(Value::Array(values)) => Some(values),
        _ => None,
    }
}

impl CoreLoop {
    /// Merge the overlay for `params.txn_id` into `matched` (base columnar
    /// scan rows). Mirrors [`super::merge::merge_overlay_into_scan`]'s
    /// supersede / tombstone / add structure, keyed by surrogate instead of
    /// hex doc-id, and producing the columnar row tuple
    /// [`ColumnarMatchedRow`] instead of `(doc_id, body)` pairs. No-op when
    /// the transaction has no overlay entries for this collection.
    pub(in crate::data::executor) fn merge_overlay_into_columnar_scan(
        &self,
        params: ColumnarOverlayMergeParams<'_>,
        matched: &mut Vec<ColumnarMatchedRow>,
    ) -> crate::Result<()> {
        let ColumnarOverlayMergeParams {
            txn_id,
            coll_key,
            schema,
            projection,
            filter_predicates,
            computed_cols,
            all_versions,
        } = params;

        // Read-your-own-writes refreshes the lease so a long read-only txn
        // never ages out of the overlay reaper.
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };

        let predicate = |row: &[Value]| -> Result<bool, nodedb_query::EvalError> {
            if filter_predicates.is_empty() {
                return Ok(true);
            }
            row_matches_filters(row, schema, filter_predicates)
        };

        // Surrogates already represented in the base result. Additions
        // consult this to avoid re-adding a row base already carries (or
        // that the retain pass below has just superseded in place).
        let mut seen: HashSet<u32> = matched
            .iter()
            .filter_map(|(surrogate, _, _)| surrogate.map(|s| s.0))
            .collect();

        // Base-minus-superseded: a tombstoned row is dropped; a staged put
        // replaces the row's decoded values + JSON and is re-checked against
        // the scan predicate (an update may have moved the row out of the
        // result). A row with no recorded surrogate has no overlay identity
        // to resolve and is left untouched, matching the base scan's own
        // "no prefilter possible" treatment of unrecorded surrogates.
        //
        // `Vec::retain_mut`'s closure must return `bool`, so a
        // division/modulo-by-zero hit while projecting a computed column
        // can't `?` out of it directly; it's captured in `first_err` and
        // checked once the retain pass finishes, aborting the merge before
        // the overlay-addition pass below runs.
        let mut first_err: Option<crate::Error> = None;
        matched.retain_mut(|(surrogate, row, json)| {
            if first_err.is_some() {
                return true;
            }
            let Some(s) = surrogate else {
                return true;
            };
            match overlay.get(coll_key, s.0) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(body)) => match decode_staged_row(body) {
                    Some(new_row) => {
                        match predicate(&new_row) {
                            Ok(true) => {}
                            Ok(false) => return false,
                            Err(e) => {
                                first_err = Some(crate::Error::from(e));
                                return true;
                            }
                        }
                        match row_to_projected_json(
                            &new_row,
                            schema,
                            projection,
                            computed_cols,
                            all_versions,
                        ) {
                            Ok(v) => *json = v,
                            Err(e) => {
                                first_err = Some(e);
                                return true;
                            }
                        }
                        *row = new_row;
                        true
                    }
                    // A staged body that fails to decode carries no usable
                    // row: drop it rather than surface stale base data.
                    None => false,
                },
                None => true,
            }
        });
        if let Some(e) = first_err {
            return Err(e);
        }

        // Overlay additions: staged puts for surrogates the base scan did
        // not return, appended when the decoded row satisfies the scan's
        // WHERE predicate — this is what makes a staged INSERT visible.
        for (surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            let Staged::Put(body) = staged else {
                continue;
            };
            let Some(new_row) = decode_staged_row(body) else {
                continue;
            };
            if !predicate(&new_row)? {
                continue;
            }
            let json =
                row_to_projected_json(&new_row, schema, projection, computed_cols, all_versions)?;
            matched.push((Some(Surrogate::new(surrogate)), new_row, json));
            seen.insert(surrogate);
        }
        Ok(())
    }
}
