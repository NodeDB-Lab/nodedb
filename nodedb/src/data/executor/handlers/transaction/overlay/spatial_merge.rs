// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base spatial (R-tree /
//! full-scan) result, so an in-transaction `WHERE ST_Contains/ST_Intersects/
//! ST_Within/ST_DWithin(...)` scan observes the transaction's own
//! uncommitted spatial-row writes (read-your-own-writes).
//!
//! Row identity is the surrogate each [`SpatialHit`] carries beside its row —
//! the same identity [`super::columnar_merge`] uses, since a mainstream SQL
//! `INSERT INTO
//! <spatial_collection> VALUES(...)` stages through `ColumnarOp::Insert`
//! (`stage_columnar_insert`), not `SpatialOp::Insert`.
//!
//! A staged overlay body for a spatial collection can therefore be either of
//! two shapes:
//! - a **columnar row** (`Value::Array`, schema-column-ordered) — the
//!   primary case, staged by `stage_columnar_insert` for a plain SQL INSERT
//!   against a `WITH (engine='spatial')` collection;
//! - a **spatial-sync geometry document** (`Value::Object`, `{field:
//!   geometry, "id": hex}`) — staged by
//!   `transaction::stage_write::stage_spatial` for a `SpatialOp::Insert`
//!   (the Lite-sync ingest path), for parity.
//!
//! Both are normalised to a full (unprojected) `nodedb_types::Value::Object`
//! before `extract_geometry` / `apply_predicate` (reused verbatim from
//! `handlers::spatial`) are run, exactly mirroring
//! [`super::columnar_merge::merge_overlay_into_columnar_scan`]'s
//! supersede / tombstone / add structure.

use std::collections::HashSet;

use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_spatial::Metric;
use nodedb_types::Surrogate;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::geometry::SridGeometry;
use nodedb_types::value::Value;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::convert::row_to_value;
use crate::data::executor::handlers::spatial_refine::{
    SpatialHit, apply_predicate, extract_geometry, mixed_srid_error, project_doc,
};
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::document::store::StorageKey;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Inputs for [`CoreLoop::merge_overlay_into_spatial_scan`].
pub(in crate::data::executor) struct SpatialOverlayMergeParams<'a> {
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geom: &'a SridGeometry,
    pub distance_meters: f64,
    pub projection: &'a [String],
    pub attr_filters: &'a [ScanFilter],
    pub row_level_filters: &'a [ScanFilter],
}

/// Decode a staged spatial-collection overlay body into a full (unprojected)
/// `Value::Object`, handling both possible staged shapes (see module doc).
/// Returns `Ok(None)` for a `Value::Array` staged row whose collection has no
/// known columnar schema, and for a body of any other shape: neither is a
/// row this search can match. A body that does not decode fails with
/// `Serialization`: the transaction staged it, so it is corrupt. A
/// computed-column projection error propagates.
fn decode_staged_spatial_row(
    body: &[u8],
    schema: Option<&ColumnarSchema>,
) -> crate::Result<Option<Value>> {
    let value =
        nodedb_types::value_from_msgpack(body).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!("staged spatial row does not decode: {e}"),
        })?;
    Ok(match value {
        Value::Array(row) => schema.map(|schema| row_to_value(&row, schema)),
        obj @ Value::Object(_) => Some(obj),
        _ => None,
    })
}

impl CoreLoop {
    /// Merge the overlay for `params.txn_id` into `results` (base spatial
    /// scan hits, already projected via `project_doc`). No-op when the
    /// transaction has no overlay entries for this collection.
    pub(in crate::data::executor) fn merge_overlay_into_spatial_scan(
        &self,
        params: SpatialOverlayMergeParams<'_>,
        results: &mut Vec<SpatialHit>,
    ) -> crate::Result<()> {
        let crs = self.crs_context();
        let SpatialOverlayMergeParams {
            txn_id,
            coll_key,
            field,
            predicate,
            query_geom,
            distance_meters,
            projection,
            attr_filters,
            row_level_filters,
        } = params;

        let metric = Metric::resolve(&crs, query_geom.srid)?;

        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };

        // A staged columnar-row body needs the collection's columnar schema
        // to decode its positional column order — the same schema
        // `execute_columnar_scan` / `stage_columnar_insert` use, obtained
        // via `ensure_columnar_engine_schema` at insert time.
        let schema = self.columnar_engines.get(coll_key).map(|e| e.schema());

        // A division/modulo-by-zero in an attribute or row-level filter is
        // WHERE-shaped: it fails the whole scan, same as
        // `handlers::spatial`'s direct `matches_value` calls.
        let row_matches = |doc: &Value| -> crate::Result<bool> {
            let Some(doc_geom) = extract_geometry(doc, field, &crs)? else {
                return Ok(false);
            };
            // A staged row in another SRID fails the scan, as a base row does.
            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters, &metric)
                .map_err(|m| mixed_srid_error(&coll_key.2, field, m))?
            {
                return Ok(false);
            }
            Ok(ScanFilter::all_match_value(attr_filters, doc, &crs)?
                && ScanFilter::all_match_value(row_level_filters, doc, &crs)?)
        };

        // Surrogates already represented in the base result.
        let mut seen: HashSet<u32> = results.iter().filter_map(|hit| hit.surrogate).collect();
        let identity_column =
            self.identity_column(coll_key.0.as_u64(), coll_key.1.as_u64(), &coll_key.2);

        // Base-minus-superseded: a tombstoned row is dropped; a staged put
        // replaces the row with the re-projected staged geometry and is
        // re-checked against the spatial predicate (an update may have moved
        // the geometry out of the query region). A row with no resolvable
        // surrogate identity has no overlay identity to resolve and is left
        // untouched.
        //
        // `Vec::retain_mut`'s closure must return `bool`, so an evaluation
        // error is captured in `first_err` and checked once the retain pass
        // finishes, aborting the merge before the overlay-addition pass runs.
        let mut first_err: Option<crate::Error> = None;
        let base_visible = overlay.base_visible(coll_key);
        results.retain_mut(|hit| {
            if first_err.is_some() {
                return true;
            }
            let Some(raw) = hit.surrogate else {
                return base_visible;
            };
            match overlay.get(coll_key, raw) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(body)) => {
                    let doc = match decode_staged_spatial_row(body, schema) {
                        Ok(Some(doc)) => doc,
                        // A staged row with no matchable shape: drop it
                        // rather than surface stale base data.
                        Ok(None) => return false,
                        Err(e) => {
                            first_err = Some(e);
                            return true;
                        }
                    };
                    match row_matches(&doc) {
                        Ok(true) => {}
                        Ok(false) => return false,
                        Err(e) => {
                            first_err = Some(e);
                            return true;
                        }
                    }
                    // A staged row that lacks its identity column renders the
                    // client-visible identity there, never the storage key.
                    let identity = StorageKey::for_surrogate(Surrogate(raw)).to_identity();
                    hit.row = project_doc(&doc, identity.as_str(), projection, &identity_column);
                    true
                }
                None => base_visible,
            }
        });
        if let Some(e) = first_err {
            return Err(e);
        }

        // Overlay additions: staged puts for surrogates the base scan did
        // not return, appended when the decoded geometry satisfies the
        // scan's spatial predicate (plus attribute/row-level filters) — this
        // is what makes a staged spatial INSERT visible (read-your-own-writes).
        for (surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            let Staged::Put(body) = staged else {
                continue;
            };
            let Some(doc) = decode_staged_spatial_row(body, schema)? else {
                continue;
            };
            if !row_matches(&doc)? {
                continue;
            }
            let identity = StorageKey::for_surrogate(Surrogate(surrogate)).to_identity();
            results.push(SpatialHit {
                surrogate: Some(surrogate),
                row: project_doc(&doc, identity.as_str(), projection, &identity_column),
            });
            seen.insert(surrogate);
        }
        Ok(())
    }
}
