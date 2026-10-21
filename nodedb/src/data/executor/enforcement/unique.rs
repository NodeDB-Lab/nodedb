// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE judged on the post-state of a write unit.
//!
//! A unit is every row write that lands together: one statement, one
//! committed redo record, one coalesced write batch. UNIQUE holds at the end
//! of the unit, so only the unit's post-state counts:
//!
//! * A value one row of the unit releases is free for another row of the
//!   unit, in whatever order the writes run. A swap is legal.
//! * Two rows of the post-state that claim one value are a violation.
//! * A committed row the unit leaves untouched still owns its values.
//!
//! A per-row probe of the committed index sees none of the unit's other
//! writes. It refuses a handover and admits two rows of one unit claiming
//! one value, so every multi-row unit judges here before its first write.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::handlers::write_body;
use crate::engine::document::store::{
    CollectionConfig, DocumentEngine, IndexPath, extract_index_values,
};
use crate::engine::sparse::btree::SparseEngine;
use crate::types::{DatabaseId, TenantId};

/// Who judges the UNIQUE claims of one row put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum UniqueJudge {
    /// The put judges its own claims against the committed index. Correct
    /// only for a unit of one row.
    Row,
    /// The caller judged the post-state of the whole unit the row belongs to.
    Unit,
}

/// One row of a unit's post-state.
pub(in crate::data::executor) struct PostImage<'a> {
    pub surrogate: u32,
    /// The row after the unit. `None`: the unit removes the row.
    pub doc: Option<&'a serde_json::Value>,
    /// Whether this row's claims are judged. An unjudged row still blocks a
    /// judged row from claiming one of its values.
    pub judged: bool,
}

/// The collection a unit writes, and the committed index its claims meet.
pub(in crate::data::executor) struct UniqueScope<'a> {
    pub sparse: &'a SparseEngine,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub paths: &'a [IndexPath],
    /// Bitemporal collections keep index entries in the versioned index only.
    pub bitemporal: bool,
    /// `false` when the unit truncated the collection: no committed row
    /// survives it.
    pub base_visible: bool,
    /// Resolves SRIDs in partial-index predicates.
    pub crs: &'a nodedb_crs::CrsContext<'a>,
}

/// One submitted row write of a unit, as its caller holds it.
pub(in crate::data::executor) struct SubmittedWrite<'a> {
    pub collection: &'a str,
    pub surrogate: u32,
    /// The submitted MessagePack body. `None`: the unit removes the row.
    pub body: Option<&'a [u8]>,
    /// See [`PostImage::judged`].
    pub judged: bool,
}

/// Refuse the unit when its post-state gives one unique value two owners.
///
/// When a row appears more than once in `rows`, its last entry is its
/// post-image. Every row in `rows` releases the values it holds committed.
pub(in crate::data::executor) fn check_unique_post_state(
    scope: &UniqueScope<'_>,
    rows: &[PostImage<'_>],
) -> crate::Result<()> {
    if !scope.paths.iter().any(|path| path.unique) {
        return Ok(());
    }
    let last_write: HashMap<u32, usize> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.surrogate, index))
        .collect();
    let doc_engine = DocumentEngine::new(scope.sparse, scope.database_id, scope.tid);
    for path in scope.paths.iter().filter(|path| path.unique) {
        // Value → (owning row, whether that row is judged).
        let mut claims: HashMap<String, (u32, bool)> = HashMap::new();
        // Judged claims in unit order, so every replica names the same value.
        let mut judged_claims: Vec<(String, u32)> = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if last_write.get(&row.surrogate) != Some(&index) {
                continue;
            }
            let Some(doc) = row.doc else {
                continue;
            };
            for needle in claimed_values(doc, path, scope.crs) {
                match claims.entry(needle) {
                    Entry::Vacant(slot) => {
                        if row.judged {
                            judged_claims.push((slot.key().clone(), row.surrogate));
                        }
                        slot.insert((row.surrogate, row.judged));
                    }
                    // `claimed_values` is a set, so the owner is another row.
                    Entry::Occupied(slot) => {
                        if row.judged || slot.get().1 {
                            return Err(violation(scope.collection, path, slot.key()));
                        }
                    }
                }
            }
        }
        if !scope.base_visible {
            continue;
        }
        for (needle, owner) in judged_claims {
            let stored =
                doc_engine.index_lookup(scope.collection, &path.path, &needle, scope.bitemporal)?;
            let foreign_owner = stored.iter().any(|key| {
                let holder = key.surrogate().as_u32();
                holder != owner && !last_write.contains_key(&holder)
            });
            if foreign_owner {
                return Err(violation(scope.collection, path, &needle));
            }
        }
    }
    Ok(())
}

/// The values `doc` claims under `path`: the stored, case-folded index keys.
/// A row the partial-index predicate rejects claims none.
fn claimed_values(
    doc: &serde_json::Value,
    path: &IndexPath,
    crs: &nodedb_crs::CrsContext<'_>,
) -> BTreeSet<String> {
    if let Some(predicate) = &path.predicate
        && !predicate.evaluate_json(doc, crs)
    {
        return BTreeSet::new();
    }
    extract_index_values(doc, &path.path, path.is_array)
        .into_iter()
        .map(|raw| {
            if path.case_insensitive {
                raw.to_lowercase()
            } else {
                raw
            }
        })
        .collect()
}

fn violation(collection: &str, path: &IndexPath, needle: &str) -> crate::Error {
    crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: "unique".to_string(),
        detail: format!(
            "unique index '{}' violation on field '{}' (value '{}')",
            path.name, path.path, needle
        ),
    }
}

impl CoreLoop {
    /// The collection's config when it declares a UNIQUE index.
    pub(in crate::data::executor) fn unique_config(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<&CollectionConfig> {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs
            .get(&key)
            .filter(|config| config.index_paths.iter().any(|path| path.unique))
    }

    /// Judge UNIQUE on the post-state of a unit's rows in one collection,
    /// against the committed index.
    pub(in crate::data::executor) fn check_unit_unique(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        rows: &[PostImage<'_>],
    ) -> crate::Result<()> {
        let Some(config) = self.unique_config(database_id, tid, collection) else {
            return Ok(());
        };
        check_unique_post_state(
            &UniqueScope {
                sparse: &self.sparse,
                database_id,
                tid,
                collection,
                paths: &config.index_paths,
                bitemporal: config.bitemporal,
                base_visible: true,
                crs: &self.crs_context(),
            },
            rows,
        )
    }

    /// Judge UNIQUE on the post-state of a unit of submitted row writes,
    /// collection by collection. A collection with no UNIQUE index decodes
    /// nothing.
    pub(in crate::data::executor) fn check_submitted_unit_unique(
        &self,
        database_id: u64,
        tid: u64,
        writes: &[SubmittedWrite<'_>],
    ) -> crate::Result<()> {
        let mut by_collection: BTreeMap<&str, Vec<&SubmittedWrite<'_>>> = BTreeMap::new();
        for write in writes {
            by_collection
                .entry(write.collection)
                .or_default()
                .push(write);
        }
        for (collection, writes) in by_collection {
            let Some(config) = self.unique_config(database_id, tid, collection) else {
                continue;
            };
            let docs = writes
                .iter()
                .map(|write| match write.body {
                    Some(body) => self.unique_image(config, body).map(Some),
                    None => Ok(None),
                })
                .collect::<crate::Result<Vec<_>>>()?;
            let rows: Vec<PostImage<'_>> = writes
                .iter()
                .zip(&docs)
                .map(|(write, doc)| PostImage {
                    surrogate: write.surrogate,
                    doc: doc.as_ref(),
                    judged: write.judged,
                })
                .collect();
            self.check_unit_unique(database_id, tid, collection, &rows)?;
        }
        Ok(())
    }

    /// The document a submitted body is indexed as: typeguards injected and
    /// generated columns evaluated, as the put stores it. A body that does
    /// not decode is an
    /// error: its UNIQUE claims cannot be judged, so the unit is refused
    /// rather than let it take a value unseen.
    pub(in crate::data::executor) fn unique_image(
        &self,
        config: &CollectionConfig,
        body: &[u8],
    ) -> crate::Result<serde_json::Value> {
        let mut doc = doc_format::decode_document(body)?;
        evaluate_generated(config, &mut doc, &self.crs_context())?;
        Ok(doc)
    }
}

/// Derive `doc` as the put stores it: typeguards injected, then `config`'s
/// generated columns evaluated.
///
/// The guards only inject here: the write that stores the body judges them.
/// An error keeps its own code, as the put path reports it.
pub(in crate::data::executor) fn evaluate_generated(
    config: &CollectionConfig,
    doc: &mut serde_json::Value,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    write_body::materialize_write_body(config, doc, GuardScope::InjectOnly, crs)
        .map_err(crate::Error::DataPlane)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use nodedb_types::{StorageKey, Surrogate};

    const TID: u64 = 1;
    const COLL: &str = "codes";

    const DB: u64 = 0;

    /// A core whose `codes` collection declares a UNIQUE index on `code`,
    /// with its bridge ends kept alive.
    fn unique_core(
        dir: &std::path::Path,
    ) -> (CoreLoop, Box<dyn std::any::Any>, Box<dyn std::any::Any>) {
        let (mut core, req, resp) = make_core_with_dir(dir);
        let mut config = CollectionConfig::new(COLL);
        config.index_paths.push(IndexPath {
            unique: true,
            ..IndexPath::new("code")
        });
        core.doc_configs.insert(
            (DatabaseId::new(DB), TenantId::new(TID), COLL.to_string()),
            config,
        );
        (core, Box::new(req), Box::new(resp))
    }

    /// Commit row `surrogate` holding `code` into the unique index.
    fn seed(core: &mut CoreLoop, surrogate: u32, code: &str) {
        let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
        let txn = core.sparse.begin_write().expect("begin");
        core.sparse
            .index_put_in_txn(
                &txn,
                crate::engine::sparse::btree_index::IndexEntryTxn {
                    database_id: DB,
                    tenant_id: TID,
                    collection: COLL,
                    field: "code",
                    value: code,
                    document_id: &key,
                },
            )
            .expect("index put");
        txn.commit().expect("commit");
    }

    fn doc(code: &str) -> serde_json::Value {
        serde_json::json!({ "code": code })
    }

    fn judge(core: &CoreLoop, rows: &[PostImage<'_>]) -> crate::Result<()> {
        core.check_unit_unique(DB, TID, COLL, rows)
    }

    fn is_unique_violation(result: crate::Result<()>) -> bool {
        matches!(
            result,
            Err(crate::Error::RejectedConstraint { ref constraint, .. }) if constraint == "unique"
        )
    }

    #[test]
    fn a_swap_is_legal_in_either_write_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 1, "A");
        seed(&mut core, 2, "B");
        let (a, b) = (doc("A"), doc("B"));
        for rows in [[(1, &b), (2, &a)], [(2, &a), (1, &b)]] {
            let rows: Vec<PostImage<'_>> = rows
                .iter()
                .map(|(surrogate, doc)| PostImage {
                    surrogate: *surrogate,
                    doc: Some(*doc),
                    judged: true,
                })
                .collect();
            judge(&core, &rows).expect("a swap leaves each value one owner");
        }
    }

    #[test]
    fn a_value_released_by_a_later_write_is_free() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 1, "A");
        let (a, x) = (doc("A"), doc("X"));
        let claim_first = [
            PostImage {
                surrogate: 2,
                doc: Some(&a),
                judged: true,
            },
            PostImage {
                surrogate: 1,
                doc: Some(&x),
                judged: true,
            },
        ];
        judge(&core, &claim_first).expect("row 1 releases A in the same unit");
        let delete_after = [
            PostImage {
                surrogate: 2,
                doc: Some(&a),
                judged: true,
            },
            PostImage {
                surrogate: 1,
                doc: None,
                judged: true,
            },
        ];
        judge(&core, &delete_after).expect("row 1 is removed in the same unit");
    }

    #[test]
    fn two_rows_claiming_one_value_are_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _req, _resp) = unique_core(dir.path());
        let z = doc("Z");
        let rows = [
            PostImage {
                surrogate: 1,
                doc: Some(&z),
                judged: true,
            },
            PostImage {
                surrogate: 2,
                doc: Some(&z),
                judged: true,
            },
        ];
        assert!(is_unique_violation(judge(&core, &rows)));
    }

    #[test]
    fn a_value_an_untouched_row_holds_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 3, "C");
        let c = doc("C");
        let rows = [PostImage {
            surrogate: 1,
            doc: Some(&c),
            judged: true,
        }];
        assert!(is_unique_violation(judge(&core, &rows)));
    }

    #[test]
    fn a_rows_own_committed_value_is_not_a_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 1, "A");
        let a = doc("A");
        let rows = [PostImage {
            surrogate: 1,
            doc: Some(&a),
            judged: true,
        }];
        judge(&core, &rows).expect("a re-put keeps its own value");
    }

    #[test]
    fn unjudged_rows_block_a_judged_claim_but_are_not_judged_themselves() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 9, "Q");
        let (q, z) = (doc("Q"), doc("Z"));
        // Two unjudged rows share Z and one holds a committed foreign value:
        // nothing is judged, so nothing is refused.
        let unjudged = [
            PostImage {
                surrogate: 1,
                doc: Some(&z),
                judged: false,
            },
            PostImage {
                surrogate: 2,
                doc: Some(&z),
                judged: false,
            },
            PostImage {
                surrogate: 3,
                doc: Some(&q),
                judged: false,
            },
        ];
        judge(&core, &unjudged).expect("unjudged rows are not judged");
        let judged_claim = [
            PostImage {
                surrogate: 1,
                doc: Some(&z),
                judged: false,
            },
            PostImage {
                surrogate: 4,
                doc: Some(&z),
                judged: true,
            },
        ];
        assert!(is_unique_violation(judge(&core, &judged_claim)));
    }

    #[test]
    fn a_truncating_unit_ignores_committed_owners() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = unique_core(dir.path());
        seed(&mut core, 3, "C");
        let config = core.unique_config(DB, TID, COLL).expect("unique config");
        let c = doc("C");
        let rows = [PostImage {
            surrogate: 1,
            doc: Some(&c),
            judged: true,
        }];
        check_unique_post_state(
            &UniqueScope {
                sparse: &core.sparse,
                database_id: DB,
                tid: TID,
                collection: COLL,
                paths: &config.index_paths,
                bitemporal: false,
                base_visible: false,
                crs: &nodedb_crs::CrsContext::builtin_only(),
            },
            &rows,
        )
        .expect("no committed row survives a truncate");
    }
}
