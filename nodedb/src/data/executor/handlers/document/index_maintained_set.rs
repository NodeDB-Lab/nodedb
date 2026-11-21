// SPDX-License-Identifier: BUSL-1.1

//! Bring one maintained path's entries to its owners' spec, inside the
//! transaction that changes the collection's maintained set.
//!
//! A build adds an owner to a path and a retire removes one. The entries
//! follow the path's effective owner. When that owner's entry shape
//! changes, the entries are purged and written again from every stored
//! version under the new shape. A path with no owner left keeps no entry.
//! A change that keeps the shape writes nothing.
//!
//! Every step is fenced first. A stale step changes nothing, and a step of
//! a newer incarnation discards the set and every entry of the collection.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::engine::document::predicate::IndexPredicate;
use crate::engine::sparse::btree::{EntryShape, FenceVerdict, MaintainedIndexSet};
use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner};

/// The path a set change touches, and the collection it lives in.
pub(super) struct PathChange<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    /// The path's entry shape before the change. `None` when no owner kept
    /// it.
    pub before: Option<EntryShape>,
    /// How the collection's stored bodies are encoded.
    pub body_format: SparseBodyFormatRef<'a>,
}

impl CoreLoop {
    /// Make the entries of `change.path` match `set`, the collection's set
    /// after the change, within `txn`. Does NOT commit. Returns how many
    /// entries the purge removed.
    pub(super) fn settle_maintained_path_in_txn(
        &self,
        txn: &WriteTransaction,
        set: &MaintainedIndexSet,
        change: PathChange<'_>,
    ) -> crate::Result<usize> {
        let PathChange {
            database_id,
            tid,
            collection,
            path,
            before,
            body_format,
        } = change;
        let after = set.effective(path);
        if before == after.map(|spec| spec.shape()) {
            return Ok(0);
        }
        let removed = self.sparse.versioned_index_drop_field_in_txn(
            txn,
            database_id,
            tid,
            collection,
            path,
        )?;
        let Some(spec) = after else {
            return Ok(removed);
        };
        let predicate = parse_predicate(spec.predicate.as_deref())?;
        self.backfill_versioned_index(
            txn,
            &super::index_backfill_versions::VersionedBackfill {
                database_id,
                tid,
                collection,
                path,
                is_array: spec.is_array,
                case_insensitive: spec.case_insensitive,
                predicate: predicate.as_ref(),
                body_format,
            },
        )?;
        Ok(removed)
    }
}

/// The path and owner a fenced step names, and its fence.
pub(super) struct FencedStep<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub owner: &'a DocIndexOwner,
    pub fence: DocIndexFence,
}

impl CoreLoop {
    /// Judge `step` against `set`, read inside `txn`. A step of a newer
    /// incarnation removes every index entry of the collection within `txn`
    /// and resets `set` to that incarnation, then applies. Does NOT commit.
    /// The caller raises the step's fence once it applied.
    pub(super) fn fence_step_in_txn(
        &self,
        txn: &WriteTransaction,
        set: &mut MaintainedIndexSet,
        step: &FencedStep<'_>,
    ) -> crate::Result<FenceVerdict> {
        let verdict = set.judge(step.path, step.owner, step.fence);
        match verdict {
            FenceVerdict::Stale | FenceVerdict::Apply => {}
            FenceVerdict::NewIncarnation => {
                for field in self.sparse.versioned_index_fields_in_txn(
                    txn,
                    step.database_id,
                    step.tid,
                    step.collection,
                    "",
                )? {
                    self.sparse.versioned_index_drop_field_in_txn(
                        txn,
                        step.database_id,
                        step.tid,
                        step.collection,
                        &field,
                    )?;
                }
                set.reset(step.fence.incarnation);
            }
        }
        Ok(verdict)
    }

    /// Record a step that changes no path: raise its fence in the committed
    /// set, in a transaction of its own. The mirror holds no fence, so it
    /// stays as it is. A step whose fence `committed` already holds writes
    /// nothing, so a re-delivered step costs no storage write.
    pub(super) fn raise_fence_only(
        &self,
        committed: &MaintainedIndexSet,
        step: &FencedStep<'_>,
    ) -> crate::Result<()> {
        if committed
            .fence_of(step.path, step.owner)
            .is_some_and(|held| held >= step.fence)
        {
            return Ok(());
        }
        let txn = self.sparse.begin_write()?;
        let mut set = self.sparse.maintained_indexes_in_txn(
            &txn,
            step.database_id,
            step.tid,
            step.collection,
        )?;
        set.raise_fence(step.path, step.owner, step.fence);
        self.sparse.put_maintained_indexes_in_txn(&txn, &set)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("index fence commit: {e}"),
        })
    }
}

/// Parse a partial-index predicate. The DDL layer checks the text at
/// CREATE INDEX, so a text that does not parse here drifted from the
/// grammar. Writing the index without it over-populates a partial index.
pub(super) fn parse_predicate(text: Option<&str>) -> crate::Result<Option<IndexPredicate>> {
    match text {
        Some(text) => IndexPredicate::parse(text)
            .map(Some)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("partial-index predicate failed to parse: {text}"),
            }),
        None => Ok(None),
    }
}
