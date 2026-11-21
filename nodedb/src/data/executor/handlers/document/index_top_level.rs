// SPDX-License-Identifier: BUSL-1.1

//! The top-level build and retire of a schemaless collection's index set.
//!
//! A top-level build puts the collection's set in top-level mode. Every
//! write then maintains the path of each top-level key it holds, so a new
//! key needs no build of its own. The build backfills the rows already
//! stored, in one walk over every version: each top-level key that no
//! stored owner decides gets the entries the write path writes.
//!
//! A top-level retire leaves top-level mode. Every top-level path that no
//! stored owner decides loses its entries. A path a stored owner decides
//! keeps its entries in that owner's shape.
//!
//! Both are idempotent and fenced like every index step. A build of a set
//! already in top-level mode and a retire of a set not in it walk no row:
//! each only raises the top-level fence. A step below that fence changes
//! nothing.

use std::collections::BTreeSet;

use redb::WriteTransaction;

use super::index_maintained_set::FencedStep;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::maintained_indexes::PreparedSet;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::engine::document::store::{StorageKey, extract_index_values};
use crate::engine::sparse::btree::{
    FenceVerdict, MaintainedIndexSet, TOP_LEVEL_OWNER, is_top_level_path, top_level_path,
};
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;
use nodedb_physical::physical_plan::{DocIndexFence, TOP_LEVEL_BUILD_PATH};

/// The collection a top-level step changes, and the step's fence.
pub(super) struct TopLevelTarget<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub fence: DocIndexFence,
}

impl TopLevelTarget<'_> {
    /// The fenced step this target names: the top-level owner on its build
    /// path.
    fn step(&self) -> FencedStep<'_> {
        FencedStep {
            database_id: self.database_id,
            tid: self.tid,
            collection: self.collection,
            path: TOP_LEVEL_BUILD_PATH,
            owner: &TOP_LEVEL_OWNER.owner,
            fence: self.fence,
        }
    }
}

impl CoreLoop {
    /// Put the collection's set in top-level mode and backfill the rows
    /// already stored, in one transaction. The mirror takes the set after
    /// the commit. A stale build changes nothing. A build of a set already
    /// in top-level mode only raises its fence.
    pub(super) fn build_top_level_index(
        &mut self,
        target: TopLevelTarget<'_>,
    ) -> crate::Result<()> {
        let step = target.step();
        let committed =
            self.sparse
                .maintained_indexes(target.database_id, target.tid, target.collection)?;
        match committed.judge(step.path, step.owner, step.fence) {
            FenceVerdict::Stale => return Ok(()),
            FenceVerdict::Apply if committed.top_level => return self.raise_fence_only(&step),
            FenceVerdict::Apply | FenceVerdict::NewIncarnation => {}
        }
        if let Some(ref m) = self.metrics {
            m.record_document_index_backfill();
        }
        let body_format = self.sparse_body_format(
            crate::types::DatabaseId::new(target.database_id),
            crate::types::TenantId::new(target.tid),
            target.collection,
        );
        let txn = self.sparse.begin_write()?;
        let mut set = self.sparse.maintained_indexes_in_txn(
            &txn,
            target.database_id,
            target.tid,
            target.collection,
        )?;
        self.fence_step_in_txn(&txn, &mut set, &step)?;
        set.top_level = true;
        set.raise_fence(step.path, step.owner, step.fence);
        self.backfill_top_level_in_txn(&txn, &set, &target, body_format.as_format_ref())?;
        self.sparse.put_maintained_indexes_in_txn(&txn, &set)?;
        let prepared = PreparedSet::of(&set)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("top-level index build commit: {e}"),
        })?;
        self.maintained_indexes.apply(prepared);
        Ok(())
    }

    /// Take the collection's set out of top-level mode, in one transaction.
    /// Returns how many entries left. A stale retire changes nothing. Every
    /// other retire raises the top-level fence, so a build it overtook stays
    /// out.
    pub(super) fn retire_top_level_index(
        &mut self,
        target: TopLevelTarget<'_>,
    ) -> crate::Result<usize> {
        let step = target.step();
        let (database_id, tid, collection) = (target.database_id, target.tid, target.collection);
        let txn = self.sparse.begin_write()?;
        let mut set = self
            .sparse
            .maintained_indexes_in_txn(&txn, database_id, tid, collection)?;
        match self.fence_step_in_txn(&txn, &mut set, &step)? {
            FenceVerdict::Stale => return Ok(0),
            FenceVerdict::Apply | FenceVerdict::NewIncarnation => {}
        }
        set.raise_fence(step.path, step.owner, step.fence);
        // A set not in top-level mode keeps no top-level entry, so the
        // retire only records its fence.
        let fields = if set.top_level {
            self.sparse
                .versioned_index_fields_in_txn(&txn, database_id, tid, collection, "$.")?
        } else {
            Vec::new()
        };
        set.top_level = false;
        let mut removed = 0;
        for field in fields {
            if is_top_level_path(&field) && set.stored_effective(&field).is_none() {
                removed += self.sparse.versioned_index_drop_field_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    &field,
                )?;
            }
        }
        self.sparse.put_maintained_indexes_in_txn(&txn, &set)?;
        let prepared = PreparedSet::of(&set)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("top-level index retire commit: {e}"),
        })?;
        self.maintained_indexes.apply(prepared);
        Ok(removed)
    }

    /// Write the top-level entries of every version of every row, each at
    /// its version's system time, within `txn`. Does NOT commit. `set` is
    /// the set after the build: a path a stored owner decides is skipped,
    /// since that owner's build wrote its entries.
    ///
    /// A version that does not decode fails the build: an index built
    /// without it misses the row at every cutoff the version covers.
    fn backfill_top_level_in_txn(
        &self,
        txn: &WriteTransaction,
        set: &MaintainedIndexSet,
        target: &TopLevelTarget<'_>,
        body_format: SparseBodyFormatRef<'_>,
    ) -> crate::Result<()> {
        let mut row: Option<StorageKey> = None;
        let mut held: BTreeSet<(String, String)> = BTreeSet::new();
        self.sparse.versioned_for_each_version(
            target.database_id,
            target.tid,
            target.collection,
            |doc_id, sys_from, body| {
                if row != Some(*doc_id) {
                    row = Some(*doc_id);
                    held.clear();
                }
                let values = match body {
                    Some(body) => top_level_values(body, set, body_format)?,
                    None => BTreeSet::new(),
                };
                for (path, value) in held.difference(&values) {
                    self.sparse.versioned_index_tombstone_in_txn(
                        txn,
                        entry(target, path, value, doc_id, sys_from),
                    )?;
                }
                for (path, value) in values.difference(&held) {
                    self.sparse.versioned_index_put_in_txn(
                        txn,
                        entry(target, path, value, doc_id, sys_from),
                    )?;
                }
                held = values;
                Ok(())
            },
        )
    }
}

/// The versioned index entry of `value` under `path` for row `doc_id` at
/// `sys_from`.
fn entry<'a>(
    target: &TopLevelTarget<'a>,
    path: &'a str,
    value: &'a str,
    doc_id: &'a StorageKey,
    sys_from: i64,
) -> VersionedIndexEntry<'a> {
    VersionedIndexEntry {
        database_id: target.database_id,
        tenant: target.tid,
        coll: target.collection,
        field: path,
        value,
        doc_id,
        sys_from,
    }
}

/// The `(path, value)` pairs one live version contributes under the
/// top-level shape: each top-level key no stored owner decides, through the
/// extraction a declared path uses.
fn top_level_values(
    body: &[u8],
    set: &MaintainedIndexSet,
    body_format: SparseBodyFormatRef<'_>,
) -> crate::Result<BTreeSet<(String, String)>> {
    let doc = decode_scanned_document(body, body_format)?;
    let mut out = BTreeSet::new();
    let Some(object) = doc.as_object() else {
        return Ok(out);
    };
    for key in object.keys() {
        let Some(path) = top_level_path(key) else {
            continue;
        };
        if set.stored_effective(&path).is_some() {
            continue;
        }
        for value in extract_index_values(&doc, &path, TOP_LEVEL_OWNER.is_array) {
            out.insert((path.clone(), value));
        }
    }
    Ok(out)
}
