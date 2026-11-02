// SPDX-License-Identifier: BUSL-1.1

//! The current state a RESTORE sets again above the destination's newer
//! versions.
//!
//! A RESTORE re-issues every backed-up version at its own system time. It is
//! a write in the Calvin sequence, and it erases no history. A destination
//! row, KV key or edge can hold a version or a delete above the backup's
//! newest one: a write after the backup that `FORCE` overrode, or a TRUNCATE
//! sequenced before the RESTORE. The resolve reads the destination at the
//! transaction's turn. For each such row, key or edge it adds the backup's
//! newest version once more, stamped above the destination's newest version
//! and the transaction's ordinal. The current read then answers the backup's
//! state. A read at a cut below that stamp answers as the destination held
//! it. Every replica installs the stamp the resolve names.
//!
//! A RESTORE spans many transactions, and client writes run beside it. It
//! takes effect at one closed cut, its `restore_cut`, taken before its first
//! batch. Every write at or below the cut applied before any batch resolved.
//! Every write admitted after the cut stamps above it, on every data group
//! and on the sequencer. A row, KV key or array cell whose destination holds
//! a version above the cut was written after the RESTORE began. The resolve
//! leaves that version current and adds no stamp. The outcome is the
//! RESTORE applied whole at its cut, then every later write, whatever batch
//! each row falls in.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::RestoredEdgeVersion;
use nodedb_types::{StorageKey, Surrogate};
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::edge_store::temporal::keys::EdgeRef;
use crate::types::{DatabaseId, TenantId};
use crate::wal::{
    ColumnarRestoreRedo, DocumentDeleteRedo, DocumentPutRedo, KvRestoreRedo, RedoSubRecord,
    TimeseriesEndedPartitionRedo, TimeseriesRestoreRedo, VectorRestoreRedo,
};

use super::restore::restored_edge;
use super::restore_changes::RestoreChanges;

/// Where a RESTORE batch writes, the ordinal it applies at, and the cut the
/// RESTORE takes effect at.
#[derive(Debug, Clone, Copy)]
pub(super) struct RestoreScope {
    pub database_id: DatabaseId,
    pub tid: u64,
    /// The transaction's ordinal.
    pub applied: i64,
    /// The RESTORE's cut (`RestoredRedo::restore_cut`).
    pub cut: i64,
}

impl RestoreScope {
    /// The stamp the backup's current state is set again at: one above the
    /// destination's newest version, and at least the transaction's
    /// ordinal. `None` when the destination holds nothing above
    /// `backup_newest`, or its newest version is a write that followed the
    /// RESTORE's cut.
    pub(super) fn reassert_stamp(
        &self,
        backup_newest: i64,
        dest_newest: Option<i64>,
    ) -> Option<i64> {
        reassert_stamp(backup_newest, dest_newest, self.applied, self.cut)
    }

    /// The ordinal a restored edge version applies at: the RESTORE's cut,
    /// or the transaction's ordinal when it has no cut.
    ///
    /// An edge TRUNCATE hides every version applied below its ordinal. A
    /// TRUNCATE before the cut then leaves the restored versions visible,
    /// and one after it hides them: the RESTORE took effect at its cut. In
    /// a cluster the cut is the last ordinal of an epoch millisecond, which
    /// no Calvin transaction stamps (an epoch holds fewer positions), and
    /// every edge write is a Calvin transaction. A RESTORE writes each edge
    /// in one batch. So the versions applied at the cut are this batch's
    /// alone, and the other home's copy is found by that ordinal.
    pub(super) fn edge_apply(&self) -> i64 {
        self.applied.min(self.cut)
    }
}

/// [`RestoreScope::reassert_stamp`] for a batch at `applied` under `cut`.
pub(super) fn reassert_stamp(
    backup_newest: i64,
    dest_newest: Option<i64>,
    applied: i64,
    cut: i64,
) -> Option<i64> {
    let dest_newest = dest_newest.filter(|dest| *dest > backup_newest && *dest <= cut)?;
    Some(applied.max(dest_newest.saturating_add(1)))
}

fn encode_error(what: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore resolve: encode the {what} that sets a row's state again: {e}"),
    }
}

/// The newest backed-up version of one restored document row.
enum NewestVersion {
    Put(DocumentPutRedo),
    Tombstone(DocumentDeleteRedo),
}

impl NewestVersion {
    fn sys_from(&self) -> Option<i64> {
        match self {
            Self::Put(put) => put.stamp.map(|stamp| stamp.sys_from),
            Self::Tombstone(delete) => delete.sys_from,
        }
    }

    /// `(collection, surrogate)`: the row the version belongs to.
    fn row_key(&self) -> (String, u32) {
        match self {
            Self::Put(put) => (put.collection.clone(), put.surrogate),
            Self::Tombstone(delete) => (delete.collection.clone(), delete.surrogate),
        }
    }
}

impl CoreLoop {
    /// The sub-records of a RESTORE batch's rows, `ops`, as they install.
    /// Each restored KV key carries its re-assert ordinal. Each restored
    /// columnar key, vector row and timeseries collection carries the
    /// versions its install writes (`restore_key_plan`), and notes its change
    /// entry in `changes`. Each restored
    /// document row the destination holds past its newest backed-up version
    /// gets that version once more, after every op.
    pub(super) fn reassert_restored_rows(
        &self,
        scope: RestoreScope,
        ops: Vec<RedoSubRecord>,
        changes: &mut RestoreChanges,
    ) -> crate::Result<Vec<RedoSubRecord>> {
        let mut out = Vec::with_capacity(ops.len());
        let mut newest: BTreeMap<(String, u32), NewestVersion> = BTreeMap::new();
        for op in ops {
            if let Some(kv) = KvRestoreRedo::decode(&op.payload) {
                out.push(self.reassert_kv_key(scope, op.record_type, kv)?);
                continue;
            }
            if let Some(key) = ColumnarRestoreRedo::decode(&op.payload) {
                out.extend(self.resolve_restored_columnar_key(
                    scope,
                    op.record_type,
                    key,
                    changes,
                )?);
                continue;
            }
            if let Some(row) = VectorRestoreRedo::decode(&op.payload) {
                out.extend(self.resolve_restored_vector_row(
                    scope,
                    op.record_type,
                    row,
                    changes,
                )?);
                continue;
            }
            // An ended partition is history only: it installs whatever the
            // cut, unless the destination holds a partition its meta names.
            if let Some(ended) = TimeseriesEndedPartitionRedo::decode(&op.payload) {
                if !self.holds_ended_partition(scope, &ended) {
                    out.push(op);
                }
                continue;
            }
            if let Some(collection) = TimeseriesRestoreRedo::decode(&op.payload) {
                out.extend(self.resolve_restored_timeseries(scope, collection, changes)?);
                continue;
            }
            let version = match RecordType::from_raw(op.record_type) {
                Some(RecordType::Put) => {
                    DocumentPutRedo::decode(&op.payload).map(NewestVersion::Put)
                }
                Some(RecordType::Delete) => {
                    DocumentDeleteRedo::decode(&op.payload).map(NewestVersion::Tombstone)
                }
                _ => None,
            };
            if let Some(version) = version {
                let key = version.row_key();
                let newer = newest
                    .get(&key)
                    .is_none_or(|held| held.sys_from() <= version.sys_from());
                if newer {
                    newest.insert(key, version);
                }
            }
            out.push(op);
        }
        for ((collection, surrogate), version) in newest {
            if let Some(op) = self.reassert_document(scope, &collection, surrogate, version)? {
                out.push(op);
            }
        }
        Ok(out)
    }

    /// `kv` with its re-assert ordinal: set when the destination holds a
    /// version or a delete of the key above the backup's newest.
    fn reassert_kv_key(
        &self,
        scope: RestoreScope,
        record_type: u32,
        mut kv: KvRestoreRedo,
    ) -> crate::Result<RedoSubRecord> {
        let backup_newest = kv
            .live
            .as_ref()
            .map(|live| live.sys_from)
            .max(kv.history.iter().map(|v| v.sys_to).max())
            .unwrap_or(i64::MIN);
        let dest_newest = self.kv_engine.key_newest_ordinal(
            scope.database_id.as_u64(),
            scope.tid,
            &kv.collection,
            &kv.key,
        );
        kv.reassert_at = scope.reassert_stamp(backup_newest, dest_newest);
        Ok(RedoSubRecord {
            record_type,
            payload: kv.encode()?,
        })
    }

    /// The version that sets row `surrogate` of `collection` to its newest
    /// backed-up `version` again. `None` when the destination holds nothing
    /// above that version, or when both end absent.
    fn reassert_document(
        &self,
        scope: RestoreScope,
        collection: &str,
        surrogate: u32,
        version: NewestVersion,
    ) -> crate::Result<Option<RedoSubRecord>> {
        let Some(backup_newest) = version.sys_from() else {
            return Ok(None);
        };
        let database_id = scope.database_id.as_u64();
        let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
        let dest_newest =
            self.sparse
                .versioned_latest_sys_from(database_id, scope.tid, collection, &key)?;
        let Some(stamp) = scope.reassert_stamp(backup_newest, dest_newest) else {
            return Ok(None);
        };
        let op = match version {
            NewestVersion::Put(put) => {
                let Some(held) = put.stamp else {
                    return Ok(None);
                };
                let payload = zerompk::to_msgpack_vec(&(
                    put.collection,
                    put.document_id,
                    put.value,
                    put.provenance,
                    put.surrogate,
                    stamp,
                    held.valid_from_ms,
                    held.valid_until_ms,
                ))
                .map_err(|e| encode_error("document put", e))?;
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload,
                }
            }
            NewestVersion::Tombstone(delete) => {
                // A row the destination already holds absent stays as it is.
                let current =
                    self.sparse
                        .versioned_get_current(database_id, scope.tid, collection, &key)?;
                if current.is_none() {
                    return Ok(None);
                }
                let payload = zerompk::to_msgpack_vec(&(
                    delete.collection,
                    delete.document_id,
                    delete.provenance,
                    delete.surrogate,
                    stamp,
                ))
                .map_err(|e| encode_error("document tombstone", e))?;
                RedoSubRecord {
                    record_type: RecordType::Delete as u32,
                    payload,
                }
            }
        };
        Ok(Some(op))
    }

    /// Append to `ops` the version that sets each edge of `edges` to its
    /// newest backed-up version again, for every edge the destination holds
    /// past that version. Only versions other writes stored count: one core
    /// can hold both homes of an edge, and the other home's install of this
    /// transaction is no destination version.
    pub(super) fn reassert_restored_edges(
        &self,
        scope: RestoreScope,
        edges: &[RestoredEdgeVersion],
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        let mut newest: BTreeMap<(&str, &str, &str, &str), &RestoredEdgeVersion> = BTreeMap::new();
        for edge in edges {
            let key = (
                edge.collection.as_str(),
                edge.src_id.as_str(),
                edge.label.as_str(),
                edge.dst_id.as_str(),
            );
            let newer = newest
                .get(&key)
                .is_none_or(|held| held.system_from <= edge.system_from);
            if newer {
                newest.insert(key, edge);
            }
        }
        for ((collection, src, label, dst), edge) in newest {
            let dest_newest = self.edge_store.latest_version_ordinal_of_other_writes(
                EdgeRef::new(
                    scope.database_id,
                    TenantId::new(scope.tid),
                    collection,
                    src,
                    label,
                    dst,
                ),
                scope.edge_apply(),
            )?;
            // Every version of the edge travels in this batch, so a version
            // above the cut is a write that followed the RESTORE.
            if let Some(stamp) = scope.reassert_stamp(edge.system_from, dest_newest) {
                let mut again = edge.clone();
                again.system_from = stamp;
                ops.push(restored_edge(&again, scope.edge_apply())?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No stamp while the backup's newest version is the newest. Above a
    /// newer destination version the stamp is one above it, and never below
    /// the transaction's ordinal.
    #[test]
    fn the_reassert_stamp_sits_above_the_destinations_newest() {
        let open = i64::MAX;
        assert_eq!(reassert_stamp(100, None, 5_000, open), None);
        assert_eq!(reassert_stamp(100, Some(100), 5_000, open), None);
        assert_eq!(reassert_stamp(100, Some(90), 5_000, open), None);
        assert_eq!(reassert_stamp(100, Some(200), 5_000, open), Some(5_000));
        assert_eq!(reassert_stamp(100, Some(9_000), 5_000, open), Some(9_001));
    }

    /// A destination version at or below the cut predates the RESTORE: the
    /// backup's state is set again above it. A version above the cut is a
    /// write that followed the RESTORE: it stays current, in whichever batch
    /// its row falls.
    #[test]
    fn a_write_after_the_restore_cut_stays_current() {
        let cut = 3_000;
        assert_eq!(reassert_stamp(100, Some(2_500), 5_000, cut), Some(5_000));
        assert_eq!(reassert_stamp(100, Some(3_000), 5_000, cut), Some(5_000));
        assert_eq!(reassert_stamp(100, Some(3_001), 5_000, cut), None);
        assert_eq!(reassert_stamp(100, Some(4_000), 5_000, cut), None);
    }

    /// A restored edge applies at the RESTORE's cut, so a TRUNCATE after
    /// the cut hides it and one before leaves it visible. A re-issue with
    /// no cut applies at the transaction's ordinal. A batch is admitted
    /// after the cut, so its ordinal lies above the cut.
    #[test]
    fn a_restored_edge_applies_at_the_cut() {
        let applied = 3_005_000;
        let scope = |cut| RestoreScope {
            database_id: DatabaseId::DEFAULT,
            tid: 1,
            applied,
            cut,
        };
        assert_eq!(scope(2_999_999).edge_apply(), 2_999_999);
        assert_eq!(scope(i64::MAX).edge_apply(), applied);
        // A version above the cut stays current for an edge as for a row.
        assert_eq!(scope(2_999_999).reassert_stamp(100, Some(3_000_000)), None);
        assert_eq!(
            scope(2_999_999).reassert_stamp(100, Some(200)),
            Some(applied)
        );
    }
}
