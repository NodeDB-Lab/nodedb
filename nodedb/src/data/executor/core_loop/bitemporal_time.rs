// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal stamping helpers for `CoreLoop`.
//!
//! Every bitemporal document version is keyed by a system-time ordinal from
//! the clock graph edges stamp from (`CoreLoop::hlc`). Documents and edges
//! share one system-time domain, so one ordinal cut splits both alike. A
//! read compares against the plan scope's ordinal
//! (`SystemTimeScope::as_of_ordinal`): the statement's pinned cut, or the
//! last ordinal of an unpinned millisecond. `_ts_system` renders
//! `nodedb_types::ordinal_to_ms(ordinal)`.
//!
//! A bitemporal columnar row stores the same ordinal in its `_ts_system`
//! column, so one cut splits columnar versions as it splits documents and
//! edges. Every read renders the column as `ordinal_to_ms(ordinal)`.
//!
//! A write that applies a replicated data-group entry stamps the ordinal the
//! entry's leader assigned (`ApplyScope::replicated_ordinal`), so every
//! replica writes the same version keys.
//!
//! Every bitemporal write, document, columnar, or resolve, stamps by one
//! rule ([`KeyStamps`]). A row starts from its base ordinal: the Calvin
//! transaction's ordinal, else the replicated entry's or the carried one,
//! else a fresh ordinal of the shared clock. The row then takes one above
//! the newest version each key it touches holds, when that version sits at
//! or above the base. A key an earlier row of the same statement stamped
//! holds that stamp. Any other key holds its newest stored version. So every
//! version of a key sorts after the versions before it. The stamp folds into
//! the shared clock. The raise reads only the replicated log and the stored
//! rows, so the live write, every replay, and every replica stamp alike.
//!
//! A Calvin transaction stamps one ordinal, and a raise splits it across
//! stamps only on a key that already holds a version at or above that
//! ordinal. Every node computes the same stamps, and a historical read waits
//! for its cut marker, which installs every Calvin transaction sequenced
//! before it (`control::read_cut::wait`). So every node reads the same
//! versions at one cut. At a cut at or above the transaction's highest stamp
//! the transaction reads whole. At a cut below a raised stamp the raised key
//! reads its newest version at or below the cut, the version before the
//! transaction's, and per-key order holds.

use std::collections::HashMap;
use std::hash::Hash;

use nodedb_types::StorageKey;

use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Is this collection configured for bitemporal (versioned) storage?
    /// Collections flagged `bitemporal: true` on registration route every
    /// write to the versioned redb table and every read via the Ceiling
    /// resolver, enabling `FOR SYSTEM_TIME AS OF` / `FOR VALID_TIME`
    /// queries.
    #[inline]
    pub(in crate::data::executor) fn is_bitemporal(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        let key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&key).is_some_and(|c| c.bitemporal)
    }

    /// The system-time ordinal a bitemporal document write stamps.
    ///
    /// Inside a Calvin stage or resolve it is the transaction's ordinal,
    /// which every participant and replica derives from the epoch instant
    /// and position. A write that applies a replicated data-group entry
    /// takes the ordinal the entry's leader assigned. Elsewhere it is a
    /// fresh ordinal of the shared clock, strictly above every ordinal the
    /// clock handed out or folded in. An install or replay stamps the
    /// ordinal its record carries instead. An edge write stamps from the
    /// same sources.
    #[inline]
    pub(in crate::data::executor) fn bitemporal_ordinal(&self) -> i64 {
        match (
            self.apply_scope.calvin_txn_ordinal,
            self.apply_scope.replicated_ordinal,
        ) {
            (Some(ordinal), _) | (None, Some(ordinal)) => ordinal,
            (None, None) => self.hlc.next_ordinal(),
        }
    }

    /// The system-time ordinal the version of row `key` a bitemporal
    /// document write stamps: [`Self::bitemporal_ordinal`], raised one above
    /// the row's newest stored version when that version sits at or above
    /// it ([`KeyStamps`]).
    pub(in crate::data::executor) fn row_version_ordinal(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: &StorageKey,
    ) -> crate::Result<i64> {
        self.rows_version_ordinal(database_id, tid, collection, std::slice::from_ref(key))
    }

    /// The one system-time ordinal every version a bitemporal document
    /// statement writes to `keys` stamps: [`Self::bitemporal_ordinal`] (the
    /// Calvin transaction's, the replicated entry's, or a fresh one), raised
    /// one above the newest stored version of any of `keys` ([`KeyStamps`]).
    /// The statement writes each key once and keeps one system time for all
    /// its rows. The ordinal folds into the shared clock.
    ///
    /// `Err` when a stored version does not read.
    pub(in crate::data::executor) fn rows_version_ordinal(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        keys: &[StorageKey],
    ) -> crate::Result<i64> {
        self.raised_rows_ordinal(
            self.bitemporal_ordinal(),
            database_id,
            tid,
            collection,
            keys,
        )
    }

    /// [`Self::rows_version_ordinal`] from a `base` the statement already
    /// took. Returns `base` when no key holds a version at or above it.
    ///
    /// `Err` when a stored version does not read.
    pub(in crate::data::executor) fn raised_rows_ordinal(
        &self,
        base: i64,
        database_id: u64,
        tid: u64,
        collection: &str,
        keys: &[StorageKey],
    ) -> crate::Result<i64> {
        let ordinal = KeyStamps::default().stamp_row(base, keys.to_vec(), |key| {
            self.sparse
                .versioned_latest_sys_from(database_id, tid, collection, key)
        })?;
        self.observe_bitemporal_ordinal(ordinal);
        Ok(ordinal)
    }

    /// Fold the system-time ordinal a committed or replayed record carries
    /// into the shared clock. A later local write then stamps above it.
    #[inline]
    pub(in crate::data::executor) fn observe_bitemporal_ordinal(&self, ordinal: i64) {
        self.hlc.update_from_remote(ordinal);
    }

    /// The `_ts_system` ordinal row `row` of a bitemporal columnar insert
    /// stamps.
    ///
    /// Inside a Calvin stage or resolve it is the transaction's ordinal.
    /// Otherwise an insert whose plan or WAL record carries `system_from`
    /// stamps `system_from + row`, folded into the shared clock, so the live
    /// insert and every replay of its record stamp the same ordinal and a
    /// later write stamps above it. An insert with no carried stamp takes a
    /// fresh ordinal of the shared clock.
    pub(in crate::data::executor) fn columnar_system_ordinal(
        &self,
        system_from: Option<i64>,
        row: usize,
    ) -> i64 {
        if let Some(ordinal) = self.apply_scope.calvin_txn_ordinal {
            return ordinal;
        }
        match system_from {
            Some(first) => {
                let ordinal = carried_row_ordinal(first, row);
                self.observe_bitemporal_ordinal(ordinal);
                ordinal
            }
            None => self.hlc.next_ordinal(),
        }
    }
}

impl CoreLoop {
    /// Fold the `_ts_system` ordinal a stored columnar row carries into the
    /// shared clock, so a write after an install or a restore stamps above
    /// it. A row of a schema with no `_ts_system` column folds nothing.
    pub(in crate::data::executor) fn observe_columnar_row_ordinal(
        &self,
        schema: &nodedb_types::columnar::ColumnarSchema,
        row: &[nodedb_types::Value],
    ) {
        if let Some(idx) = schema.ts_system_idx()
            && let Some(nodedb_types::Value::Integer(ordinal)) = row.get(idx)
        {
            self.observe_bitemporal_ordinal(*ordinal);
        }
    }
}

/// The ordinal row `row` of an insert stamped from `first` carries.
pub(in crate::data::executor) fn carried_row_ordinal(first: i64, row: usize) -> i64 {
    first.saturating_add(i64::try_from(row).unwrap_or(i64::MAX))
}

/// `ordinal`, or one above `newest` when `newest` sits at or above
/// `ordinal`: the stamp of a version that sorts after the newest stored
/// version of its row.
pub(in crate::data::executor) fn raised_above(ordinal: i64, newest: Option<i64>) -> i64 {
    match newest {
        Some(newest) if newest >= ordinal => newest.saturating_add(1),
        _ => ordinal,
    }
}

/// The newest stamp each key of one bitemporal statement took: the one
/// raise rule every document, columnar, and resolve write stamps by (see
/// the module doc).
pub(in crate::data::executor) struct KeyStamps<K = Vec<u8>> {
    newest: HashMap<K, i64>,
}

impl<K> Default for KeyStamps<K> {
    fn default() -> Self {
        Self {
            newest: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash + Ord> KeyStamps<K> {
    /// The stamp of a row that touches `keys`: `base`, raised one above the
    /// newest version any of `keys` holds. A key an earlier row stamped holds
    /// that stamp, any other key holds `stored(key)`. Every key of the row
    /// then holds the returned stamp. A key listed twice counts once.
    ///
    /// `Err` when `stored` fails.
    pub(in crate::data::executor) fn stamp_row(
        &mut self,
        base: i64,
        mut keys: Vec<K>,
        mut stored: impl FnMut(&K) -> crate::Result<Option<i64>>,
    ) -> crate::Result<i64> {
        keys.sort_unstable();
        keys.dedup();
        let mut newest: Option<i64> = None;
        for key in &keys {
            let held = match self.newest.get(key) {
                Some(stamp) => Some(*stamp),
                None => stored(key)?,
            };
            newest = newest.max(held);
        }
        let stamp = raised_above(base, newest);
        for key in keys {
            self.newest.insert(key, stamp);
        }
        Ok(stamp)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::OrdinalClock;

    use super::{KeyStamps, carried_row_ordinal, raised_above};

    fn none(_: &Vec<u8>) -> crate::Result<Option<i64>> {
        Ok(None)
    }

    /// Two rows of one key at one base take two stamps, the second above the
    /// first. A row on another key keeps the base.
    #[test]
    fn rows_of_one_key_at_one_base_take_rising_stamps() {
        let mut stamps = KeyStamps::default();
        assert_eq!(
            stamps.stamp_row(100, vec![b"a".to_vec()], none).unwrap(),
            100
        );
        assert_eq!(
            stamps.stamp_row(100, vec![b"a".to_vec()], none).unwrap(),
            101
        );
        assert_eq!(
            stamps.stamp_row(100, vec![b"b".to_vec()], none).unwrap(),
            100
        );
        assert_eq!(
            stamps.stamp_row(100, vec![b"a".to_vec()], none).unwrap(),
            102
        );
    }

    /// A stored version at or above the base raises the row one above it.
    /// A stored version below the base leaves the base.
    #[test]
    fn a_stored_version_above_the_base_raises_the_row() {
        let stored = |key: &Vec<u8>| -> crate::Result<Option<i64>> {
            Ok(match key.as_slice() {
                b"ahead" => Some(500),
                b"behind" => Some(5),
                _ => None,
            })
        };
        let mut stamps = KeyStamps::default();
        assert_eq!(
            stamps
                .stamp_row(100, vec![b"ahead".to_vec()], stored)
                .unwrap(),
            501
        );
        assert_eq!(
            stamps
                .stamp_row(100, vec![b"behind".to_vec()], stored)
                .unwrap(),
            100
        );
        assert_eq!(
            stamps
                .stamp_row(100, vec![b"ahead".to_vec()], stored)
                .unwrap(),
            502
        );
    }

    /// A row that touches two keys rises above the newer of them, and both
    /// keys then hold its stamp. A later row on one key leaves the other
    /// key's stamp as it was.
    #[test]
    fn a_row_on_two_keys_rises_above_both() {
        let stored = |key: &Vec<u8>| -> crate::Result<Option<i64>> {
            Ok((key.as_slice() == b"old").then_some(300))
        };
        let mut stamps = KeyStamps::default();
        let moved = stamps
            .stamp_row(100, vec![b"old".to_vec(), b"new".to_vec()], stored)
            .unwrap();
        assert_eq!(moved, 301);
        assert_eq!(
            stamps
                .stamp_row(100, vec![b"new".to_vec()], stored)
                .unwrap(),
            302
        );
        // `old` still holds 301: the row on `new` did not touch it.
        assert_eq!(
            stamps
                .stamp_row(100, vec![b"old".to_vec()], stored)
                .unwrap(),
            302
        );
    }

    /// A stamp at or below the newest stored version of its row rises one
    /// above it. A stamp above it stays.
    #[test]
    fn a_stamp_rises_above_the_newest_stored_version() {
        assert_eq!(raised_above(5_000, None), 5_000);
        assert_eq!(raised_above(5_000, Some(4_999)), 5_000);
        assert_eq!(raised_above(5_000, Some(5_000)), 5_001);
        assert_eq!(raised_above(5_000, Some(9_000)), 9_001);
        assert_eq!(raised_above(5_000, Some(i64::MAX)), i64::MAX);
    }

    /// The rows of one carried insert stamp one consecutive run, so two
    /// versions of a key in one batch stay distinct.
    #[test]
    fn carried_rows_stamp_a_consecutive_run() {
        assert_eq!(carried_row_ordinal(1_000, 0), 1_000);
        assert_eq!(carried_row_ordinal(1_000, 3), 1_003);
        assert_eq!(carried_row_ordinal(i64::MAX - 1, 5), i64::MAX);
    }

    /// A folded ordinal ahead of the wall clock keeps the next local stamp
    /// above it, so a replayed version never sorts after a newer write.
    #[test]
    fn a_folded_ordinal_keeps_later_stamps_above_it() {
        let clock = OrdinalClock::new();
        let ahead = clock.next_ordinal().saturating_add(1_000_000_000_000);
        clock.update_from_remote(ahead);
        assert!(clock.next_ordinal() > ahead);
    }

    /// A write that applies a replicated entry stamps the entry's ordinal. A
    /// row whose newest stored version sits at or above it takes one above
    /// that version, and the raise folds into the clock.
    #[test]
    fn a_replicated_write_stamps_its_entry_ordinal_raised_above_the_row() {
        use nodedb_types::{StorageKey, Surrogate};

        use crate::data::executor::core_loop::CoreLoop;
        use crate::data::executor::core_loop::tests::make_core_with_dir;
        use crate::engine::sparse::btree_versioned::VersionedPut;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        let stored = StorageKey::for_surrogate(Surrogate::new(1));
        let fresh = StorageKey::for_surrogate(Surrogate::new(2));
        core.sparse
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 1,
                coll: "c",
                doc_id: &stored,
                sys_from: ahead,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: b"v",
            })
            .expect("put version");

        let entry = ahead - 1_000;
        core.apply_scope.replicated_ordinal = Some(entry);
        assert_eq!(core.bitemporal_ordinal(), entry);
        assert_eq!(
            core.row_version_ordinal(0, 1, "c", &fresh)
                .expect("fresh row"),
            entry,
            "a row with no newer version takes the entry's ordinal"
        );
        assert_eq!(
            core.rows_version_ordinal(0, 1, "c", &[fresh, stored])
                .expect("statement"),
            ahead + 1,
            "the statement rises above its rows' newest version"
        );
        core.apply_scope.replicated_ordinal = None;
        assert!(
            core.bitemporal_ordinal() > ahead + 1,
            "the raise folded into the clock"
        );
    }

    /// A Calvin document write starts at the transaction's ordinal and rises
    /// per key: a key whose stored version sits above the ordinal takes a
    /// version that sorts after it, a fresh key keeps the ordinal, and a
    /// second write of a key the transaction already wrote sorts after that
    /// version. Every stamp folds into the clock.
    #[test]
    fn a_calvin_document_write_sorts_after_a_stored_version_above_its_ordinal() {
        use nodedb_types::{StorageKey, Surrogate};

        use crate::data::executor::core_loop::CoreLoop;
        use crate::data::executor::core_loop::tests::make_core_with_dir;
        use crate::engine::sparse::btree_versioned::VersionedPut;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        let stored = StorageKey::for_surrogate(Surrogate::new(1));
        let fresh = StorageKey::for_surrogate(Surrogate::new(2));
        let put = |core: &CoreLoop, key: &StorageKey, sys_from: i64| {
            core.sparse
                .versioned_put(VersionedPut {
                    database_id: 0,
                    tenant: 1,
                    coll: "c",
                    doc_id: key,
                    sys_from,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: b"v",
                })
                .expect("put version");
        };
        put(&core, &stored, ahead);

        let txn = ahead - 1_000;
        core.apply_scope.calvin_txn_ordinal = Some(txn);
        let raised = core
            .row_version_ordinal(0, 1, "c", &stored)
            .expect("stored row");
        assert_eq!(raised, ahead + 1, "the version sorts after the stored one");
        let first = core
            .row_version_ordinal(0, 1, "c", &fresh)
            .expect("fresh row");
        assert_eq!(first, txn, "a key with no newer version keeps the ordinal");
        put(&core, &fresh, first);
        assert_eq!(
            core.row_version_ordinal(0, 1, "c", &fresh)
                .expect("second write"),
            txn + 1,
            "a second write of the key sorts after the first"
        );
        core.apply_scope.calvin_txn_ordinal = None;
        assert!(
            core.bitemporal_ordinal() > ahead + 1,
            "the raise folded into the clock"
        );
    }

    /// Two stamps in the same millisecond stay distinct and ordered, which a
    /// millisecond stamp cannot keep.
    #[test]
    fn stamps_are_strictly_increasing() {
        let clock = OrdinalClock::new();
        let a = clock.next_ordinal();
        let b = clock.next_ordinal();
        assert!(a < b);
    }
}
