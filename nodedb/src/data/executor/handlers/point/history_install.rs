// SPDX-License-Identifier: BUSL-1.1

//! Installing a document version below the row's newest stored version.
//!
//! A redo carries the exact ordinal its live write stamped. Boot can journal
//! an earlier write's group parts after a later write's, so a carried
//! ordinal can sit below a version the row already holds. That version is
//! history only:
//!
//! - The body or tombstone lands at its own ordinal.
//! - Its index entries diff against its predecessor, the newest version
//!   below it.
//! - Its index entries close at its successor, the oldest version above it.
//!   A value the version holds and the successor does not gets a tombstone
//!   at the successor's ordinal. A value the successor holds and the version
//!   does not gets a live entry there. A value both hold stays continuous.
//! - No current-state side effect runs: text index, column stats, vector,
//!   spatial and sparse-vector indexes, document cache, materialized-sum
//!   fold. The successor's write already set the current state.
//!
//! The successor entries are written only where no entry is stored at that
//! key, so a rollback removes exactly the entries the install wrote.

use std::collections::HashSet;

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_delete::PointDeleteOutcome;
use crate::engine::document::store::StorageKey;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;
use crate::types::{DatabaseId, TenantId};

/// Where a version below the row's newest version lands.
pub(in crate::data::executor) struct HistorySlot {
    /// The body of the newest version below the install. `None` when there
    /// is none, or it is a tombstone or an erasure.
    pub predecessor: Option<Vec<u8>>,
    /// The ordinal of the oldest version above the install.
    pub successor_sys_from: i64,
    /// The body of that version. `None` for a tombstone or an erasure.
    pub successor: Option<Vec<u8>>,
}

/// The index entries a history install wrote at its successor's ordinal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::data::executor) struct SuccessorIndexWrites {
    /// The successor's ordinal.
    pub sys_from: i64,
    /// `(field, value)` pairs written at `sys_from`. A rollback removes them.
    pub tuples: Vec<(String, String)>,
}

/// The index entries one history install wrote.
pub(in crate::data::executor) struct HistoryIndexWrites {
    /// `(field, value)` pairs written at the install's own ordinal.
    pub at_version: Vec<(String, String)>,
    /// The entries written at the successor's ordinal.
    pub at_successor: SuccessorIndexWrites,
}

/// One history install's index inputs.
pub(in crate::data::executor) struct HistoryIndex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a StorageKey,
    /// The install's ordinal.
    pub sys_from: i64,
    /// The installed version's document. `None` for a tombstone.
    pub doc: Option<&'a serde_json::Value>,
    pub slot: &'a HistorySlot,
}

/// One delete a carried stamp places below the row's newest version.
pub(in crate::data::executor) struct HistoryDelete<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a StorageKey,
    /// The tombstone's ordinal.
    pub sys_from: i64,
    pub slot: HistorySlot,
}

impl CoreLoop {
    /// Store a tombstone at its ordinal and write its index entries against
    /// its neighbours. A predecessor that is not live removed nothing in the
    /// live run, so the install writes nothing. Runs no cascade. Does NOT
    /// commit.
    pub(in crate::data::executor) fn apply_history_delete(
        &self,
        txn: &WriteTransaction,
        d: HistoryDelete<'_>,
    ) -> crate::Result<PointDeleteOutcome> {
        if d.slot.predecessor.is_none() {
            return Ok(PointDeleteOutcome {
                prior_value: None,
                version_sys_from: None,
                index_tuples: Vec::new(),
                history: None,
                memory_undo: Vec::new(),
            });
        }
        self.sparse.versioned_tombstone_in_txn(
            txn,
            d.database_id,
            d.tid,
            d.collection,
            d.storage_key,
            d.sys_from,
        )?;
        let index = self.write_history_index_in_txn(
            txn,
            HistoryIndex {
                database_id: d.database_id,
                tid: d.tid,
                collection: d.collection,
                storage_key: d.storage_key,
                sys_from: d.sys_from,
                doc: None,
                slot: &d.slot,
            },
        )?;
        Ok(PointDeleteOutcome {
            prior_value: d.slot.predecessor,
            version_sys_from: Some(d.sys_from),
            index_tuples: index.at_version,
            history: Some(index.at_successor),
            memory_undo: Vec::new(),
        })
    }

    /// The slot a version at `sys_from` lands in when a stored version of
    /// the row sits above it. `None` when `sys_from` is at or above the
    /// row's newest version: the install sets the current state.
    pub(in crate::data::executor) fn history_slot_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<Option<HistorySlot>> {
        let neighbors = self.sparse.versioned_neighbors_in_txn(
            txn,
            database_id,
            tid,
            collection,
            storage_key,
            sys_from,
        )?;
        let Some(successor) = neighbors.successor else {
            return Ok(None);
        };
        Ok(Some(HistorySlot {
            predecessor: neighbors.predecessor.and_then(|v| v.body),
            successor_sys_from: successor.sys_from,
            successor: successor.body,
        }))
    }

    /// Write the index entries of one history install inside `txn`. A
    /// collection without index paths writes none. A stored neighbour body
    /// that does not decode fails the install.
    pub(in crate::data::executor) fn write_history_index_in_txn(
        &self,
        txn: &WriteTransaction,
        h: HistoryIndex<'_>,
    ) -> crate::Result<HistoryIndexWrites> {
        let mut writes = HistoryIndexWrites {
            at_version: Vec::new(),
            at_successor: SuccessorIndexWrites {
                sys_from: h.slot.successor_sys_from,
                tuples: Vec::new(),
            },
        };
        let config_key = (
            DatabaseId::new(h.database_id),
            TenantId::new(h.tid),
            h.collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            return Ok(writes);
        };
        if config.index_paths.is_empty() {
            return Ok(writes);
        }
        let tuples_of = |doc: Option<&serde_json::Value>| -> HashSet<(String, String)> {
            doc.map(|d| {
                self.index_tuples_for_doc(d, &config.index_paths)
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
        };
        let predecessor_doc = match h.slot.predecessor.as_deref() {
            Some(body) => Some(self.decode_stored_document(config, body)?),
            None => None,
        };
        let successor_doc = match h.slot.successor.as_deref() {
            Some(body) => Some(self.decode_stored_document(config, body)?),
            None => None,
        };
        let predecessor = tuples_of(predecessor_doc.as_ref());
        let this = tuples_of(h.doc);
        let successor = tuples_of(successor_doc.as_ref());

        for tuple in this.difference(&predecessor) {
            self.sparse
                .versioned_index_put_in_txn(txn, index_entry(&h, tuple, h.sys_from))?;
            writes.at_version.push(tuple.clone());
        }
        for tuple in predecessor.difference(&this) {
            self.sparse
                .versioned_index_tombstone_in_txn(txn, index_entry(&h, tuple, h.sys_from))?;
            writes.at_version.push(tuple.clone());
        }

        let at = h.slot.successor_sys_from;
        for tuple in this.difference(&successor) {
            if !self
                .sparse
                .versioned_index_entry_exists_in_txn(txn, index_entry(&h, tuple, at))?
            {
                self.sparse
                    .versioned_index_tombstone_in_txn(txn, index_entry(&h, tuple, at))?;
                writes.at_successor.tuples.push(tuple.clone());
            }
        }
        for tuple in successor.difference(&this) {
            if !self
                .sparse
                .versioned_index_entry_exists_in_txn(txn, index_entry(&h, tuple, at))?
            {
                self.sparse
                    .versioned_index_put_in_txn(txn, index_entry(&h, tuple, at))?;
                writes.at_successor.tuples.push(tuple.clone());
            }
        }
        Ok(writes)
    }
}

/// The index entry of `tuple` for the row `h` installs, at `sys_from`.
fn index_entry<'a>(
    h: &HistoryIndex<'a>,
    tuple: &'a (String, String),
    sys_from: i64,
) -> VersionedIndexEntry<'a> {
    VersionedIndexEntry {
        database_id: h.database_id,
        tenant: h.tid,
        coll: h.collection,
        field: &tuple.0,
        value: &tuple.1,
        doc_id: h.storage_key,
        sys_from,
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::enforcement::typeguard::GuardScope;
    use crate::data::executor::enforcement::unique::UniqueJudge;
    use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
    use crate::engine::document::store::{CollectionConfig, StorageKey};
    use crate::types::{DatabaseId, TenantId};
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const COLL: &str = "places";
    const SURROGATE: Surrogate = Surrogate(11);

    fn db() -> u64 {
        DatabaseId::DEFAULT.as_u64()
    }

    fn key() -> StorageKey {
        StorageKey::for_surrogate(SURROGATE)
    }

    /// Register the collection with an index on `city`.
    fn seed(core: &mut CoreLoop) {
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            CollectionConfig::new(COLL).with_index("city"),
        );
    }

    fn stamp(sys_from: i64) -> BitemporalStamp {
        BitemporalStamp {
            sys_from,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        }
    }

    /// Install a put the way a redo does: at the carried `sys_from`.
    fn put_at(core: &mut CoreLoop, sys_from: i64, body: &[u8]) -> bool {
        core.apply_scope
            .bitemporal_stamps
            .insert(SURROGATE.as_u32(), stamp(sys_from));
        let txn = core.sparse.begin_write().unwrap();
        let outcome = core
            .apply_point_put(
                &txn,
                PointPutParams {
                    database_id: db(),
                    tid: TID,
                    collection: COLL,
                    storage_key: key(),
                    surrogate: SURROGATE,
                    value: body,
                    index_text: true,
                    user_roles: &[],
                    enforce: false,
                    unique: UniqueJudge::Unit,
                    wal_lsn: None,
                    resolved_targets: &[],
                    guards: GuardScope::InjectOnly,
                },
            )
            .unwrap();
        txn.commit().unwrap();
        core.apply_scope
            .bitemporal_stamps
            .remove(&SURROGATE.as_u32());
        assert_eq!(outcome.version_sys_from, sys_from);
        outcome.history.is_some()
    }

    /// Install a delete the way a redo does: at the carried `sys_from`.
    fn delete_at(core: &mut CoreLoop, sys_from: i64) -> bool {
        core.apply_scope
            .bitemporal_stamps
            .insert(SURROGATE.as_u32(), stamp(sys_from));
        let txn = core.sparse.begin_write().unwrap();
        let outcome = core
            .apply_point_delete(
                &txn,
                PointDeleteParams {
                    database_id: db(),
                    tid: TID,
                    collection: COLL,
                    document_id: "p",
                    surrogate: SURROGATE,
                    user_roles: &[],
                    enforce: false,
                    resolved_targets: &[],
                },
            )
            .unwrap();
        txn.commit().unwrap();
        core.apply_scope
            .bitemporal_stamps
            .remove(&SURROGATE.as_u32());
        outcome.history.is_some()
    }

    fn owners(core: &CoreLoop, city: &str, cut: Option<i64>) -> Vec<StorageKey> {
        core.sparse
            .versioned_index_lookup_as_of(db(), TID, COLL, "city", city, cut)
            .unwrap()
    }

    fn city_at(core: &CoreLoop, cut: Option<i64>) -> Option<String> {
        core.sparse
            .versioned_get_as_of(db(), TID, COLL, &key(), cut, None)
            .unwrap()
            .map(|body| {
                crate::data::executor::doc_format::decode_document(&body).unwrap()["city"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
    }

    /// A put stamped below the row's newest version lands as history: the
    /// stale value is found at its own cut only, and the current state, its
    /// index, its text postings and the document cache stay on the newer
    /// version.
    #[test]
    fn a_stale_put_installs_history_only() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core);
        assert!(!put_at(&mut core, 100, br#"{"city":"oslo"}"#));
        assert!(!put_at(&mut core, 300, br#"{"city":"rome"}"#));
        let (docs_before, _) = core
            .inverted
            .corpus_stats(db(), TenantId::new(TID), COLL)
            .unwrap();

        assert!(put_at(&mut core, 200, br#"{"city":"paris"}"#));

        assert_eq!(city_at(&core, None).as_deref(), Some("rome"));
        assert_eq!(city_at(&core, Some(200)).as_deref(), Some("paris"));
        assert_eq!(city_at(&core, Some(150)).as_deref(), Some("oslo"));

        assert!(owners(&core, "paris", None).is_empty());
        assert!(owners(&core, "paris", Some(299)).contains(&key()));
        assert_eq!(owners(&core, "paris", Some(200)), vec![key()]);
        assert!(owners(&core, "paris", Some(199)).is_empty());
        assert!(owners(&core, "oslo", Some(200)).is_empty());
        assert_eq!(owners(&core, "oslo", Some(199)), vec![key()]);
        assert!(owners(&core, "rome", Some(200)).is_empty());
        assert_eq!(owners(&core, "rome", None), vec![key()]);

        let cached = core
            .doc_cache
            .get(db(), TID, COLL, &key())
            .map(<[u8]>::to_vec);
        if let Some(cached) = cached {
            let doc = crate::data::executor::doc_format::decode_document(&cached).unwrap();
            assert_eq!(doc["city"], "rome", "the cache holds the newest version");
        }
        let (docs_after, _) = core
            .inverted
            .corpus_stats(db(), TenantId::new(TID), COLL)
            .unwrap();
        assert_eq!(docs_before, docs_after, "text postings are current state");
    }

    /// A value the predecessor and the successor both hold stays findable at
    /// the current cut when the stale version between them drops it.
    #[test]
    fn a_stale_put_keeps_a_value_the_successor_holds() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core);
        put_at(&mut core, 100, br#"{"city":"oslo"}"#);
        put_at(&mut core, 300, br#"{"city":"oslo"}"#);

        assert!(put_at(&mut core, 200, br#"{"city":"rome"}"#));

        assert_eq!(owners(&core, "oslo", None), vec![key()]);
        assert!(owners(&core, "oslo", Some(200)).is_empty());
        assert_eq!(owners(&core, "rome", Some(200)), vec![key()]);
        assert!(owners(&core, "rome", None).is_empty());
        assert_eq!(city_at(&core, None).as_deref(), Some("oslo"));
    }

    /// A delete stamped below the row's newest version closes only the
    /// version it deleted. The current row and its index stay.
    #[test]
    fn a_stale_delete_installs_history_only() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core);
        put_at(&mut core, 100, br#"{"city":"oslo"}"#);
        put_at(&mut core, 300, br#"{"city":"oslo"}"#);

        assert!(delete_at(&mut core, 200));

        assert_eq!(city_at(&core, None).as_deref(), Some("oslo"));
        assert_eq!(city_at(&core, Some(200)), None);
        assert_eq!(city_at(&core, Some(199)).as_deref(), Some("oslo"));
        assert_eq!(owners(&core, "oslo", None), vec![key()]);
        assert!(owners(&core, "oslo", Some(200)).is_empty());
        assert_eq!(owners(&core, "oslo", Some(100)), vec![key()]);
        let (docs, _) = core
            .inverted
            .corpus_stats(db(), TenantId::new(TID), COLL)
            .unwrap();
        assert_eq!(docs, 1, "the current row keeps its text postings");
    }

    /// Rolling back a stale put removes the version and every entry it
    /// wrote, at its own ordinal and at its successor's.
    #[test]
    fn a_stale_put_rolls_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core);
        put_at(&mut core, 100, br#"{"city":"oslo"}"#);
        put_at(&mut core, 300, br#"{"city":"oslo"}"#);

        core.apply_scope
            .bitemporal_stamps
            .insert(SURROGATE.as_u32(), stamp(200));
        let txn = core.sparse.begin_write().unwrap();
        let outcome = core
            .apply_point_put(
                &txn,
                PointPutParams {
                    database_id: db(),
                    tid: TID,
                    collection: COLL,
                    storage_key: key(),
                    surrogate: SURROGATE,
                    value: br#"{"city":"rome"}"#,
                    index_text: true,
                    user_roles: &[],
                    enforce: false,
                    unique: UniqueJudge::Unit,
                    wal_lsn: None,
                    resolved_targets: &[],
                    guards: GuardScope::InjectOnly,
                },
            )
            .unwrap();
        txn.commit().unwrap();
        core.apply_scope
            .bitemporal_stamps
            .remove(&SURROGATE.as_u32());

        let mut undo = Vec::new();
        crate::data::executor::handlers::transaction::undo::document_outcome::push_put_undo(
            &mut undo,
            crate::data::executor::handlers::transaction::undo::document_outcome::DocumentRow {
                collection: COLL,
                storage_key: key(),
            },
            outcome,
            None,
        );
        core.rollback_undo_log(db(), TID, undo).unwrap();

        assert_eq!(city_at(&core, Some(200)).as_deref(), Some("oslo"));
        assert_eq!(owners(&core, "oslo", Some(200)), vec![key()]);
        assert_eq!(owners(&core, "oslo", None), vec![key()]);
        assert!(owners(&core, "rome", Some(200)).is_empty());
        let txn = core.sparse.begin_write().unwrap();
        let live_at_300 = core
            .sparse
            .versioned_index_entry_exists_in_txn(
                &txn,
                crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                    database_id: db(),
                    tenant: TID,
                    coll: COLL,
                    field: "city",
                    value: "oslo",
                    doc_id: &key(),
                    sys_from: 300,
                },
            )
            .unwrap();
        assert!(
            !live_at_300,
            "the successor entry the install wrote is gone"
        );
    }
}
