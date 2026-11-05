// SPDX-License-Identifier: BUSL-1.1

//! Commit-time validation of a transaction's read set against its snapshot.
//!
//! A transaction read every entry at one snapshot ordinal `S`. An entry is
//! unchanged while no committed version of what it observed carries an
//! ordinal above `S`:
//! - a point read: the row's newest stored version, live or tombstone;
//! - a predicate read: the newest version of the collection's slice on the
//!   validating vShard;
//! - a document index read: the newest index entry of the value or range,
//!   which every insert, update, and delete that moved a row into or out of
//!   it wrote. Any other engine's index read takes the collection's newest
//!   version;
//! - a homed read with no collection: the vShard's newest version.
//!
//! Every value reads stored versions only, so every replica that applied
//! the same entries reaches one verdict.

use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree_versioned::IndexNewestRange;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::CoreLoop;

/// Whether a newest ordinal sits at or below `snapshot`.
pub(in crate::data::executor) fn at_or_below(newest: Option<i64>, snapshot: i64) -> bool {
    newest.is_none_or(|newest| newest <= snapshot)
}

impl CoreLoop {
    /// Whether this vShard's slice of a transaction's read set is unchanged
    /// since `snapshot`.
    ///
    /// Validates the entries homed on this request's vShard. An entry homed
    /// elsewhere passes: its home validates it. An entry's home is its
    /// `home_vshard` when set, else its collection's vShard. An entry whose
    /// home names no vShard, or whose collection name does not de-qualify,
    /// fails: every replica refuses it alike. `Err` when a store does not
    /// read.
    pub(in crate::data::executor) fn txn_reads_unchanged(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        reads: &[VersionedReadEntry],
        snapshot: i64,
    ) -> crate::Result<bool> {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        let local = task.request.vshard_id;
        for entry in reads {
            let home = match entry.home_vshard {
                Some(home) if home < VShardId::COUNT => VShardId::new(home),
                Some(_) => return Ok(false),
                None => {
                    match nodedb_types::CollectionKey::from_qualified_str(db, &entry.collection) {
                        Ok(key) => key.vshard(),
                        Err(_) => return Ok(false),
                    }
                }
            };
            if home != local {
                continue;
            }
            if !self.read_unchanged(home, db, tenant, entry, snapshot)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether one entry homed on `home` is unchanged since `snapshot`.
    fn read_unchanged(
        &mut self,
        home: VShardId,
        db: DatabaseId,
        tenant: TenantId,
        entry: &VersionedReadEntry,
        snapshot: i64,
    ) -> crate::Result<bool> {
        let collection = entry.collection.as_str();
        if collection.is_empty() {
            return Ok(at_or_below(self.vshard_newest(home)?, snapshot));
        }
        let exact = match &entry.key {
            ReadKeyIdent::Point(key) => self.row_newest(db, tenant, collection, key)?,
            ReadKeyIdent::Predicate => None,
            ReadKeyIdent::IndexEq { field, value } => self.index_newest(
                entry.engine,
                db,
                tenant,
                collection,
                field,
                (Some(value.as_str()), Some(value.as_str())),
            )?,
            ReadKeyIdent::IndexRange { field, lo, hi } => self.index_newest(
                entry.engine,
                db,
                tenant,
                collection,
                field,
                (lo.as_deref(), hi.as_deref()),
            )?,
        };
        let newest = match exact {
            Some(newest) => newest,
            None => self.slice_newest(home, db, tenant, collection)?,
        };
        Ok(at_or_below(newest, snapshot))
    }

    /// The newest versioned index entry of `field` within `bounds`
    /// (inclusive), for a document collection. `None` for any other engine,
    /// whose index reads validate against the collection.
    fn index_newest(
        &self,
        engine: EngineTag,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        field: &str,
        bounds: (Option<&str>, Option<&str>),
    ) -> crate::Result<Option<Option<i64>>> {
        if engine != EngineTag::Document || !self.rows_keyed_as_documents(db, tenant, collection) {
            return Ok(None);
        }
        let (lower, upper) = bounds;
        self.sparse
            .versioned_index_newest_in(IndexNewestRange {
                database_id: db.as_u64(),
                tenant: tenant.as_u64(),
                coll: collection,
                field,
                lower,
                upper,
            })
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::calvin::EngineTag;
    use nodedb_types::{StorageKey, Surrogate};

    use super::*;
    use crate::data::executor::core_loop::newest_ordinals::collection_vshard;
    use crate::data::executor::core_loop::tests::{make_core, task_with_vshard};
    use crate::engine::graph::edge_store::EdgeRef;
    use crate::engine::kv::KvPutParams;
    use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
    use crate::types::KeyRepr;

    const TID: u64 = 1;

    fn task_for(collection: &str) -> ExecutionTask {
        task_with_vshard(collection_vshard(DatabaseId::DEFAULT, collection).unwrap())
    }

    fn entry(engine: EngineTag, collection: &str, key: ReadKeyIdent) -> VersionedReadEntry {
        VersionedReadEntry {
            engine,
            collection: collection.to_string(),
            key,
            home_vshard: None,
        }
    }

    fn doc_key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    fn doc_version(core: &CoreLoop, collection: &str, surrogate: u32, ordinal: i64) {
        core.sparse
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: TID,
                coll: collection,
                doc_id: &doc_key(surrogate),
                sys_from: ordinal,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &[0xc0],
            })
            .expect("doc version");
    }

    fn doc_point(collection: &str, surrogate: u32) -> VersionedReadEntry {
        entry(
            EngineTag::Document,
            collection,
            ReadKeyIdent::Point(KeyRepr::Surrogate(surrogate)),
        )
    }

    fn unchanged(core: &mut CoreLoop, read: VersionedReadEntry, snapshot: i64) -> bool {
        let task = task_for(&read.collection);
        core.txn_reads_unchanged(&task, TID, &[read], snapshot)
            .expect("read stores")
    }

    #[test]
    fn a_document_point_read_fails_once_a_version_lands_above_its_snapshot() {
        let (mut core, _, _, _dir) = make_core();
        doc_version(&core, "docs", 7, 100);
        assert!(unchanged(&mut core, doc_point("docs", 7), 100));
        assert!(
            unchanged(&mut core, doc_point("docs", 8), 100),
            "absent row"
        );
        doc_version(&core, "docs", 7, 150);
        assert!(!unchanged(&mut core, doc_point("docs", 7), 120));
        assert!(unchanged(&mut core, doc_point("docs", 7), 150));
    }

    #[test]
    fn a_tombstone_above_the_snapshot_fails_a_point_read() {
        let (mut core, _, _, _dir) = make_core();
        doc_version(&core, "docs", 7, 100);
        core.sparse
            .versioned_tombstone(0, TID, "docs", &doc_key(7), 160)
            .expect("tombstone");
        assert!(!unchanged(&mut core, doc_point("docs", 7), 150));
        assert!(unchanged(&mut core, doc_point("docs", 7), 160));
    }

    /// A history version installed below the snapshot leaves the row's
    /// newest version where it was.
    #[test]
    fn a_history_install_below_the_snapshot_does_not_trip_a_read() {
        let (mut core, _, _, _dir) = make_core();
        doc_version(&core, "docs", 7, 100);
        assert!(unchanged(
            &mut core,
            entry(EngineTag::Document, "docs", ReadKeyIdent::Predicate),
            120
        ));
        doc_version(&core, "docs", 7, 40);
        core.track_newest_write(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "docs",
            Some(&KeyRepr::Surrogate(7)),
            collection_vshard(DatabaseId::DEFAULT, "docs").unwrap(),
        )
        .unwrap();
        assert!(unchanged(&mut core, doc_point("docs", 7), 120));
        assert!(unchanged(
            &mut core,
            entry(EngineTag::Document, "docs", ReadKeyIdent::Predicate),
            120
        ));
    }

    /// A predicate read fails when any row of its collection changed, and
    /// the cached value tracks the change.
    #[test]
    fn a_predicate_read_fails_when_any_row_changed() {
        let (mut core, _, _, _dir) = make_core();
        let predicate = || entry(EngineTag::Document, "docs", ReadKeyIdent::Predicate);
        doc_version(&core, "docs", 1, 100);
        assert!(unchanged(&mut core, predicate(), 100), "caches the slice");
        doc_version(&core, "docs", 2, 130);
        core.track_newest_write(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "docs",
            Some(&KeyRepr::Surrogate(2)),
            collection_vshard(DatabaseId::DEFAULT, "docs").unwrap(),
        )
        .unwrap();
        assert!(!unchanged(&mut core, predicate(), 120));
        assert!(unchanged(&mut core, predicate(), 130));
        // Another collection's write leaves the slice alone.
        doc_version(&core, "other", 1, 999);
        assert!(unchanged(&mut core, predicate(), 130));
    }

    #[test]
    fn a_kv_point_read_counts_puts_and_deletes() {
        let (mut core, _, _, _dir) = make_core();
        let put = |core: &mut CoreLoop, ordinal: i64| {
            let scope = core.kv_engine.open_write_scope(Some(ordinal));
            core.kv_engine
                .put(KvPutParams {
                    database_id: 0,
                    tenant_id: TID,
                    collection: "kv",
                    key: b"k",
                    value: b"v",
                    ttl_ms: 0,
                    now_ms: 0,
                    surrogate: Surrogate::new(3),
                })
                .expect("put");
            core.kv_engine.close_write_scope(scope);
        };
        let point = || {
            entry(
                EngineTag::Kv,
                "kv",
                ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"k".as_slice()))),
            )
        };
        put(&mut core, 100);
        assert!(unchanged(&mut core, point(), 100));
        let scope = core.kv_engine.open_write_scope(Some(200));
        core.kv_engine.delete(0, TID, "kv", &[b"k".to_vec()], 0);
        core.kv_engine.close_write_scope(scope);
        assert!(!unchanged(&mut core, point(), 150));
        assert!(unchanged(&mut core, point(), 200));
    }

    #[test]
    fn an_edge_point_read_fails_on_a_newer_edge_version() {
        let (mut core, _, _, _dir) = make_core();
        let edge = EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(TID), "g", "a", "L", "b");
        core.edge_store
            .put_edge_versioned(edge, b"", 100, i64::MIN, i64::MAX)
            .expect("edge");
        let point = || {
            entry(
                EngineTag::Graph,
                "g",
                ReadKeyIdent::Point(KeyRepr::Edge {
                    src: Box::from("a"),
                    label: Box::from("L"),
                    dst: Box::from("b"),
                }),
            )
        };
        assert!(unchanged(&mut core, point(), 100));
        core.edge_store.soft_delete_edge(edge, 140).expect("delete");
        assert!(!unchanged(&mut core, point(), 120));
    }

    /// An index range read fails on an entry that moved into its range
    /// after the snapshot (a phantom), and not on one outside it.
    #[test]
    fn an_index_range_read_catches_a_phantom_in_its_range_only() {
        let (mut core, _, _, _dir) = make_core();
        let index = |core: &CoreLoop, value: &str, surrogate: u32, ordinal: i64| {
            doc_version(core, "docs", surrogate, ordinal);
            core.sparse
                .versioned_index_put(VersionedIndexEntry {
                    database_id: 0,
                    tenant: TID,
                    coll: "docs",
                    field: "age",
                    value,
                    doc_id: &doc_key(surrogate),
                    sys_from: ordinal,
                })
                .expect("index entry");
        };
        let range = || {
            entry(
                EngineTag::Document,
                "docs",
                ReadKeyIdent::IndexRange {
                    field: "age".into(),
                    lo: Some("30".into()),
                    hi: Some("40".into()),
                },
            )
        };
        index(&core, "35", 1, 100);
        assert!(unchanged(&mut core, range(), 100));
        index(&core, "50", 2, 150);
        assert!(unchanged(&mut core, range(), 100), "outside the range");
        index(&core, "40", 3, 160);
        assert!(!unchanged(&mut core, range(), 100), "phantom at the bound");
        let eq = entry(
            EngineTag::Document,
            "docs",
            ReadKeyIdent::IndexEq {
                field: "age".into(),
                value: "35".into(),
            },
        );
        assert!(
            unchanged(&mut core, eq, 100),
            "the equality value is untouched"
        );
    }

    /// A homed read with no collection validates against its vShard's newest
    /// version. A read homed on another vShard passes here.
    #[test]
    fn a_homed_read_of_every_collection_validates_against_the_vshard() {
        let (mut core, _, _, _dir) = make_core();
        doc_version(&core, "docs", 1, 100);
        let home = collection_vshard(DatabaseId::DEFAULT, "docs").unwrap();
        let homed = VersionedReadEntry {
            engine: EngineTag::Graph,
            collection: String::new(),
            key: ReadKeyIdent::Predicate,
            home_vshard: Some(home.as_u32()),
        };
        let task = task_for("docs");
        let check = |core: &mut CoreLoop, snapshot: i64| {
            core.txn_reads_unchanged(&task, TID, std::slice::from_ref(&homed), snapshot)
                .expect("read stores")
        };
        assert!(check(&mut core, 100));
        assert!(!check(&mut core, 99));

        let elsewhere = VersionedReadEntry {
            home_vshard: Some((home.as_u32() + 1) % VShardId::COUNT),
            ..homed.clone()
        };
        assert!(
            core.txn_reads_unchanged(&task, TID, &[elsewhere], 0)
                .expect("read stores"),
            "its home validates it"
        );
    }

    #[test]
    fn a_write_over_a_row_changed_after_the_snapshot_conflicts() {
        let (core, _, _, _dir) = make_core();
        doc_version(&core, "docs", 7, 100);
        let task = task_for("docs");
        let writes = [
            super::super::writes::TxnWrite {
                collection: "docs".into(),
                key: KeyRepr::Surrogate(7),
            },
            super::super::writes::TxnWrite {
                collection: "docs".into(),
                key: KeyRepr::Surrogate(8),
            },
        ];
        assert!(
            core.txn_writes_unchanged(&task, TID, &writes, 100)
                .expect("read")
        );
        assert!(
            !core
                .txn_writes_unchanged(&task, TID, &writes, 99)
                .expect("read")
        );
    }
}
