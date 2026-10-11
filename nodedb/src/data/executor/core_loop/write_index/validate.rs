// SPDX-License-Identifier: BUSL-1.1

//! Calvin read-set validation against the per-core write versions.

use nodedb_types::calvin::VersionedReadEntry;

use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, VShardId};

use super::super::CoreLoop;

impl CoreLoop {
    /// Whether this shard's slice of a transaction's versioned read-set is
    /// still current against the local write versions.
    ///
    /// Filters the read-set to the entries homed on this request's vShard,
    /// the only reads this core holds versions for, then checks each against
    /// the write-version index. Short-circuits on the first entry that is no
    /// longer current. An empty or fully-remote slice is vacuously current.
    /// The `(database, tenant)` scope mirrors the write-version recorder so a
    /// read validates against the same key space it was recorded in.
    ///
    /// Read versions are data-group log positions, so the verdict is the same
    /// on every replica of the vShard, whichever node served the read.
    ///
    /// An entry's home is its `home_vshard` when set (a cross-shard graph read
    /// names the key vShard it read edges on), else its collection's vShard,
    /// by the same collection-in-database function the scheduler routes plans
    /// with. A homed entry with no collection observed every collection on its
    /// vShard, so it is current only while the vShard's latest version has
    /// not passed its read version.
    pub(in crate::data::executor) fn read_set_still_current(
        &self,
        task: &ExecutionTask,
        tid: u64,
        versioned_reads: &[VersionedReadEntry],
    ) -> bool {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        let local_vshard = task.request.vshard_id;
        versioned_reads.iter().all(|entry| {
            let home = match entry.home_vshard {
                Some(home) if home < VShardId::COUNT => VShardId::new(home),
                // A home past the vShard space names no shard: fail closed.
                Some(_) => return false,
                // An entry carries the plan's database-qualified name. One
                // that does not de-qualify cannot be homed or validated, so
                // the read set fails closed.
                None => {
                    match nodedb_types::CollectionKey::from_qualified_str(db, &entry.collection) {
                        Ok(key) => key.vshard(),
                        Err(_) => return false,
                    }
                }
            };
            if home != local_vshard {
                return true;
            }
            if entry.collection.is_empty() {
                return self.write_index.latest(home) <= entry.read_version;
            }
            self.write_index.read_is_valid(
                db,
                tenant,
                home,
                &entry.collection,
                &entry.key,
                entry.read_version,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::calvin::{EngineTag, ReadKeyIdent};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, WriteVersion};

    use super::super::keys::{KeyRepr, WriteStamp};
    use super::super::tests::{doc_value, make_core, make_request, task_with_vshard};
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::Lsn;

    /// The vShard `collection` homes to in the default database: mirrors the
    /// homing `read_set_still_current` filters entries by.
    fn local_vshard(collection: &str) -> VShardId {
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard()
    }

    /// Some vShard other than `than`, for exercising the cross-shard filter.
    fn other_vshard(than: VShardId) -> VShardId {
        VShardId::new((than.as_u32() + 1) % VShardId::COUNT)
    }

    fn at(index: u64) -> WriteVersion {
        WriteVersion::logged(0, index)
    }

    /// Record a write applying the entry at `index` on `vshard`.
    fn write(
        core: &mut CoreLoop,
        vshard: VShardId,
        collection: &str,
        key: Option<KeyRepr>,
        index: u64,
    ) {
        core.note_write(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection,
            key,
            WriteStamp {
                vshard,
                lsn: Lsn::new(index),
                entry: Some(at(index)),
            },
        );
    }

    fn point_entry(collection: &str, surrogate: u32, read: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            engine: EngineTag::Document,
            collection: collection.to_string(),
            key: ReadKeyIdent::Point(KeyRepr::Surrogate(surrogate)),
            read_version: at(read),
            home_vshard: None,
        }
    }

    fn predicate_entry(collection: &str, read: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            key: ReadKeyIdent::Predicate,
            ..point_entry(collection, 0, read)
        }
    }

    fn homed_entry(collection: &str, home: VShardId, read: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            home_vshard: Some(home.as_u32()),
            ..predicate_entry(collection, read)
        }
    }

    #[test]
    fn a_stale_point_read_is_not_current_and_a_fresh_one_is() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("orders");
        write(&mut core, vshard, "orders", Some(KeyRepr::Surrogate(7)), 20);
        let task = task_with_vshard(vshard);
        assert!(!core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 10)]));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 20)]));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 30)]));
    }

    /// The read was served on another replica of the vShard. Its version is
    /// the log position that replica had applied, which this replica records
    /// for the same writes, so the read validates here by version alone.
    #[test]
    fn a_read_served_on_another_replica_validates_by_its_log_position() {
        let vshard = local_vshard("orders");
        let (mut serving, _, _, _serving_dir) = make_core();
        let (mut validating, _, _, _validating_dir) = make_core();
        // Both replicas apply the entry at index 20, at different WAL LSNs.
        for (core, lsn) in [(&mut serving, 7_000u64), (&mut validating, 31u64)] {
            core.note_write(
                DatabaseId::DEFAULT,
                TenantId::new(1),
                "orders",
                Some(KeyRepr::Surrogate(7)),
                WriteStamp {
                    vshard,
                    lsn: Lsn::new(lsn),
                    entry: Some(at(20)),
                },
            );
        }
        let read_version = serving
            .write_index
            .key_version(&super::super::keys::WriteKey {
                vshard,
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
                collection: Box::from("orders"),
                key: KeyRepr::Surrogate(7),
            })
            .expect("serving replica recorded the write");
        let read = VersionedReadEntry {
            read_version,
            ..point_entry("orders", 7, 0)
        };
        let task = task_with_vshard(vshard);
        assert!(validating.read_set_still_current(&task, 1, std::slice::from_ref(&read)));

        // A conflicting write the validating replica applies after the read
        // makes the same read stale.
        write(
            &mut validating,
            vshard,
            "orders",
            Some(KeyRepr::Surrogate(7)),
            21,
        );
        assert!(!validating.read_set_still_current(&task, 1, &[read]));
    }

    /// A read that found no row answers `NotFound` with the version it
    /// observed. That version, carried to the coordinator on the wire, keeps
    /// the miss valid until a write of the missed key lands after it.
    #[test]
    fn a_missed_read_carries_its_version_and_stays_valid_until_the_key_is_written() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("orders");
        write(&mut core, vshard, "orders", Some(KeyRepr::Surrogate(7)), 20);
        let read = ExecutionTask::new(make_request(PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            document_id: "missing".into(),
            surrogate: Some(Surrogate::new(8)),
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })));
        let miss = core.response_error(&read, crate::bridge::envelope::ErrorCode::NotFound);
        assert_eq!(miss.status, Status::Error);
        let observed = miss
            .read_versions
            .of(vshard)
            .expect("a miss reports the version it observed");
        assert_eq!(observed, at(20));

        let entry = VersionedReadEntry {
            read_version: observed,
            ..point_entry("orders", 8, 0)
        };
        let task = task_with_vshard(vshard);
        assert!(core.read_set_still_current(&task, 1, std::slice::from_ref(&entry)));
        write(&mut core, vshard, "orders", Some(KeyRepr::Surrogate(8)), 21);
        assert!(!core.read_set_still_current(&task, 1, &[entry]));
    }

    fn array_slice(vshard: VShardId) -> ExecutionTask {
        ExecutionTask::new(crate::bridge::envelope::Request {
            vshard_id: vshard,
            ..make_request(PhysicalPlan::Array(
                nodedb_physical::physical_plan::ArrayOp::Slice {
                    array_id: nodedb_array::types::ArrayId::in_database(
                        TenantId::new(1),
                        DatabaseId::DEFAULT,
                        "grid",
                    ),
                    slice_msgpack: Vec::new(),
                    attr_projection: Vec::new(),
                    limit: 0,
                    cell_filter: None,
                    hilbert_range: None,
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                },
            ))
        })
    }

    /// An array read reports the vShard it covers even when no write of it
    /// reached this core. A first write of that vShard then makes the read
    /// stale: no tile can appear after the read unseen.
    #[test]
    fn an_array_read_covers_a_vshard_no_write_reached_and_a_first_write_makes_it_stale() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("grid");
        let observed = core
            .read_versions(&array_slice(vshard))
            .of(vshard)
            .expect("an array read reports its vShard with no write recorded");
        assert_eq!(observed, WriteVersion::ZERO);

        let entry = VersionedReadEntry {
            engine: EngineTag::Array,
            read_version: observed,
            ..predicate_entry("grid", 0)
        };
        let task = task_with_vshard(vshard);
        assert!(core.read_set_still_current(&task, 1, std::slice::from_ref(&entry)));
        write(&mut core, vshard, "grid", None, 5);
        assert!(!core.read_set_still_current(&task, 1, &[entry]));
    }

    /// An installed vShard with no recorded array write reports its install
    /// bound, so the read stays current until a write lands above it.
    #[test]
    fn an_array_read_of_an_installed_vshard_reports_the_install_bound() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("grid");
        core.install_version_floor(&[nodedb_types::ShardVersion {
            vshard: vshard.as_u32(),
            version: at(9),
        }]);
        let observed = core
            .read_versions(&array_slice(vshard))
            .of(vshard)
            .expect("an array read reports its vShard");
        assert_eq!(observed, at(9));
    }

    #[test]
    fn read_entry_homing_to_a_different_vshard_is_filtered_out() {
        let (mut core, _, _, _dir) = make_core();
        let local = local_vshard("orders");
        write(&mut core, local, "orders", Some(KeyRepr::Surrogate(7)), 20);
        let remote_task = task_with_vshard(other_vshard(local));
        assert!(core.read_set_still_current(&remote_task, 1, &[point_entry("orders", 7, 10)]));
    }

    #[test]
    fn a_stale_predicate_read_is_not_current_and_a_fresh_one_is() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("orders");
        write(&mut core, vshard, "orders", None, 20);
        let task = task_with_vshard(vshard);
        assert!(!core.read_set_still_current(&task, 1, &[predicate_entry("orders", 10)]));
        assert!(core.read_set_still_current(&task, 1, &[predicate_entry("orders", 20)]));
    }

    #[test]
    fn a_homed_read_validates_on_its_home_not_its_collection_vshard() {
        let (mut core, _, _, _dir) = make_core();
        let home = other_vshard(local_vshard("edges"));
        write(&mut core, home, "edges", None, 20);
        let stale = vec![homed_entry("edges", home, 10)];
        assert!(!core.read_set_still_current(&task_with_vshard(home), 1, &stale));
        assert!(
            core.read_set_still_current(&task_with_vshard(local_vshard("edges")), 1, &stale),
            "the collection's own vShard does not hold a homed read"
        );
        let fresh = vec![homed_entry("edges", home, 20)];
        assert!(core.read_set_still_current(&task_with_vshard(home), 1, &fresh));
    }

    #[test]
    fn a_homed_read_of_every_collection_validates_against_the_vshard_latest_version() {
        let (mut core, _, _, _dir) = make_core();
        let home = local_vshard("edges");
        write(&mut core, home, "unrelated", None, 20);
        // A write to another vShard on the same core never moves the home.
        write(&mut core, other_vshard(home), "unrelated", None, 900);
        let task = task_with_vshard(home);
        assert!(!core.read_set_still_current(&task, 1, &[homed_entry("", home, 10)]));
        assert!(core.read_set_still_current(&task, 1, &[homed_entry("", home, 20)]));
    }

    #[test]
    fn empty_read_set_is_vacuously_current() {
        let (core, _, _, _dir) = make_core();
        let task = task_with_vshard(VShardId::new(0));
        assert!(core.read_set_still_current(&task, 1, &[]));
    }

    #[test]
    fn a_read_that_predates_a_committed_write_is_no_longer_current() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("orders");
        let entry = at(10);
        let write_task = ExecutionTask::with_wal_lsn(
            crate::bridge::envelope::Request {
                entry_version: Some(entry),
                ..task_with_vshard(vshard).request
            },
            Some(Lsn::new(77)),
        );
        let write_plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            document_id: "o7".into(),
            value: doc_value("a", "1"),
            surrogate: Surrogate::new(7),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })];
        let write_resp = core.commit_plans_for_test(&write_task, 1, &write_plans, 77);
        assert_eq!(write_resp.status, Status::Ok, "{:?}", write_resp.error_code);

        let task = task_with_vshard(vshard);
        assert!(!core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 5)]));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 10)]));
    }
}
