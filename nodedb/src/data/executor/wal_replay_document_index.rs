// SPDX-License-Identifier: BUSL-1.1

//! Restart replay of the document secondary-index records
//! (`DocumentIndexBuild` / `DocumentIndexRetire`).
//!
//! The document arm hands each record here at its place in LSN order. A
//! build adds its owner to the path in the maintained set and writes the
//! entries the live build wrote. A purge removes its owner, and every entry
//! of the field when no owner is left, so a later build of the same field
//! starts from no entry. Both are idempotent: a second run leaves the same
//! set and the same keys.
//!
//! Each record carries the fence of the descriptor its step derives from.
//! Replay judges it against the set's fences exactly as the live apply did,
//! so a step the live core found stale changes nothing at replay either.
//!
//! Row writes replayed after a record maintain the paths the set holds at
//! that point. A row replayed above a purge writes no entry for the purged
//! path.
//!
//! A replayed build judges no UNIQUE duplicate. The live build judged the
//! rows at this position. A build it refused carries an abort marker, and
//! replay never sees its record. Stores that already hold writes above the
//! record hold a duplicate only after a later purge of the index. That purge
//! replays after this build and removes the entries it writes.

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::index_maintenance::BackfillIndexParams;
use crate::types::{DatabaseId, VShardId, core_for_vshard};
use crate::wal::{DocumentIndexBuildRecord, DocumentIndexRetireRecord};

impl CoreLoop {
    /// Replay `record` when it is a document index record. Returns `false`
    /// for every other record type, which the caller replays. A record
    /// another core owns, or one a tombstone covers, is skipped.
    pub(in crate::data::executor) fn replay_document_index_step(
        &mut self,
        record: &WalRecord,
        record_type: Option<RecordType>,
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let is_build = record_type == Some(RecordType::DocumentIndexBuild);
        let is_retire = record_type == Some(RecordType::DocumentIndexRetire);
        if !is_build && !is_retire {
            return false;
        }
        let owner = core_for_vshard(VShardId::new(record.header.vshard_id), num_cores);
        // A committed redo carries no index step. Left unclaimed, the
        // validate pass refuses a redo that does.
        if owner != self.core_id || self.applying_committed_redo() {
            return true;
        }
        if is_build {
            self.replay_index_build(record, tombstones);
        } else {
            self.replay_index_retire(record, tombstones);
        }
        true
    }

    fn replay_index_build(&mut self, record: &WalRecord, tombstones: &nodedb_wal::TombstoneSet) {
        let lsn = record.header.lsn;
        let tenant_id = record.header.tenant_id;
        let database_id = record.header.database_id;
        let build = match DocumentIndexBuildRecord::from_bytes(&record.payload) {
            Ok(build) => build,
            Err(error) => {
                self.replay_record_unapplied("document", "index_build", lsn, &error.to_string());
                return;
            }
        };
        if tombstones.is_tombstoned(database_id, tenant_id, &build.collection, lsn) {
            return;
        }
        let built = self.backfill_document_index(
            DatabaseId::new(database_id),
            BackfillIndexParams {
                tid: tenant_id,
                collection: &build.collection,
                path: &build.path,
                is_array: build.is_array,
                unique: build.unique,
                // See the module doc: the live build judged UNIQUE here.
                judge_unique: false,
                case_insensitive: build.case_insensitive,
                predicate: build.predicate.as_deref(),
                owner: &build.owner,
                fence: build.fence,
            },
        );
        if let Err(error) = built {
            self.replay_record_unapplied(
                "document",
                "index_build",
                lsn,
                &format!(
                    "index build of '{}' on '{}': {error}",
                    build.path, build.collection
                ),
            );
        }
    }

    fn replay_index_retire(&mut self, record: &WalRecord, tombstones: &nodedb_wal::TombstoneSet) {
        let lsn = record.header.lsn;
        let tenant_id = record.header.tenant_id;
        let database_id = record.header.database_id;
        let retire = match DocumentIndexRetireRecord::from_bytes(&record.payload) {
            Ok(retire) => retire,
            Err(error) => {
                self.replay_record_unapplied("document", "index_retire", lsn, &error.to_string());
                return;
            }
        };
        if tombstones.is_tombstoned(database_id, tenant_id, &retire.collection, lsn) {
            return;
        }
        let purged = self.purge_document_index_field(
            database_id,
            tenant_id,
            &retire.collection,
            &retire.field,
            &retire.owner,
            retire.fence,
        );
        if let Err(error) = purged {
            self.replay_record_unapplied(
                "document",
                "index_retire",
                lsn,
                &format!(
                    "index purge of '{}' on '{}': {error}",
                    retire.field, retire.collection
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner, DocumentOp};
    use nodedb_types::{QualifiedCollection, Surrogate, Value};
    use nodedb_wal::{TombstoneSet, WalRecord};

    use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request, Response, Status};
    use crate::control::server::wal_dispatch::{
        WalAppendRequest, WriteSetTarget, append_group_origin, append_group_parts,
        plan_post_apply_redo, wal_append,
    };
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const USERS: &str = "users";
    const OWNER: &str = "$.owner";
    const OWNER_INDEX: &str = "by_owner";

    /// A WAL the writes journal into, the way the write funnel journals them.
    struct Journal {
        wal: WalManager,
        _dir: tempfile::TempDir,
    }

    impl Journal {
        fn open() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let wal =
                WalManager::open_for_testing(&dir.path().join("index.wal")).expect("open wal");
            Self { wal, _dir: dir }
        }

        fn appender(&self) -> crate::wal::manager::WalAppender<'_> {
            self.wal
                .appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User)
        }

        fn append_request<'a>(&'a self, plan: &'a PhysicalPlan) -> WalAppendRequest<'a> {
            WalAppendRequest {
                wal: self.appender(),
                event_source: crate::event::EventSource::User,
                tenant_id: TenantId::new(TID),
                vshard_id: VShardId::new(0),
                database_id: DatabaseId::DEFAULT,
                plan,
                credentials: None,
                now_override: None,
            }
        }

        /// Journal and apply a row write: its group origin, the apply, and
        /// the write set as the group's parts.
        fn write(&self, core: &mut CoreLoop, plan: PhysicalPlan) {
            let collection = plan_post_apply_redo(&plan).expect("the plan stores rows");
            let origin = append_group_origin(self.append_request(&plan))
                .expect("pre-dispatch append")
                .origin;
            let response = core.execute(&ExecutionTask::new(request(plan, Some(origin.lsn))));
            append_group_parts(
                self.appender(),
                WriteSetTarget {
                    tenant_id: TenantId::new(TID),
                    vshard_id: VShardId::new(0),
                    database_id: DatabaseId::DEFAULT,
                    collection: &collection,
                    origin,
                },
                &response.write_set,
            )
            .expect("post-apply append");
            assert_eq!(response.status, Status::Ok, "{response:?}");
        }

        /// Journal and apply an index step: its own record, then the apply.
        fn index_step(&self, core: &mut CoreLoop, plan: PhysicalPlan) -> Response {
            let lsn = wal_append(self.append_request(&plan))
                .expect("index step append")
                .lsn;
            assert!(lsn.is_some(), "an index step journals its own record");
            core.execute(&ExecutionTask::new(request(plan, lsn)))
        }

        fn records(&self) -> Vec<WalRecord> {
            self.wal.sync().expect("sync wal");
            self.wal.replay().expect("read wal")
        }
    }

    fn request(plan: PhysicalPlan, wal_lsn: Option<crate::types::Lsn>) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            stream: crate::bridge::envelope::RequestStream::Internal,
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn,
            resolved_now_ms: None,
            commit_hlc: None,
            system_ordinal: None,
            config_pins: None,
            admission: Admission::Admitted,
        }
    }

    /// The catalog state a booted core holds before replay.
    fn register(core: &mut CoreLoop) {
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), USERS.to_string()),
            CollectionConfig::new(USERS),
        );
    }

    fn qualified() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, USERS)
    }

    fn put(id: &str, surrogate: u32, owner: &str) -> PhysicalPlan {
        let mut obj = HashMap::new();
        obj.insert("owner".to_string(), Value::String(owner.into()));
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: qualified(),
            document_id: id.to_string(),
            value: nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode value"),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// The fence of descriptor version `version` of the collection's first
    /// incarnation.
    fn fence(version: u64) -> DocIndexFence {
        DocIndexFence {
            incarnation: nodedb_types::Hlc::new(1, 0),
            descriptor_version: version,
        }
    }

    fn build(unique: bool) -> PhysicalPlan {
        build_at(unique, fence(1))
    }

    fn build_at(unique: bool, fence: DocIndexFence) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BackfillIndex {
            collection: qualified(),
            path: OWNER.into(),
            is_array: false,
            unique,
            case_insensitive: false,
            predicate: None,
            owner: DocIndexOwner::Named(OWNER_INDEX.into()),
            fence,
        })
    }

    fn retire() -> PhysicalPlan {
        retire_at(fence(1))
    }

    fn retire_at(fence: DocIndexFence) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::DropIndex {
            collection: qualified(),
            field: OWNER.into(),
            owner: DocIndexOwner::Named(OWNER_INDEX.into()),
            fence,
        })
    }

    /// Every index entry of the tenant, and the rows one owner value finds.
    #[derive(Debug, PartialEq)]
    struct IndexState {
        entries: Vec<(String, Vec<u8>)>,
        alice: usize,
        /// The paths the core maintains for `users`, in memory and on disk.
        maintained: Vec<String>,
        stored: Vec<String>,
    }

    fn index_state(core: &CoreLoop) -> IndexState {
        let stored = core
            .sparse
            .load_maintained_indexes()
            .expect("maintained sets")
            .into_iter()
            .flat_map(|set| set.paths.into_iter().map(|p| p.path))
            .collect();
        IndexState {
            entries: core
                .sparse
                .scan_versioned_indexes_for_tenant(DB, TID)
                .expect("versioned indexes"),
            alice: core
                .sparse
                .versioned_index_lookup_as_of(DB, TID, USERS, OWNER, "alice", None)
                .expect("lookup")
                .len(),
            maintained: core
                .maintained_indexes
                .stored_paths(DB, TID, USERS)
                .iter()
                .map(|p| p.path.clone())
                .collect(),
            stored,
        }
    }

    fn run_live(
        dir: &std::path::Path,
        workload: impl FnOnce(&Journal, &mut CoreLoop),
    ) -> (IndexState, Journal) {
        let journal = Journal::open();
        let (mut core, _req, _resp) = make_core_with_dir(dir);
        register(&mut core);
        workload(&journal, &mut core);
        (index_state(&core), journal)
    }

    fn replay_at(dir: &std::path::Path, records: &[WalRecord]) -> IndexState {
        let (mut core, _req, _resp) = make_core_with_dir(dir);
        register(&mut core);
        core.replay_all_wal(records, 1, &TombstoneSet::new())
            .expect("replay");
        index_state(&core)
    }

    fn rows_then_build(journal: &Journal, core: &mut CoreLoop) {
        journal.write(core, put("u1", 1, "alice"));
        journal.write(core, put("u2", 2, "bob"));
        journal.write(core, put("u3", 3, "alice"));
        let built = journal.index_step(core, build(false));
        assert_eq!(built.status, Status::Ok, "{built:?}");
    }

    /// A replayed build writes the entries the live build wrote, on a base
    /// and over the live core's own storage after a restart.
    #[test]
    fn an_index_build_replays_to_the_entries_the_live_build_wrote() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), rows_then_build);
        assert_eq!(live.alice, 2, "the live build indexed both alice rows");

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "first restart");
        assert_eq!(replay_at(dir.path(), &records), live, "second restart");
    }

    /// A replayed purge removes every entry of the field, so the core ends
    /// with the index purged, as the live core did.
    #[test]
    fn an_index_purge_replays_to_no_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            rows_then_build(journal, core);
            let purged = journal.index_step(core, retire());
            assert_eq!(purged.status, Status::Ok, "{purged:?}");
        });
        assert_eq!(live.alice, 0, "the live purge removed every entry");
        assert!(live.entries.is_empty(), "{:?}", live.entries);

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "after a restart");
    }

    /// A purge and a second build of the same field on new values replay
    /// to the second build's entries alone.
    #[test]
    fn a_rebuild_after_a_purge_replays_without_a_stale_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            rows_then_build(journal, core);
            let purged = journal.index_step(core, retire());
            assert_eq!(purged.status, Status::Ok, "{purged:?}");
            journal.write(core, put("u1", 1, "carol"));
            journal.write(core, put("u3", 3, "dave"));
            let rebuilt = journal.index_step(core, build(false));
            assert_eq!(rebuilt.status, Status::Ok, "{rebuilt:?}");
        });
        assert!(!live.entries.is_empty(), "the rebuild wrote entries");

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        let replayed = replay_at(base.path(), &records);
        assert_eq!(replayed, live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "after a restart");
    }

    /// A build from an older descriptor than an applied retire of the same
    /// owner changes nothing, live and at every replay: the owner stays out
    /// of the set and the path keeps no entry.
    #[test]
    fn a_build_below_an_applied_retire_leaves_the_owner_out() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            journal.write(core, put("u1", 1, "alice"));
            let built = journal.index_step(core, build_at(false, fence(1)));
            assert_eq!(built.status, Status::Ok, "{built:?}");
            let retired = journal.index_step(core, retire_at(fence(3)));
            assert_eq!(retired.status, Status::Ok, "{retired:?}");
            let stale = journal.index_step(core, build_at(false, fence(2)));
            assert_eq!(stale.status, Status::Ok, "a stale build answers Ok");
            journal.write(core, put("u2", 2, "alice"));
        });
        assert_eq!(live.alice, 0, "the stale build indexed nothing");
        assert!(live.entries.is_empty(), "{:?}", live.entries);
        assert!(live.maintained.is_empty(), "the owner stays retired");
        assert!(live.stored.is_empty());

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "after a restart");
    }

    /// A step of a newer incarnation discards the older incarnation's set
    /// and entries. A step of the older incarnation after it changes
    /// nothing.
    #[test]
    fn a_newer_incarnation_resets_the_set_and_fences_the_older_one() {
        let newer = DocIndexFence {
            incarnation: nodedb_types::Hlc::new(2, 0),
            descriptor_version: 1,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            journal.write(core, put("u1", 1, "alice"));
            let old = journal.index_step(core, build_at(false, fence(9)));
            assert_eq!(old.status, Status::Ok, "{old:?}");
            let retired = journal.index_step(core, retire_at(newer));
            assert_eq!(retired.status, Status::Ok, "{retired:?}");
            let stale = journal.index_step(core, build_at(false, fence(9)));
            assert_eq!(stale.status, Status::Ok, "{stale:?}");
        });
        assert!(live.entries.is_empty(), "{:?}", live.entries);
        assert!(live.maintained.is_empty());

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "after a restart");
    }

    /// A UNIQUE build that finds a duplicate writes no entry. Its record
    /// takes an abort marker on the live path, so replay sees no record.
    #[test]
    fn a_refused_unique_build_writes_no_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, _journal) = run_live(dir.path(), |journal, core| {
            journal.write(core, put("u1", 1, "alice"));
            journal.write(core, put("u2", 2, "alice"));
            let refused = journal.index_step(core, build(true));
            assert_eq!(refused.status, Status::Error, "{refused:?}");
            assert!(
                matches!(
                    refused.error_code.as_deref(),
                    Some(crate::bridge::envelope::ErrorCode::RejectedConstraint { .. })
                ),
                "a duplicate refuses as a constraint violation: {refused:?}"
            );
        });
        assert!(live.entries.is_empty(), "{:?}", live.entries);
        assert!(live.maintained.is_empty(), "a refused build adds no path");
        assert!(live.stored.is_empty(), "a refused build stores no path");
    }

    /// The config of `users` as a Register that names an index on `owner`
    /// leaves it.
    fn register_owner_index(core: &mut CoreLoop) {
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), USERS.to_string()),
            CollectionConfig::new(USERS).with_index(OWNER),
        );
    }

    /// A Register that drops the index from the config stops nothing: the
    /// core keeps maintaining the path until it applies the retire.
    #[test]
    fn a_register_that_drops_the_path_keeps_it_maintained_until_the_retire() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, _journal) = run_live(dir.path(), |journal, core| {
            register_owner_index(core);
            journal.write(core, put("u1", 1, "alice"));
            let built = journal.index_step(core, build(false));
            assert_eq!(built.status, Status::Ok, "{built:?}");

            // The metadata log reaches this core first: its config loses P.
            register(core);
            journal.write(core, put("u2", 2, "alice"));
            assert_eq!(index_state(core).alice, 2, "P stays maintained");

            let purged = journal.index_step(core, retire());
            assert_eq!(purged.status, Status::Ok, "{purged:?}");
            journal.write(core, put("u3", 3, "alice"));
        });
        assert_eq!(
            live.alice, 0,
            "the retire purged P and no later write fed it"
        );
        assert!(live.entries.is_empty(), "{:?}", live.entries);
        assert!(live.maintained.is_empty());
        assert!(live.stored.is_empty());
    }

    /// A Register that adds the index to the config maintains nothing for
    /// it: the path is maintained from the build on.
    #[test]
    fn a_register_that_adds_the_path_maintains_nothing_until_the_build() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, _journal) = run_live(dir.path(), |journal, core| {
            register_owner_index(core);
            journal.write(core, put("u1", 1, "alice"));
            let before = index_state(core);
            assert!(before.entries.is_empty(), "{:?}", before.entries);
            assert!(before.maintained.is_empty());

            let built = journal.index_step(core, build(false));
            assert_eq!(built.status, Status::Ok, "{built:?}");
            assert_eq!(index_state(core).alice, 1, "the build indexed u1");
            journal.write(core, put("u2", 2, "alice"));
        });
        assert_eq!(live.alice, 2, "u2 was maintained after the build");
        assert_eq!(live.maintained, vec![OWNER.to_string()]);
        assert_eq!(live.stored, vec![OWNER.to_string()]);
    }

    /// A restart after a retire, with rows written above the retire, ends
    /// with no entry and no maintained path, onto a base and over the live
    /// core's own storage.
    #[test]
    fn a_restart_after_a_retire_replays_to_no_entry_for_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            rows_then_build(journal, core);
            journal.write(core, put("u4", 4, "alice"));
            let purged = journal.index_step(core, retire());
            assert_eq!(purged.status, Status::Ok, "{purged:?}");
            journal.write(core, put("u5", 5, "alice"));
            journal.write(core, put("u1", 1, "alice"));
        });
        assert_eq!(live.alice, 0);
        assert!(live.entries.is_empty(), "{:?}", live.entries);
        assert!(live.maintained.is_empty());

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "first restart");
        assert_eq!(replay_at(dir.path(), &records), live, "second restart");
    }

    /// A DROP INDEX of an explicit index on a declared field removes only
    /// the explicit owner. The declared field's entries stay, and later
    /// writes keep them.
    #[test]
    fn a_retire_of_one_owner_keeps_the_entries_of_the_other() {
        let declared = |plan: PhysicalPlan| match plan {
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
                ..
            }) => PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
                owner: DocIndexOwner::Declared,
                fence: fence(1),
            }),
            other => other,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let (live, journal) = run_live(dir.path(), |journal, core| {
            journal.write(core, put("u1", 1, "alice"));
            let auto = journal.index_step(core, declared(build(false)));
            assert_eq!(auto.status, Status::Ok, "{auto:?}");
            let named = journal.index_step(core, build(false));
            assert_eq!(named.status, Status::Ok, "{named:?}");
            let dropped = journal.index_step(core, retire());
            assert_eq!(dropped.status, Status::Ok, "{dropped:?}");
            journal.write(core, put("u2", 2, "alice"));
        });
        assert_eq!(live.alice, 2, "the declared field keeps the path");
        assert_eq!(live.maintained, vec![OWNER.to_string()]);

        let records = journal.records();
        let base = tempfile::tempdir().expect("tempdir");
        assert_eq!(replay_at(base.path(), &records), live, "onto a base");
        assert_eq!(replay_at(dir.path(), &records), live, "after a restart");
    }

    fn fetch_alice() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: qualified(),
            path: OWNER.into(),
            value: "alice".into(),
            also: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: usize::MAX,
            offset: 0,
            sort_keys: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    /// An indexed fetch on a core that does not maintain the path is
    /// refused as retryable, never answered empty. After the build it
    /// answers.
    #[test]
    fn an_indexed_fetch_through_an_unmaintained_path_is_refused_as_retryable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open();
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_owner_index(&mut core);
        journal.write(&mut core, put("u1", 1, "alice"));

        let refused = core.execute(&ExecutionTask::new(request(fetch_alice(), None)));
        assert_eq!(refused.status, Status::Error, "{refused:?}");
        assert!(
            matches!(
                refused.error_code.as_deref(),
                Some(crate::bridge::envelope::ErrorCode::RetryableRefusal { .. })
            ),
            "an unmaintained path refuses as retryable: {refused:?}"
        );

        let built = journal.index_step(&mut core, build(false));
        assert_eq!(built.status, Status::Ok, "{built:?}");
        let answered = core.execute(&ExecutionTask::new(request(fetch_alice(), None)));
        assert_eq!(answered.status, Status::Ok, "{answered:?}");
    }
}
