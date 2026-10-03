// SPDX-License-Identifier: BUSL-1.1

//! A document delete stamps no edge tombstone, and replay never stamps one.
//!
//! Each test drives writes through the funnel's three steps: the
//! pre-dispatch append, the apply, and the post-apply append of the write
//! set. It then replays the WAL onto an empty base and compares every edge
//! version with the live store. A replay that stamps a tombstone of its
//! own writes it at another ordinal, and the stores differ.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{CrdtOp, CrdtWriteVerb, GraphOp};
use nodedb_types::{QualifiedCollection, Surrogate};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bootstrap::write_group_settle::settle::settle_against;
use crate::bridge::envelope::{
    Admission, PhysicalPlan, Priority, Request, Response, RowEffect, Status,
};
use crate::control::server::wal_dispatch::{
    WalAppendRequest, WriteSetTarget, append_group_origin, append_group_parts, plan_post_apply_redo,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::csr::Direction;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use crate::wal::{
    CascadedEdge, GroupMembership, NodeCascadeRedo, RedoRecord, RedoSubRecord, WriteGroup,
    WriteGroupRecord,
};

const TID: u64 = 1;
const SOCIAL: &str = "social";
const USERS: &str = "users";

struct Live {
    wal: WalManager,
    _wal_dir: tempfile::TempDir,
    _store_dir: tempfile::TempDir,
    core: CoreLoop,
}

impl Live {
    fn open() -> Self {
        let wal_dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&wal_dir.path().join("wal")).expect("open wal");
        let store_dir = tempfile::tempdir().expect("tempdir");
        let (core, _req, _resp) = make_core_with_dir(store_dir.path());
        Self {
            wal,
            _wal_dir: wal_dir,
            _store_dir: store_dir,
            core,
        }
    }

    /// Run `plan` through the funnel's steps. A grouped plan journals its
    /// origin before the apply and its write set after it, the way the
    /// funnel runs a journalled task.
    fn write(&mut self, plan: PhysicalPlan) -> Response {
        let Some(collection) = plan_post_apply_redo(&plan) else {
            let lsn = crate::control::server::wal_dispatch::wal_append_if_write(
                &self.wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &plan,
            )
            .expect("append")
            .lsn;
            let response = self.core.execute(&ExecutionTask::new(request(plan, lsn)));
            assert_eq!(response.status, Status::Ok, "{response:?}");
            return response;
        };
        let origin = append_group_origin(WalAppendRequest {
            wal: self.wal.appender(NO_APPLY_KEY),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: None,
        })
        .expect("origin")
        .origin;
        let task = ExecutionTask::new(request(plan, Some(origin.lsn)));
        let response = self.core.execute(&task);
        assert_eq!(response.status, Status::Ok, "{response:?}");
        append_group_parts(
            self.wal
                .appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            WriteSetTarget {
                tenant_id: TenantId::new(TID),
                vshard_id: VShardId::new(0),
                database_id: DatabaseId::DEFAULT,
                collection: &collection,
                origin,
            },
            &response.write_set,
        )
        .expect("parts");
        response
    }

    fn records(&self) -> Vec<WalRecord> {
        self.wal.sync().expect("sync");
        self.wal.replay().expect("replay")
    }
}

fn request(plan: PhysicalPlan, wal_lsn: Option<Lsn>) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(TID),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
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
        admission: Admission::Admitted,
    }
}

fn edge_put(src: (&str, u32), dst: (&str, u32)) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOCIAL),
        src_id: src.0.to_string(),
        label: "KNOWS".to_string(),
        dst_id: dst.0.to_string(),
        properties: Vec::new(),
        src_surrogate: Surrogate::new(src.1),
        dst_surrogate: Surrogate::new(dst.1),
    })
}

fn crdt_upsert(id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, USERS),
        document_id: id.to_string(),
        fields_json: r#"{"a":1}"#.to_string(),
        surrogate: Surrogate::new(surrogate),
        partial: false,
        verb: CrdtWriteVerb::Insert,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn crdt_delete(id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocDelete {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, USERS),
        document_id: id.to_string(),
        surrogate: Some(Surrogate::new(surrogate)),
        returning: None,
        rls_filters: Vec::new(),
    })
}

/// Every stored edge version, by key.
type EdgeVersions = crate::engine::graph::edge_store::TenantEdgeScan;

/// The live `(label, neighbor)` pairs of one node.
type Neighbors = Vec<(String, String)>;

/// Every stored edge version, and the live neighbors of `node`.
fn edges(core: &mut CoreLoop, node: &str) -> (EdgeVersions, Neighbors) {
    let mut neighbors = core
        .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
        .neighbors(node, &[], Direction::Out);
    neighbors.sort();
    let versions = core
        .edge_store
        .scan_edges_for_tenant(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID))
        .expect("edges");
    (versions, neighbors)
}

/// Replay `records` onto an empty base.
fn replayed_edges(records: &[WalRecord], node: &str) -> (EdgeVersions, Neighbors) {
    let base = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(base.path());
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    edges(&mut core, node)
}

/// A CRDT document delete tombstones no edge of its node: the Control
/// Plane tombstones them with `EdgeDelete` tasks of the delete's own
/// transaction. Replay stamps none either.
#[test]
fn a_crdt_document_delete_leaves_its_nodes_edges() {
    let mut live = Live::open();
    live.write(crdt_upsert("u1", 7));
    live.write(edge_put(("u1", 7), ("u2", 8)));
    let deleted = live.write(crdt_delete("u1", 7));
    assert!(
        !deleted
            .write_set
            .iter()
            .any(|entry| matches!(&entry.effect, RowEffect::Edge(_))),
        "{:?}",
        deleted.write_set
    );

    let live_edges = edges(&mut live.core, "u1");
    assert_eq!(
        live_edges.1,
        vec![("KNOWS".to_string(), "u2".to_string())],
        "the edge of u1 is still live"
    );
    assert_eq!(replayed_edges(&live.records(), "u1"), live_edges);
}

/// A node cascade an older WAL journalled replays at its own ordinals, and
/// the replayed store holds exactly those tombstones.
#[test]
fn a_journalled_node_cascade_replays_at_its_ordinals() {
    let mut live = Live::open();
    live.write(edge_put(("a", 1), ("b", 2)));
    let (before_versions, before) = replayed_edges(&live.records(), "a");
    assert_eq!(before, vec![("KNOWS".to_string(), "b".to_string())]);

    let (t, v, d) = (TenantId::new(TID), VShardId::new(0), DatabaseId::DEFAULT);
    let appender = live
        .wal
        .appender(NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::User);
    let origin = appender
        .append_write_group(
            t,
            v,
            d,
            &WriteGroupRecord {
                group: WriteGroup::OPENING,
                ops: Vec::new(),
                redo: None,
            },
        )
        .expect("open");
    let cascade = NodeCascadeRedo {
        node: "a".into(),
        edges: vec![CascadedEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOCIAL)
                .as_str()
                .to_string(),
            src: "a".into(),
            label: "KNOWS".into(),
            dst: "b".into(),
            system_from: i64::MAX / 2,
        }],
    };
    appender
        .append_write_group(
            t,
            v,
            d,
            &WriteGroupRecord {
                group: WriteGroup::part_of(origin.as_u64(), 1, 1),
                ops: vec![RedoSubRecord {
                    record_type: nodedb_wal::record::RecordType::GraphNodeCascade as u32,
                    payload: zerompk::to_msgpack_vec(&cascade).expect("encode"),
                }],
                redo: None,
            },
        )
        .expect("part");

    let (versions, neighbors) = replayed_edges(&live.records(), "a");
    assert!(
        neighbors.is_empty(),
        "the journalled tombstone removes a -> b"
    );
    assert_eq!(
        versions.visible.len(),
        before_versions.visible.len() + 1,
        "the tombstone is stored beside the version it covers"
    );
}

/// A committed transaction record whose install never became durable keeps
/// applying: boot closes its group with an empty part and never cancels it.
#[test]
fn a_committed_record_without_a_stored_write_set_is_closed_not_cancelled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    let appender = wal
        .appender(NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::User);
    let (t, v, d) = (TenantId::new(TID), VShardId::new(0), DatabaseId::DEFAULT);
    let redo = RedoRecord {
        version: 1,
        ops: Vec::new(),
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
    };
    let origin = appender
        .append_transaction_redo(t, v, d, &redo)
        .expect("redo");
    appender
        .append_write_group(
            t,
            v,
            d,
            &WriteGroupRecord {
                group: WriteGroup::announcing(origin.as_u64()),
                ops: Vec::new(),
                redo: None,
            },
        )
        .expect("announce");
    wal.sync().expect("sync");

    let records = wal.replay().expect("replay");
    assert!(settle_against(&wal, &records, &[]).expect("settle"));
    wal.sync().expect("sync");
    let settled = wal.replay().expect("replay");

    assert!(
        settled.iter().any(|r| r.header.lsn == origin.as_u64()),
        "the committed record still applies"
    );
    let mut groups = GroupMembership::default();
    for record in &settled {
        if nodedb_wal::record::RecordType::from_raw(record.logical_record_type())
            == Some(nodedb_wal::record::RecordType::WriteGroup)
        {
            groups.observe(
                record.header.lsn,
                WriteGroupRecord::from_bytes(&record.payload)
                    .expect("group")
                    .group,
            );
        }
    }
    assert!(
        groups.broken_through(u64::MAX).is_empty(),
        "the committed record's group is whole"
    );
}
