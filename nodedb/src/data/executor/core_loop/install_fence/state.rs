// SPDX-License-Identifier: BUSL-1.1

//! The install fence: the state an install holds back while it installs.
//!
//! A task proceeds only when its footprint is disjoint from the fence keys
//! and from the footprint of every task already waiting:
//! - a waiting read or write holds every later task of its keys
//! - a waiting whole-database task holds every later task of that database
//! - a waiting write holds every later write of its event vShard
//! - a waiting core task holds every later task with a footprint
//! - a waiting ordered read holds every later internal task
//! - background core work waits and holds nothing
//!
//! A client task also waits while an earlier task of its connection and
//! vShard waits, so each client stream keeps its FIFO order. A task with an
//! empty footprint always proceeds.

use std::collections::{BTreeSet, VecDeque};

use nodedb_types::{NodeDbError, QualifiedCollection};

use crate::bridge::envelope::{ClientStream, Request};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, RequestId, TenantId, TxnId, VShardId};

use super::footprint::Footprint;

/// One piece of state an install writes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::data::executor) enum FenceKey {
    /// A collection of `db`. `name` is the stored name a plan carries.
    Collection { db: DatabaseId, name: String },
    /// The graph state of `tenant` in `db`: its CSR partition, its interned
    /// labels and its edge store.
    Graph { db: DatabaseId, tenant: TenantId },
    /// The sync high-water mark of one producer's stream. The stream id
    /// hashes the engine and the bare collection name, so the key spans
    /// databases.
    SyncStream { producer_id: u64, stream_id: u64 },
    /// The tenant-wide CRDT state of `tenant` in `db`: the constraint
    /// validator, its dead-letter queue, its deferred queue and the array
    /// surrogate registry.
    CrdtTenant { db: DatabaseId, tenant: TenantId },
    /// The staging overlays, savepoints and resolve cursor of one
    /// transaction on this core.
    TxnOverlay(TxnId),
    /// The event partition of a vShard. The install's held events carry
    /// it. A write whose events carry it advances the partition past them.
    VShard(VShardId),
}

impl FenceKey {
    /// The key of the collection stored as `stored`, in the database the
    /// name carries. A name that carries no valid database is an error.
    pub(in crate::data::executor) fn of_stored_collection(
        stored: &str,
    ) -> Result<Self, NodeDbError> {
        Ok(FenceKey::Collection {
            db: QualifiedCollection::database_of_stored(stored)?,
            name: stored.to_owned(),
        })
    }

    /// The keys of every collection in `stored`, such as the collections a
    /// committed redo writes.
    pub(in crate::data::executor) fn of_stored_collections(
        stored: &[String],
    ) -> Result<Vec<Self>, NodeDbError> {
        stored
            .iter()
            .map(|name| Self::of_stored_collection(name))
            .collect()
    }

    /// The key of the stream mark `provenance` advances. Producer zero
    /// takes no part in producer sequencing: its frame advances no mark,
    /// and it has no key.
    pub(in crate::data::executor) fn sync_stream(
        provenance: &nodedb_types::sync::wire::SyncProvenance,
    ) -> Option<Self> {
        (provenance.producer_id != 0).then_some(FenceKey::SyncStream {
            producer_id: provenance.producer_id,
            stream_id: provenance.stream_id,
        })
    }

    /// The database the key belongs to. A vShard, a sync stream and a
    /// transaction's overlay span databases.
    pub(in crate::data::executor) fn database(&self) -> Option<DatabaseId> {
        match self {
            FenceKey::Collection { db, .. }
            | FenceKey::Graph { db, .. }
            | FenceKey::CrdtTenant { db, .. } => Some(*db),
            FenceKey::SyncStream { .. } | FenceKey::TxnOverlay(_) | FenceKey::VShard(_) => None,
        }
    }
}

/// The ordered stream of one client task: its node-qualified connection and
/// its vShard.
pub(in crate::data::executor) type StreamKey = (ClientStream, VShardId);

/// The stream of `request`. `None` for internal work, which orders against
/// no client stream.
pub(in crate::data::executor) fn stream_of(request: &Request) -> Option<StreamKey> {
    request
        .stream
        .connection()
        .map(|connection| (connection, request.vshard_id))
}

/// The union of the footprints and streams of every waiting task.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct Blocked {
    /// The keys of the waiting reads and writes.
    keys: BTreeSet<FenceKey>,
    /// The databases of the waiting whole-database tasks.
    databases: BTreeSet<DatabaseId>,
    /// The event vShards of the waiting writes and whole-database tasks.
    vshards: BTreeSet<VShardId>,
    /// The client streams of every waiting task.
    streams: BTreeSet<StreamKey>,
    core: bool,
    /// An ordered read waits. It holds every later internal task.
    ordered: bool,
}

impl Blocked {
    /// Note a waiter of `footprint` on `stream`.
    fn add_waiter(&mut self, footprint: &Footprint, stream: Option<StreamKey>) {
        if let Some(stream) = stream {
            self.streams.insert(stream);
        }
        self.add(footprint);
    }

    fn add(&mut self, footprint: &Footprint) {
        match footprint {
            Footprint::Keys(keys) => self.keys.extend(keys.iter().cloned()),
            Footprint::Write { keys, vshard } => {
                self.keys.extend(keys.iter().cloned());
                self.vshards.insert(*vshard);
            }
            Footprint::WholeDatabase { db, vshard } => {
                self.databases.insert(*db);
                self.vshards.insert(*vshard);
            }
            Footprint::Core => self.core = true,
            // Its own stream holds its connection's later tasks.
            Footprint::Ordered => self.ordered = true,
            // It orders against no later task.
            Footprint::Background => {}
        }
    }

    fn holds_database(&self, db: DatabaseId) -> bool {
        self.databases.contains(&db) || self.keys.iter().any(|key| key.database() == Some(db))
    }
}

/// A task held back, with the footprint it was held by.
pub(in crate::data::executor) struct Waiter {
    task: ExecutionTask,
    footprint: Footprint,
}

/// The keys an install holds and the tasks that wait for its release.
pub(in crate::data::executor) struct InstallFence {
    /// The state the install writes.
    pub(in crate::data::executor) keys: BTreeSet<FenceKey>,
    /// Tasks held back, in arrival order.
    pub(in crate::data::executor) waiting: VecDeque<Waiter>,
    /// The union of the waiting tasks' footprints.
    pub(in crate::data::executor) blocked: Blocked,
    /// Maintenance came due while the fence was armed and skipped. The
    /// release runs it before any waiter re-enters.
    pub(in crate::data::executor) maintenance_owed: bool,
    /// The lowest LSN of an install whose events are still held. A heartbeat
    /// publishes below it.
    pub(in crate::data::executor) event_floor: Option<u64>,
}

impl InstallFence {
    /// A fence over `keys` with no task waiting.
    pub(in crate::data::executor) fn new(keys: BTreeSet<FenceKey>) -> Self {
        Self {
            keys,
            waiting: VecDeque::new(),
            blocked: Blocked::default(),
            maintenance_owed: false,
            event_floor: None,
        }
    }

    /// Whether a task of `footprint` proceeds past the fence now.
    pub(in crate::data::executor) fn admits(&self, footprint: &Footprint) -> bool {
        match footprint {
            Footprint::Keys(keys) => {
                keys.is_empty()
                    || (!self.blocked.core && keys.iter().all(|key| self.admits_key(key)))
            }
            Footprint::Write { keys, vshard } => {
                !self.blocked.core
                    && keys.iter().all(|key| self.admits_key(key))
                    && self.admits_vshard(*vshard)
            }
            Footprint::WholeDatabase { db, vshard } => {
                self.admits_database(*db) && self.admits_vshard(*vshard)
            }
            Footprint::Core | Footprint::Ordered | Footprint::Background => false,
        }
    }

    /// Whether a task of `footprint` on `stream` proceeds now. It passes
    /// the footprint check, and no earlier task of its stream waits. An
    /// internal task waits while an ordered read waits. A task with an
    /// empty footprint touches no fenced state and always proceeds.
    pub(in crate::data::executor) fn admits_task(
        &self,
        footprint: &Footprint,
        stream: Option<StreamKey>,
    ) -> bool {
        if matches!(footprint, Footprint::Keys(keys) if keys.is_empty()) {
            return true;
        }
        let stream_free = match stream {
            Some(stream) => !self.blocked.streams.contains(&stream),
            None => !self.blocked.ordered,
        };
        stream_free && self.admits(footprint)
    }

    /// Whether a task of `key` proceeds now. Neither the fence nor a
    /// waiting task holds the key, and no waiting whole-database task holds
    /// its database.
    fn admits_key(&self, key: &FenceKey) -> bool {
        !self.keys.contains(key)
            && !self.blocked.keys.contains(key)
            && key
                .database()
                .is_none_or(|db| !self.blocked.databases.contains(&db))
    }

    /// Whether a write whose events carry `vshard` proceeds now.
    fn admits_vshard(&self, vshard: VShardId) -> bool {
        !self.keys.contains(&FenceKey::VShard(vshard)) && !self.blocked.vshards.contains(&vshard)
    }

    /// Whether a task of any state of `db` proceeds now.
    fn admits_database(&self, db: DatabaseId) -> bool {
        !self.blocked.core
            && !self.blocked.holds_database(db)
            && !self.keys.iter().any(|key| key.database() == Some(db))
    }

    /// Hold `task`, whose footprint is `footprint`, behind every task
    /// already waiting.
    pub(in crate::data::executor) fn hold(&mut self, task: ExecutionTask, footprint: Footprint) {
        self.blocked
            .add_waiter(&footprint, stream_of(&task.request));
        self.waiting.push_back(Waiter { task, footprint });
    }

    /// Rebuild `blocked` from the footprints and streams of the tasks still
    /// waiting.
    fn recompute_blocked(&mut self) {
        let mut blocked = Blocked::default();
        for waiter in &self.waiting {
            blocked.add_waiter(&waiter.footprint, stream_of(&waiter.task.request));
        }
        self.blocked = blocked;
    }

    /// Remove the waiting task of `request_id`. Returns whether one waited.
    pub(in crate::data::executor) fn remove_waiter(&mut self, request_id: RequestId) -> bool {
        let Some(pos) = self
            .waiting
            .iter()
            .position(|waiter| waiter.task.request_id() == request_id)
        else {
            return false;
        };
        self.waiting.remove(pos);
        self.recompute_blocked();
        true
    }

    /// Take every waiting task past its deadline, in arrival order. The
    /// rest keep waiting.
    pub(in crate::data::executor) fn take_expired(&mut self) -> Vec<ExecutionTask> {
        if !self.waiting.iter().any(|waiter| waiter.task.is_expired()) {
            return Vec::new();
        }
        let (expired, live): (VecDeque<Waiter>, VecDeque<Waiter>) =
            std::mem::take(&mut self.waiting)
                .into_iter()
                .partition(|waiter| waiter.task.is_expired());
        self.waiting = live;
        self.recompute_blocked();
        expired.into_iter().map(|waiter| waiter.task).collect()
    }

    /// Take every waiting task, in arrival order, and clear `blocked`.
    pub(in crate::data::executor) fn take_waiters(&mut self) -> VecDeque<ExecutionTask> {
        self.blocked = Blocked::default();
        std::mem::take(&mut self.waiting)
            .into_iter()
            .map(|waiter| waiter.task)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection(name: &str) -> FenceKey {
        FenceKey::Collection {
            db: DatabaseId::DEFAULT,
            name: name.into(),
        }
    }

    fn fence(keys: &[FenceKey]) -> InstallFence {
        InstallFence::new(keys.iter().cloned().collect())
    }

    fn write(keys: Vec<FenceKey>, vshard: u32) -> Footprint {
        Footprint::Write {
            keys,
            vshard: VShardId::new(vshard),
        }
    }

    fn whole(db: u64, vshard: u32) -> Footprint {
        Footprint::WholeDatabase {
            db: DatabaseId::new(db),
            vshard: VShardId::new(vshard),
        }
    }

    #[test]
    fn a_read_of_another_collection_is_admitted() {
        let fence = fence(&[collection("a")]);
        assert!(fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("a")])));
    }

    #[test]
    fn a_write_of_disjoint_keys_is_admitted() {
        let fence = fence(&[collection("a"), FenceKey::VShard(VShardId::new(0))]);
        assert!(fence.admits(&write(vec![collection("b")], 1)));
        assert!(!fence.admits(&write(vec![collection("a")], 1)));
        assert!(!fence.admits(&write(vec![collection("b"), collection("a")], 1)));
    }

    #[test]
    fn a_waiting_write_holds_a_later_read_and_write_of_its_keys() {
        let mut fence = fence(&[collection("a")]);
        fence.blocked.add(&write(vec![collection("b")], 1));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&write(vec![collection("b")], 2)));
        assert!(fence.admits(&Footprint::Keys(vec![collection("c")])));
    }

    #[test]
    fn a_whole_database_task_waits_for_any_key_of_its_database() {
        let fence = fence(&[FenceKey::Graph {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
        }]);
        assert!(!fence.admits(&whole(0, 1)));
        assert!(fence.admits(&whole(7, 1)));
    }

    /// A sync stream spans databases: a frame of the stream into any
    /// database waits, and a whole-database task of another database runs.
    #[test]
    fn a_fenced_sync_stream_holds_its_frames_in_every_database() {
        let stream = FenceKey::SyncStream {
            producer_id: 3,
            stream_id: 4,
        };
        let fence = fence(std::slice::from_ref(&stream));
        let other = FenceKey::Collection {
            db: DatabaseId::new(7),
            name: "7/x".into(),
        };
        assert!(!fence.admits(&write(vec![other.clone(), stream], 1)));
        assert!(fence.admits(&write(vec![other], 1)));
        assert!(fence.admits(&whole(0, 1)), "the stream names no database");
    }

    /// A waiting CRDT apply holds its tenant's CRDT state and its database.
    #[test]
    fn a_waiting_crdt_tenant_key_holds_its_database() {
        let tenant = FenceKey::CrdtTenant {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
        };
        let mut fence = fence(&[collection("a")]);
        fence
            .blocked
            .add(&write(vec![collection("a"), tenant.clone()], 1));
        assert!(!fence.admits(&write(vec![collection("c"), tenant], 2)));
        assert!(!fence.admits(&whole(0, 2)));
        assert!(fence.admits(&whole(7, 2)));
    }

    /// A transaction's overlay orders its own tasks alone.
    #[test]
    fn a_waiting_overlay_key_holds_its_transaction_alone() {
        let overlay = |txn: u64| FenceKey::TxnOverlay(TxnId::new(txn));
        let mut fence = fence(&[collection("a")]);
        fence
            .blocked
            .add(&Footprint::Keys(vec![collection("a"), overlay(7)]));
        assert!(!fence.admits(&Footprint::Keys(vec![overlay(7)])));
        assert!(fence.admits(&Footprint::Keys(vec![overlay(8)])));
    }

    #[test]
    fn a_core_task_never_passes_an_armed_fence() {
        assert!(!fence(&[]).admits(&Footprint::Core));
    }

    #[test]
    fn an_empty_footprint_passes_a_waiting_core_task() {
        let mut fence = fence(&[]);
        fence.blocked.add(&Footprint::Core);
        assert!(fence.admits(&Footprint::Keys(Vec::new())));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&write(Vec::new(), 1)));
    }

    #[test]
    fn waiting_background_work_holds_no_later_task() {
        let mut fence = fence(&[collection("a")]);
        assert!(
            !fence.admits(&Footprint::Background),
            "it waits for the install"
        );
        fence.blocked.add(&Footprint::Background);
        assert!(fence.admits(&whole(7, 1)));
        assert!(fence.admits(&Footprint::Keys(vec![collection("b")])));
    }

    #[test]
    fn a_write_of_a_fenced_vshard_waits_in_any_database() {
        let mut fence = fence(&[FenceKey::VShard(VShardId::new(3))]);
        let other = |name: &str| FenceKey::Collection {
            db: DatabaseId::new(7),
            name: name.into(),
        };
        assert!(!fence.admits(&write(vec![other("x")], 3)));
        assert!(!fence.admits(&whole(7, 3)));
        assert!(fence.admits(&write(vec![other("x")], 4)));
        fence.blocked.add(&write(vec![other("y")], 4));
        assert!(
            !fence.admits(&write(vec![other("x")], 4)),
            "a waiting write holds its vShard"
        );
        assert!(!fence.admits(&whole(8, 4)));
    }

    #[test]
    fn a_waiting_whole_database_task_holds_every_later_task_of_its_database() {
        let mut fence = fence(&[collection("a")]);
        fence.blocked.add(&whole(0, 1));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&write(vec![collection("b")], 2)));
        assert!(!fence.admits(&whole(0, 2)));
        assert!(fence.admits(&whole(7, 2)));
    }

    #[test]
    fn a_waiter_keeps_the_footprint_it_was_held_by() {
        let mut fence = fence(&[collection("a")]);
        let task = ExecutionTask::new(crate::data::executor::core_loop::tests::make_request(
            crate::bridge::envelope::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::Compact,
            ),
        ));
        let request_id = task.request_id();
        fence.hold(task, write(vec![collection("b")], 1));
        fence.recompute_blocked();
        assert!(
            !fence.admits(&Footprint::Keys(vec![collection("b")])),
            "the stored footprint holds the key, not the plan's"
        );
        assert!(fence.remove_waiter(request_id));
        assert!(fence.admits(&Footprint::Keys(vec![collection("b")])));
    }

    /// Connection `id` accepted by node `origin_node`.
    fn connection_of(origin_node: u64, id: u64) -> ClientStream {
        ClientStream {
            origin_node,
            id: crate::control::server::shared::session::ConnectionId::new(id)
                .expect("non-zero id"),
        }
    }

    /// Connection `id` accepted by this test's local node.
    fn connection(id: u64) -> ClientStream {
        connection_of(1, id)
    }

    /// A task of `connection` on `vshard`, or internal work with `None`.
    fn task_on(connection_id: Option<u64>, vshard: u32) -> ExecutionTask {
        use crate::bridge::envelope::RequestStream;
        let stream = connection_id.map_or(RequestStream::Internal, |id| {
            RequestStream::Connection(connection(id))
        });
        task_on_stream(stream, vshard)
    }

    /// A task of `stream` on `vshard`.
    fn task_on_stream(
        stream: crate::bridge::envelope::RequestStream,
        vshard: u32,
    ) -> ExecutionTask {
        ExecutionTask::new(Request {
            stream,
            vshard_id: VShardId::new(vshard),
            ..crate::data::executor::core_loop::tests::make_request(
                crate::bridge::envelope::PhysicalPlan::Meta(
                    nodedb_physical::physical_plan::MetaOp::Compact,
                ),
            )
        })
    }

    fn stream(connection_id: u64, vshard: u32) -> Option<StreamKey> {
        Some((connection(connection_id), VShardId::new(vshard)))
    }

    fn read(name: &str) -> Footprint {
        Footprint::Keys(vec![collection(name)])
    }

    /// Two nodes' connections with one id are distinct streams. A waiter
    /// forwarded from one node holds no task of the other node's client.
    #[test]
    fn equal_connection_ids_of_two_origin_nodes_are_distinct_streams() {
        use crate::bridge::envelope::RequestStream;
        let first = connection_of(1, 7);
        let second = connection_of(2, 7);
        assert_ne!(first, second);
        let mut fence = fence(&[]);
        fence.hold(
            task_on_stream(RequestStream::Connection(first), 0),
            Footprint::Ordered,
        );
        let on = |client: ClientStream| Some((client, VShardId::new(0)));
        assert!(!fence.admits_task(&read("b"), on(first)));
        assert!(fence.admits_task(&read("b"), on(second)));
        assert!(fence.admits_task(&write(vec![collection("b")], 0), on(second)));
    }

    /// A waiting client list holds its own connection's later task on its
    /// vShard. Another connection's task and another vShard's task run.
    #[test]
    fn a_waiting_client_list_holds_its_own_stream_alone() {
        let mut fence = fence(&[]);
        assert!(!fence.admits_task(&Footprint::Ordered, stream(1, 0)));
        fence.hold(task_on(Some(1), 0), Footprint::Ordered);
        assert!(!fence.admits_task(&read("b"), stream(1, 0)));
        assert!(!fence.admits_task(&write(vec![collection("b")], 0), stream(1, 0)));
        assert!(fence.admits_task(&read("b"), stream(2, 0)));
        assert!(fence.admits_task(&write(vec![collection("b")], 0), stream(2, 0)));
        assert!(fence.admits_task(&read("b"), stream(1, 1)));
    }

    /// A waiting ordered read holds every later internal task, which no
    /// connection orders.
    #[test]
    fn a_waiting_ordered_read_holds_later_internal_tasks() {
        let mut fence = fence(&[]);
        fence.hold(task_on(Some(1), 0), Footprint::Ordered);
        assert!(!fence.admits_task(&read("b"), None));
        assert!(!fence.admits_task(&whole(7, 1), None));
    }

    /// An internal unkeyed read is a core task: it holds every later task
    /// of every stream.
    #[test]
    fn a_waiting_internal_meta_read_holds_every_later_task() {
        let mut fence = fence(&[]);
        fence.hold(task_on(None, 0), Footprint::Core);
        assert!(!fence.admits_task(&read("b"), stream(2, 0)));
        assert!(!fence.admits_task(&read("b"), stream(1, 3)));
        assert!(!fence.admits_task(&read("b"), None));
    }

    /// A read and a later write pipelined on one connection and vShard keep
    /// their order, though the write touches no key the read holds.
    #[test]
    fn a_read_and_a_write_pipelined_on_one_stream_keep_their_order() {
        let mut fence = fence(&[collection("a")]);
        fence.hold(task_on(Some(1), 0), read("a"));
        let later_write = write(vec![collection("b")], 0);
        assert!(fence.admits(&later_write), "the footprints are disjoint");
        assert!(!fence.admits_task(&later_write, stream(1, 0)));
        assert!(fence.admits_task(&later_write, stream(2, 0)));
        assert!(fence.admits_task(&later_write, None));
    }

    /// A cancel touches no fenced state: it reaches a waiter of its own
    /// stream.
    #[test]
    fn an_empty_footprint_passes_a_waiter_of_its_stream() {
        let mut fence = fence(&[]);
        fence.hold(task_on(Some(1), 0), Footprint::Ordered);
        assert!(fence.admits_task(&Footprint::Keys(Vec::new()), stream(1, 0)));
        assert!(fence.admits_task(&Footprint::Keys(Vec::new()), None));
    }

    /// A removed waiter no longer holds its stream.
    #[test]
    fn a_removed_waiter_frees_its_stream() {
        let mut fence = fence(&[]);
        let task = task_on(Some(1), 0);
        let request_id = task.request_id();
        fence.hold(task, Footprint::Ordered);
        assert!(fence.remove_waiter(request_id));
        assert!(fence.admits_task(&read("b"), stream(1, 0)));
        assert!(fence.admits_task(&read("b"), None));
    }
}
