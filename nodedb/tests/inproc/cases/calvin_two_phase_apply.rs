// SPDX-License-Identifier: BUSL-1.1

//! Staged Calvin apply on the Data Plane: `MetaOp::CalvinExecuteStatic`
//! VALIDATES + STAGES a transaction's write plans into the commit-pending
//! buffer WITHOUT mutating base, returning the local commit vote on
//! `read_set_valid`. `MetaOp::CalvinResolve` then resolves the staged plans
//! into the transaction's redo record, and `MetaOp::CalvinFlush` installs it
//! at its LSN (making the write visible), or `MetaOp::CalvinDrop` discards
//! the staged state (leaving base unchanged).
//!
//! These drive a `CoreLoop` directly through the SPSC ring so the atomicity
//! seam is observed without any scheduler timing: nothing a stage writes is
//! visible until the flush, and a drop never makes it visible.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Response, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, MetaOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;
use nodedb_types::Surrogate;
use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(nodedb_types::OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

/// Build a request for `plan` on `vshard`, carrying an optional committed WAL
/// LSN (present on the seed write so its version is recorded).
fn make_request(plan: PhysicalPlan, vshard: u32, wal_lsn: Option<Lsn>) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: DatabaseId::DEFAULT,
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: nodedb_types::TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: nodedb::event::EventSource::RaftFollower,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn,
        resolved_now_ms: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

fn send(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
    vshard: u32,
    wal_lsn: Option<Lsn>,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(make_request(
        plan, vshard, wal_lsn,
    )))
    .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

/// Resolve the transaction staged at `(epoch, 0)` on `vshard` and build the
/// flush that installs its redo record.
fn resolved_flush(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    vshard: u32,
) -> PhysicalPlan {
    let resolved = send(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position: 0 }),
        vshard,
        None,
    );
    assert_eq!(
        resolved.status,
        Status::Ok,
        "resolve must succeed: {resolved:?}"
    );
    PhysicalPlan::Meta(MetaOp::CalvinFlush {
        epoch,
        position: 0,
        redo: resolved.payload.to_vec(),
        collections: Vec::new(),
        sum_targets: Vec::new(),
    })
}

/// A write committed through the Calvin path to seed a write version.
struct CalvinSeed<'a> {
    epoch: u64,
    vshard: u32,
    /// The collection `plans` write; the install records its floor at `lsn`.
    collection: &'a str,
    plans: Vec<PhysicalPlan>,
    lsn: u64,
}

/// Commit `seed.plans` as the Calvin transaction at `(seed.epoch, 0)` on
/// `seed.vshard` and install its redo record at `seed.lsn`: the path a
/// committed multi-shard write takes. The install records each written key
/// and the collection floor at that LSN.
fn commit_calvin(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    seed: CalvinSeed<'_>,
) -> Response {
    let staged = send(
        core,
        tx,
        rx,
        stage_static(seed.epoch, 0, seed.plans, Vec::new()),
        seed.vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "seed stage must succeed: {staged:?}"
    );
    let resolved = send(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve {
            epoch: seed.epoch,
            position: 0,
        }),
        seed.vshard,
        None,
    );
    assert_eq!(
        resolved.status,
        Status::Ok,
        "seed resolve must succeed: {resolved:?}"
    );
    send(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinFlush {
            epoch: seed.epoch,
            position: 0,
            redo: resolved.payload.to_vec(),
            collections: vec![seed.collection.to_string()],
            sum_targets: Vec::new(),
        }),
        seed.vshard,
        Some(Lsn::new(seed.lsn)),
    )
}

fn kv_put(coll: &str, key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

fn kv_get(coll: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

/// A minimal msgpack-encoded document body, `{"a": "1"}`.
fn doc_value() -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert("a".to_string(), nodedb_types::Value::String("1".into()));
    zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap()
}

/// A minimal document `PointInsert` plan, used to record a write-version at a
/// specific surrogate (the document engine's per-key identity).
fn doc_insert(coll: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        document_id: document_id.to_string(),
        value: doc_value(),
        if_absent: false,
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

fn stage_static(
    epoch: u64,
    position: u32,
    plans: Vec<PhysicalPlan>,
    versioned_reads: Vec<VersionedReadEntry>,
) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
        epoch,
        position,
        tenant_id: TenantId::new(1),
        plans,
        epoch_system_ms: 0,
        is_group_leader: true,
        versioned_reads,
    })
}

/// A valid (empty read-set) staged write is NOT visible until the flush, and
/// the flush makes it visible — the stage/flush atomicity seam.
#[test]
fn flush_makes_staged_calvin_write_visible() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Stage a write with an empty read-set → vote is valid (commit).
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(6, 0, vec![kv_put("flushcoll", b"fk", b"fv")], Vec::new()),
        0,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed");
    assert_eq!(
        staged.read_set_valid,
        Some(true),
        "empty read-set is vacuously current → commit vote"
    );

    // Not yet applied: the staged write is invisible before the flush.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "staged write must NOT be visible before flush; got {before:?}"
    );

    // The flush installs the resolved redo record to base.
    let flush_plan = resolved_flush(&mut core, &mut tx, &mut rx, 6, 0);
    let flush = send(
        &mut core,
        &mut tx,
        &mut rx,
        flush_plan,
        0,
        Some(Lsn::new(60)),
    );
    assert_eq!(flush.status, Status::Ok, "flush must succeed: {flush:?}");

    // Now visible.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert_eq!(
        after.status,
        Status::Ok,
        "read after flush must succeed: {after:?}"
    );
    assert!(
        !after.payload.is_empty(),
        "flushed write must be visible after flush"
    );
}

/// An invalid vote (a stale versioned read against a newer local write) STAGES
/// but never applies; the drop discards it and base stays unchanged.
#[test]
fn drop_discards_invalid_staged_calvin_write() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to `dropcoll` at LSN 100 so its collection write
    // version floor is 100. The seed carries a WAL LSN so the version records.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: 0,
            collection: "dropcoll",
            plans: vec![kv_put("dropcoll", b"seed", b"v")],
            lsn: 100,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    // The read entry's collection must home to the staged request's vShard for
    // the read-set check to consider it.
    let read_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "dropcoll").as_u32();

    // A read of `dropcoll` observed at LSN 50 — stale against the seed's write
    // at LSN 100 → the read-set is no longer current → abort vote.
    let stale_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "dropcoll".to_string(),
        key: ReadKeyIdent::Predicate,
        read_lsn: Lsn::new(50),
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            7,
            0,
            vec![kv_put("targetcoll", b"tk", b"tv")],
            vec![stale_read],
        ),
        read_vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "stage must succeed even on abort vote"
    );
    assert_eq!(
        staged.read_set_valid,
        Some(false),
        "stale read-set must produce an abort vote"
    );

    // Staged, not applied: the target write is invisible.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "aborted staged write must NOT be visible; got {before:?}"
    );

    // Drop discards the staged plans and fires nothing. The drop must target
    // the SAME vShard the stage keyed under (as production dispatches it), so it
    // actually pops this participant's staged slice.
    let dropped = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 7,
            position: 0,
        }),
        read_vshard,
        None,
    );
    assert_eq!(dropped.status, Status::Ok, "drop must succeed: {dropped:?}");

    // Still invisible after the drop — base was never mutated.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        after.payload.is_empty() || after.status == Status::Error,
        "dropped write must never be visible; got {after:?}"
    );
}

/// A `Point` read (not just a `Predicate` read) at or after the recorded write
/// LSN is current → commit vote, and the flush applies the staged write.
#[test]
fn point_read_at_write_lsn_commits_and_flush_applies() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to key `pk` in `pointcoll` at LSN 10.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: 0,
            collection: "pointcoll",
            plans: vec![kv_put("pointcoll", b"pk", b"v1")],
            lsn: 10,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    let point_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "pointcoll").as_u32();

    // A Point read of the exact same key observed at LSN 10 (== the write) is
    // still current: no write happened AFTER the read.
    let current_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "pointcoll".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"pk".as_slice()))),
        read_lsn: Lsn::new(10),
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            8,
            0,
            vec![kv_put("committarget", b"ck", b"cv")],
            vec![current_read],
        ),
        point_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.read_set_valid,
        Some(true),
        "a read at or after the last write LSN must be current -> commit vote"
    );

    let flush_plan = resolved_flush(&mut core, &mut tx, &mut rx, 8, point_vshard);
    let flush = send(
        &mut core,
        &mut tx,
        &mut rx,
        flush_plan,
        point_vshard,
        Some(Lsn::new(80)),
    );
    assert_eq!(flush.status, Status::Ok, "flush must succeed: {flush:?}");

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("committarget", b"ck"),
        0,
        None,
    );
    assert_eq!(after.status, Status::Ok, "read after flush must succeed");
    assert!(
        !after.payload.is_empty(),
        "flushed write must be visible after flush"
    );
}

/// A `Point` read of a key STALE against a later write to that same key
/// (read_lsn=5 vs. a committed write at LSN=10) aborts the stage vote, and the
/// drop discards the staged write with no base mutation — mirrors
/// `drop_discards_invalid_staged_calvin_write` but for a Point key (not a
/// collection-scoped Predicate).
#[test]
fn stale_point_read_of_kv_key_aborts_stage_and_drop_discards() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to key `pk` in `stalecoll` at LSN 10.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: 0,
            collection: "stalecoll",
            plans: vec![kv_put("stalecoll", b"pk", b"v1")],
            lsn: 10,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    let stale_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "stalecoll").as_u32();

    // A Point read of the exact same key observed at LSN 5 — stale against the
    // write at LSN 10 → the read-set is no longer current → abort vote.
    let stale_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "stalecoll".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"pk".as_slice()))),
        read_lsn: Lsn::new(5),
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            9,
            0,
            vec![kv_put("aborttarget", b"ak", b"av")],
            vec![stale_read],
        ),
        stale_vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "stage must succeed even on abort vote"
    );
    assert_eq!(
        staged.read_set_valid,
        Some(false),
        "a stale Point read (write after read_lsn) must produce an abort vote"
    );

    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("aborttarget", b"ak"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "aborted staged write must NOT be visible; got {before:?}"
    );

    let dropped = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 9,
            position: 0,
        }),
        stale_vshard,
        None,
    );
    assert_eq!(dropped.status, Status::Ok, "drop must succeed: {dropped:?}");

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("aborttarget", b"ak"),
        0,
        None,
    );
    assert!(
        after.payload.is_empty() || after.status == Status::Error,
        "dropped write must never be visible; got {after:?}"
    );
}

/// Phantom-insert proof for the KV engine: a Point read observed a key as
/// ABSENT at LSN 5; a concurrent INSERT of that EXACT key commits at LSN 8.
/// The KV read-key identity is the raw key bytes — the same identity the
/// insert writes under — so the per-key write-version check catches the
/// phantom: validating the stale absent-read against the now-present key
/// aborts the stage.
#[test]
fn absent_kv_key_phantom_insert_causes_abort() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let phantom_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "phantomkv").as_u32();

    // The key was absent when read at (the then-current watermark) LSN 5 — no
    // write is seeded yet.
    let absent_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "phantomkv".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"newkey".as_slice()))),
        read_lsn: Lsn::new(5),
    };

    // Concurrently, the exact same key is inserted and commits at LSN 8.
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: phantom_vshard,
            collection: "phantomkv",
            plans: vec![PhysicalPlan::Kv(KvOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "phantomkv"),
                key: b"newkey".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            })],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "phantom insert must commit: {insert:?}"
    );

    // Validating the (now stale) absent-read against the current write index
    // must abort: the insert at LSN 8 is AFTER the read at LSN 5.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            10,
            0,
            vec![kv_put("phantomtarget", b"tk", b"tv")],
            vec![absent_read],
        ),
        phantom_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.read_set_valid,
        Some(false),
        "a phantom insert into a key observed absent must abort the stage vote"
    );
}

/// Absent-DOCUMENT phantom safety: a document `PointGet` on an absent document
/// cannot record `Point(KeyRepr::Surrogate(s))`, because the placeholder
/// surrogate `s` a miss carries is allocated from a monotonic counter unrelated
/// to `document_id` — it never coincides with the freshly-minted surrogate a
/// subsequent INSERT of that `document_id` receives, so a per-key OCC check
/// would never catch the phantom. The capture layer therefore degrades an
/// absent document read to `Predicate` (collection floor). A concurrent INSERT
/// into that collection advances `coll_write_lsn` past the read_lsn, so
/// `WriteVersionIndex::read_is_valid` (predicate branch) judges the stale read
/// invalid and the stage VOTES ABORT — collection-granular phantom safety.
#[test]
fn absent_document_phantom_insert_is_caught() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let doc_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "phantomdocs").as_u32();

    // The document was absent when read: capture degraded the miss to a
    // collection-scoped predicate on "phantomdocs" at read_lsn 5.
    let absent_doc_read = VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "phantomdocs".to_string(),
        key: ReadKeyIdent::Predicate,
        read_lsn: Lsn::new(5),
    };

    // Concurrently, a document with the same document_id the read targeted is
    // actually inserted -- allocated a freshly-minted surrogate (42), committing
    // at LSN 8. Its collection floor advance (phantomdocs -> 8) is what the
    // predicate read validates against.
    const NEWLY_ALLOCATED_SURROGATE: u32 = 42;
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: doc_vshard,
            collection: "phantomdocs",
            plans: vec![doc_insert(
                "phantomdocs",
                "the-doc-id",
                NEWLY_ALLOCATED_SURROGATE,
            )],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "phantom insert must commit: {insert:?}"
    );

    // Validate the stale absent-read: the predicate check sees the phantomdocs
    // collection floor (LSN 8) is AFTER the read (LSN 5), so the read is no
    // longer current and the stage must abort.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            11,
            0,
            vec![kv_put("docphantomtarget", b"tk", b"tv")],
            vec![absent_doc_read],
        ),
        doc_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.read_set_valid,
        Some(false),
        "a phantom insert into a collection observed absent must abort the stage \
         vote via the collection floor"
    );
}

/// No-over-abort companion to `absent_document_phantom_insert_is_caught`: the
/// collection-floor degrade must NOT abort every absent-read transaction. An
/// absent document read (predicate on "phantomdocs" at LSN 5) stays valid when
/// no insert lands in THAT collection — a concurrent insert into an UNRELATED
/// collection advances only its own floor, leaving phantomdocs at zero, so the
/// stale read is still current and the stage VOTES COMMIT.
#[test]
fn absent_document_read_without_matching_insert_still_commits() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let doc_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "phantomdocs").as_u32();

    let absent_doc_read = VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "phantomdocs".to_string(),
        key: ReadKeyIdent::Predicate,
        read_lsn: Lsn::new(5),
    };

    // A concurrent insert into a DIFFERENT collection commits at LSN 8. It
    // advances only "othercoll"'s floor; phantomdocs is untouched.
    const UNRELATED_SURROGATE: u32 = 42;
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: doc_vshard,
            collection: "othercoll",
            plans: vec![doc_insert("othercoll", "other-id", UNRELATED_SURROGATE)],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "unrelated insert must commit: {insert:?}"
    );

    // The predicate read on phantomdocs is still current: its collection floor
    // never advanced past read_lsn 5, so the stage must commit.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            11,
            0,
            vec![kv_put("docphantomtarget", b"tk", b"tv")],
            vec![absent_doc_read],
        ),
        doc_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.read_set_valid,
        Some(true),
        "an absent-document read must NOT over-abort when no insert lands in its \
         collection"
    );
}

/// Push one prebuilt request through the ring and return its response.
fn send_request(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    request: Request,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(request)).unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

/// Calvin sub-operations are already ordered: every replica must run them.
/// A stage and a flush whose envelope deadline has already passed still
/// execute, and the write becomes visible. They never answer
/// `DeadlineExceeded`.
#[test]
fn already_ordered_stage_and_flush_run_past_their_deadline() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let already_ordered = |plan: PhysicalPlan| Request {
        deadline: Instant::now() - Duration::from_secs(1),
        admission: nodedb::bridge::envelope::Admission::Exempt(
            nodedb::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
        ..make_request(plan, 0, None)
    };

    let staged = send_request(
        &mut core,
        &mut tx,
        &mut rx,
        already_ordered(stage_static(
            9,
            0,
            vec![kv_put("latecoll", b"lk", b"lv")],
            Vec::new(),
        )),
    );
    assert_eq!(staged.status, Status::Ok, "late stage must run: {staged:?}");
    assert_eq!(staged.read_set_valid, Some(true));

    let flush_plan = resolved_flush(&mut core, &mut tx, &mut rx, 9, 0);
    let flush = send_request(
        &mut core,
        &mut tx,
        &mut rx,
        Request {
            wal_lsn: Some(Lsn::new(90)),
            ..already_ordered(flush_plan)
        },
    );
    assert_eq!(flush.status, Status::Ok, "late flush must run: {flush:?}");

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("latecoll", b"lk"),
        0,
        None,
    );
    assert_eq!(after.status, Status::Ok, "read after flush: {after:?}");
    assert!(
        !after.payload.is_empty(),
        "the late flush must make the staged write visible"
    );
}
