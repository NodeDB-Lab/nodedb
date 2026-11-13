// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::data::executor::applied_prefix::ReplayStamp;
use crate::data::executor::applied_prefix::stamp::LsnRange;
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::hnsw::HnswParams;
use std::sync::Arc;

/// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
/// tests drive `replay_vector_wal` directly and never tick the event loop,
/// so the far ends are unused — they just must not be dropped.
struct CoreHarness {
    core: CoreLoop,
    _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
    _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn make_core() -> CoreHarness {
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use nodedb_bridge::buffer::RingBuffer;

    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        crate::data::executor::core_loop::CoreOpenParams::unencrypted(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        ),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

/// An unfielded single-insert `VectorPut` WAL record at `lsn`, its vector
/// bound to a surrogate numbered after the LSN.
fn vector_put_record(
    lsn: u64,
    tenant_id: u64,
    collection: &str,
    vector: Vec<f32>,
) -> nodedb_wal::WalRecord {
    let dim = vector.len();
    let payload = crate::control::server::wal_dispatch::encode_vector_put_payload(
        collection,
        &vector,
        dim,
        "",
        nodedb_types::Surrogate::new(lsn as u32),
        None,
    )
    .expect("encode vector put");
    nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
        record_type: nodedb_wal::record::RecordType::VectorPut as u32,
        lsn,
        tenant_id,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// Simulate `load_vector_checkpoints` restoring a checkpoint that holds
/// one vector of `collection` and names the records in `stamp`.
fn restore_checkpoint(
    core: &mut CoreLoop,
    tenant_id: u64,
    collection: &str,
    vector: Vec<f32>,
    stamp: ReplayStamp,
) {
    let dim = vector.len();
    let mut coll = VectorCollection::new(dim, HnswParams::default());
    coll.insert(vector).unwrap();
    let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
    core.vector_collections.insert(key, coll);
    core.floors.replay_floors.vector.set(stamp);
}

fn coll_len(core: &CoreLoop, tenant_id: u64, collection: &str) -> Option<usize> {
    let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
    core.vector_collections.get(&key).map(|c| c.len())
}

fn replay(core: &mut CoreLoop, records: &[nodedb_wal::WalRecord]) {
    core.replay_vector_wal(
        records,
        1,
        &nodedb_wal::TombstoneSet::new(),
        &crate::wal::redo::replay_origin::GroupedReplayOrigins::default(),
    );
}

/// A record the restored checkpoint's stamp names is not replayed: an HNSW
/// insert never dedups, so replaying it appends a second node.
#[test]
fn a_record_the_stamp_names_is_not_reapplied() {
    let mut h = make_core();
    restore_checkpoint(
        &mut h.core,
        7,
        "emb",
        vec![1.0, 2.0, 3.0],
        ReplayStamp::through(10),
    );
    replay(
        &mut h.core,
        &[vector_put_record(10, 7, "emb", vec![1.0, 2.0, 3.0])],
    );
    assert_eq!(coll_len(&h.core, 7, "emb"), Some(1));
}

/// A record above the stamp's prefix and outside its applied ranges is the
/// WAL tail the checkpoint does not hold, and it replays.
#[test]
fn a_record_the_stamp_does_not_name_replays() {
    let mut h = make_core();
    restore_checkpoint(
        &mut h.core,
        7,
        "emb",
        vec![1.0, 2.0, 3.0],
        ReplayStamp::through(10),
    );
    replay(
        &mut h.core,
        &[vector_put_record(11, 7, "emb", vec![4.0, 5.0, 6.0])],
    );
    assert_eq!(coll_len(&h.core, 7, "emb"), Some(2));
}

/// Record 7 was still on its way when record 10 applied and the checkpoint
/// was written. The checkpoint holds 10 and not 7. A stamp at the highest
/// applied LSN would skip 7 and lose its vector.
#[test]
fn a_record_in_flight_below_an_applied_one_replays_once() {
    let mut h = make_core();
    let stamp = ReplayStamp {
        prefix: 5,
        applied_above: vec![LsnRange { start: 10, end: 10 }],
    };
    restore_checkpoint(&mut h.core, 7, "emb", vec![1.0, 2.0, 3.0], stamp);
    replay(
        &mut h.core,
        &[
            vector_put_record(7, 7, "emb", vec![4.0, 5.0, 6.0]),
            vector_put_record(10, 7, "emb", vec![1.0, 2.0, 3.0]),
        ],
    );
    assert_eq!(
        coll_len(&h.core, 7, "emb"),
        Some(2),
        "record 7 applies once and record 10 does not apply again"
    );
}
