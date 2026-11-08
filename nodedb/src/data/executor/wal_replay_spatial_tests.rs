// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::util::fnv1a_hash;
use nodedb_types::TenantId;
use nodedb_types::geometry::Geometry;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::WalRecordArgs;
use std::sync::Arc;

const DB: u64 = 0;
const TENANT: u64 = 7;
const COLLECTION: &str = "places";
const FIELD: &str = "geom";
const SURROGATE: u32 = 0x2a;

/// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
/// tests drive `replay_spatial_wal` directly and never tick the event loop,
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
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(nodedb_types::OrdinalClock::new()),
        crate::data::executor::core_loop::test_governor(),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

fn storage_key() -> nodedb_types::StorageKey {
    nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(SURROGATE))
}

fn point(x: f64, y: f64) -> Geometry {
    Geometry::Point {
        coordinates: [x, y],
    }
}

fn wal_record(record_type: RecordType, lsn: u64, payload: Vec<u8>) -> nodedb_wal::WalRecord {
    nodedb_wal::WalRecord::new(WalRecordArgs {
        record_type: record_type as u32,
        lsn,
        tenant_id: TENANT,
        vshard_id: 0,
        database_id: DB,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// A put record carrying the local/unidentified producer sentinel, which is
/// what a non-sync write records. It deliberately bypasses the sync
/// idempotency gate, so these tests exercise the engine's OWN idempotency
/// rather than the gate's — the property replay actually depends on.
fn put_record(lsn: u64, geometry: &Geometry) -> nodedb_wal::WalRecord {
    let tagged = nodedb_types::geometry::SridGeometry::wgs84(geometry.clone());
    let geometry_bytes = zerompk::to_msgpack_vec(&tagged).expect("encode geometry");
    let payload = nodedb_wal::record::SpatialPutPayload::new(
        SyncProvenance::default(),
        COLLECTION,
        FIELD,
        storage_key().to_string(),
        geometry_bytes,
    )
    .to_bytes()
    .expect("encode SpatialPutPayload");
    wal_record(RecordType::SpatialPut, lsn, payload)
}

fn delete_record(lsn: u64) -> nodedb_wal::WalRecord {
    let payload = nodedb_wal::record::SpatialDeletePayload::new(
        SyncProvenance::default(),
        COLLECTION,
        FIELD,
        Some(storage_key().to_string()),
    )
    .to_bytes()
    .expect("encode SpatialDeletePayload");
    wal_record(RecordType::SpatialDelete, lsn, payload)
}

fn replay(core: &mut CoreLoop, record: &nodedb_wal::WalRecord) {
    core.replay_spatial_wal(
        std::slice::from_ref(record),
        1,
        &nodedb_wal::TombstoneSet::new(),
        &crate::wal::redo::replay_origin::GroupedReplayOrigins::default(),
    );
}

/// Both halves of the spatial state for the test's surrogate: the R-tree
/// entry count (the half that would silently double), the docmap binding,
/// and the decoded sparse document body. The body is compared decoded
/// rather than as raw bytes because the map it is built from does not fix
/// a field order — the VALUE is what must be stable, not the encoding.
fn spatial_state(core: &CoreLoop) -> (usize, Option<String>, Option<nodedb_types::Value>) {
    let db = DatabaseId::new(DB);
    let tid = TenantId::new(TENANT);
    let entry_id = fnv1a_hash(storage_key().to_string().as_bytes());
    let entries = core
        .spatial_indexes
        .get(&(db, tid, COLLECTION.to_string(), FIELD.to_string()))
        .map(|rtree| rtree.len())
        .unwrap_or(0);
    let mapped = core
        .spatial_doc_map
        .get(&(db, tid, COLLECTION.to_string(), FIELD.to_string(), entry_id))
        .cloned();
    let body = core
        .sparse
        .get(DB, TENANT, COLLECTION, &storage_key())
        .expect("sparse read")
        .map(|bytes| nodedb_types::value_from_msgpack(&bytes).expect("decode body"));
    (entries, mapped, body)
}

/// The property spatial replay relies on instead of a floor: applying the
/// SAME `SpatialPut` record a second time must leave one R-tree entry, not
/// two. `RTree::insert` never deduplicates, so without the docmap-guarded
/// delete-first this leaves a second entry with the same id that no delete
/// can ever fully remove (`RTree::delete` removes one match per call).
#[test]
fn replaying_a_put_record_twice_leaves_one_entry() {
    let mut h = make_core();
    let record = put_record(10, &point(1.0, 2.0));

    replay(&mut h.core, &record);
    let after_first = spatial_state(&h.core);
    assert_eq!(after_first.0, 1, "one geometry indexed");
    assert_eq!(
        after_first.1.as_deref(),
        Some(storage_key().to_string().as_str())
    );
    assert!(after_first.2.is_some(), "the document body must be written");

    replay(&mut h.core, &record);
    assert_eq!(
        spatial_state(&h.core),
        after_first,
        "re-applying a durable SpatialPut record must be a no-op"
    );
}

/// A put whose geometry genuinely moved is not a replay and must take
/// effect — the idempotency above must come from replacing the surrogate's
/// entry, not from ignoring repeat writes to it.
#[test]
fn a_moved_geometry_still_takes_effect() {
    let mut h = make_core();
    replay(&mut h.core, &put_record(10, &point(1.0, 2.0)));
    let before = spatial_state(&h.core);

    replay(&mut h.core, &put_record(11, &point(50.0, 60.0)));
    let after = spatial_state(&h.core);
    assert_eq!(after.0, 1, "a move must replace the entry, not add one");
    assert_ne!(after.2, before.2, "the stored geometry must be the new one");
}

/// The delete arm carries the same obligation: a repeated `SpatialDelete`
/// must remove nothing the second time, in the R-tree, the docmap, and the
/// document store alike.
#[test]
fn replaying_a_delete_record_twice_leaves_nothing_behind() {
    let mut h = make_core();
    replay(&mut h.core, &put_record(10, &point(1.0, 2.0)));

    let record = delete_record(11);
    replay(&mut h.core, &record);
    let after_first = spatial_state(&h.core);
    assert_eq!(after_first, (0, None, None), "the geometry is fully gone");

    replay(&mut h.core, &record);
    assert_eq!(
        spatial_state(&h.core),
        after_first,
        "re-applying a durable SpatialDelete record must be a no-op"
    );
}
