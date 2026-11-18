// SPDX-License-Identifier: BUSL-1.1

//! An install's held state spills past the threshold and reads back as the
//! whole-record install holds it: the same write notes, KV prior images,
//! events and row-source attribution.

use nodedb_wal::record::RecordType;

use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::newest_ordinals::{SliceKey, collection_vshard};
use crate::data::executor::core_loop::tests::make_core;
use crate::data::executor::core_loop::write_note::{KeyRepr, WriteStamp};
use crate::data::executor::handlers::spill::tiered::SpillSpace;
use crate::event::bus::EventConsumerRx;
use crate::event::{EventSource, WriteEvent};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::{RedoPublish, RedoRowSource, RedoSubRecord};

use super::super::install_refusal_tests as refusal;
use super::super::state::{DeferredNote, DeferredNoteKind};
use super::super::stream_validate::tests::{Hold, TID, hold, plain_core, record};
use super::equivalence_tests::{
    FILL, FILL_FROM, fill, install_batched_at, install_whole, resolved,
};
use super::notes::CollapsedNotes;
use crate::data::executor::core_loop::held_events::read_as::{as_read, as_read_all};

// ── Write notes ──────────────────────────────────────────────────────────

const NOTE_COLLECTIONS: [&str; 2] = ["a", "b"];

/// A deterministic stream of pseudo-random numbers.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

fn edge_key() -> KeyRepr {
    KeyRepr::Edge {
        src: "n1".into(),
        label: "l".into(),
        dst: "n2".into(),
    }
}

/// `count` notes of rows and wipes over [`NOTE_COLLECTIONS`], at stamps of
/// two vShards and two LSNs. One note in twelve is a wipe, so most
/// sequences leave cache entries standing.
fn note_sequence(seed: u64, count: usize) -> Vec<DeferredNote> {
    let mut rng = Lcg(seed);
    (0..count)
        .map(|_| {
            let collection = NOTE_COLLECTIONS[rng.below(2) as usize].to_string();
            let kind = match rng.below(12) {
                0 => DeferredNoteKind::Wipe,
                1 => DeferredNoteKind::Row(None),
                2 | 3 => DeferredNoteKind::Row(Some(KeyRepr::KvKey(Box::from(&b"k1"[..])))),
                4 | 5 => DeferredNoteKind::Row(Some(edge_key())),
                _ => DeferredNoteKind::Row(Some(KeyRepr::Surrogate(1 + rng.below(3) as u32))),
            };
            DeferredNote {
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(TID),
                collection,
                stamp: WriteStamp {
                    vshard: VShardId::new(rng.below(2) as u32),
                    lsn: Lsn::new(50 + rng.below(2)),
                },
                kind,
            }
        })
        .collect()
}

/// Every vShard a note of the sequences touches.
fn note_vshards() -> Vec<VShardId> {
    let mut vshards = vec![
        VShardId::new(0),
        VShardId::new(1),
        VShardId::from_key(b"n1"),
        VShardId::from_key(b"n2"),
    ];
    for collection in NOTE_COLLECTIONS {
        vshards.push(collection_vshard(DatabaseId::DEFAULT, collection).expect("home vshard"));
    }
    vshards
}

fn slice_key(collection: &str, vshard: VShardId) -> SliceKey {
    SliceKey {
        vshard,
        db: DatabaseId::DEFAULT,
        tenant: TenantId::new(TID),
        collection: Box::from(collection),
    }
}

/// A core whose rows of [`NOTE_COLLECTIONS`] hold versions, with one KV
/// key and one edge of each, so every keyed note raises its slice to a
/// stored ordinal.
fn note_core() -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp, dir) = make_core();
    // Every seeded version takes a fixed ordinal, so both cores of a test
    // store the same ordinals and their raises match.
    for (index, collection) in NOTE_COLLECTIONS.into_iter().enumerate() {
        let base = 1_000 * (index as i64 + 1);
        for surrogate in 1..=3u32 {
            core.apply_scope.replicated_ordinal = Some(base + i64::from(surrogate));
            let response = crate::data::executor::core_loop::tests::point_put(
                &mut core,
                &crate::data::executor::core_loop::tests::wal_task(5),
                collection,
                &format!("{collection}{surrogate}"),
                surrogate,
                "x",
            );
            core.apply_scope.replicated_ordinal = None;
            assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        }
        let kv_scope = core.kv_engine.open_write_scope(Some(base + 10));
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: TID,
                collection,
                key: b"k1",
                value: &[0x80],
                ttl_ms: 0,
                now_ms: crate::engine::kv::current_ms(),
                surrogate: nodedb_types::Surrogate::new(60),
            })
            .expect("seed kv key");
        core.kv_engine.close_write_scope(kv_scope);
        core.edge_store
            .put_edge_versioned(
                crate::engine::graph::edge_store::EdgeRef::new(
                    DatabaseId::DEFAULT,
                    TenantId::new(TID),
                    collection,
                    "n1",
                    "l",
                    "n2",
                ),
                &[],
                base + 20,
                0,
                i64::MAX,
            )
            .expect("seed edge");
    }
    core.floors
        .applied_prefix
        .observe_outcome_floor(Lsn::new(40));
    (core, Box::new((req, resp, dir)))
}

/// Fill the cache with a known value for every vShard the notes touch,
/// and for each collection's slice on its home vShard. An edge's endpoint
/// vShards hold no slice of the collection, so a wipe leaves them.
fn seed_cache(core: &mut CoreLoop) {
    core.newest_ordinals.clear();
    for vshard in note_vshards() {
        core.newest_ordinals.set_vshard(vshard, Some(1));
    }
    for collection in NOTE_COLLECTIONS {
        let home = collection_vshard(DatabaseId::DEFAULT, collection).expect("home vshard");
        core.newest_ordinals
            .set_slice(slice_key(collection, home), Some(1));
    }
}

/// What the notes left: every cached slice and vShard the notes touch, the
/// watermark, and the applied records.
fn note_state(core: &CoreLoop) -> String {
    let mut state = Vec::new();
    for vshard in note_vshards() {
        state.push(format!(
            "{vshard:?}={:?}",
            core.newest_ordinals.vshard(vshard)
        ));
        for collection in NOTE_COLLECTIONS {
            let slice = core.newest_ordinals.slice(&slice_key(collection, vshard));
            state.push(format!("{collection}@{vshard:?}={slice:?}"));
        }
    }
    let applied = core
        .floors
        .applied_prefix
        .stamp()
        .ok()
        .map(|stamp| [49u64, 50, 51, 52].map(|lsn| stamp.skips(lsn)));
    state.push(format!(
        "watermark={:?} applied={applied:?}",
        core.watermark
    ));
    state.join("\n")
}

/// Publish `notes` one by one, in order, as an install without collapse.
fn publish_in_order(core: &mut CoreLoop, notes: Vec<DeferredNote>) {
    for note in notes {
        match note.kind {
            DeferredNoteKind::Wipe => {
                core.publish_collection_wipe(note.db, note.tenant, &note.collection, note.stamp)
            }
            DeferredNoteKind::Row(key) => {
                core.publish_write(note.db, note.tenant, &note.collection, key, note.stamp)
            }
        }
        .expect("publish note");
    }
}

/// Collapsed notes, spilled every few notes, publish what the ordered
/// notes publish.
#[test]
fn notes_collapse_matches_ordered_notes() {
    let (mut ordered, _ordered_ends) = note_core();
    let (mut collapsed, _collapsed_ends) = note_core();
    let spill_dir = tempfile::tempdir().expect("tempdir");
    let mut space = SpillSpace::new(spill_dir.path().join("notes"), "test", 0);
    let mut standing = 0;
    for seed in 0..40u64 {
        seed_cache(&mut ordered);
        seed_cache(&mut collapsed);
        publish_in_order(&mut ordered, note_sequence(seed, 48));
        if note_vshards()
            .into_iter()
            .any(|vshard| ordered.newest_ordinals.vshard(vshard).is_some())
        {
            standing += 1;
        }
        let mut notes = CollapsedNotes::default();
        for (index, note) in note_sequence(seed, 48).into_iter().enumerate() {
            notes.hold(note);
            if index % 7 == 6 {
                notes.spill(&mut space).expect("spill notes");
            }
        }
        collapsed
            .publish_collapsed_notes(notes)
            .expect("publish collapsed notes");
        assert_eq!(note_state(&collapsed), note_state(&ordered), "seed {seed}");
    }
    assert!(
        standing > 20,
        "most sequences leave a vShard cached: {standing} of 40"
    );
}

// ── Events ───────────────────────────────────────────────────────────────

fn kv_put(collection: &str, key: &[u8], amount: i64, surrogate: u32) -> RedoSubRecord {
    let value =
        nodedb_types::json_to_msgpack(&serde_json::json!({"n": amount})).expect("encode value");
    RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: zerompk::to_msgpack_vec(&(
            "kv_put",
            collection,
            key.to_vec(),
            value,
            0u64,
            None::<u64>,
            surrogate,
        ))
        .expect("encode kv put"),
    }
}

/// Attach an event bus of `capacity` events to the held core.
fn listen(held: &mut Hold, capacity: usize) -> EventConsumerRx {
    let (mut producers, mut consumers) =
        crate::event::bus::create_event_bus_with_capacity(1, capacity);
    held.core
        .set_event_producer(producers.pop().expect("producer"));
    consumers.pop().expect("consumer")
}

fn received(events: &mut EventConsumerRx) -> Vec<WriteEvent> {
    let mut out = Vec::new();
    while let Some(event) = events.try_recv() {
        out.push(event);
    }
    out
}

/// A KV key written in the first batch and again in a late one reports the
/// value it held before the install for both writes.
#[test]
fn kv_priors_come_from_before_the_install() {
    let mut ops = vec![kv_put("cache", b"k", 2, 51)];
    ops.extend(fill(FILL_FROM, 24));
    ops.push(kv_put("cache", b"k", 3, 51));
    let redo = record(ops);
    let collections = vec!["cache".to_string(), FILL.to_string()];

    let mut whole = hold(plain_core);
    refusal::seed_kv(&mut whole.core);
    let mut whole_events = listen(&mut whole, 1024);
    let mut batched = hold(plain_core);
    refusal::seed_kv(&mut batched.core);
    let mut batched_events = listen(&mut batched, 1024);

    let response = install_whole(&mut whole.core, &redo, &[], &collections);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let response = install_batched_at(&mut batched, &redo, &[], &collections, 0);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert!(
        batched.core.redo_apply.last_held.spilled_entries > 0,
        "the spools spilled"
    );

    let kv = |events: Vec<WriteEvent>| -> Vec<WriteEvent> {
        events
            .into_iter()
            .filter(|event| event.collection.as_ref() == "cache")
            .collect()
    };
    let batched_kv = kv(received(&mut batched_events));
    assert_eq!(batched_kv.len(), 2, "one event per write of the key");
    let whole_kv = kv(received(&mut whole_events));
    assert_eq!(as_read_all(&batched_kv), as_read_all(&whole_kv));
    assert_eq!(
        batched_kv[0].old_value, batched_kv[1].old_value,
        "both writes report the value before the install"
    );
    assert!(batched_kv[0].old_value.is_some());
    assert!(
        batched_kv
            .iter()
            .all(|event| event.op == crate::event::WriteOp::Update),
        "the key held a value before each write"
    );
}

/// The batched install, spilling after each batch, sends the event stream
/// the whole-record install sends: the same events, in the same order,
/// with the same row-source attribution.
#[test]
fn events_match_whole_record_install() {
    fn none(_: &mut CoreLoop) {}
    let plans = [refusal::crdt_upsert()];
    let mut redo = resolved(&plans, none);
    let mut ops = fill(FILL_FROM, 8);
    ops.push(kv_put("cache", b"k", 2, 51));
    ops.append(&mut redo.ops);
    ops.extend(fill(FILL_FROM + 8, 8));
    ops.push(kv_put("cache", b"k2", 4, 52));
    redo.ops = ops;
    redo.row_sources = vec![
        RedoRowSource {
            collection: FILL.into(),
            event_source: EventSource::Trigger.wal_code(),
            rows: vec![format!("f{}", FILL_FROM + 1), format!("f{}", FILL_FROM + 9)],
        },
        RedoRowSource {
            collection: "cache".into(),
            event_source: EventSource::Deferred.wal_code(),
            rows: Vec::new(),
        },
    ];
    redo.publishes = vec![RedoPublish {
        owner: "trigger/0/notify".into(),
        database_id: 0,
        tenant_id: TID,
        topic: "feed".into(),
        payload: "done".into(),
        metadata_floor: 0,
        position: None,
    }];
    let collections = vec!["cache".to_string(), "tasks".to_string(), FILL.to_string()];

    let mut whole = hold(plain_core);
    refusal::seed_kv(&mut whole.core);
    let mut whole_events = listen(&mut whole, 1024);
    let mut batched = hold(plain_core);
    refusal::seed_kv(&mut batched.core);
    let mut batched_events = listen(&mut batched, 1024);

    let response = install_whole(&mut whole.core, &redo, &[], &collections);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let response = install_batched_at(&mut batched, &redo, &[], &collections, 0);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert!(batched.core.redo_apply.last_held.spilled_entries > 0);

    let batched_stream = received(&mut batched_events);
    let whole_stream = received(&mut whole_events);
    assert_eq!(batched_stream.len(), whole_stream.len());
    for (index, (batched, whole)) in batched_stream.iter().zip(&whole_stream).enumerate() {
        assert_eq!(as_read(batched), as_read(whole), "event {index}");
    }
    let source_of = |row: &str| {
        batched_stream
            .iter()
            .find(|event| event.row_id.as_str() == row)
            .map(|event| event.source)
    };
    assert_eq!(
        source_of(&format!("f{}", FILL_FROM + 1)),
        Some(EventSource::Trigger),
        "a listed row carries its group's source"
    );
    assert_eq!(
        source_of(&format!("f{}", FILL_FROM + 2)),
        Some(EventSource::Deferred),
        "an unlisted document row carries the record's committed source"
    );
    assert!(
        batched_stream
            .iter()
            .any(|event| event.collection.as_ref() == "tasks"),
        "the CRDT handler's held event is sent"
    );
    assert!(
        batched_stream
            .iter()
            .any(|event| event.op == crate::event::WriteOp::Publish),
        "the publish is sent"
    );
}

// ── RAM bound ────────────────────────────────────────────────────────────

/// Hot bytes past which the install's spools spill: a small part of what
/// the record holds in all.
const SMALL_THRESHOLD: usize = 4 * 1024;
/// What one batch of [`super::equivalence_tests::BATCH_BYTES`] adds to the
/// spools between two checks, with room to spare.
const ONE_BATCH: usize = 16 * 1024;

/// 500 ops install with their held state bounded by the threshold plus one
/// batch, while the spill files hold the rest.
#[test]
fn scope_ram_stays_under_threshold() {
    const DOCS: u32 = 450;
    const KEYS: u32 = 50;
    let mut ops = fill(1, DOCS);
    for index in 0..KEYS {
        ops.push(kv_put(
            "cache",
            format!("k{index}").as_bytes(),
            1,
            20_000 + index,
        ));
    }
    let redo = record(ops);
    let collections = vec!["cache".to_string(), FILL.to_string()];

    let mut batched = hold(plain_core);
    let mut events = listen(&mut batched, 1 << 15);
    let response = install_batched_at(&mut batched, &redo, &[], &collections, SMALL_THRESHOLD);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);

    let report = batched.core.redo_apply.last_held;
    assert!(
        report.peak_hot_bytes <= SMALL_THRESHOLD + ONE_BATCH,
        "held {} hot bytes at most, past {SMALL_THRESHOLD} + {ONE_BATCH}",
        report.peak_hot_bytes
    );
    // Each document row holds its write and its note, each KV key its
    // prior image and its note: far more than the threshold in all.
    assert!(
        report.spilled_entries >= u64::from(DOCS + KEYS) * 9 / 10,
        "the spill holds the rest: {} entries",
        report.spilled_entries
    );
    let sent = received(&mut events);
    assert_eq!(
        sent.len(),
        (DOCS + KEYS) as usize,
        "every row's event is sent"
    );
}
