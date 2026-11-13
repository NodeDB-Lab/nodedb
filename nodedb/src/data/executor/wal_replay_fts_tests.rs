// SPDX-License-Identifier: BUSL-1.1

use super::*;
use nodedb_types::TenantId;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload, WalRecordArgs};
use std::sync::Arc;

const DB: u64 = 0;
const TENANT: u64 = 7;
const COLLECTION: &str = "articles";
const SURROGATE: u32 = 0x2a;
const TEXT: &str = "alpha bravo charlie bravo";

/// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
/// tests drive `replay_fts_wal` directly and never tick the event loop, so
/// the far ends are unused — they just must not be dropped.
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

/// Provenance with the local/unidentified producer sentinel, which is what
/// a non-sync write records. It deliberately bypasses the sync idempotency
/// gate, so these tests exercise the engine's OWN idempotency rather than
/// the gate's — the property replay actually depends on.
fn local_provenance() -> SyncProvenance {
    SyncProvenance::default()
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

fn index_record(lsn: u64, text: &str) -> nodedb_wal::WalRecord {
    fields_record(lsn, vec![("body".to_string(), text.to_string())])
}

fn fields_record(lsn: u64, fields: Vec<(String, String)>) -> nodedb_wal::WalRecord {
    let payload = FtsIndexPayload::new(
        local_provenance(),
        COLLECTION,
        format!("{SURROGATE:08x}"),
        fields,
    )
    .to_bytes()
    .expect("encode FtsIndexPayload");
    wal_record(RecordType::FtsIndex, lsn, payload)
}

fn delete_record(lsn: u64) -> nodedb_wal::WalRecord {
    let payload = FtsDeletePayload::new(
        local_provenance(),
        COLLECTION,
        Some(format!("{SURROGATE:08x}")),
    )
    .to_bytes()
    .expect("encode FtsDeletePayload");
    wal_record(RecordType::FtsDelete, lsn, payload)
}

fn replay(core: &mut CoreLoop, record: &nodedb_wal::WalRecord) {
    core.replay_fts_wal(
        std::slice::from_ref(record),
        1,
        &nodedb_wal::TombstoneSet::new(),
        &crate::wal::redo::replay_origin::GroupedReplayOrigins::default(),
    );
}

/// Everything the Origin FTS reads: the corpus counters and the length of
/// every posting list the indexed text produces. Postings catch a duplicate
/// entry; the counters catch a double-counted document or token sum, which
/// a keyed overwrite of the posting alone would NOT prevent.
fn fts_state(core: &CoreLoop) -> ((u32, f32), Vec<(String, u32)>) {
    let tid = TenantId::new(TENANT);
    let stats = core
        .inverted
        .corpus_stats(DB, tid, COLLECTION)
        .expect("corpus stats");
    // Ask the analyzer for the canonical terms rather than hardcoding
    // stems, so the assertion follows the collection's analyzer config.
    let mut terms = core
        .inverted
        .analyze_for_collection(DB, tid, COLLECTION, TEXT)
        .expect("analyze");
    terms.sort();
    terms.dedup();
    let dfs = terms
        .into_iter()
        .map(|term| {
            let df = core
                .inverted
                .term_df(DB, tid, COLLECTION, &term)
                .expect("term df");
            (term, df)
        })
        .collect();
    (stats, dfs)
}

/// The property FTS replay relies on instead of a floor: applying the SAME
/// `FtsIndex` record a second time must leave the index bit-for-bit
/// equivalent — no duplicated posting, no second document counted, no
/// inflated token sum (which would skew avgdl and therefore every BM25
/// score in the collection).
#[test]
fn replaying_an_index_record_twice_leaves_the_index_unchanged() {
    let mut h = make_core();
    let record = index_record(10, TEXT);

    replay(&mut h.core, &record);
    let after_first = fts_state(&h.core);
    assert_eq!(after_first.0.0, 1, "one document indexed");
    assert!(
        after_first.1.iter().all(|(_, df)| *df == 1),
        "each term must list the document exactly once: {:?}",
        after_first.1
    );

    replay(&mut h.core, &record);
    assert_eq!(
        fts_state(&h.core),
        after_first,
        "re-applying a durable FtsIndex record must be a no-op"
    );
}

/// A record whose text genuinely changed is not a replay, and must still
/// take effect — the idempotency above must come from comparing state, not
/// from ignoring repeat writes to a surrogate.
#[test]
fn a_changed_index_record_still_takes_effect() {
    let mut h = make_core();
    replay(&mut h.core, &index_record(10, TEXT));
    let ((count, avg_len), _) = fts_state(&h.core);
    assert_eq!(count, 1);

    replay(&mut h.core, &index_record(11, "alpha"));
    let ((count_after, avg_after), _) = fts_state(&h.core);
    assert_eq!(count_after, 1, "still one document, not two");
    assert!(
        avg_after < avg_len,
        "the shorter text must shrink the average document length"
    );
}

/// The delete arm carries the same obligation: a repeated `FtsDelete`
/// must not decrement the corpus counters a second time (which would
/// underflow the document count toward zero while other documents remain).
#[test]
fn replaying_a_delete_record_twice_leaves_the_index_unchanged() {
    let mut h = make_core();
    replay(&mut h.core, &index_record(10, TEXT));

    let record = delete_record(11);
    replay(&mut h.core, &record);
    let after_first = fts_state(&h.core);
    assert_eq!(after_first.0.0, 0, "the only document is gone");
    assert!(
        after_first.1.iter().all(|(_, df)| *df == 0),
        "no term may still list the deleted document: {:?}",
        after_first.1
    );

    replay(&mut h.core, &record);
    assert_eq!(
        fts_state(&h.core),
        after_first,
        "re-applying a durable FtsDelete record must be a no-op"
    );
}

/// An `FtsIndex` record with several fields replays into each field's
/// index and into the whole-document index.
#[test]
fn an_index_record_replays_into_its_field_scopes() {
    let mut h = make_core();
    replay(
        &mut h.core,
        &fields_record(
            10,
            vec![
                ("title".to_string(), "alpha".to_string()),
                ("body".to_string(), "bravo".to_string()),
            ],
        ),
    );
    let tid = TenantId::new(TENANT);
    let title = nodedb_fts::IndexScope::field(COLLECTION, "title").expect("field scope");
    let body = nodedb_fts::IndexScope::field(COLLECTION, "body").expect("field scope");
    let df = |index: nodedb_fts::IndexScope<'_>, word: &str| {
        let term = h
            .core
            .inverted
            .analyze_for_collection(DB, tid, COLLECTION, word)
            .expect("analyze")
            .remove(0);
        h.core
            .inverted
            .term_df(DB, tid, index, &term)
            .expect("term df")
    };
    assert_eq!(df(title, "alpha"), 1);
    assert_eq!(df(title, "bravo"), 0);
    assert_eq!(df(body, "bravo"), 1);
    assert_eq!(df(body, "alpha"), 0);
    assert_eq!(df(COLLECTION.into(), "alpha"), 1);
    assert_eq!(df(COLLECTION.into(), "bravo"), 1);
}
