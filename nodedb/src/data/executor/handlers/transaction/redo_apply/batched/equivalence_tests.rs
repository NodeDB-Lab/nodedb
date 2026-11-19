// SPDX-License-Identifier: BUSL-1.1

//! The batched install of a stream marker leaves the state the whole-record
//! install leaves, and the state restart replay rebuilds from the marker.
//!
//! Each case resolves one record, then installs it on two cores set up
//! alike: whole on one, and batched at a bound that puts a few ops in each
//! batch on the other. Filler rows before and after the engine's own ops
//! spread the record over several batches. What each core reads back must
//! match.

use nodedb_physical::physical_plan::{PhysicalPlan, RedoOrigin, RedoSumTargets};
use nodedb_types::{DatabaseId, StorageKey, Surrogate, TenantId};
use nodedb_wal::record::{RecordType, WalRecordArgs};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_default_task;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::engine::document::store::CollectionConfig;
use crate::types::{Lsn, TxnId};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::super::entry::CommittedRedo;
use super::super::install_refusal_tests as refusal;
use super::super::stream_validate::tests::{
    A1, A2, BALANCES, CHARGES, Hold, LSN, TID, charge, default_task, fold_core, fold_targets,
    framed, hold, plain_core, record,
};
use super::super::stream_validate::{StreamLimits, StreamValidateInput};
use super::super::test_commit::{doc_put_sub_record, failing_install_sub_record};
use super::StreamInstallInput;

/// A batch bound that puts a few ops in each batch.
pub(super) const BATCH_BYTES: usize = 256;
pub(super) const FILL: &str = "fill";

/// Install `redo` whole on `core`, folding into `sum_targets`.
pub(super) fn install_whole(
    core: &mut CoreLoop,
    redo: &RedoRecord,
    sum_targets: &[RedoSumTargets],
    collections: &[String],
) -> Response {
    let bytes = redo.to_bytes().expect("encode redo");
    core.execute_apply_transaction_redo(
        &default_task(),
        TID,
        CommittedRedo {
            redo: &bytes,
            collections,
            sum_targets,
        },
    )
}

/// Write `redo` as a stream into the held core's WAL, then validate and
/// install it batch by batch at [`BATCH_BYTES`].
fn install_batched(
    held: &mut Hold,
    redo: &RedoRecord,
    sum_targets: &[RedoSumTargets],
    collections: &[String],
) -> Response {
    install_batched_at(held, redo, sum_targets, collections, usize::MAX)
}

/// [`install_batched`] with spools that spill past `spill_threshold` hot
/// bytes. A record that folds installs under a root journal, as the funnel
/// installs every committed redo.
pub(super) fn install_batched_at(
    held: &mut Hold,
    redo: &RedoRecord,
    sum_targets: &[RedoSumTargets],
    collections: &[String],
    spill_threshold: usize,
) -> Response {
    if !sum_targets.is_empty() {
        let (task, mut response) =
            install_journalled(held, redo, sum_targets, collections, spill_threshold);
        held.core
            .finish_journalled_task(&task, &mut response)
            .expect("finish journal");
        return response;
    }
    let reference = held.wal.write(&framed(redo, sum_targets, &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = default_task();
    let limits = StreamLimits {
        batch_bytes: BATCH_BYTES,
        spill_threshold,
    };
    let lsn = Lsn::new(LSN);
    let validated = held.core.validate_redo_stream(&StreamValidateInput {
        task: &task,
        tid: TID,
        marker: &marker,
        reference: &reference,
        origin: RedoOrigin::Commit,
        snapshot: None,
        judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
        lsn,
        limits,
    });
    let validated = match validated {
        Ok(validated) => validated,
        Err(code) => return held.core.response_error(&task, code),
    };
    held.core.install_redo_stream(
        StreamInstallInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            lsn,
            limits,
            permit: None,
            collections,
        },
        validated,
        &mut Vec::new(),
    )
}

/// Document puts into [`FILL`], one row each, from surrogate `from`.
pub(super) fn fill(from: u32, count: u32) -> Vec<RedoSubRecord> {
    (from..from + count)
        .map(|surrogate| {
            let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": surrogate}))
                .expect("encode body");
            doc_put_sub_record(FILL, &format!("f{surrogate}"), &body, surrogate)
        })
        .collect()
}

pub(super) const FILL_FROM: u32 = 9000;
pub(super) const FILL_EACH: u32 = 6;

/// `ops` between two runs of filler rows.
pub(super) fn spread(ops: Vec<RedoSubRecord>) -> Vec<RedoSubRecord> {
    let mut spread = fill(FILL_FROM, FILL_EACH);
    spread.extend(ops);
    spread.extend(fill(FILL_FROM + FILL_EACH, FILL_EACH));
    spread
}

/// What one core holds of every engine the cases write.
#[derive(Debug, PartialEq)]
pub(super) struct Digest {
    documents: Vec<Option<Vec<u8>>>,
    kv: Option<Vec<u8>>,
    vector_primary: Option<usize>,
    crdt: Option<String>,
    spatial: (usize, usize),
    array_empty: Option<bool>,
    text: (Option<u32>, Option<u32>),
}

fn document(core: &CoreLoop, collection: &str, surrogate: u32) -> Option<Vec<u8>> {
    core.sparse
        .get(
            0,
            TID,
            collection,
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read row")
}

fn spatial_entries(core: &CoreLoop, collection: &str, field: &str) -> usize {
    core.spatial_indexes
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
            field.to_string(),
        ))
        .map_or(0, |rtree| rtree.len())
}

pub(super) fn digest(core: &CoreLoop) -> Digest {
    let mut documents = vec![document(core, "notes", 41), document(core, "articles", 101)];
    for surrogate in FILL_FROM..FILL_FROM + 2 * FILL_EACH {
        documents.push(document(core, FILL, surrogate));
    }
    let text = |collection: &str| {
        core.inverted
            .corpus_stats(0, TenantId::new(TID), collection)
            .ok()
            .map(|stats| stats.0)
    };
    Digest {
        documents,
        kv: core
            .kv_engine
            .get(0, TID, "cache", b"k", crate::engine::kv::current_ms()),
        vector_primary: core
            .vector_collections
            .get(&CoreLoop::vector_index_key(0, TID, "vp", "vec"))
            .map(|collection| collection.len()),
        crdt: core
            .crdt_engines
            .get(&(DatabaseId::DEFAULT, TenantId::new(TID)))
            .and_then(|engine| engine.read_row("tasks", "t1"))
            .map(|row| format!("{row:?}")),
        spatial: (
            spatial_entries(core, "places", "geom"),
            spatial_entries(core, "articles", "location"),
        ),
        array_empty: core
            .array_engine
            .store(&nodedb_array::types::ArrayId::new(
                TenantId::new(TID),
                "grid",
            ))
            .ok()
            .map(|store| store.memtable.is_empty()),
        text: (text("docs"), text("articles")),
    }
}

/// The batched install left no fold ledger or target row of the record.
fn assert_no_install_rows(core: &CoreLoop) {
    assert!(
        core.sparse
            .load_fold_ledgers()
            .expect("read ledger")
            .is_empty(),
        "the fold ledger rows are gone"
    );
    assert!(
        core.stored_install_targets(LSN)
            .expect("read targets")
            .is_empty(),
        "the fold target rows are gone"
    );
}

/// Resolve `plans` on a core set up by `setup`, into the record they commit.
pub(super) fn resolved(plans: &[PhysicalPlan], setup: fn(&mut CoreLoop)) -> RedoRecord {
    let mut scratch = hold(plain_core);
    setup(&mut scratch.core);
    let bytes = match scratch
        .core
        .stage_and_resolve_for_test(&make_default_task(), TID, plans, LSN)
    {
        Ok(bytes) => bytes,
        Err(response) => panic!("resolve: {:?}", response.error_code),
    };
    scratch
        .core
        .drop_overlay_entry(TxnId::new(LSN))
        .expect("drop overlay");
    RedoRecord::from_bytes(&bytes).expect("decode resolved redo")
}

fn error_text(response: &Response) -> String {
    format!("{:?}", response.error_code)
}

/// Install `redo` whole and batched on two cores `setup` prepared, and
/// check both answer alike and hold the same state.
fn assert_installs_alike(
    redo: &RedoRecord,
    sum_targets: &[RedoSumTargets],
    collections: &[String],
    setup: fn(&mut CoreLoop),
    case: &str,
) {
    let mut whole = hold(plain_core);
    setup(&mut whole.core);
    let mut batched = hold(plain_core);
    setup(&mut batched.core);
    let before = digest(&batched.core);

    let whole_response = install_whole(&mut whole.core, redo, sum_targets, collections);
    let batched_response = install_batched(&mut batched, redo, sum_targets, collections);

    assert_eq!(
        batched_response.status,
        whole_response.status,
        "{case}: {}",
        error_text(&batched_response)
    );
    assert_eq!(
        error_text(&batched_response),
        error_text(&whole_response),
        "{case}"
    );
    assert_eq!(digest(&batched.core), digest(&whole.core), "{case}");
    if whole_response.status != Status::Ok {
        assert_eq!(
            digest(&batched.core),
            before,
            "{case}: the refused install leaves the state as before"
        );
    }
    assert_no_install_rows(&batched.core);
}

#[test]
fn every_engine_record_installs_alike_batched_and_whole() {
    type Setup = fn(&mut CoreLoop);
    fn none(_: &mut CoreLoop) {}
    let cases: [(&str, PhysicalPlan, Setup); 7] = [
        ("document", refusal::document_insert(), none),
        ("kv", refusal::kv_predicate_delete(), refusal::seed_kv),
        ("vector", refusal::vector_direct_insert(), none),
        ("crdt", refusal::crdt_upsert(), none),
        ("spatial", refusal::spatial_insert(), none),
        ("array", refusal::array_put(), refusal::open_array),
        (
            "indexed",
            refusal::indexed_document_put(),
            refusal::declare_text,
        ),
    ];
    for (case, plan, setup) in cases {
        let plans = std::slice::from_ref(&plan);
        let mut redo = resolved(plans, setup);
        redo.ops = spread(std::mem::take(&mut redo.ops));
        let mut collections = nodedb_physical::physical_plan::written_collections(plans);
        collections.push(FILL.to_string());
        assert_installs_alike(&redo, &redo_sum_targets(plans), &collections, setup, case);
    }
    let text = record(spread(vec![refusal::fts_index_sub_record()]));
    assert_installs_alike(
        &text,
        &[],
        &["docs".to_string(), FILL.to_string()],
        refusal::declare_text,
        "text",
    );
}

/// A sub-record of the record's last batch fails while it installs. The
/// batched install refuses with the error the whole-record install refuses
/// with, and every batch before it rolls back.
#[test]
fn install_failure_refuses_with_the_whole_record_error() {
    fn none(_: &mut CoreLoop) {}
    let plans = [refusal::document_insert()];
    let mut redo = resolved(&plans, none);
    let mut ops = spread(std::mem::take(&mut redo.ops));
    ops.push(failing_install_sub_record());
    redo.ops = ops;
    let collections = vec!["notes".to_string(), FILL.to_string()];

    let mut probe = hold(plain_core);
    let response = install_batched(&mut probe, &redo, &[], &collections);
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "{:?}",
        response.error_code
    );
    assert!(
        probe
            .core
            .sparse
            .load_fold_ledgers()
            .expect("ledger")
            .is_empty(),
        "the rollback removes the fold ledger rows"
    );
    assert_installs_alike(&redo, &[], &collections, none, "late failure");
}

/// Folds read the target balance the earlier batches left, and the
/// response names the same target rows.
#[test]
fn folds_install_alike_across_batches() {
    let ops: Vec<RedoSubRecord> = (1..=12)
        .map(|surrogate| charge(surrogate, if surrogate % 3 == 0 { "a2" } else { "a1" }, 1))
        .collect();
    let redo = record(ops);
    let targets = fold_targets();
    let collections = vec![CHARGES.to_string(), BALANCES.to_string()];

    let mut whole = hold(fold_core);
    let mut batched = hold(fold_core);
    let whole_response = install_whole(&mut whole.core, &redo, &targets, &collections);
    let batched_response = install_batched(&mut batched, &redo, &targets, &collections);
    assert_eq!(
        whole_response.status,
        Status::Ok,
        "{}",
        error_text(&whole_response)
    );
    assert_eq!(
        batched_response.status,
        Status::Ok,
        "{}",
        error_text(&batched_response)
    );
    assert_eq!(
        batched_response.write_set.len(),
        whole_response.write_set.len()
    );
    for account in [A1, A2] {
        assert_eq!(
            document(&batched.core, BALANCES, account.as_u32()),
            document(&whole.core, BALANCES, account.as_u32()),
        );
    }
    for surrogate in 1..=12 {
        assert_eq!(
            document(&batched.core, CHARGES, surrogate),
            document(&whole.core, CHARGES, surrogate),
        );
    }
    assert_no_install_rows(&batched.core);
}

// ── Restart replay ───────────────────────────────────────────────────────

const CHAINED: &str = "chained";
const HISTORY: &str = "history";

/// A core where `chained` links a hash chain and `history` keeps every
/// version.
fn versioned_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, ends) = plain_core(dir);
    let key = |name: &str| (DatabaseId::DEFAULT, TenantId::new(TID), name.to_string());
    let mut chained = CollectionConfig::new(CHAINED);
    chained.enforcement.append_only = true;
    chained.enforcement.hash_chain = true;
    let mut history = CollectionConfig::new(HISTORY);
    history.bitemporal = true;
    core.seed_doc_configs(&[(key(CHAINED), chained), (key(HISTORY), history)]);
    (core, ends)
}

fn versioned_rows(core: &CoreLoop, surrogates: &[u32]) -> Vec<(Option<Vec<u8>>, Option<i64>)> {
    let mut rows = Vec::new();
    for collection in [CHAINED, HISTORY] {
        for surrogate in surrogates {
            let key = StorageKey::for_surrogate(Surrogate::new(*surrogate));
            let latest = core
                .sparse
                .versioned_latest_sys_from(0, TID, collection, &key)
                .expect("read version");
            rows.push((document(core, collection, *surrogate), latest));
        }
    }
    rows
}

fn chain_head(core: &CoreLoop) -> Option<String> {
    core.chain
        .heads
        .get(&(DatabaseId::DEFAULT, TenantId::new(TID), CHAINED.to_string()))
        .map(|head| head.hash.clone())
}

/// The batched install of chained and versioned rows leaves what restart
/// replay of the same marker rebuilds on a core that never installed it.
#[test]
fn batched_install_matches_restart_replay() {
    let surrogates: Vec<u32> = (1..=10).collect();
    let mut ops = Vec::new();
    for surrogate in &surrogates {
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({"amount": surrogate}))
            .expect("encode body");
        ops.push(doc_put_sub_record(
            CHAINED,
            &format!("c{surrogate}"),
            &body,
            *surrogate,
        ));
        ops.push(doc_put_sub_record(
            HISTORY,
            &format!("h{surrogate}"),
            &body,
            *surrogate,
        ));
    }
    let redo = record(ops);
    let collections = vec![CHAINED.to_string(), HISTORY.to_string()];

    let mut installed = hold(versioned_core);
    let response = install_batched(&mut installed, &redo, &[], &collections);
    assert_eq!(response.status, Status::Ok, "{}", error_text(&response));
    assert!(
        chain_head(&installed.core).is_some(),
        "the rows link a chain"
    );

    let mut replayed = hold(versioned_core);
    let reference = replayed.wal.write(&framed(&redo, &[], &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let marker_record = nodedb_wal::WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: LSN,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DatabaseId::DEFAULT.as_u64(),
        payload: marker.to_bytes().expect("encode marker"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");
    replayed
        .core
        .replay_transaction_redo_wal(
            std::slice::from_ref(&marker_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("restart replay");

    assert_eq!(
        versioned_rows(&installed.core, &surrogates),
        versioned_rows(&replayed.core, &surrogates)
    );
    assert_eq!(chain_head(&installed.core), chain_head(&replayed.core));
}

// ── Vector index across batches ──────────────────────────────────────────

const VECTOR_DOCS: &str = "vdocs";

fn vector_core(core: &mut CoreLoop) {
    core.seed_vector_index_params(&[nodedb_types::StoredVectorIndexParams {
        database_id: 0,
        tenant_id: TID,
        collection: VECTOR_DOCS.to_string(),
        field_name: "emb".to_string(),
        dim: 2,
        metric: "l2".to_string(),
        m: 16,
        ef_construction: 200,
        index_type: "hnsw".to_string(),
        pq_m: 0,
        ivf_cells: 0,
        ivf_nprobe: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    }]);
}

fn vector_len(core: &CoreLoop) -> Option<usize> {
    core.vector_collections
        .get(&CoreLoop::vector_index_key(0, TID, VECTOR_DOCS, "emb"))
        .map(|collection| collection.len())
}

/// The index parameters register before the record. Document rows spread
/// over many batches re-index their vectors as the document arm reaches
/// them, beside a vector-primary write the vector arms apply first.
#[test]
fn a_vector_param_and_document_reindex_cross_batches_alike() {
    let mut ops = resolved(&[refusal::vector_direct_insert()], |_| {}).ops;
    for surrogate in 1..=16u32 {
        let x = surrogate as f32;
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({"emb": [x, x + 1.0]}))
            .expect("encode body");
        ops.push(doc_put_sub_record(
            VECTOR_DOCS,
            &format!("v{surrogate}"),
            &body,
            surrogate,
        ));
    }
    let redo = record(ops);
    let collections = vec!["vp".to_string(), VECTOR_DOCS.to_string()];

    let mut whole = hold(plain_core);
    vector_core(&mut whole.core);
    let mut batched = hold(plain_core);
    vector_core(&mut batched.core);
    let whole_response = install_whole(&mut whole.core, &redo, &[], &collections);
    let batched_response = install_batched(&mut batched, &redo, &[], &collections);
    assert_eq!(
        whole_response.status,
        Status::Ok,
        "{}",
        error_text(&whole_response)
    );
    assert_eq!(
        batched_response.status,
        Status::Ok,
        "{}",
        error_text(&batched_response)
    );
    assert_eq!(vector_len(&batched.core), vector_len(&whole.core));
    assert_eq!(digest(&batched.core), digest(&whole.core));
    for surrogate in 1..=16u32 {
        assert_eq!(
            document(&batched.core, VECTOR_DOCS, surrogate),
            document(&whole.core, VECTOR_DOCS, surrogate)
        );
    }
}

// ── Timeseries pre-record flush ──────────────────────────────────────────

pub(super) const METRICS: &str = "metrics_tag";

/// One timeseries ingest of `line` into [`METRICS`].
pub(super) fn ts_line(line: &str) -> RedoSubRecord {
    let lines = zerompk::to_msgpack_vec(&vec![line.to_string()]).expect("encode lines");
    RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
            crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                collection: METRICS,
                payload: &lines,
                provenance: None,
                format: "ilp-msgpack",
                default_timestamp_ms: 0,
            },
        )
        .expect("encode ingest"),
    }
}

/// A core whose [`METRICS`] memtable holds one row and a full tag
/// dictionary: each symbol column takes one value.
fn tag_pressed(core: &mut CoreLoop) {
    core.ts_tuning.max_tag_cardinality = 1;
    let seed = record(vec![ts_line("metrics_tag,host=h0 value=1i 1000000000")])
        .to_bytes()
        .expect("encode seed");
    let mut task = default_task();
    task.wal_lsn = Some(Lsn::new(5));
    let response = core.execute_apply_transaction_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &seed,
            collections: &[METRICS.to_string()],
            sum_targets: &[],
        },
    );
    assert_eq!(response.status, Status::Ok, "{}", error_text(&response));
}

/// The partitions' row counts, sorted, and the memtable's rows.
pub(super) fn ts_layout(core: &CoreLoop) -> (Vec<u64>, u64) {
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), METRICS.to_string());
    let mut partitions: Vec<u64> = core
        .ts_registries
        .get(&key)
        .map(|registry| {
            registry
                .iter()
                .map(|(_, entry)| entry.meta.row_count)
                .collect()
        })
        .unwrap_or_default();
    partitions.sort_unstable();
    let memtable = core
        .columnar_memtables
        .get(&key)
        .map_or(0, |mt| mt.row_count());
    (partitions, memtable)
}

/// The record's new tag value has no room in the seeded dictionary. Both
/// installs flush the memtable once before the record's first timeseries
/// row, so the record lands whole in a fresh generation and no core stops.
#[test]
fn a_tag_pressed_memtable_flushes_before_a_batched_record() {
    let mut ops = fill(FILL_FROM, FILL_EACH);
    for (index, value) in (2..6).enumerate() {
        ops.push(ts_line(&format!(
            "metrics_tag,host=h1 value={value}i {}",
            (index as u64 + 2) * 1_000_000_000
        )));
        ops.extend(fill(FILL_FROM + FILL_EACH + index as u32, 1));
    }
    let redo = record(ops);
    let collections = vec![METRICS.to_string(), FILL.to_string()];

    let mut whole = hold(plain_core);
    tag_pressed(&mut whole.core);
    let mut batched = hold(plain_core);
    tag_pressed(&mut batched.core);
    let whole_response = install_whole(&mut whole.core, &redo, &[], &collections);
    let batched_response = install_batched(&mut batched, &redo, &[], &collections);

    assert_eq!(
        whole_response.status,
        Status::Ok,
        "{}",
        error_text(&whole_response)
    );
    assert_eq!(
        batched_response.status,
        Status::Ok,
        "{}",
        error_text(&batched_response)
    );
    assert!(
        !batched.core.fail_stop.is_stopped(),
        "the core keeps serving"
    );
    assert_eq!(
        ts_layout(&whole.core),
        (vec![1], 4),
        "the seed flushed alone, and the record sits whole in the memtable"
    );
    assert_eq!(ts_layout(&batched.core), ts_layout(&whole.core));
    assert_eq!(digest(&batched.core), digest(&whole.core));
}

// ── Root journal ─────────────────────────────────────────────────────────

/// A task that installs the stream marker of `redo` under a root journal,
/// as the funnel dispatches a grouped commit.
fn journalled_task(
    marker: &RedoRecord,
    collections: &[String],
) -> crate::data::executor::task::ExecutionTask {
    let mut request = make_default_task().request;
    request.plan = PhysicalPlan::Meta(
        nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo {
            redo: marker.to_bytes().expect("encode marker"),
            collections: collections.to_vec(),
            sum_targets: Vec::new(),
            origin: RedoOrigin::Commit,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: None,
            pins: Default::default(),
        },
    );
    request.wal_lsn = Some(Lsn::new(LSN));
    crate::data::executor::task::ExecutionTask::new(request)
}

/// Install the stream marker of `redo`, folding into `targets`, under a
/// root journal as the funnel dispatches a grouped commit. Returns the task
/// and the install's answer, before the journal ends.
fn install_journalled(
    held: &mut Hold,
    redo: &RedoRecord,
    targets: &[RedoSumTargets],
    collections: &[String],
    spill_threshold: usize,
) -> (crate::data::executor::task::ExecutionTask, Response) {
    let reference = held.wal.write(&framed(redo, targets, &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = journalled_task(&marker, collections);
    let group = crate::bridge::dispatch::JournalGroup {
        origin: Lsn::new(LSN),
        collection: CHARGES.into(),
        apply_key: 0,
        commit_hlc: None,
        change_position: None,
    };
    let permit = held
        .core
        .begin_task_journal(&task, group)
        .expect("root permit");
    let limits = StreamLimits {
        batch_bytes: BATCH_BYTES,
        spill_threshold,
    };
    let lsn = Lsn::new(LSN);
    let validated = held.core.validate_redo_stream(&StreamValidateInput {
        task: &task,
        tid: TID,
        marker: &marker,
        reference: &reference,
        origin: RedoOrigin::Commit,
        snapshot: None,
        judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
        lsn,
        limits,
    });
    let validated = match validated {
        Ok(validated) => validated,
        Err(code) => {
            let response = held.core.response_error(&task, code);
            return (task, response);
        }
    };
    let response = held.core.install_redo_stream(
        StreamInstallInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            lsn,
            limits,
            permit: Some(permit),
            collections,
        },
        validated,
        &mut Vec::new(),
    );
    (task, response)
}

/// Under a root journal the batched install writes the fold ledger with
/// each row, and each batch makes it durable. The commit that stores the
/// write set removes it, and a refused install's rollback removes it.
#[test]
fn a_journalled_batched_install_drops_its_ledger_rows() {
    for refuse in [false, true] {
        let mut ops: Vec<RedoSubRecord> = (1..=12)
            .map(|surrogate| charge(surrogate, "a1", 1))
            .collect();
        if refuse {
            ops.push(failing_install_sub_record());
        }
        let redo = record(ops);
        let collections = vec![CHARGES.to_string(), BALANCES.to_string()];
        let mut held = hold(fold_core);
        let (task, mut response) =
            install_journalled(&mut held, &redo, &fold_targets(), &collections, usize::MAX);
        if refuse {
            assert_eq!(response.status, Status::Error, "refuse {refuse}");
        } else {
            assert_eq!(response.status, Status::Ok, "{}", error_text(&response));
            assert!(
                !response.write_set.is_empty(),
                "the folds name their targets"
            );
        }
        held.core
            .finish_journalled_task(&task, &mut response)
            .expect("finish journal");
        assert_no_install_rows(&held.core);
    }
    // A cut after a durable batch finds the batch's ledger and target rows
    // on disk: the rows above are not removed for want of being written.
    #[cfg(feature = "failpoints")]
    {
        use nodedb_types::fail_point::{FailAction, FailGuard};

        let ops: Vec<RedoSubRecord> = (1..=12)
            .map(|surrogate| charge(surrogate, "a1", 1))
            .collect();
        let redo = record(ops);
        let collections = vec![CHARGES.to_string(), BALANCES.to_string()];
        let mut held = hold(fold_core);
        let dir = held
            .core
            .wal_source()
            .dir
            .parent()
            .expect("the data directory")
            .to_path_buf();
        let stores = {
            let _cut = FailGuard::install(
                "redo_apply::batched::after_batch_durable::Document::1",
                FailAction::Panic,
            );
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                install_journalled(&mut held, &redo, &fold_targets(), &collections, usize::MAX)
            }));
            assert!(ran.is_err(), "the install stops after its second batch");
            super::super::replay_fold::tests::crash_image(&dir)
        };
        let (ledgers, targets) = super::super::replay_fold::tests::install_rows(stores.path(), LSN);
        assert!(
            matches!(ledgers.as_slice(), [(lsn, folded)] if *lsn == LSN && *folded > 0),
            "the durable batches' fold ledger is on disk: {ledgers:?}"
        );
        assert!(targets > 0, "the durable batches' target rows are on disk");
    }
}
