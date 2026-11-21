// SPDX-License-Identifier: BUSL-1.1

//! A pinned record is judged under its pins on the whole-record path and
//! the stream path, whatever config the core holds.
//!
//! The `codes` collection holds rows 1 `A` and 2 `B`. The record puts row
//! 3 `A`, which only a UNIQUE index on `code` refuses.

use std::sync::Arc;

use nodedb_physical::physical_plan::{
    ConfigPin, ConfigPinKey, CrsPin, DocConfigWire, DocIndexFence, DocIndexOwner,
    EnforcementOptions, RedoOrigin, RedoPins, RedoSumTargets, RegisteredIndex,
    RegisteredIndexState, ResolvedSumTarget, StorageMode,
};
use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::server::dispatch_utils::error_is_final_refusal;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::data::executor::handlers::document::index_maintenance::BackfillIndexParams;
use crate::engine::document::store::CollectionConfig;
use crate::types::Lsn;
use crate::wal::RedoSubRecord;

use super::stream_validate::tests::{
    BALANCES, CHARGES, Hold, LSN, TID, charge, default_task, fold_core, fold_targets,
    framed_pinned, hold, plain_core, record,
};
use super::stream_validate::{StreamLimits, StreamValidateInput};
use super::unique_handover_tests::{COLL, install, put, seeded};
use super::whole_validate::WholeValidateInput;

/// A batch bound that puts about one record in each batch.
const SMALL_BATCH_BYTES: usize = 64;

fn seeded_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (core, req, resp) = seeded(dir);
    (core, Box::new((req, resp)))
}

pub(super) fn config_key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string())
}

/// The wire form of `codes`, with a UNIQUE index on `code` when `unique`.
pub(super) fn wire(unique: bool) -> DocConfigWire {
    let index_paths = if unique {
        vec![RegisteredIndex {
            name: "code".into(),
            path: "code".into(),
            unique: true,
            case_insensitive: false,
            state: RegisteredIndexState::Ready,
            predicate: None,
        }]
    } else {
        Vec::new()
    };
    DocConfigWire {
        bitemporal: false,
        crdt_enabled: false,
        enforcement: EnforcementOptions::default(),
        storage_mode: StorageMode::Schemaless,
        index_paths,
        declared_key: None,
        vector_primary: None,
        declared_columns: Vec::new(),
        vector_fields: Vec::new(),
    }
}

pub(super) fn pin_of(collection: &str, wire: &DocConfigWire) -> ConfigPin {
    let (bytes, digest) = wire.pin_parts().expect("pin parts");
    ConfigPin {
        key: ConfigPinKey {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TID),
            collection: collection.into(),
        },
        digest,
        wire: bytes.into(),
    }
}

/// The pin of the empty CRS set every test core holds.
fn crs_pin() -> CrsPin {
    let bytes = zerompk::to_msgpack_vec(&nodedb_crs::CustomCrsSet::new()).expect("encode set");
    CrsPin {
        digest: nodedb_physical::physical_plan::pin_digest(&bytes),
        bytes: bytes.into(),
    }
}

/// Pins as a proposer sets them: `configs` and the CRS set.
pub(super) fn pins(configs: Vec<ConfigPin>) -> RedoPins {
    RedoPins {
        configs: configs.into_iter().map(Arc::new).collect(),
        crs: Some(Arc::new(crs_pin())),
    }
}

/// The pins of a record whose proposer saw `codes` with or without UNIQUE.
pub(super) fn codes_pins(unique: bool) -> RedoPins {
    pins(vec![pin_of(COLL, &wire(unique))])
}

/// Give `codes` on `core` a current config with no UNIQUE index.
pub(super) fn drop_current_unique(core: &mut CoreLoop) {
    core.doc_configs
        .insert(config_key(), CollectionConfig::new(COLL));
}

/// The whole-record validate of `ops` under `pins`.
fn whole(core: &mut CoreLoop, pins: &RedoPins, ops: Vec<RedoSubRecord>) -> Result<(), ErrorCode> {
    whole_with(core, pins, ops, &[])
}

fn whole_with(
    core: &mut CoreLoop,
    pins: &RedoPins,
    ops: Vec<RedoSubRecord>,
    sum_targets: &[RedoSumTargets],
) -> Result<(), ErrorCode> {
    let mut redo = record(ops);
    redo.pins = pins.clone();
    let bytes = redo.to_bytes().expect("encode redo");
    let task = default_task();
    core.validate_whole_redo(&WholeValidateInput {
        task: &task,
        tid: TID,
        lsn: Lsn::new(LSN),
        redo: &redo,
        redo_bytes: &bytes,
        sum_targets,
        pins,
        origin: RedoOrigin::Commit,
        snapshot: None,
        judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
    })
    .map(drop)
}

/// The streamed validate of `ops` under `pins`, at `spill_threshold`.
fn streamed(
    held: &mut Hold,
    pins: &RedoPins,
    ops: Vec<RedoSubRecord>,
    sum_targets: &[RedoSumTargets],
    spill_threshold: usize,
) -> Result<(), ErrorCode> {
    let reference = held
        .wal
        .write(&framed_pinned(&record(ops), sum_targets, &[], pins));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = default_task();
    held.core
        .validate_redo_stream(&StreamValidateInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            snapshot: None,
            judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
            lsn: Lsn::new(LSN),
            limits: StreamLimits {
                batch_bytes: SMALL_BATCH_BYTES,
                spill_threshold,
            },
        })
        .map(drop)
}

fn is_unique_violation(verdict: &Result<(), ErrorCode>) -> bool {
    matches!(
        verdict,
        Err(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
    )
}

/// Every path's verdict on the record that puts row 3 `A` under `pins`,
/// on a seeded core that `prepare` changes first.
fn verdicts(pins: &RedoPins, prepare: fn(&mut CoreLoop)) -> Vec<Result<(), ErrorCode>> {
    let mut held = hold(seeded_core);
    prepare(&mut held.core);
    let mut verdicts = vec![whole(&mut held.core, pins, vec![put(3, "A")])];
    for spill in [0, usize::MAX] {
        verdicts.push(streamed(&mut held, pins, vec![put(3, "A")], &[], spill));
    }
    verdicts
}

fn keep(_: &mut CoreLoop) {}

/// The core holds a UNIQUE index the proposer had not seen. The record
/// passes on every path.
#[test]
fn a_newer_unique_on_the_core_passes_the_pinned_record() {
    for verdict in verdicts(&codes_pins(false), keep) {
        assert!(verdict.is_ok(), "{verdict:?}");
    }
}

/// The proposer saw a UNIQUE index the core's current config lacks, and
/// the core maintains its path as unique. The record refuses on every
/// path.
#[test]
fn a_pinned_unique_the_core_lacks_refuses() {
    for verdict in verdicts(&codes_pins(true), drop_current_unique) {
        assert!(is_unique_violation(&verdict), "{verdict:?}");
    }
}

/// The fence of descriptor version `version` of the first incarnation.
fn fence(version: u64) -> DocIndexFence {
    DocIndexFence {
        incarnation: nodedb_types::Hlc::new(1, 0),
        descriptor_version: version,
    }
}

/// The owner the `code` index builds and retires as.
fn code_owner() -> DocIndexOwner {
    DocIndexOwner::Named("code".into())
}

/// A build of the UNIQUE `code` index on `core`, judging the stored rows.
fn build_unique_code(core: &mut CoreLoop) -> crate::Result<()> {
    core.backfill_document_index(
        DatabaseId::DEFAULT,
        BackfillIndexParams {
            tid: TID,
            collection: COLL,
            path: "code",
            is_array: false,
            unique: true,
            judge_unique: true,
            case_insensitive: false,
            predicate: None,
            owner: &code_owner(),
            fence: fence(1),
        },
    )
}

/// Build the UNIQUE `code` index, then retire it, as a DROP INDEX does on
/// the data log. The core's current config has no UNIQUE index.
fn retire_unique(core: &mut CoreLoop) {
    drop_current_unique(core);
    build_unique_code(core).expect("the build applies");
    core.purge_document_index_field(0, TID, COLL, "code", &code_owner(), fence(2))
        .expect("the retire applies");
    assert!(
        core.maintained_indexes.path(0, TID, COLL, "code").is_none(),
        "the retire leaves the path unmaintained"
    );
}

fn is_stale_pin(verdict: &Result<(), ErrorCode>) -> bool {
    matches!(
        verdict,
        Err(ErrorCode::StalePinnedSchema { detail }) if detail.contains(COLL)
    )
}

/// A record pinned to a `Ready` UNIQUE index lands after this core applied
/// the index's retire. The core holds no entry to probe, so the record is
/// refused as stale on every path, and the refusal is final.
#[test]
fn a_pinned_ready_unique_the_core_retired_is_a_final_stale_refusal() {
    for verdict in verdicts(&codes_pins(true), retire_unique) {
        assert!(is_stale_pin(&verdict), "{verdict:?}");
        if let Err(code) = verdict {
            assert!(error_is_final_refusal(&crate::Error::DataPlane(code)));
        }
    }
}

/// The wire form of `codes` with its UNIQUE index on `code` in `state`.
fn unique_wire_in(state: RegisteredIndexState) -> DocConfigWire {
    let mut wire = wire(true);
    for index in &mut wire.index_paths {
        index.state = state;
    }
    wire
}

/// A core whose `codes` rows 1 and 2 both hold `A`, where a UNIQUE build of
/// `code` was refused for the duplicate. The catalog keeps the index
/// `Building`.
fn failed_build_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    core.doc_configs
        .insert(config_key(), CollectionConfig::new(COLL));
    let installed = install(&mut core, 10, vec![put(1, "A"), put(2, "A")]);
    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    let built = build_unique_code(&mut core);
    assert!(
        matches!(
            &built,
            Err(crate::Error::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ),
        "{built:?}"
    );
    assert!(
        core.maintained_indexes.path(0, TID, COLL, "code").is_none(),
        "a refused build maintains nothing"
    );
    (core, Box::new((req, resp)))
}

/// After a UNIQUE build refused for duplicates, a record pinned with the
/// index still `Building` is admitted on every path. Its UNIQUE path is
/// skipped, so no stale refusal blocks the collection.
#[test]
fn a_record_pinned_with_a_building_unique_after_a_failed_build_is_admitted() {
    let pinned = pins(vec![pin_of(
        COLL,
        &unique_wire_in(RegisteredIndexState::Building),
    )]);
    let mut held = hold(failed_build_core);
    let mut verdicts = vec![whole(&mut held.core, &pinned, vec![put(3, "A")])];
    for spill in [0, usize::MAX] {
        verdicts.push(streamed(&mut held, &pinned, vec![put(3, "A")], &[], spill));
    }
    for verdict in &verdicts {
        assert!(verdict.is_ok(), "{verdict:?}");
    }

    // The same record pinned `Ready` is refused as stale, so the case bites.
    let ready = pins(vec![pin_of(
        COLL,
        &unique_wire_in(RegisteredIndexState::Ready),
    )]);
    let verdict = whole(&mut held.core, &ready, vec![put(3, "A")]);
    assert!(is_stale_pin(&verdict), "{verdict:?}");
}

/// Two cores whose current configs differ reach one verdict on one
/// pinned record: one reads its current config by the fast path, the
/// other decodes the pin.
#[test]
fn two_cores_with_different_configs_give_one_verdict() {
    fn matching_digest(core: &mut CoreLoop) {
        let digest = pin_of(COLL, &wire(true)).digest;
        core.doc_configs
            .get_mut(&config_key())
            .expect("codes config")
            .digest = digest;
    }
    for unique in [true, false] {
        let pinned = codes_pins(unique);
        let first = verdicts(&pinned, matching_digest);
        let second = verdicts(&pinned, drop_current_unique);
        assert_eq!(
            format!("{first:?}"),
            format!("{second:?}"),
            "unique {unique}"
        );
        assert_eq!(first.iter().all(is_unique_violation), unique, "{first:?}");
    }
}

/// A pinned stream whose op writes a collection it carries no pin of is
/// malformed: the stream validate refuses it as final.
#[test]
fn a_pinned_stream_with_an_unpinned_document_write_is_refused() {
    for spill in [0, usize::MAX] {
        let mut held = hold(plain_core);
        let verdict = streamed(&mut held, &pins(Vec::new()), vec![put(3, "A")], &[], spill);
        assert!(
            matches!(
                &verdict,
                Err(ErrorCode::RejectedPrevalidation { reason }) if reason.contains(COLL)
            ),
            "spill {spill}: {verdict:?}"
        );
    }
}

/// A pinned record pins every sum target it folds into. One that does not
/// is refused on both paths.
#[test]
fn a_pinned_record_with_an_unpinned_sum_target_is_refused() {
    let targets = vec![RedoSumTargets {
        collection: COLL.into(),
        resolved: vec![ResolvedSumTarget::new("accounts", "a1", Surrogate::new(1))],
    }];
    let refused = |verdict: &Result<(), ErrorCode>| {
        matches!(
            verdict,
            Err(ErrorCode::RejectedPrevalidation { reason }) if reason.contains("accounts")
        )
    };
    let pinned = codes_pins(false);
    let mut held = hold(seeded_core);
    let verdict = whole_with(&mut held.core, &pinned, vec![put(3, "C")], &targets);
    assert!(refused(&verdict), "{verdict:?}");
    for spill in [0, usize::MAX] {
        let verdict = streamed(&mut held, &pinned, vec![put(3, "C")], &targets, spill);
        assert!(refused(&verdict), "spill {spill}: {verdict:?}");
    }
}

// ── Folded sum targets ───────────────────────────────────────────────────

fn charges_wire() -> DocConfigWire {
    let mut wire = wire(false);
    wire.enforcement.materialized_sum_sources =
        vec![nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: BALANCES.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }];
    wire
}

fn balances_wire() -> DocConfigWire {
    let mut wire = wire(false);
    wire.index_paths = vec![RegisteredIndex {
        name: "balance".into(),
        path: "balance".into(),
        unique: true,
        case_insensitive: false,
        state: RegisteredIndexState::Ready,
        predicate: None,
    }];
    wire
}

/// The pins of a charge whose proposer saw `balances` with no typeguard.
fn fold_pins() -> RedoPins {
    pins(vec![
        pin_of(BALANCES, &balances_wire()),
        pin_of(CHARGES, &charges_wire()),
    ])
}

/// Give `balances` on `core` a later typeguard whose DEFAULT fails to
/// evaluate, so every folded image of it the core builds refuses.
fn add_failing_default(core: &mut CoreLoop) {
    let key = (
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        BALANCES.to_string(),
    );
    let config = core.doc_configs.get_mut(&key).expect("balances config");
    config.type_guards =
        crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
            BALANCES,
            &[nodedb_types::TypeGuardFieldDef {
                field: "note".into(),
                type_expr: "INT".into(),
                required: false,
                check_expr: None,
                default_expr: Some("1 / 0".into()),
                value_expr: None,
            }],
        );
}

/// Every path's verdict on one pinned charge, on a fold core that
/// `prepare` changes first.
fn fold_verdicts(pins: &RedoPins, prepare: fn(&mut CoreLoop)) -> Vec<Result<(), ErrorCode>> {
    let mut held = hold(fold_core);
    prepare(&mut held.core);
    let ops = || vec![charge(1, "a1", 5)];
    let mut verdicts = vec![whole_with(&mut held.core, pins, ops(), &fold_targets())];
    for spill in [0, usize::MAX] {
        verdicts.push(streamed(&mut held, pins, ops(), &fold_targets(), spill));
    }
    verdicts
}

/// A replica with a later typeguard DEFAULT on the sum target judges the
/// folded target image under the pin, as a replica without it does.
#[test]
fn a_later_typeguard_default_on_the_sum_target_changes_no_verdict() {
    let without = fold_verdicts(&fold_pins(), keep);
    let with = fold_verdicts(&fold_pins(), add_failing_default);
    for verdict in &without {
        assert!(verdict.is_ok(), "{verdict:?}");
    }
    assert_eq!(format!("{with:?}"), format!("{without:?}"));
    // With no pins the later DEFAULT does refuse, so the case bites.
    let unpinned = fold_verdicts(&RedoPins::default(), add_failing_default);
    for verdict in &unpinned {
        assert!(verdict.is_err(), "{verdict:?}");
    }
}
