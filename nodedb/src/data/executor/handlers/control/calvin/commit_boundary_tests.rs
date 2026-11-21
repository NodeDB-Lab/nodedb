// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice that its install would refuse votes abort at its stage.
//!
//! Each case is one the statement stage lets through and only the install's
//! commit boundary refused: a UNIQUE value a balance move takes, a BALANCED
//! journal with one leg, and a RESTORE batch's KV row over the row image
//! limit. The stage refuses each one with the install's code, and the
//! verdict test shows the stage and the install judge every input alike.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{BalancedDef, DocumentOp, MetaOp, PhysicalPlan, RestoredRedo};
use nodedb_types::{
    DatabaseId, MAX_ROW_IMAGE_BYTES, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId, Value,
};
use nodedb_wal::record::RecordType;

use super::shared::CalvinExecCtx;
use super::snapshot_vote::CalvinSnapshot;
use crate::bridge::envelope::{ErrorCode, Response, StageVote, Status};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
use crate::data::executor::core_loop::newest_ordinals::collection_vshard;
use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
use crate::data::executor::handlers::control::calvin_reply::CalvinStaging;
use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::wal::{KvRestoreLive, KvRestoreRedo, RedoRecord, RedoSubRecord};

const TID: u64 = 1;
const EPOCH_MS: i64 = 1_700_000_000_000;
const ACCOUNTS: &str = "accounts";
const LEDGER: &str = "ledger";
const CACHE: &str = "cache";

/// A task homed on the vShard that owns `collection`.
fn task_on(collection: &str) -> ExecutionTask {
    task_with_vshard(collection_vshard(DatabaseId::DEFAULT, collection).expect("collection vshard"))
}

/// A collection on another vShard than `accounts`.
fn orders_collection() -> String {
    let accounts = task_on(ACCOUNTS).request.vshard_id;
    (0..)
        .map(|n| format!("orders_{n}"))
        .find(|name| task_on(name).request.vshard_id != accounts)
        .expect("a collection on another vShard")
}

/// `collection` declares `config`.
fn declare(core: &mut CoreLoop, config: CollectionConfig) {
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), config.name.clone()),
        config,
    );
    core.maintain_registered_paths_for_test();
}

/// `accounts` declares a UNIQUE index on `balance`, and `ledger` a BALANCED
/// journal.
fn declare_collections(core: &mut CoreLoop) {
    let mut accounts = CollectionConfig::new(ACCOUNTS);
    accounts.index_paths.push(IndexPath {
        unique: true,
        ..IndexPath::new("balance")
    });
    declare(core, accounts);
    let mut ledger = CollectionConfig::new(LEDGER);
    ledger.enforcement.balanced = Some(BalancedDef {
        group_key_column: "journal_id".into(),
        entry_type_column: "entry_type".into(),
        debit_value: "DEBIT".into(),
        credit_value: "CREDIT".into(),
        amount_column: "amount".into(),
    });
    declare(core, ledger);
}

fn insert(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    fields: &[(&str, &str)],
) -> PhysicalPlan {
    let object: HashMap<String, Value> = fields
        .iter()
        .map(|(name, value)| ((*name).to_string(), Value::String((*value).to_string())))
        .collect();
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        document_id: document_id.to_string(),
        value: nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body"),
        if_absent: false,
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// A materialized-sum move of `delta` onto the balance of account `id`.
fn balance_delta(id: &str, surrogate: u32, delta: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ACCOUNTS),
        document_id: id.to_string(),
        surrogate: Surrogate::new(surrogate),
        column: "balance".to_string(),
        delta: delta.to_string(),
        join_column: "id".to_string(),
        join_value: id.to_string(),
        declared_primary_key: None,
    })
}

fn leg(document_id: &str, surrogate: u32, entry_type: &str, amount: &str) -> PhysicalPlan {
    insert(
        LEDGER,
        document_id,
        surrogate,
        &[
            ("journal_id", "j1"),
            ("entry_type", entry_type),
            ("amount", amount),
        ],
    )
}

/// A RESTORE batch whose one KV key holds `value_len` bytes under a one-byte
/// key.
fn restored_kv(value_len: usize) -> PhysicalPlan {
    let key = KvRestoreRedo {
        collection: CACHE.to_string(),
        key: b"k".to_vec(),
        surrogate: 5,
        live: Some(KvRestoreLive {
            value: vec![0u8; value_len],
            expire_at_ms: 0,
            sys_from: 10,
        }),
        history: Vec::new(),
        reassert_at: None,
        clone_copy: false,
    };
    let rows_redo = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: key.encode().expect("encode restored key"),
        }],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
        pins: Default::default(),
    }
    .to_bytes()
    .expect("encode restored rows");
    PhysicalPlan::Meta(MetaOp::RestoreRedo(Box::new(RestoredRedo {
        vshard: task_on(CACHE).request.vshard_id.as_u32(),
        rows_redo,
        rows: Vec::new(),
        kv_keys: Vec::new(),
        edges: Vec::new(),
        node_labels: Vec::new(),
        arrays: Vec::new(),
        columnar_keys: Vec::new(),
        vector_rows: Vec::new(),
        synced_text: Vec::new(),
        timeseries: Vec::new(),
        collections: vec![CACHE.to_string()],
        identities: Vec::new(),
        restore_cut: i64::MAX,
    })))
}

fn stage(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    epoch: u64,
    plans: &[PhysicalPlan],
) -> Response {
    core.execute_calvin_execute_static(
        task,
        CalvinExecCtx {
            epoch,
            position: 0,
            epoch_system_ms: EPOCH_MS,
        },
        &TenantId::new(TID),
        plans,
        CalvinSnapshot::NONE,
        &[],
    )
}

fn commit(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    plans: &[PhysicalPlan],
    epoch: u64,
) -> Response {
    core.calvin_commit_at_for_test(task, TID, plans, epoch, EPOCH_MS, 100 + epoch)
}

fn assert_ok(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

/// The slice voted abort with an error `matches` accepts, and holds nothing
/// staged. Answers the error.
fn assert_aborted(
    core: &CoreLoop,
    task: &ExecutionTask,
    epoch: u64,
    response: &Response,
    matches: impl Fn(&ErrorCode) -> bool,
) -> ErrorCode {
    assert_eq!(response.status, Status::Error, "{response:?}");
    assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
    let code = response.error_code.as_deref().cloned().expect("error code");
    assert!(matches(&code), "{code:?}");
    let vshard = task.request.vshard_id.as_u32();
    assert!(!core.calvin.commit_pending.contains_key(&(epoch, 0, vshard)));
    let synthetic = calvin_synthetic_txn_id(epoch, 0, vshard).expect("synthetic id");
    assert!(!core.txn_overlays.contains_key(&synthetic));
    code
}

/// `accounts` holds `acc1` at balance 10 and `acc2` at balance 15.
fn seed_accounts(core: &mut CoreLoop) {
    assert_ok(&commit(
        core,
        &task_on(ACCOUNTS),
        &[
            insert(ACCOUNTS, "acc1", 9, &[("id", "acc1"), ("balance", "10")]),
            insert(ACCOUNTS, "acc2", 10, &[("id", "acc2"), ("balance", "15")]),
        ],
        1,
    ));
}

/// One transaction writes `orders` and `accounts`, two vShards. Its balance
/// move gives `acc1` the balance `acc2` holds, which the statement stage
/// does not judge. The accounts slice votes abort with 23505, the abort
/// verdict drops the orders slice, and each vShard commits its next
/// transaction.
#[test]
fn a_unique_violation_across_two_vshards_aborts_with_23505_and_both_vshards_serve() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    declare_collections(&mut core);
    seed_accounts(&mut core);
    let orders_name = orders_collection();
    let orders = task_on(&orders_name);
    let accounts = task_on(ACCOUNTS);

    let order_slice = stage(
        &mut core,
        &orders,
        2,
        &[insert(&orders_name, "o1", 20, &[("a", "1")])],
    );
    assert_ok(&order_slice);
    assert_eq!(order_slice.stage_vote, Some(StageVote::Commit));
    let account_slice = stage(&mut core, &accounts, 2, &[balance_delta("acc1", 9, "5")]);
    let code = assert_aborted(
        &core,
        &accounts,
        2,
        &account_slice,
        |code| matches!(code, ErrorCode::RejectedConstraint { constraint, .. } if constraint == "unique"),
    );
    assert_eq!(error_code_to_sqlstate(&code).1, "23505");

    // The abort verdict drops the slice that staged.
    assert_ok(&core.execute_calvin_drop(&orders, 2, 0));
    assert!(core.calvin.commit_pending.is_empty());

    assert_ok(&commit(
        &mut core,
        &orders,
        &[insert(&orders_name, "o2", 21, &[("a", "2")])],
        3,
    ));
    assert_ok(&commit(
        &mut core,
        &accounts,
        &[balance_delta("acc1", 9, "1")],
        4,
    ));
}

/// A journal with one leg breaks BALANCED. The statement stage does not
/// judge BALANCED, so the stage of the slice does, with its code. A
/// balanced journal commits.
#[test]
fn an_unbalanced_journal_aborts_at_its_stage_with_the_balanced_code() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    declare_collections(&mut core);
    let ledger = task_on(LEDGER);

    let unbalanced = stage(&mut core, &ledger, 2, &[leg("l1", 30, "DEBIT", "100")]);
    let code = assert_aborted(
        &core,
        &ledger,
        2,
        &unbalanced,
        |code| matches!(code, ErrorCode::BalanceViolation { collection, .. } if collection == LEDGER),
    );
    let balanced_sqlstate = error_code_to_sqlstate(&ErrorCode::BalanceViolation {
        collection: LEDGER.to_string(),
        detail: String::new(),
    })
    .1;
    assert_eq!(error_code_to_sqlstate(&code).1, balanced_sqlstate);

    assert_ok(&commit(
        &mut core,
        &ledger,
        &[
            leg("l1", 30, "DEBIT", "100"),
            leg("l2", 31, "CREDIT", "100"),
        ],
        3,
    ));
}

/// A RESTORE batch stages nothing, so no statement stage judges its rows.
/// A restored KV row over the row image limit aborts at the stage with
/// SQLSTATE 54000 instead of at the install.
#[test]
fn a_restored_row_over_the_limit_aborts_at_its_stage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let cache = task_on(CACHE);

    let response = stage(&mut core, &cache, 2, &[restored_kv(MAX_ROW_IMAGE_BYTES)]);
    let code = assert_aborted(&core, &cache, 2, &response, |code| {
        matches!(code, ErrorCode::ProgramLimitExceeded { .. })
    });
    assert_eq!(error_code_to_sqlstate(&code).1, "54000");
}

/// Stage `plans` at `(epoch, 0)` on `task`'s vShard with the commit
/// boundary judged beside the staging, not in place of the vote, and keep
/// the slice staged. Answers the stage's boundary verdict, or `None` when a
/// plan refuses at its statement.
fn stage_and_judge(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    epoch: u64,
    plans: &[PhysicalPlan],
) -> Option<Result<(), ErrorCode>> {
    let vshard = task.request.vshard_id.as_u32();
    let txn = calvin_synthetic_txn_id(epoch, 0, vshard).expect("synthetic id");
    let tenant = TenantId::new(TID);
    core.epoch_system_ms = Some(EPOCH_MS);
    core.apply_scope.calvin_txn_ordinal = Some(nodedb_types::calvin_txn_ordinal(EPOCH_MS, 0));
    let mut staging = CalvinStaging::default();
    let mut staged = true;
    for plan in plans {
        core.begin_plan_staging(
            txn,
            task.request.event_source,
            task.request.database_id,
            tenant,
            plan,
        );
        if core
            .stage_calvin_plan(task, txn, tenant, plan, &mut staging)
            .is_err()
        {
            staged = false;
            break;
        }
    }
    let judged = staged.then(|| core.check_calvin_commit_boundary(task, txn, tenant, plans));
    let finished = core.finish_calvin_staging(txn, TID, staging);
    core.apply_scope.calvin_txn_ordinal = None;
    core.epoch_system_ms = None;
    let verdict = judged?;
    let (reply, _) = finished.expect("finish staging");
    core.calvin.commit_pending.insert(
        (epoch, 0, vshard),
        PendingCommit {
            plans: plans.to_vec(),
            tenant_id: tenant,
            epoch_system_ms: EPOCH_MS,
            reply,
        },
    );
    Some(verdict)
}

/// The stage and the install judge the same writes with one function, so
/// they reach one verdict on every input. Each case stages its slice, takes
/// the stage's boundary verdict, then resolves and installs the slice and
/// takes the install's.
#[test]
fn the_stage_and_the_install_reach_one_verdict_on_every_input() {
    let mut cases: Vec<(String, Vec<PhysicalPlan>)> = Vec::new();
    for delta in ["-5", "1", "5", "6"] {
        cases.push((
            format!("balance move {delta}"),
            vec![balance_delta("acc1", 9, delta)],
        ));
    }
    for debit in 1..=3 {
        for credit in 1..=3 {
            cases.push((
                format!("journal {debit}/{credit}"),
                vec![
                    leg("l1", 30, "DEBIT", &debit.to_string()),
                    leg("l2", 31, "CREDIT", &credit.to_string()),
                ],
            ));
        }
        cases.push((
            format!("one leg {debit}"),
            vec![leg("l1", 30, "DEBIT", &debit.to_string())],
        ));
    }
    cases.push((
        "restored row over the limit".into(),
        vec![restored_kv(MAX_ROW_IMAGE_BYTES)],
    ));

    let mut refused = 0usize;
    for (name, plans) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_collections(&mut core);
        seed_accounts(&mut core);
        let collection = plans
            .first()
            .and_then(PhysicalPlan::collection)
            .unwrap_or(CACHE)
            .to_string();
        let task = task_on(&collection);
        let Some(staged) = stage_and_judge(&mut core, &task, 2, &plans) else {
            continue;
        };
        let installed = core.calvin_install_staged_for_test(&task, &plans, (2, EPOCH_MS), 200);
        let installed = match installed.status {
            Status::Ok => Ok(()),
            _ => Err(installed
                .error_code
                .as_deref()
                .cloned()
                .expect("error code")),
        };
        assert_eq!(
            staged, installed,
            "{name}: the stage and the install disagree"
        );
        refused += usize::from(staged.is_err());
    }
    assert!(refused > 0, "the cases include refusals");
}

/// A source row that folds 25 into account `a1` of the fold fixture.
fn charge() -> PhysicalPlan {
    use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
        SOURCE, TARGET,
    };
    let body = crate::data::executor::doc_format::encode_to_msgpack(
        &serde_json::json!({"account_id": "a1", "amount": 25}),
    );
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
        document_id: "c1".to_string(),
        value: body,
        if_absent: false,
        surrogate: Surrogate::new(61),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: vec![nodedb_physical::physical_plan::ResolvedSumTarget::new(
            TARGET,
            "a1",
            Surrogate::new(4242),
        )],
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// A fold onto a target row that is not stored fails the install part way
/// through, which halts the vShard. The stage judges the fold and votes
/// abort with the install's code. With the target row stored, the stage
/// and the install both commit.
#[test]
fn a_fold_onto_a_missing_target_aborts_at_its_stage() {
    use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
        SOURCE, configure_sum, core_with_sum,
    };
    let source = task_on(SOURCE);

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    configure_sum(&mut core);
    let response = stage(&mut core, &source, 2, &[charge()]);
    assert_aborted(
        &core,
        &source,
        2,
        &response,
        |code| matches!(code, ErrorCode::BalanceViolation { detail, .. } if detail.contains("'a1'")),
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _held) = core_with_sum(dir.path());
    let plans = [charge()];
    let staged = stage_and_judge(&mut core, &source, 2, &plans).expect("the plan stages");
    let installed = core.calvin_install_staged_for_test(&source, &plans, (2, EPOCH_MS), 200);
    assert_eq!(staged, Ok(()));
    assert_ok(&installed);
}

/// A fold whose target row's new body is over the row image limit fails
/// the install's `apply_point_put` part way through. The stage judges the
/// folded body with the same limit and votes abort with SQLSTATE 54000, and
/// the install of the same slice refuses it alike.
#[test]
fn a_fold_onto_a_target_over_the_row_limit_aborts_at_its_stage() {
    use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
        SOURCE, TARGET, configure_sum,
    };
    let source = task_on(SOURCE);
    let seed_large_target = |core: &mut CoreLoop| {
        let body = crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({
            "id": "a1",
            "balance": "100",
            "note": "x".repeat(MAX_ROW_IMAGE_BYTES),
        }));
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(4242)),
                &body,
                1_000,
            )
            .expect("seed target row");
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    configure_sum(&mut core);
    seed_large_target(&mut core);
    let response = stage(&mut core, &source, 2, &[charge()]);
    let code = assert_aborted(&core, &source, 2, &response, |code| {
        matches!(code, ErrorCode::ProgramLimitExceeded { .. })
    });
    assert_eq!(error_code_to_sqlstate(&code).1, "54000");

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    configure_sum(&mut core);
    seed_large_target(&mut core);
    let plans = [charge()];
    let staged = stage_and_judge(&mut core, &source, 2, &plans).expect("the plan stages");
    let installed = core.calvin_install_staged_for_test(&source, &plans, (2, EPOCH_MS), 200);
    let installed = installed.error_code.as_deref().cloned();
    assert_eq!(
        staged.err(),
        installed,
        "the stage and the install disagree"
    );
}

/// The folded target body of the fold fixture: account `a1` at balance 125,
/// with a note of `note_len` bytes.
fn folded_target(note_len: usize) -> serde_json::Value {
    serde_json::json!({"id": "a1", "balance": "125", "note": "x".repeat(note_len)})
}

/// The largest image of `doc` as the stage encoder builds it, and as the
/// install's `apply_point_put` builds it, on the fold fixture's target.
fn encoder_sizes(core: &CoreLoop, doc: &serde_json::Value) -> (usize, usize) {
    use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::TARGET;
    let submitted = crate::data::executor::doc_format::encode_to_msgpack(doc);
    let canonical =
        crate::data::executor::doc_format::canonicalize_document_for_storage(&submitted);
    let staged = core
        .stage_encode_put_body(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            TARGET,
            Surrogate::new(4242),
            &submitted,
        )
        .expect("stage encoder");
    let config_key = (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string());
    let installed = core
        .folded_target_image(&config_key, Surrogate::new(4242), &submitted)
        .expect("install image");
    let key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(4242));
    let event = core
        .prepare_installed_event_image(
            &crate::data::executor::core_loop::redo_image::WriteSetRow {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: TARGET,
                identity: nodedb_types::RowIdentity::of_stored_row(&submitted, None, key),
            },
            &installed,
        )
        .expect("event image");
    (
        canonical.len().max(staged.len()),
        installed
            .canonical
            .len()
            .max(installed.stored.len())
            .max(event.len()),
    )
}

/// A fold whose target's image sits at the row limit by the install's
/// measure. The stage measures the bytes the install builds, so it refuses
/// exactly the bodies the install refuses. When the stage encoder measures
/// the fixture differently, the case between the two measures is judged by
/// the install's. When it does not, the two agree on the fixture.
#[test]
fn a_fold_at_the_row_limit_is_judged_on_the_installs_bytes() {
    use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
        SOURCE, TARGET, configure_sum,
    };
    let source = task_on(SOURCE);
    let probe_dir = tempfile::tempdir().expect("tempdir");
    let (mut probe, _ptx, _prx) = make_core_with_dir(probe_dir.path());
    configure_sum(&mut probe);
    let base = MAX_ROW_IMAGE_BYTES / 2;
    let (stage_base, install_base) = encoder_sizes(&probe, &folded_target(base));
    // A note one byte longer grows every image by one byte at this size: a
    // string past 64 KiB keeps one header width.
    let at_limit = |size: usize| base + (MAX_ROW_IMAGE_BYTES - size);

    // Each case is a note length and whether the install refuses its fold.
    let at_install_limit = at_limit(install_base);
    let cases: Vec<(usize, bool)> = if stage_base == install_base {
        assert_eq!(
            encoder_sizes(&probe, &folded_target(at_install_limit)),
            (MAX_ROW_IMAGE_BYTES, MAX_ROW_IMAGE_BYTES),
            "the encoders agree on the fixture"
        );
        vec![(at_install_limit, false), (at_install_limit + 1, true)]
    } else if install_base > stage_base {
        // One byte past the limit by the install's measure is within it by
        // the stage encoder's.
        let (stage_size, install_size) =
            encoder_sizes(&probe, &folded_target(at_install_limit + 1));
        assert!(install_size > MAX_ROW_IMAGE_BYTES && stage_size <= MAX_ROW_IMAGE_BYTES);
        vec![(at_install_limit, false), (at_install_limit + 1, true)]
    } else {
        // One byte past the limit by the stage encoder's measure is within it
        // by the install's.
        let note = at_limit(stage_base) + 1;
        let (stage_size, install_size) = encoder_sizes(&probe, &folded_target(note));
        assert!(stage_size > MAX_ROW_IMAGE_BYTES && install_size <= MAX_ROW_IMAGE_BYTES);
        vec![(note, false), (at_install_limit + 1, true)]
    };

    for (note, refused) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        configure_sum(&mut core);
        let mut seeded = folded_target(note);
        seeded["balance"] = serde_json::Value::String("100".into());
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(4242)),
                &crate::data::executor::doc_format::encode_to_msgpack(&seeded),
                1_000,
            )
            .expect("seed target row");
        let plans = [charge()];
        let staged = stage_and_judge(&mut core, &source, 2, &plans).expect("the plan stages");
        let installed = core.calvin_install_staged_for_test(&source, &plans, (2, EPOCH_MS), 200);
        let installed = installed.error_code.as_deref().cloned();
        assert_eq!(
            staged.clone().err(),
            installed,
            "note {note}: the stage and the install disagree"
        );
        assert_eq!(
            matches!(staged, Err(ErrorCode::ProgramLimitExceeded { .. })),
            refused,
            "note {note}: {staged:?}"
        );
    }
}
