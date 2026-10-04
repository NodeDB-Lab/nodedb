// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for A6: `DocumentOp::Register.conflict_policy` must
//! rehydrate the per-core CRDT `PolicyRegistry`, and the rehydration must
//! survive a restart.
//!
//! `ALTER COLLECTION ... SET ON CONFLICT ...` must mutate the catalog, not
//! only the in-memory `PolicyRegistry` — a catalog-only mutation is lost on
//! restart, when the registry starts empty and `PolicyRegistry::get_owned`
//! silently falls back to `CollectionPolicy::ephemeral()` (UNIQUE ->
//! `RenameSuffix`) even though the operator explicitly set
//! `ESCALATE_TO_DLQ`. These tests dispatch
//! the exact production path — `DocumentOp::Register` carrying the persisted
//! `conflict_policy` JSON, as `build_doc_config_from_stored` /
//! `dispatch_register_from_stored_inner` produce it from the catalog on both
//! live DDL apply and boot rehydration (`rehydrate_schema_registry`) — and
//! verify the registry reflects the durable policy, including across a
//! simulated restart (a fresh `CoreLoop`, i.e. an empty `PolicyRegistry`).

use nodedb::bridge::envelope::Status;
use nodedb_crdt::policy::{CollectionPolicy, ConflictPolicy};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, EnforcementOptions, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::{TestCtx, make_ctx};

const COLLECTION: &str = "orders";

/// Build the JSON payload `ALTER COLLECTION ... SET ON CONFLICT
/// ESCALATE_TO_DLQ FOR UNIQUE` would persist onto the catalog record —
/// ephemeral defaults for every other constraint kind, `EscalateToDlq` for
/// UNIQUE.
fn escalate_unique_policy_json() -> String {
    let mut policy = CollectionPolicy::ephemeral();
    policy.unique = ConflictPolicy::EscalateToDlq;
    sonic_rs::to_string(&policy).expect("serialize policy")
}

fn register(ctx: &mut TestCtx, collection: &str, conflict_policy: Option<String>) {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: Default::default(),
            enforcement: Box::new(EnforcementOptions::default()),
            bitemporal: false,
            conflict_policy,
            timeseries: None,
            vector_primary: None,
            vector_fields: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "register document collection");
}

fn get_policy(ctx: &mut TestCtx, collection: &str) -> CollectionPolicy {
    let payload = super::helpers::send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        }),
    );
    sonic_rs::from_slice(&payload).expect("decode CollectionPolicy JSON")
}

/// `conflict_policy: None` (no persisted policy) must leave the registry at
/// its ephemeral default — the pre-existing, still-correct behavior for a
/// never-configured collection.
#[test]
fn register_without_conflict_policy_uses_ephemeral_default() {
    let mut ctx = make_ctx();
    register(&mut ctx, COLLECTION, None);

    let policy = get_policy(&mut ctx, COLLECTION);
    assert!(
        matches!(policy.unique, ConflictPolicy::RenameSuffix),
        "unconfigured collection must resolve to the ephemeral RenameSuffix default"
    );
}

/// `conflict_policy: Some(json)` must rehydrate the registry with the exact
/// persisted policy — this is the live-DDL-apply half of the fix
/// (`execute_register_document_collection` applying `RegisterDocumentCollectionParams::conflict_policy`).
#[test]
fn register_with_conflict_policy_rehydrates_registry() {
    let mut ctx = make_ctx();
    register(&mut ctx, COLLECTION, Some(escalate_unique_policy_json()));

    let policy = get_policy(&mut ctx, COLLECTION);
    assert!(
        matches!(policy.unique, ConflictPolicy::EscalateToDlq),
        "persisted ESCALATE_TO_DLQ policy must be visible immediately after Register"
    );
}

/// The durability requirement: a brand-new `CoreLoop` (simulating a
/// restart — the in-memory `PolicyRegistry` starts empty, exactly as it does
/// on a real process restart) must ALSO resolve `ESCALATE_TO_DLQ` once it
/// replays the same `DocumentOp::Register { conflict_policy: Some(json), .. }`
/// that `rehydrate_schema_registry` / `dispatch_register_from_stored` issue
/// from the durable catalog record at boot.
///
/// The policy must not live only in a process's in-memory registry —
/// that would silently revert the new (post-restart) registry to
/// `RenameSuffix`, auto-renaming a UNIQUE violation that should route to the
/// DLQ.
#[test]
fn conflict_policy_survives_simulated_restart() {
    let policy_json = escalate_unique_policy_json();

    // "Before restart": operator has ALTERed the policy; live registry
    // reflects it (mirrors what `alter_set_on_conflict` + the PutCollection
    // post-apply broadcast do on the running node).
    {
        let mut ctx = make_ctx();
        register(&mut ctx, COLLECTION, Some(policy_json.clone()));
        let policy = get_policy(&mut ctx, COLLECTION);
        assert!(matches!(policy.unique, ConflictPolicy::EscalateToDlq));
        // `ctx` (and its `CoreLoop`) is dropped here — the in-memory
        // registry is gone, exactly as it would be after a process restart.
    }

    // "After restart": a fresh `CoreLoop` with an empty `PolicyRegistry`,
    // fed the same catalog-persisted `conflict_policy` JSON via the same
    // `DocumentOp::Register` boot rehydration replays.
    let mut ctx = make_ctx();
    register(&mut ctx, COLLECTION, Some(policy_json));
    let policy = get_policy(&mut ctx, COLLECTION);
    assert!(
        matches!(policy.unique, ConflictPolicy::EscalateToDlq),
        "conflict policy must survive a restart via catalog-sourced Register \
         rehydration, not silently revert to the ephemeral RenameSuffix default"
    );
}
