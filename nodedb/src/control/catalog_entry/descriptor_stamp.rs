// SPDX-License-Identifier: BUSL-1.1

//! Descriptor versioning stamp helpers.
//!
//! The metadata commit applier calls this right before writing any `Put*`
//! `CatalogEntry` to `SystemCatalog` redb. It reads the prior persisted
//! record, sets `descriptor_version` to prior + 1 (or `1` on create), and
//! stamps `modification_hlc` from the node-local [`HlcClock`].
//!
//! The stamp is a pure function of prior state, clock, and entry, with no side
//! effect beyond advancing the local HLC. Safe on every node's apply path.
//!
//! Every prior read is committed-only. Deriving a version from the
//! transaction's own uncommitted DDL overlay stamps one descriptor twice.
//!
//! Every proposer stamps. There is no unstamped compat path.
//!
//! Variants without descriptor fields pass through unchanged: `PutUser`,
//! `PutRole`, `PutPermission`, `PutOwner`, `PutTenant`, `PutApiKey`,
//! `PutAuthUser`, `PutRlsPolicy`, `PutSchedule`, `PutSequenceState`, and
//! every unfenced `Delete*` variant.
//! `DeactivateCollection` does carry them: a soft delete rewrites the row, so it
//! consumes a version like any other mutation.
//!
//! A fenced delete (`PurgeCollection` and the `Delete*` of every stored type
//! with an incarnation clock) freezes the target row's incarnation instead.
//! `PutSynonymGroup`, `PutVectorIndexParams`, `CreateTopicIfAbsent`,
//! `PutChangeStream`, `PutConsumerGroupIfAbsent`,
//! `MigrateConsumerGroupStream`, and `PutArray` stamp only `modification_hlc`. See
//! [`super::incarnation`].
//! `CloneDatabase` stamps the fresh incarnation its shadow collections take.
//! The match is exhaustive on [`CatalogEntry`], so a new variant is a compile
//! error here. Deciding whether it needs a stamp is deliberate.

use nodedb_types::{Hlc, HlcClock};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};

/// Derive `(descriptor_version, modification_hlc)` for a collection mutation
/// from its prior committed row. A soft delete advances the same metadata.
fn next_collection_stamp(
    prior: Option<&StoredCollection>,
    clock: &HlcClock,
    hlc: Hlc,
) -> (u64, Hlc) {
    let hlc = super::incarnation::stamp::after(prior.map(|c| c.modification_hlc), clock, hlc);
    let prior_descriptor = prior.map(|c| c.descriptor_version).unwrap_or(0);
    (prior_descriptor.saturating_add(1), hlc)
}

/// Read the prior persisted descriptor, assign `descriptor_version = prior + 1`
/// (or `1` on create), stamp `modification_hlc = clock.now()`, return the entry.
///
/// A failed catalog read fails the stamp: a guessed prior emits a
/// version-1 put or an unfenced delete. Version `0` is never emitted.
pub fn stamp(
    entry: CatalogEntry,
    clock: &HlcClock,
    catalog: &SystemCatalog,
) -> crate::Result<CatalogEntry> {
    let mut hlc = clock.now();
    Ok(match entry {
        CatalogEntry::PutCollection(mut stored) => {
            let prior = catalog.get_committed_collection(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            let (descriptor_version, stamped_hlc) =
                next_collection_stamp(prior.as_ref(), clock, hlc);
            stored.descriptor_version = descriptor_version;
            hlc = stamped_hlc;
            // Constraint version bumps ONLY when the derived set changes, so an
            // unrelated ALTER never advances the apply-time fence key nor rejects
            // in-flight CRDT deltas. Set comparison is exact and order-stable.
            let prior_constraint_version =
                prior.as_ref().map(|c| c.constraint_version).unwrap_or(0);
            let prior_set = prior
                .as_ref()
                .map(crate::control::security::catalog::collection_constraints)
                .unwrap_or_default();
            let new_set = crate::control::security::catalog::collection_constraints(&stored);
            stored.constraint_version = if new_set != prior_set {
                prior_constraint_version.saturating_add(1)
            } else {
                prior_constraint_version
            };
            stored.modification_hlc = hlc;
            stored.incarnation = super::incarnation::stamp::collection_incarnation(
                stored.incarnation,
                prior.as_ref(),
                hlc,
            );
            CatalogEntry::PutCollection(stored)
        }
        CatalogEntry::PutCollectionIfAbsent(mut stored) => {
            let prior = catalog.get_committed_collection(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            // Existing is a semantic no-op: freeze the exact persisted record, so
            // replay stays payload-identical and later batch entries see the prior.
            if let Some(prior) = prior {
                return Ok(CatalogEntry::PutCollectionIfAbsent(Box::new(prior)));
            }
            stored.descriptor_version = 1;
            let new_set = crate::control::security::catalog::collection_constraints(&stored);
            stored.constraint_version = u64::from(!new_set.is_empty());
            stored.modification_hlc = hlc;
            stored.incarnation =
                super::incarnation::stamp::collection_incarnation(stored.incarnation, None, hlc);
            CatalogEntry::PutCollectionIfAbsent(stored)
        }
        CatalogEntry::PutMaterializedView(mut stored) => {
            let prior = catalog
                .get_committed_materialized_view(
                    stored.database_id,
                    stored.tenant_id,
                    &stored.name,
                )?
                .map(|v| v.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutMaterializedView(stored)
        }
        CatalogEntry::PutFunction(mut stored) => {
            let prior = catalog
                .get_committed_function_in_database(
                    stored.database_id,
                    stored.tenant_id,
                    &stored.name,
                )?
                .map(|f| f.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutFunction(stored)
        }
        CatalogEntry::PutProcedure(mut stored) => {
            let prior = catalog
                .get_committed_procedure_in_database(
                    stored.database_id,
                    stored.tenant_id,
                    &stored.name,
                )?
                .map(|p| p.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutProcedure(stored)
        }
        CatalogEntry::PutTrigger(mut stored) => {
            let prior = catalog
                .get_committed_trigger_in_database(
                    stored.database_id,
                    stored.tenant_id,
                    &stored.name,
                )?
                .map(|t| t.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutTrigger(stored)
        }
        CatalogEntry::PutSequence(mut stored) => {
            let prior = catalog
                .get_sequence(stored.database_id, stored.tenant_id, &stored.name)?
                .map(|s| s.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutSequence(stored)
        }
        CatalogEntry::PutContinuousAggregate(mut stored) => {
            let prior = catalog
                .get_continuous_aggregate(stored.database_id, stored.tenant_id, &stored.name)?
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutContinuousAggregate(stored)
        }
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            // A soft delete rewrites the row, advancing the same ordering
            // metadata a `PutCollection` does.
            let prior = catalog.get_committed_collection(
                crate::types::DatabaseId::new(database_id),
                tenant_id,
                &name,
            )?;
            let (descriptor_version, stamped_hlc) =
                next_collection_stamp(prior.as_ref(), clock, hlc);
            CatalogEntry::DeactivateCollection {
                database_id,
                tenant_id,
                name,
                descriptor_version,
                modification_hlc: stamped_hlc,
            }
        }
        entry @ CatalogEntry::CloneDatabase { .. } => {
            super::incarnation::stamp::stamp_clone(entry, hlc)
        }
        entry @ (CatalogEntry::PurgeCollection { .. }
        | CatalogEntry::DeleteSequence { .. }
        | CatalogEntry::DeleteTrigger { .. }
        | CatalogEntry::DeleteFunction { .. }
        | CatalogEntry::DeleteProcedure { .. }
        | CatalogEntry::DeleteMaterializedView { .. }
        | CatalogEntry::DeleteContinuousAggregate { .. }
        | CatalogEntry::DeleteSynonymGroup { .. }
        | CatalogEntry::DeleteTopicWithConsumerGroups { .. }
        | CatalogEntry::DeleteVectorIndexParams { .. }
        | CatalogEntry::DeleteChangeStream { .. }
        | CatalogEntry::DeleteConsumerGroup { .. }
        | CatalogEntry::DeleteArray { .. }) => {
            super::incarnation::stamp::stamp_delete(entry, catalog)?
        }
        entry @ (CatalogEntry::PutSynonymGroup(_)
        | CatalogEntry::PutVectorIndexParams(_)
        | CatalogEntry::CreateTopicIfAbsent(_)
        | CatalogEntry::PutChangeStream(_)
        | CatalogEntry::PutConsumerGroupIfAbsent(_)
        | CatalogEntry::MigrateConsumerGroupStream { .. }
        | CatalogEntry::PutArray(_)) => {
            super::incarnation::stamp::stamp_put(entry, clock, catalog, hlc)?
        }
        // Variants without descriptor versioning pass through unchanged.
        entry @ (CatalogEntry::PutStreamingMaterializedView(_)
        | CatalogEntry::DeleteStreamingMaterializedView { .. }
        | CatalogEntry::PutSequenceState(_)
        | CatalogEntry::PutSchedule(_)
        | CatalogEntry::DeleteSchedule { .. }
        | CatalogEntry::PutUser(_)
        | CatalogEntry::DropUser { .. }
        | CatalogEntry::PutRole(_)
        | CatalogEntry::DeleteRole { .. }
        | CatalogEntry::PutApiKey(_)
        | CatalogEntry::RevokeApiKey { .. }
        | CatalogEntry::PutAuthUser(_)
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::PutTenantWithAdmin { .. }
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        | CatalogEntry::PutRedactionPolicy(_)
        | CatalogEntry::DeleteRedactionPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        | CatalogEntry::PutScopeGrant(_)
        | CatalogEntry::DeleteScopeGrant { .. }
        | CatalogEntry::PutIndexRecord(_)
        | CatalogEntry::DeleteIndexRecord { .. }
        | CatalogEntry::PutOwner(_)
        | CatalogEntry::DeleteOwner { .. }
        | CatalogEntry::PutCustomType(_)
        | CatalogEntry::DeleteCustomType { .. }
        | CatalogEntry::PutDatabase(_)
        | CatalogEntry::DeleteDatabase { .. }
        | CatalogEntry::PutDatabaseGrant { .. }
        | CatalogEntry::DeleteDatabaseGrant { .. }
        | CatalogEntry::PutOidcProvider(_)
        | CatalogEntry::DeleteOidcProvider { .. }
        | CatalogEntry::RecordWalTombstone { .. }
        | CatalogEntry::PutDatabaseQuota { .. }
        | CatalogEntry::DeleteDatabaseQuota { .. }
        | CatalogEntry::PutTenantQuota { .. }
        | CatalogEntry::DeleteTenantQuota { .. }
        | CatalogEntry::PutScopeQuota(_)
        | CatalogEntry::DeleteScopeQuota { .. }
        | CatalogEntry::PutRetentionPolicy(_)
        | CatalogEntry::DeleteRetentionPolicy { .. }
        | CatalogEntry::PutAlertRule(_)
        | CatalogEntry::DeleteAlertRule { .. }
        | CatalogEntry::PutCheckpoint(_)
        | CatalogEntry::DeleteCheckpoint { .. }
        | CatalogEntry::CompactHistory { .. }
        | CatalogEntry::PutVectorModel(_)
        | CatalogEntry::DeleteVectorModel { .. }
        | CatalogEntry::PutColumnStats(_)
        | CatalogEntry::PutCloneCopyup { .. }
        | CatalogEntry::PutCloneTombstone { .. }
        | CatalogEntry::PutKvCloneTombstone { .. }
        | CatalogEntry::PutCloneSourceDrain(_)
        | CatalogEntry::DeleteCloneSourceDrain { .. }
        | CatalogEntry::MoveTenantCutover { .. }
        | CatalogEntry::CommitConsumerOffsets(_)
        | CatalogEntry::PutBackupScheduleMark(_)
        | CatalogEntry::PutSpatialRefSys(_)
        | CatalogEntry::DropSpatialRefSys { .. }
        | CatalogEntry::PutCredential(_)
        | CatalogEntry::DropCredential { .. }) => entry,
    })
}

/// Stamp a transactional DDL batch in statement order. Persisted catalog state
/// seeds each descriptor's first mutation, a prior batch mutation the next.
pub fn stamp_batch(
    entries: Vec<CatalogEntry>,
    clock: &HlcClock,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<CatalogEntry>> {
    let mut stamped_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let mut stamped = stamp(entry, clock, catalog)?;
        if let Some(prior) = stamped_entries
            .iter()
            .rev()
            .find(|prior| same_descriptor(prior, &stamped))
        {
            stamped = advance_after(prior, stamped);
        }
        let stamped = super::incarnation::stamp::retarget_in_batch(&stamped_entries, stamped);
        stamped_entries.push(stamped);
    }
    Ok(stamped_entries)
}

/// The `(database, tenant, name)` a versioned collection entry mutates. A soft
/// delete shares this key with the puts, so `CREATE t; DROP t;` is one descriptor.
fn collection_key(entry: &CatalogEntry) -> Option<(u64, u64, &str)> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            Some((
                stored.database_id.as_u64(),
                stored.tenant_id,
                stored.name.as_str(),
            ))
        }
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            ..
        } => Some((*database_id, *tenant_id, name.as_str())),
        _ => None,
    }
}

fn same_descriptor(prior: &CatalogEntry, current: &CatalogEntry) -> bool {
    if let (Some(prior_key), Some(current_key)) = (collection_key(prior), collection_key(current)) {
        return prior_key == current_key;
    }
    match (prior, current) {
        (CatalogEntry::PutMaterializedView(a), CatalogEntry::PutMaterializedView(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutFunction(a), CatalogEntry::PutFunction(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutProcedure(a), CatalogEntry::PutProcedure(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutTrigger(a), CatalogEntry::PutTrigger(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutSequence(a), CatalogEntry::PutSequence(b)) => {
            a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutContinuousAggregate(a), CatalogEntry::PutContinuousAggregate(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        _ => false,
    }
}

fn advance_after(prior: &CatalogEntry, current: CatalogEntry) -> CatalogEntry {
    match (prior, current) {
        (
            CatalogEntry::PutCollection(prior) | CatalogEntry::PutCollectionIfAbsent(prior),
            CatalogEntry::PutCollection(mut current),
        ) => {
            advance_collection(prior, &mut current);
            CatalogEntry::PutCollection(current)
        }
        (
            CatalogEntry::PutCollection(prior) | CatalogEntry::PutCollectionIfAbsent(prior),
            CatalogEntry::PutCollectionIfAbsent(mut current),
        ) => {
            advance_collection(prior, &mut current);
            CatalogEntry::PutCollectionIfAbsent(current)
        }
        (
            CatalogEntry::PutMaterializedView(prior),
            CatalogEntry::PutMaterializedView(mut current),
        ) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutMaterializedView(current)
        }
        (CatalogEntry::PutFunction(prior), CatalogEntry::PutFunction(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutFunction(current)
        }
        (CatalogEntry::PutProcedure(prior), CatalogEntry::PutProcedure(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutProcedure(current)
        }
        (CatalogEntry::PutTrigger(prior), CatalogEntry::PutTrigger(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutTrigger(current)
        }
        (CatalogEntry::PutSequence(prior), CatalogEntry::PutSequence(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutSequence(current)
        }
        (
            CatalogEntry::PutContinuousAggregate(prior),
            CatalogEntry::PutContinuousAggregate(mut current),
        ) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutContinuousAggregate(current)
        }
        (
            CatalogEntry::DeactivateCollection {
                descriptor_version: prior_version,
                ..
            },
            CatalogEntry::PutCollection(mut current),
        ) => {
            // A soft delete leaves the constraint set untouched.
            current.descriptor_version = prior_version.saturating_add(1);
            CatalogEntry::PutCollection(current)
        }
        (
            CatalogEntry::DeactivateCollection {
                descriptor_version: prior_version,
                ..
            },
            CatalogEntry::PutCollectionIfAbsent(mut current),
        ) => {
            current.descriptor_version = prior_version.saturating_add(1);
            CatalogEntry::PutCollectionIfAbsent(current)
        }
        (
            prior,
            CatalogEntry::DeactivateCollection {
                database_id,
                tenant_id,
                name,
                modification_hlc,
                ..
            },
        ) => CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            descriptor_version: collection_version(prior).saturating_add(1),
            modification_hlc,
        },
        (_, current) => current,
    }
}

/// The version a preceding batch entry stamped, or `0` when it is not a
/// versioned collection mutation.
fn collection_version(entry: &CatalogEntry) -> u64 {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            stored.descriptor_version
        }
        CatalogEntry::DeactivateCollection {
            descriptor_version, ..
        } => *descriptor_version,
        _ => 0,
    }
}

fn advance_collection(
    prior: &crate::control::security::catalog::StoredCollection,
    current: &mut crate::control::security::catalog::StoredCollection,
) {
    current.descriptor_version = prior.descriptor_version.saturating_add(1);
    // A later put of a row an earlier entry of the batch leaves active keeps
    // that entry's incarnation.
    if prior.is_active && prior.incarnation != Hlc::ZERO {
        current.incarnation = prior.incarnation;
    }
    let prior_set = crate::control::security::catalog::collection_constraints(prior);
    let current_set = crate::control::security::catalog::collection_constraints(current);
    current.constraint_version = if prior_set == current_set {
        prior.constraint_version
    } else {
        prior.constraint_version.saturating_add(1)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::{
        ParamDirection, ProcedureParam, ProcedureRoutability,
    };
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::security::catalog::{
        FunctionLanguage, FunctionSecurity, FunctionVolatility, StoredCollection, StoredFunction,
        StoredProcedure, StoredSequence, StoredTrigger,
    };
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::DatabaseId;
    use std::sync::Arc;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    #[test]
    fn stamp_on_create_assigns_version_one() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog();
        let stored = StoredCollection::new(1, "orders", "tester");
        let entry = CatalogEntry::PutCollection(Box::new(stored));

        let stamped = stamp(entry, &clock, catalog).expect("stamp");
        let CatalogEntry::PutCollection(boxed) = stamped else {
            panic!("expected PutCollection");
        };
        assert_eq!(boxed.descriptor_version, 1);
        assert!(boxed.modification_hlc > nodedb_types::Hlc::ZERO);
    }

    #[test]
    fn stamp_monotonic_across_updates() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog();

        let mut prior_hlc = nodedb_types::Hlc::ZERO;
        for expected in 1u64..=5 {
            let stored = StoredCollection::new(1, "orders", "tester");
            let entry = CatalogEntry::PutCollection(Box::new(stored));
            let stamped = stamp(entry, &clock, catalog).expect("stamp");
            let CatalogEntry::PutCollection(boxed) = stamped else {
                panic!("expected PutCollection");
            };
            assert_eq!(boxed.descriptor_version, expected);
            assert!(boxed.modification_hlc > prior_hlc);
            prior_hlc = boxed.modification_hlc;
            // Persist so the next iteration reads this as prior.
            catalog
                .put_collection(DatabaseId::DEFAULT, &boxed)
                .expect("put_collection");
        }
    }

    #[test]
    fn stamp_batch_advances_repeated_collection_mutations() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let entries = vec![
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "orders", "tester"))),
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "orders", "tester"))),
        ];
        let stamped = stamp_batch(entries, &clock, store.catalog()).expect("stamp batch");
        let CatalogEntry::PutCollection(first) = &stamped[0] else {
            panic!("expected first collection");
        };
        let CatalogEntry::PutCollection(second) = &stamped[1] else {
            panic!("expected second collection");
        };
        assert_eq!(first.descriptor_version, 1);
        assert_eq!(second.descriptor_version, 2);
    }

    /// Persist a collection at `version` so the next stamp reads it as prior.
    fn seed_prior(catalog: &SystemCatalog, name: &str, version: u64) {
        let mut stored = StoredCollection::stamped_for_test(1, name, "tester");
        stored.descriptor_version = version;
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .expect("put_collection");
    }

    fn function(database_id: DatabaseId) -> StoredFunction {
        StoredFunction {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![],
            return_type: "INT".into(),
            body_sql: "1".into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Sql,
            wasm_hash: None,
            wasm_module: None,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn procedure(database_id: DatabaseId) -> StoredProcedure {
        StoredProcedure {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![ProcedureParam {
                name: "input".into(),
                data_type: "INT".into(),
                direction: ParamDirection::In,
            }],
            body_sql: "BEGIN END".into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            routability: ProcedureRoutability::MultiCollection,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn trigger(database_id: DatabaseId) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            collection: "orders".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn same_named_cross_database_routines_do_not_coalesce() {
        let database_a = DatabaseId::new(11);
        let database_b = DatabaseId::new(12);
        let pairs = [
            (
                CatalogEntry::PutFunction(Box::new(function(database_a))),
                CatalogEntry::PutFunction(Box::new(function(database_b))),
            ),
            (
                CatalogEntry::PutProcedure(Box::new(procedure(database_a))),
                CatalogEntry::PutProcedure(Box::new(procedure(database_b))),
            ),
            (
                CatalogEntry::PutTrigger(Box::new(trigger(database_a))),
                CatalogEntry::PutTrigger(Box::new(trigger(database_b))),
            ),
        ];

        for (first, second) in pairs {
            assert!(!same_descriptor(&first, &second));
        }
    }

    #[test]
    fn stamp_batch_existing_if_absent_does_not_consume_a_version() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        seed_prior(store.catalog(), "orders", 1);
        let mut announcement = StoredCollection::new(1, "orders", "remote");
        announcement.descriptor_version = 0;
        let update = StoredCollection::new(1, "orders", "updated");
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutCollectionIfAbsent(Box::new(announcement)),
                CatalogEntry::PutCollection(Box::new(update)),
            ],
            &clock,
            store.catalog(),
        )
        .expect("stamp batch");
        let CatalogEntry::PutCollectionIfAbsent(noop) = &stamped[0] else {
            panic!("expected create-only entry");
        };
        let CatalogEntry::PutCollection(update) = &stamped[1] else {
            panic!("expected real update");
        };
        assert_eq!(noop.descriptor_version, 1);
        assert_eq!(noop.owner, "tester");
        assert_eq!(update.descriptor_version, 2);
    }

    #[test]
    fn stamp_batch_advances_repeated_sequence_mutations() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let sequence = StoredSequence::new(0, 1, "invoice_seq".into(), "tester".into());
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutSequence(Box::new(sequence.clone())),
                CatalogEntry::PutSequence(Box::new(sequence)),
            ],
            &clock,
            store.catalog(),
        )
        .expect("stamp batch");
        let CatalogEntry::PutSequence(first) = &stamped[0] else {
            panic!("expected first sequence");
        };
        let CatalogEntry::PutSequence(second) = &stamped[1] else {
            panic!("expected second sequence");
        };
        assert_eq!(first.descriptor_version, 1);
        assert_eq!(second.descriptor_version, 2);
    }

    /// A soft delete (`DeactivateCollection`) advances the descriptor version
    /// and HLC the same way a `PutCollection` does. It mutates the row.
    #[test]
    fn stamp_advances_deactivate_collection_version_and_hlc() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog();

        let stored = StoredCollection::new(1, "orders", "tester");
        let create = stamp(
            CatalogEntry::PutCollection(Box::new(stored)),
            &clock,
            catalog,
        )
        .expect("stamp");
        let CatalogEntry::PutCollection(created) = &create else {
            panic!("expected PutCollection");
        };
        let create_version = created.descriptor_version;
        let create_hlc = created.modification_hlc;
        catalog
            .put_collection(DatabaseId::DEFAULT, created)
            .expect("put_collection");

        let stamped = stamp(
            CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 1,
                name: "orders".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            },
            &clock,
            catalog,
        )
        .expect("stamp");
        let CatalogEntry::DeactivateCollection {
            descriptor_version,
            modification_hlc,
            ..
        } = stamped
        else {
            panic!("expected DeactivateCollection");
        };
        assert_eq!(descriptor_version, create_version + 1);
        assert!(modification_hlc > create_hlc);
    }
}
