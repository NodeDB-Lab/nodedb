// SPDX-License-Identifier: BUSL-1.1

//! Validate a stamped entry against the locally persisted descriptor before it
//! is applied.
//!
//! The stamping half lives in [`super::descriptor_stamp`]; this module decides
//! whether an entry that arrives at the applier is the next version, a
//! historical replay to acknowledge, or a divergence to reject. Fenced deletes
//! and unversioned puts decide by incarnation clock in [`super::incarnation`].

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationOutcome {
    Apply,
    AlreadyApplied,
}

/// Validate every descriptor-bearing `Put*` entry against the locally
/// persisted version before applying it. Historical replay is idempotent for
/// all descriptor families, while equal-version conflicts and forward gaps
/// remain loud anomalies. A create (carried version 1) carrying a strictly
/// newer clock is the one exception: it is a recreate of the same name and
/// applies over the incarnation this node still holds.
///
/// A fenced delete, a soft delete, or an unversioned put that a later
/// incarnation has superseded is acknowledged without applying.
pub fn validate(
    entry: &CatalogEntry,
    catalog: &SystemCatalog,
) -> Result<ValidationOutcome, crate::Error> {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            let current =
                catalog.get_collection(stored.database_id, stored.tenant_id, &stored.name)?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            let current =
                catalog.get_collection(stored.database_id, stored.tenant_id, &stored.name)?;
            if current.is_some() {
                Ok(ValidationOutcome::AlreadyApplied)
            } else {
                validate_one(
                    &stored.name,
                    stored.descriptor_version,
                    stored.as_ref(),
                    None,
                    0,
                    stored.modification_hlc,
                    nodedb_types::Hlc::ZERO,
                )
            }
        }
        CatalogEntry::PutMaterializedView(stored) => {
            let current = catalog.get_materialized_view(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutFunction(stored) => {
            let current = catalog.get_function_in_database(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutProcedure(stored) => {
            let current = catalog.get_procedure_in_database(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutTrigger(stored) => {
            let current = catalog.get_trigger_in_database(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutSequence(stored) => {
            let current =
                catalog.get_sequence(stored.database_id, stored.tenant_id, &stored.name)?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PutContinuousAggregate(stored) => {
            let current = catalog.get_continuous_aggregate(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?;
            validate_one(
                &stored.name,
                stored.descriptor_version,
                stored.as_ref(),
                current.as_ref(),
                current.as_ref().map_or(0, |value| value.descriptor_version),
                stored.modification_hlc,
                current
                    .as_ref()
                    .map_or(nodedb_types::Hlc::ZERO, |value| value.modification_hlc),
            )
        }
        CatalogEntry::PurgeCollection { .. }
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
        | CatalogEntry::DeleteArray { .. } => {
            super::incarnation::fence::check_delete(entry, catalog)
        }
        CatalogEntry::DeactivateCollection { .. }
        | CatalogEntry::PutSynonymGroup(_)
        | CatalogEntry::PutVectorIndexParams(_)
        | CatalogEntry::CreateTopicIfAbsent(_)
        | CatalogEntry::PutChangeStream(_)
        | CatalogEntry::PutConsumerGroupIfAbsent(_)
        | CatalogEntry::PutArray(_) => super::incarnation::fence::check_superseded(entry, catalog),
        CatalogEntry::CloneDatabase {
            target_descriptor,
            source_db_id,
            ..
        } => check_clone_once(target_descriptor.id, *source_db_id, catalog),
        _ => Ok(ValidationOutcome::Apply),
    }
}

/// A clone applies once. Its lineage edge is the last write of `clone_apply`
/// and outlives a later drop of the child, so the edge marks a completed
/// clone. Re-applying re-reads the source's current collections into the
/// child.
fn check_clone_once(
    child: crate::types::DatabaseId,
    source_db_id: u64,
    catalog: &SystemCatalog,
) -> Result<ValidationOutcome, crate::Error> {
    let children = catalog.get_clone_children(crate::types::DatabaseId::new(source_db_id))?;
    Ok(if children.contains(&child) {
        ValidationOutcome::AlreadyApplied
    } else {
        ValidationOutcome::Apply
    })
}

fn validate_one<T: zerompk::ToMessagePack>(
    name: &str,
    carried: u64,
    incoming: &T,
    current: Option<&T>,
    prior: u64,
    incoming_hlc: nodedb_types::Hlc,
    current_hlc: nodedb_types::Hlc,
) -> Result<ValidationOutcome, crate::Error> {
    if carried == 0 {
        return Ok(ValidationOutcome::Apply);
    }
    // An older clock is a prior incarnation, whatever its version number.
    if current.is_some() && incoming_hlc < current_hlc {
        return Ok(ValidationOutcome::AlreadyApplied);
    }
    // A regressed version is a stale replay only if its clock is not ahead.
    // Records the HLC stamp has not set share the ZERO clock.
    if carried < prior && incoming_hlc <= current_hlc {
        return Ok(ValidationOutcome::AlreadyApplied);
    }
    // The stamp emits 1 only when no prior row existed — a later incarnation.
    if carried == 1 && incoming_hlc > current_hlc {
        return Ok(ValidationOutcome::Apply);
    }
    if carried == prior {
        let same_payload = current
            .map(|persisted| {
                let incoming = zerompk::to_msgpack_vec(incoming);
                let persisted = zerompk::to_msgpack_vec(persisted);
                matches!((incoming, persisted), (Ok(a), Ok(b)) if a == b)
            })
            .unwrap_or(false);
        return if same_payload {
            Ok(ValidationOutcome::AlreadyApplied)
        } else {
            Err(crate::Error::DescriptorVersionAnomaly {
                descriptor: name.to_string(),
                carried,
                prior,
            })
        };
    }
    if carried == prior.saturating_add(1) {
        return Ok(ValidationOutcome::Apply);
    }
    Err(crate::Error::DescriptorVersionAnomaly {
        descriptor: name.to_string(),
        carried,
        prior,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::{
        StoredCollection, StoredMaterializedView, StoredSequence, StoredSynonymGroup,
    };
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::DatabaseId;
    use std::sync::Arc;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    /// A stamped entry at `version`. Its clock follows every row seeded
    /// before the call.
    fn collection_with_version(name: &str, version: u64) -> CatalogEntry {
        let mut stored = StoredCollection::stamped_for_test(1, name, "tester");
        stored.descriptor_version = version;
        CatalogEntry::PutCollection(Box::new(stored))
    }

    fn seed_prior(catalog: &SystemCatalog, name: &str, version: u64) {
        let mut stored = StoredCollection::stamped_for_test(1, name, "tester");
        stored.descriptor_version = version;
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .expect("put_collection");
    }

    #[test]
    fn validate_allows_create() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        // No prior record (prior = 0), carried = 1 → prior + 1.
        assert!(matches!(
            validate(&collection_with_version("orders", 1), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    #[test]
    fn validate_allows_idempotent_replay() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let entry = collection_with_version("orders", 3);
        let CatalogEntry::PutCollection(stored) = &entry else {
            unreachable!();
        };
        catalog
            .put_collection(DatabaseId::DEFAULT, stored)
            .expect("seed exact prior");
        // Re-delivery / full-log replay: carried == prior and payload-identical.
        assert!(matches!(
            validate(&entry, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_allows_next_version() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 3);
        assert!(matches!(
            validate(&collection_with_version("orders", 4), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    #[test]
    fn validate_skips_sentinel_zero() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 3);
        // An unstamped entry carries version 0, which is never validated.
        assert!(matches!(
            validate(&collection_with_version("orders", 0), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    #[test]
    fn validate_rejects_gap() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 1);
        // carried = 3 skips version 2 → gap anomaly.
        let err = validate(&collection_with_version("orders", 3), catalog)
            .expect_err("gap must be rejected");
        assert!(matches!(
            err,
            crate::Error::DescriptorVersionAnomaly {
                carried: 3,
                prior: 1,
                ..
            }
        ));
    }

    #[test]
    fn validate_acknowledges_stale_historical_replay() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        // Version 2 was stamped before the row reached version 5.
        let historical = collection_with_version("orders", 2);
        seed_prior(catalog, "orders", 5);
        assert!(matches!(
            validate(&historical, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_treats_older_higher_version_as_prior_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "new_owner");
        current.descriptor_version = 1;
        current.modification_hlc = nodedb_types::Hlc::new(20, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed recreated collection");

        let mut historical = StoredCollection::new(1, "orders", "old_owner");
        historical.descriptor_version = 5;
        historical.modification_hlc = nodedb_types::Hlc::new(10, 0);
        assert!(matches!(
            validate(&CatalogEntry::PutCollection(Box::new(historical)), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_rejects_newer_divergent_equal_version() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "first");
        current.descriptor_version = 2;
        current.modification_hlc = nodedb_types::Hlc::new(10, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed current collection");
        let mut conflict = current;
        conflict.owner = "conflict".into();
        conflict.modification_hlc = nodedb_types::Hlc::new(11, 0);
        assert!(matches!(
            validate(&CatalogEntry::PutCollection(Box::new(conflict)), catalog),
            Err(crate::Error::DescriptorVersionAnomaly { .. })
        ));
    }

    #[test]
    fn validate_acknowledges_stale_sequence_replay() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut persisted = StoredSequence::new(0, 1, "invoice_seq".into(), "tester".into());
        persisted.descriptor_version = 4;
        catalog.put_sequence(&persisted).expect("seed sequence");

        let mut historical = persisted.clone();
        historical.descriptor_version = 2;
        historical.increment = 5;
        assert!(matches!(
            validate(&CatalogEntry::PutSequence(Box::new(historical)), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_treats_existing_if_absent_create_as_applied() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 1);
        let mut retry = StoredCollection::new(1, "orders", "tester");
        retry.descriptor_version = 1;
        // An if-absent create for a descriptor that exists is a no-op whatever
        // the payload says — it never compares versions at all.
        assert!(matches!(
            validate(
                &CatalogEntry::PutCollectionIfAbsent(Box::new(retry)),
                catalog
            ),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_rejects_locally_accreted_fields_at_same_version() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut persisted = StoredCollection::stamped_for_test(1, "metrics", "tester");
        persisted.descriptor_version = 1;
        persisted.fields = vec![
            ("host".to_owned(), "VARCHAR".to_owned()),
            ("cpu".to_owned(), "FLOAT".to_owned()),
        ];
        catalog
            .put_collection(DatabaseId::DEFAULT, &persisted)
            .expect("seed persisted collection");

        // A local write that appended `cpu` without advancing the version
        // makes the two disagree about what version 1 means — a genuine
        // divergence that must stay an error.
        let mut replicated = persisted.clone();
        replicated.fields = vec![("host".to_owned(), "VARCHAR".to_owned())];
        let err = validate(&CatalogEntry::PutCollection(Box::new(replicated)), catalog)
            .expect_err("locally mutated payload at the same version must be rejected");
        assert!(matches!(
            err,
            crate::Error::DescriptorVersionAnomaly {
                carried: 1,
                prior: 1,
                ..
            }
        ));
    }

    /// CREATE → DROP → re-CREATE of the same name. The re-create's stamp saw
    /// no prior row, so it carries version 1 with a strictly newer clock while
    /// this node still holds the first incarnation at version 1.
    #[test]
    fn validate_applies_recreate_at_version_one_with_newer_clock() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "tester");
        current.modification_hlc = nodedb_types::Hlc::new(1_787_734_007_496_107_753, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed first incarnation");

        let mut recreated = current.clone();
        recreated.modification_hlc = nodedb_types::Hlc::new(1_787_734_009_023_967_051, 0);
        assert!(matches!(
            validate(&CatalogEntry::PutCollection(Box::new(recreated)), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    /// Same re-create, but the DROP did deactivate the persisted row first.
    #[test]
    fn validate_applies_recreate_over_deactivated_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "tester");
        current.is_active = false;
        current.modification_hlc = nodedb_types::Hlc::new(10, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed soft-dropped incarnation");

        let mut recreated = current.clone();
        recreated.is_active = true;
        recreated.modification_hlc = nodedb_types::Hlc::new(20, 0);
        assert!(matches!(
            validate(&CatalogEntry::PutCollection(Box::new(recreated)), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    /// Re-delivery of the identical stamped entry: equal clock, byte-identical
    /// payload. Idempotent, and never reaches the recreate branch.
    #[test]
    fn validate_acknowledges_stamped_redelivery_at_equal_clock() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "tester");
        current.modification_hlc = nodedb_types::Hlc::new(1_787_734_007_496_107_753, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed stamped record");

        assert!(matches!(
            validate(&CatalogEntry::PutCollection(Box::new(current)), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A version-1 divergence with no clock advance stays a loud anomaly: the
    /// recreate carve-out needs a strictly newer clock, not only version 1.
    #[test]
    fn validate_rejects_version_one_divergence_at_equal_clock() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::stamped_for_test(1, "orders", "first");
        current.modification_hlc = nodedb_types::Hlc::new(10, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed current collection");

        let mut divergent = current;
        divergent.owner = "second".into();
        let err = validate(&CatalogEntry::PutCollection(Box::new(divergent)), catalog)
            .expect_err("equal-clock version-1 divergence must be rejected");
        assert!(matches!(
            err,
            crate::Error::DescriptorVersionAnomaly {
                carried: 1,
                prior: 1,
                ..
            }
        ));
    }

    #[test]
    fn validate_rejects_divergent_payload_at_same_version() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 3);
        let mut divergent = StoredCollection::stamped_for_test(1, "orders", "different-owner");
        divergent.descriptor_version = 3;
        let err = validate(&CatalogEntry::PutCollection(Box::new(divergent)), catalog)
            .expect_err("same-version divergent payload must be rejected");
        assert!(matches!(
            err,
            crate::Error::DescriptorVersionAnomaly {
                carried: 3,
                prior: 3,
                ..
            }
        ));
    }

    /// Seeds a stored collection at descriptor version 3, stamped h3.
    fn seed_current_incarnation(catalog: &SystemCatalog, name: &str, h3: nodedb_types::Hlc) {
        let mut current = StoredCollection::stamped_for_test(1, name, "tester");
        current.descriptor_version = 3;
        current.modification_hlc = h3;
        catalog
            .put_collection(DatabaseId::DEFAULT, &current)
            .expect("seed current incarnation");
    }

    /// A `PurgeCollection` tombstone stamped for an older incarnation (h1)
    /// must not remove the currently persisted row (stamped h3): the same
    /// name has been dropped and recreated since the purge was proposed.
    #[test]
    fn validate_acknowledges_purge_of_prior_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let h1 = nodedb_types::Hlc::new(10, 0);
        let h3 = nodedb_types::Hlc::new(30, 0);
        seed_current_incarnation(catalog, "orders", h3);

        let purge = CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
            target_descriptor_version: 1,
            target_hlc: h1,
        };
        assert!(matches!(
            validate(&purge, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A `PurgeCollection` tombstone stamped for the incarnation that is
    /// actually persisted (h3) must apply.
    #[test]
    fn validate_applies_purge_of_current_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let h3 = nodedb_types::Hlc::new(30, 0);
        seed_current_incarnation(catalog, "orders", h3);

        let purge = CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
            target_descriptor_version: 3,
            target_hlc: h3,
        };
        assert!(matches!(
            validate(&purge, catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    /// The same fencing on the soft-delete step: a `DeactivateCollection`
    /// stamped for an older incarnation (h1) must not deactivate the
    /// recreated row (stamped h3).
    #[test]
    fn validate_acknowledges_deactivate_of_prior_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let h1 = nodedb_types::Hlc::new(10, 0);
        let h3 = nodedb_types::Hlc::new(30, 0);
        seed_current_incarnation(catalog, "orders", h3);

        let deactivate = CatalogEntry::DeactivateCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
            descriptor_version: 1,
            modification_hlc: h1,
        };
        assert!(matches!(
            validate(&deactivate, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    fn seed_view(catalog: &SystemCatalog, name: &str, hlc: nodedb_types::Hlc) {
        catalog
            .put_materialized_view(&StoredMaterializedView {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: 1,
                name: name.to_string(),
                source: "orders".to_string(),
                query_sql: "SELECT id FROM orders".to_string(),
                refresh_mode: "auto".to_string(),
                owner: "tester".to_string(),
                created_at: 0,
                descriptor_version: 1,
                modification_hlc: hlc,
            })
            .expect("seed materialized view");
    }

    fn drop_view(name: &str, target_hlc: nodedb_types::Hlc) -> CatalogEntry {
        CatalogEntry::DeleteMaterializedView {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: name.to_string(),
            target_descriptor_version: 1,
            target_hlc,
        }
    }

    /// A drop stamped for the first incarnation of a view must not remove a
    /// recreated view of the same name: the recreate also restarts at
    /// version 1, so only the clock tells the two apart.
    #[test]
    fn validate_acknowledges_view_drop_of_prior_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_view(catalog, "mv_orders", nodedb_types::Hlc::new(30, 0));
        assert!(matches!(
            validate(
                &drop_view("mv_orders", nodedb_types::Hlc::new(10, 0)),
                catalog
            ),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A follower that never held the view applies the drop.
    #[test]
    fn validate_applies_view_drop_when_row_is_absent() {
        let (store, _tmp) = make_catalog();
        assert!(matches!(
            validate(
                &drop_view("mv_orders", nodedb_types::Hlc::new(10, 0)),
                store.catalog()
            ),
            Ok(ValidationOutcome::Apply)
        ));
    }

    /// A row older than the drop's target means this node missed a mutation
    /// the proposer saw. That divergence stays a loud anomaly.
    #[test]
    fn validate_rejects_view_drop_ahead_of_the_local_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_view(catalog, "mv_orders", nodedb_types::Hlc::new(10, 0));
        assert!(matches!(
            validate(
                &drop_view("mv_orders", nodedb_types::Hlc::new(30, 0)),
                catalog
            ),
            Err(crate::Error::DescriptorVersionAnomaly { .. })
        ));
    }

    /// An unstamped drop was proposed against an absent row, so a stamped row
    /// is a later incarnation.
    #[test]
    fn validate_acknowledges_unstamped_view_drop_against_a_stamped_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_view(catalog, "mv_orders", nodedb_types::Hlc::new(30, 0));
        assert!(matches!(
            validate(&drop_view("mv_orders", nodedb_types::Hlc::ZERO), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A row written before HLC stamping carries `Hlc::ZERO` and matches an
    /// unstamped drop.
    #[test]
    fn validate_applies_unstamped_view_drop_against_an_unstamped_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_view(catalog, "mv_orders", nodedb_types::Hlc::ZERO);
        assert!(matches!(
            validate(&drop_view("mv_orders", nodedb_types::Hlc::ZERO), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }

    /// A purge proposed against an absent row carries no target. Replayed
    /// over a collection created later, it must not remove that collection.
    #[test]
    fn validate_acknowledges_unstamped_purge_against_a_present_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_current_incarnation(catalog, "orders", nodedb_types::Hlc::new(30, 0));
        let purge = CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        };
        assert!(matches!(
            validate(&purge, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// Unversioned families fence on the clock alone: a replayed synonym put
    /// from before a drop and recreate must not overwrite the recreated row.
    #[test]
    fn validate_acknowledges_stale_synonym_put() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let group = |terms: &[&str], hlc| StoredSynonymGroup {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "db_terms".to_string(),
            terms: terms.iter().map(|t| (*t).to_string()).collect(),
            created_at: 0,
            modification_hlc: hlc,
        };
        catalog
            .put_synonym_group(&group(&["database", "db"], nodedb_types::Hlc::new(30, 0)))
            .expect("seed synonym group");
        let replayed = group(&["database"], nodedb_types::Hlc::new(10, 0));
        assert!(matches!(
            validate(&CatalogEntry::PutSynonymGroup(Box::new(replayed)), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
        let drop = CatalogEntry::DeleteSynonymGroup {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "db_terms".to_string(),
            target_hlc: nodedb_types::Hlc::new(10, 0),
        };
        assert!(matches!(
            validate(&drop, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A `DeleteConsumerGroup` stamped for the first incarnation of a group
    /// must not remove a recreated group of the same name: recreate offsets
    /// stay committed, so only the clock tells the two incarnations apart.
    #[test]
    fn validate_acknowledges_consumer_group_drop_of_prior_incarnation() {
        use crate::event::cdc::ConsumerGroupDef;

        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let h1 = nodedb_types::Hlc::new(10, 0);
        let h3 = nodedb_types::Hlc::new(30, 0);
        let group = ConsumerGroupDef {
            tenant_id: 1,
            name: "analytics".to_string(),
            stream_name: "orders_stream".to_string(),
            owner: "tester".to_string(),
            created_at: 0,
            database_id: DatabaseId::DEFAULT,
            modification_hlc: h3,
        };
        catalog
            .put_consumer_group(&group)
            .expect("seed current incarnation");

        let drop = CatalogEntry::DeleteConsumerGroup {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            stream_name: "orders_stream".to_string(),
            name: "analytics".to_string(),
            target_hlc: h1,
        };
        assert!(matches!(
            validate(&drop, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// A `DeleteChangeStream` stamped for the first incarnation of a stream
    /// must not remove a recreated stream of the same name: the recreate's
    /// buffer and its groups' offsets stay live, so only the clock tells the
    /// two incarnations apart.
    #[test]
    fn validate_acknowledges_change_stream_drop_of_prior_incarnation() {
        use crate::event::cdc::ChangeStreamDef;
        use crate::event::cdc::stream_def::{
            CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
        };

        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let h1 = nodedb_types::Hlc::new(10, 0);
        let h3 = nodedb_types::Hlc::new(30, 0);
        let stream = ChangeStreamDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: "orders_feed".to_string(),
            collection: "orders".to_string(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "tester".to_string(),
            created_at: 0,
            subscriber_roles: Vec::new(),
            modification_hlc: h3,
        };
        catalog
            .put_change_stream(&stream)
            .expect("seed current incarnation");

        let drop = CatalogEntry::DeleteChangeStream {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders_feed".to_string(),
            target_hlc: h1,
        };
        assert!(matches!(
            validate(&drop, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    fn clone_entry(source: DatabaseId, child: DatabaseId) -> CatalogEntry {
        use crate::control::security::catalog::database_types::{
            DatabaseDescriptor, DatabaseStatus, ParentCloneRef,
        };

        CatalogEntry::CloneDatabase {
            target_descriptor: Box::new(DatabaseDescriptor {
                id: child,
                name: "clone_target".into(),
                status: DatabaseStatus::Cloning,
                created_at_lsn: 20,
                quota_ref: 0,
                parent_clone: Some(ParentCloneRef {
                    source_db_id: source,
                    as_of_lsn: 10,
                    as_of_ms: 0,
                    kv_surrogate_ceiling: None,
                }),
                mirror_origin: None,
                audit_dml: nodedb_types::AuditDmlMode::None,
                idle_session_timeout_secs: 0,
            }),
            source_db_id: source.as_u64(),
            incarnation: nodedb_types::Hlc::new(1_000_000, 0),
        }
    }

    /// A replayed clone whose lineage edge exists must not re-read the source
    /// into the child.
    #[test]
    fn validate_acknowledges_replayed_clone() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let source = DatabaseId::new(1024);
        let child = DatabaseId::new(1025);
        catalog
            .add_clone_child(source, child)
            .expect("seed completed clone");
        assert!(matches!(
            validate(&clone_entry(source, child), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// The edge still marks the clone after the child database is dropped.
    #[test]
    fn validate_acknowledges_replayed_clone_of_a_dropped_child() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let source = DatabaseId::new(1024);
        let child = DatabaseId::new(1025);
        let entry = clone_entry(source, child);
        crate::control::catalog_entry::apply::apply_to(&entry, catalog).expect("apply clone");
        catalog.delete_database(child).expect("drop child");
        assert!(matches!(
            validate(&entry, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    /// The first apply finds no edge and applies.
    #[test]
    fn validate_applies_first_clone() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let source = DatabaseId::new(1024);
        let other_child = DatabaseId::new(1026);
        catalog
            .add_clone_child(source, other_child)
            .expect("seed a sibling clone");
        assert!(matches!(
            validate(&clone_entry(source, DatabaseId::new(1025)), catalog),
            Ok(ValidationOutcome::Apply)
        ));
    }
}
