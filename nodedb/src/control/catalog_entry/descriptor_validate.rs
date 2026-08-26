// SPDX-License-Identifier: BUSL-1.1

//! Validate a stamped `Put*` entry against the locally persisted descriptor
//! version before it is applied.
//!
//! The stamping half lives in [`super::descriptor_stamp`]; this module decides
//! whether an entry that arrives at the applier is the next version, a
//! historical replay to acknowledge, or a divergence to reject.

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
/// remain loud anomalies.
pub fn validate(
    entry: &CatalogEntry,
    catalog: &SystemCatalog,
) -> Result<ValidationOutcome, crate::Error> {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            let current = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
            // B10 (2026-08-26): a DROP (deactivate) retains the collection row
            // and its descriptor_version (is_active=false), so a subsequent
            // CREATE restarts the lifecycle with the SAME numeric version but
            // a strictly newer HLC and a byte-different (active) payload.
            // Comparing versions alone would flag this legit recreate as an
            // anomaly and wedge the metadata applier permanently on replay.
            // A newer HLC on a deactivated row is a new lifecycle — apply it.
            if let Some(current) = &current {
                if !current.is_active
                    && stored.descriptor_version == current.descriptor_version
                    && stored.modification_hlc > current.modification_hlc
                {
                    return Ok(ValidationOutcome::Apply);
                }
            }
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
            let current = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_materialized_view(stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_function(stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_procedure(stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_trigger(stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_sequence(stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
            let current = catalog
                .get_continuous_aggregate(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
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
        _ => Ok(ValidationOutcome::Apply),
    }
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
    // A recreated descriptor restarts its numeric version namespace. Once a
    // newer lifecycle is persisted, every older-HLC record is historical even
    // if its old numeric version is greater than the recreated version.
    if current.is_some() && incoming_hlc < current_hlc {
        return Ok(ValidationOutcome::AlreadyApplied);
    }
    // A lower carried version is a stale historical replay only when its clock
    // is not ahead of the persisted record (older or equal — legacy records
    // predating HLC stamping share the ZERO clock). A regressed version paired
    // with a strictly newer HLC is a genuine anomaly (a corrupted or misordered
    // proposal, a stamping race) and must fall through to be rejected loudly.
    if carried < prior && incoming_hlc <= current_hlc {
        return Ok(ValidationOutcome::AlreadyApplied);
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
    use crate::control::security::catalog::{StoredCollection, StoredSequence};
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::DatabaseId;
    use std::sync::Arc;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn collection_with_version(name: &str, version: u64) -> CatalogEntry {
        let mut stored = StoredCollection::new(1, name, "tester");
        stored.descriptor_version = version;
        CatalogEntry::PutCollection(Box::new(stored))
    }

    fn seed_prior(catalog: &SystemCatalog, name: &str, version: u64) {
        let mut stored = StoredCollection::new(1, name, "tester");
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
    fn validate_allows_recreate_after_deactivate() {
        // B10 (2026-08-26): DROP retains the row with is_active=false and the
        // SAME descriptor_version; a later CREATE restarts the lifecycle with
        // the same numeric version, a newer HLC and a byte-different (active)
        // payload. Must Apply, not wedge as DescriptorVersionAnomaly.
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut deactivated = StoredCollection::new(1, "orders", "tester");
        deactivated.descriptor_version = 1;
        deactivated.is_active = false;
        deactivated.modification_hlc = nodedb_types::Hlc::new(100, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &deactivated)
            .expect("seed deactivated prior");

        let mut recreated = StoredCollection::new(1, "orders", "tester");
        recreated.descriptor_version = 1;
        recreated.is_active = true;
        recreated.modification_hlc = nodedb_types::Hlc::new(200, 0);
        let entry = CatalogEntry::PutCollection(Box::new(recreated));
        assert!(
            matches!(validate(&entry, catalog), Ok(ValidationOutcome::Apply)),
            "recreate after deactivate must Apply (B10), got: {:?}",
            validate(&entry, catalog)
        );
    }

    #[test]
    fn validate_treats_stale_recreate_as_already_applied() {
        // Same version + deactivated row but OLDER HLC = stale replay — the
        // existing incoming_hlc < current_hlc guard must treat it as applied
        // (not a new lifecycle, not an anomaly).
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut deactivated = StoredCollection::new(1, "orders", "tester");
        deactivated.descriptor_version = 1;
        deactivated.is_active = false;
        deactivated.modification_hlc = nodedb_types::Hlc::new(200, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &deactivated)
            .expect("seed deactivated prior");

        let mut recreated = StoredCollection::new(1, "orders", "tester");
        recreated.descriptor_version = 1;
        recreated.is_active = true;
        recreated.modification_hlc = nodedb_types::Hlc::new(100, 0);
        let entry = CatalogEntry::PutCollection(Box::new(recreated));
        assert!(matches!(
            validate(&entry, catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_skips_sentinel_zero() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 3);
        // Compat mode / unstamped entry: version 0 is never validated.
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
        seed_prior(catalog, "orders", 5);
        assert!(matches!(
            validate(&collection_with_version("orders", 2), catalog),
            Ok(ValidationOutcome::AlreadyApplied)
        ));
    }

    #[test]
    fn validate_treats_older_higher_version_as_prior_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut current = StoredCollection::new(1, "orders", "new_owner");
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
        let mut current = StoredCollection::new(1, "orders", "first");
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
        let mut persisted = StoredSequence::new(1, "invoice_seq".into(), "tester".into());
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
        let mut persisted = StoredCollection::new(1, "metrics", "tester");
        persisted.descriptor_version = 1;
        persisted.fields = vec![
            ("host".to_owned(), "VARCHAR".to_owned()),
            ("cpu".to_owned(), "FLOAT".to_owned()),
        ];
        catalog
            .put_collection(DatabaseId::DEFAULT, &persisted)
            .expect("seed persisted collection");

        // The replicated entry at version 1 carried only `host`. A local write
        // that appended `cpu` without advancing the version makes the two
        // disagree about what version 1 means, which is a genuine divergence
        // and must stay an error — any schema projection an ingest path infers
        // has to travel as its own descriptor version instead.
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

    #[test]
    fn validate_rejects_divergent_payload_at_same_version() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        seed_prior(catalog, "orders", 3);
        let mut divergent = StoredCollection::new(1, "orders", "different-owner");
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
}
