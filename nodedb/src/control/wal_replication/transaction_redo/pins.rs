// SPDX-License-Identifier: BUSL-1.1

//! The config content a committed redo's proposer pins for its validate.
//!
//! The pin set is one config pin per written collection and per
//! materialized-sum target the redo folds into, plus the CRS set pin. The
//! read set needs no pin: the validate reads no config of it.
//!
//! A core refuses a pinned redo that leaves a document-row write or a sum
//! target unpinned. So such a collection with no active committed catalog
//! row refuses the commit here, with SQLSTATE `42P01`. Any other written
//! collection, such as a graph edge collection, is pinned when it has a
//! row and skipped when it has none. `PinnedWrite` holds that one rule.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{ConfigPinKey, PinnedWrite, RedoPins, RedoSumTargets};
use nodedb_types::{CollectionKey, QualifiedCollection};

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::control::state::config_pin_cache::ConfigPinCache;
use crate::types::{DatabaseId, TenantId};

/// The pins of a redo that writes `written`, database-qualified names, and
/// folds into the targets `sum_targets` names. `written` names no array:
/// an array has no document config. `required` holds the written names
/// whose writes require a pin, as `pin_required_collections` names them.
pub fn redo_pins<'a>(
    state: &SharedState,
    scope: PinScope,
    written: impl IntoIterator<Item = &'a str>,
    required: &[String],
    sum_targets: &[RedoSumTargets],
) -> crate::Result<RedoPins> {
    pins_from_catalog(
        &state.config_pins,
        state.credentials.catalog(),
        scope,
        PinnedNames {
            written,
            required,
            sum_targets,
        },
    )
}

/// The names a redo's pins derive from.
pub(crate) struct PinnedNames<'r, W> {
    /// Every written user collection, database-qualified.
    pub written: W,
    /// The written names whose writes require a pin.
    pub required: &'r [String],
    /// The materialized-sum targets the redo folds into. Each requires a
    /// pin.
    pub sum_targets: &'r [RedoSumTargets],
}

/// The database and tenant a redo's pins key under.
#[derive(Debug, Clone, Copy)]
pub struct PinScope {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
}

/// [`redo_pins`] over `catalog` through `cache`.
///
/// A name whose write requires a pin, or a sum target, with no active
/// committed row refuses the commit. Any other name with no row, such as a
/// graph edge collection, takes no pin.
pub(crate) fn pins_from_catalog<'a, 'r>(
    cache: &ConfigPinCache,
    catalog: &SystemCatalog,
    scope: PinScope,
    names: PinnedNames<'r, impl IntoIterator<Item = &'a str>>,
) -> crate::Result<RedoPins> {
    let PinScope {
        database_id,
        tenant_id,
    } = scope;
    let PinnedNames {
        written,
        required,
        sum_targets,
    } = names;
    let key = |collection: String| ConfigPinKey {
        database_id,
        tenant_id,
        collection,
    };
    // Each key maps to whether its write requires a pin.
    let mut keys: BTreeMap<ConfigPinKey, bool> = BTreeMap::new();
    for name in written {
        keys.entry(key(name.to_owned())).or_default();
    }
    for name in required {
        *keys.entry(key(name.clone())).or_default() |= PinnedWrite::DocumentRows.requires_pin();
    }
    // A binding names its target bare. The core keys the target's config
    // by its qualified name.
    for target in sum_targets.iter().flat_map(|targets| &targets.resolved) {
        let name = QualifiedCollection::new(database_id, &target.target_collection)
            .as_str()
            .to_owned();
        *keys.entry(key(name)).or_default() |= PinnedWrite::SumTarget.requires_pin();
    }
    let mut configs = Vec::with_capacity(keys.len());
    for (key, requires) in &keys {
        match cache.config_pin(catalog, key)? {
            Some(pin) => configs.push(pin),
            None if *requires => return Err(unpinnable(key)),
            None => {}
        }
    }
    Ok(RedoPins {
        configs,
        crs: Some(cache.crs_pin(catalog)?),
    })
}

/// The refusal of a commit that writes or folds into `key`'s collection,
/// which no active committed catalog row holds. It names the collection
/// the user wrote.
fn unpinnable(key: &ConfigPinKey) -> crate::Error {
    let collection = CollectionKey::from_qualified_str(key.database_id, &key.collection)
        .map_or_else(|_| key.collection.clone(), |name| name.name().to_owned());
    crate::Error::CollectionNotFound {
        tenant_id: key.tenant_id,
        collection,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::ResolvedSumTarget;

    use super::*;
    use crate::control::security::catalog::StoredCollection;

    const SCOPE: PinScope = PinScope {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TenantId::new(1),
    };

    fn catalog_with(names: &[&str]) -> SystemCatalog {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        for name in names {
            catalog
                .put_collection(
                    DatabaseId::DEFAULT,
                    &StoredCollection::stamped_for_test(1, name, "owner"),
                )
                .expect("put");
        }
        catalog
    }

    /// The pins of a redo whose document rows write `written`.
    fn pins(
        catalog: &SystemCatalog,
        written: &[&str],
        sum_targets: &[RedoSumTargets],
    ) -> crate::Result<RedoPins> {
        let required: Vec<String> = written.iter().map(|name| name.to_string()).collect();
        pins_from_catalog(
            &ConfigPinCache::new(),
            catalog,
            SCOPE,
            PinnedNames {
                written: written.iter().copied(),
                required: &required,
                sum_targets,
            },
        )
    }

    /// A written collection whose write requires no pin, such as a graph
    /// edge collection, is pinned when it has a row and skipped when it has
    /// none.
    #[test]
    fn an_edge_collection_with_no_row_takes_no_pin() {
        let catalog = catalog_with(&["orders", "follows"]);
        let pinned = pins_from_catalog(
            &ConfigPinCache::new(),
            &catalog,
            SCOPE,
            PinnedNames {
                written: ["orders", "social", "follows"].into_iter(),
                required: &["orders".to_string()],
                sum_targets: &[],
            },
        )
        .expect("pins");
        let names: Vec<&str> = pinned
            .configs
            .iter()
            .map(|pin| pin.key.collection.as_str())
            .collect();
        assert_eq!(names, vec!["follows", "orders"]);
    }

    #[test]
    fn every_written_collection_with_a_row_is_pinned() {
        let catalog = catalog_with(&["orders", "items"]);
        let pinned = pins(&catalog, &["orders", "items"], &[]).expect("pins");
        assert!(pinned.pinned());
        assert_eq!(pinned.configs.len(), 2);
    }

    #[test]
    fn a_written_collection_with_no_row_is_refused() {
        let catalog = catalog_with(&["orders"]);
        let refused = pins(&catalog, &["orders", "ghost"], &[]).expect_err("refused");
        assert!(
            matches!(&refused, crate::Error::CollectionNotFound { collection, .. } if collection == "ghost"),
            "{refused:?}"
        );
    }

    #[test]
    fn a_dropped_collection_is_refused() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let mut gone = StoredCollection::stamped_for_test(1, "gone", "owner");
        gone.is_active = false;
        catalog
            .put_collection(DatabaseId::DEFAULT, &gone)
            .expect("put");
        assert!(matches!(
            pins(&catalog, &["gone"], &[]),
            Err(crate::Error::CollectionNotFound { .. })
        ));
    }

    #[test]
    fn a_sum_target_with_no_row_is_refused() {
        let catalog = catalog_with(&["entries"]);
        let targets = vec![RedoSumTargets {
            collection: "entries".into(),
            resolved: vec![ResolvedSumTarget::new(
                "accounts",
                "a1",
                nodedb_types::Surrogate::new(1),
            )],
        }];
        let refused = pins(&catalog, &["entries"], &targets).expect_err("refused");
        assert!(
            matches!(&refused, crate::Error::CollectionNotFound { collection, .. } if collection == "accounts"),
            "{refused:?}"
        );
    }
}
