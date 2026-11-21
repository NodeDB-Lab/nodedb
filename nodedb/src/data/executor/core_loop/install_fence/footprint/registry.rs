// SPDX-License-Identifier: BUSL-1.1

//! The core state a footprint reads to name what an op touches beyond its
//! plan.
//!
//! A sorted-index op names only its index, an aggregate op only its
//! aggregate, a CRDT apply only its collection, and an overlay drop only
//! its transaction. The core's registries name the rest. A footprint is
//! computed only while a fence is armed, so these lookups cost nothing
//! otherwise.

use nodedb_crdt::constraint::ConstraintKind;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId, TxnId};

/// What a footprint asks the core.
pub(in crate::data::executor) trait FenceRegistry {
    /// The stored name of the collection the sorted index `index` of `db`
    /// and `tenant` covers. `None` when no such index is registered.
    fn sorted_index_collection(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        index: &str,
    ) -> Option<String>;

    /// The stored name of the root collection of the registered aggregate
    /// `name`. `None` when it is not registered or its chain loops.
    fn aggregate_root(&self, db: DatabaseId, tenant: TenantId, name: &str) -> Option<String>;

    /// The stored name of the root collection a chain reading `source`
    /// reads. `None` when the chain loops.
    fn aggregate_source_root(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        source: &str,
    ) -> Option<String>;

    /// The stored names of the collections the foreign-key constraints of
    /// the stored name `collection` read. A tenant with no CRDT state holds
    /// no constraint.
    fn crdt_referents(&self, db: DatabaseId, tenant: TenantId, collection: &str) -> Vec<String>;

    /// The collections whose state the drop of transaction `txn`'s overlay
    /// changes: the columnar engines it created and the timeseries
    /// collections it holds a resolve on.
    fn overlay_collections(&self, txn: TxnId) -> Vec<(DatabaseId, String)>;
}

impl FenceRegistry for CoreLoop {
    fn sorted_index_collection(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        index: &str,
    ) -> Option<String> {
        self.kv_engine
            .sorted_index_def(db.as_u64(), tenant.as_u64(), index)
            .map(|def| def.collection.clone())
    }

    fn aggregate_root(&self, db: DatabaseId, tenant: TenantId, name: &str) -> Option<String> {
        self.continuous_agg_mgr
            .root_of(db.as_u64(), tenant.as_u64(), name)
    }

    fn aggregate_source_root(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        source: &str,
    ) -> Option<String> {
        self.continuous_agg_mgr
            .root_of_source(db.as_u64(), tenant.as_u64(), source)
    }

    fn crdt_referents(&self, db: DatabaseId, tenant: TenantId, collection: &str) -> Vec<String> {
        let Some(engine) = self.crdt_engines.get(&(db, tenant)) else {
            return Vec::new();
        };
        let mut referents: Vec<String> = engine
            .validator
            .constraints_for(collection)
            .into_iter()
            .filter_map(|constraint| match &constraint.kind {
                ConstraintKind::ForeignKey { ref_collection, .. }
                | ConstraintKind::BiTemporalFK { ref_collection, .. } => {
                    Some(ref_collection.clone())
                }
                ConstraintKind::Unique | ConstraintKind::NotNull | ConstraintKind::Check { .. } => {
                    None
                }
            })
            .collect();
        referents.sort();
        referents.dedup();
        referents
    }

    fn overlay_collections(&self, txn: TxnId) -> Vec<(DatabaseId, String)> {
        let created = self
            .txn_created_columnar_engines
            .get(&txn)
            .into_iter()
            .flatten()
            .map(|(db, _, collection)| (*db, collection.clone()));
        let held = self
            .ts_resolve_holds
            .iter()
            .filter(|(_, holders)| holders.contains(&txn))
            .map(|((db, _, collection), _)| (*db, collection.clone()));
        let mut collections: Vec<(DatabaseId, String)> = created.chain(held).collect();
        collections.sort();
        collections.dedup();
        collections
    }
}

#[cfg(test)]
pub(in crate::data::executor::core_loop::install_fence) mod tests {
    use super::*;

    /// A registry of fixed entries, each of the default database.
    pub(in crate::data::executor::core_loop::install_fence) struct FakeRegistry {
        /// `(index, collection)`: the sorted index and the collection it
        /// covers.
        pub(in crate::data::executor::core_loop::install_fence) sorted_index:
            Option<(&'static str, &'static str)>,
        /// `(aggregate, root)`: the aggregate and its root collection.
        pub(in crate::data::executor::core_loop::install_fence) aggregate:
            Option<(&'static str, &'static str)>,
        /// `(collection, referent)`: a foreign key of a CRDT collection.
        pub(in crate::data::executor::core_loop::install_fence) referents:
            &'static [(&'static str, &'static str)],
        /// `(txn, collection)`: a collection an overlay drop changes.
        pub(in crate::data::executor::core_loop::install_fence) overlay:
            Option<(u64, &'static str)>,
    }

    /// A registry that holds nothing: every lookup misses.
    pub(in crate::data::executor::core_loop::install_fence) const NO_REGISTRY: FakeRegistry =
        FakeRegistry {
            sorted_index: None,
            aggregate: None,
            referents: &[],
            overlay: None,
        };

    impl FenceRegistry for FakeRegistry {
        fn sorted_index_collection(
            &self,
            _: DatabaseId,
            _: TenantId,
            index: &str,
        ) -> Option<String> {
            self.sorted_index
                .filter(|(name, _)| *name == index)
                .map(|(_, collection)| collection.to_string())
        }

        fn aggregate_root(&self, _: DatabaseId, _: TenantId, name: &str) -> Option<String> {
            self.aggregate
                .filter(|(aggregate, _)| *aggregate == name)
                .map(|(_, root)| root.to_string())
        }

        fn aggregate_source_root(
            &self,
            _: DatabaseId,
            _: TenantId,
            source: &str,
        ) -> Option<String> {
            Some(
                self.aggregate_root(DatabaseId::DEFAULT, TenantId::new(0), source)
                    .unwrap_or_else(|| source.to_string()),
            )
        }

        fn crdt_referents(&self, _: DatabaseId, _: TenantId, collection: &str) -> Vec<String> {
            self.referents
                .iter()
                .filter(|(of, _)| *of == collection)
                .map(|(_, referent)| referent.to_string())
                .collect()
        }

        fn overlay_collections(&self, txn: TxnId) -> Vec<(DatabaseId, String)> {
            self.overlay
                .filter(|(of, _)| *of == txn.as_u64())
                .map(|(_, collection)| (DatabaseId::DEFAULT, collection.to_string()))
                .into_iter()
                .collect()
        }
    }

    #[test]
    fn a_core_resolves_an_overlay_drop_to_the_engines_it_created() {
        let (mut core, _tx, _rx, _dir) = crate::data::executor::core_loop::tests::make_core();
        let txn = TxnId::new(4);
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "metrics".to_string());
        core.txn_created_columnar_engines
            .entry(txn)
            .or_default()
            .insert(key.clone());
        core.ts_resolve_holds
            .entry((DatabaseId::DEFAULT, TenantId::new(1), "ts".to_string()))
            .or_default()
            .insert(txn);
        assert_eq!(
            core.overlay_collections(txn),
            vec![
                (DatabaseId::DEFAULT, "metrics".to_string()),
                (DatabaseId::DEFAULT, "ts".to_string()),
            ]
        );
        assert!(core.overlay_collections(TxnId::new(5)).is_empty());
    }

    #[test]
    fn a_core_resolves_the_foreign_keys_of_a_crdt_collection() {
        let (mut core, _tx, _rx, _dir) = crate::data::executor::core_loop::tests::make_core();
        let tenant = TenantId::new(1);
        assert!(
            core.crdt_referents(DatabaseId::DEFAULT, tenant, "orders")
                .is_empty(),
            "a tenant with no CRDT state holds no constraint"
        );
        let engine = core
            .get_crdt_engine(DatabaseId::DEFAULT, tenant)
            .expect("crdt engine");
        let mut constraints = nodedb_crdt::constraint::ConstraintSet::new();
        constraints.add_foreign_key("orders_user_fk", "orders", "user_id", "users", "id");
        constraints.add_unique("orders_ref_unique", "orders", "ref");
        let set: Vec<_> = constraints.all().to_vec();
        engine.validator.set_collection_constraints("orders", set);
        assert_eq!(
            core.crdt_referents(DatabaseId::DEFAULT, tenant, "orders"),
            vec!["users".to_string()]
        );
    }
}
