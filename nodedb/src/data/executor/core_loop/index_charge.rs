// SPDX-License-Identifier: BUSL-1.1

//! The standing memory charge of vector, sparse-vector, and R-tree indexes,
//! live state and history together.
//!
//! Each index's resident bytes (`VectorCollection::resident_bytes`,
//! `SparseInvertedIndex::resident_bytes`, `RTree::resident_bytes`) are
//! charged to the governor's vector or spatial budget under the index's
//! database and tenant. Each structure keeps its byte count as it changes,
//! so reading it never walks a graph or a tree.
//!
//! The charge is exact at every write. Every task that writes
//! (`CoreLoop::execute`) recharges the indexes of its tenant before it
//! answers, so each insert, end, revive, purge, compaction, truncate, and
//! drop moves the charge by the bytes it moved. A history purge and a
//! compaction recharge the index they changed. Boot replay and a snapshot
//! install, which carry no per-write delta, recharge every index.
//!
//! A fresh write to a collection with a vector or R-tree index passes the
//! index engine's pressure gate before it changes anything
//! (`CoreLoop::index_pressure_refusal`), as a document write passes the
//! document gate.

use nodedb_mem::EngineId;
use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::{Admission, Response};
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};

/// One charged index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::data::executor) enum IndexChargeKey {
    Vector((DatabaseId, TenantId, String)),
    Sparse((DatabaseId, TenantId, String, String)),
    Spatial(SpatialIndexKey),
}

impl IndexChargeKey {
    fn scope(&self) -> (DatabaseId, TenantId, EngineId) {
        match self {
            Self::Vector((db, tenant, _)) | Self::Sparse((db, tenant, _, _)) => {
                (*db, *tenant, EngineId::Vector)
            }
            Self::Spatial((db, tenant, _, _)) => (*db, *tenant, EngineId::Spatial),
        }
    }

    fn in_tenant(&self, db: DatabaseId, tenant: TenantId) -> bool {
        let (d, t, _) = self.scope();
        d == db && t == tenant
    }
}

/// Whether the index key part `stored` belongs to `collection`: the name
/// itself, or the name with a `:field` suffix.
fn of_collection(stored: &str, collection: &str) -> bool {
    stored
        .strip_prefix(collection)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
}

impl super::CoreLoop {
    /// The resident bytes of `key`'s index, `0` when the core holds none.
    fn index_resident_bytes(&self, key: &IndexChargeKey) -> usize {
        match key {
            IndexChargeKey::Vector(k) => self
                .vector_collections
                .get(k)
                .map_or(0, |c| c.resident_bytes()),
            IndexChargeKey::Sparse(k) => self
                .sparse_vector_indexes
                .get(k)
                .map_or(0, |i| i.resident_bytes()),
            IndexChargeKey::Spatial(k) => self
                .spatial_indexes
                .get(k)
                .map_or(0, |t| t.resident_bytes()),
        }
    }

    /// Bring `key`'s charge in line with its index's resident bytes. An
    /// index the core no longer holds releases its charge.
    pub(in crate::data::executor) fn recharge_index(&mut self, key: &IndexChargeKey) {
        let bytes = self.index_resident_bytes(key);
        let charges = &mut self.maintenance.version_purge.index_charges;
        if charges
            .get(key)
            .is_some_and(|(charged, _)| *charged == bytes)
        {
            return;
        }
        // Release the prior charge first, so a recharge never counts the
        // index twice.
        charges.remove(key);
        if bytes == 0 {
            return;
        }
        let (db, tenant, engine) = key.scope();
        let token = self.governor.charge(db, tenant, engine, bytes);
        self.maintenance
            .version_purge
            .index_charges
            .insert(key.clone(), (bytes, token));
    }

    /// Recharge every index on this core, and release the charge of every
    /// index that left it. Boot replay and a snapshot install run it.
    pub(in crate::data) fn recharge_all_indexes(&mut self) {
        self.recharge_indexes_where(|_| true);
    }

    /// Recharge every index of `(db, tenant)`, and release the charge of
    /// every one of its indexes that left the core. Every write task runs it
    /// before it answers.
    pub(in crate::data::executor) fn recharge_tenant_indexes(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
    ) {
        self.recharge_indexes_where(|key| key.in_tenant(db, tenant));
    }

    fn recharge_indexes_where(&mut self, selected: impl Fn(&IndexChargeKey) -> bool) {
        let mut keys: Vec<IndexChargeKey> = self
            .vector_collections
            .keys()
            .cloned()
            .map(IndexChargeKey::Vector)
            .chain(
                self.sparse_vector_indexes
                    .keys()
                    .cloned()
                    .map(IndexChargeKey::Sparse),
            )
            .chain(
                self.spatial_indexes
                    .keys()
                    .cloned()
                    .map(IndexChargeKey::Spatial),
            )
            .filter(|key| selected(key))
            .collect();
        keys.extend(
            self.maintenance
                .version_purge
                .index_charges
                .keys()
                .filter(|key| selected(key) && self.index_resident_bytes(key) == 0)
                .cloned()
                .collect::<Vec<_>>(),
        );
        for key in keys {
            self.recharge_index(&key);
        }
    }

    /// Refuse a fresh write `task` when it loads a vector or R-tree index
    /// whose engine is under emergency memory pressure. Runs before the
    /// write changes anything. A write another node or the Calvin scheduler
    /// already ordered applies whatever the pressure: every replica applies
    /// it alike.
    pub(in crate::data::executor) fn index_pressure_refusal(
        &self,
        task: &ExecutionTask,
    ) -> Option<Response> {
        if !matches!(task.request.admission, Admission::Admitted) {
            return None;
        }
        let (vector, spatial) = match task.plan() {
            PhysicalPlan::Vector(_) => (true, false),
            PhysicalPlan::Spatial(_) => (false, true),
            plan => match plan.collection() {
                Some(collection) => self.index_engines_of(
                    task.request.database_id,
                    task.request.tenant_id,
                    collection,
                ),
                None => (false, false),
            },
        };
        if vector && let Some(refusal) = self.check_engine_pressure(task, EngineId::Vector) {
            return Some(refusal);
        }
        if spatial && let Some(refusal) = self.check_engine_pressure(task, EngineId::Spatial) {
            return Some(refusal);
        }
        None
    }

    /// Whether `collection` of `(db, tenant)` holds a vector or sparse-vector
    /// index, and whether it holds an R-tree, on this core.
    fn index_engines_of(&self, db: DatabaseId, tenant: TenantId, collection: &str) -> (bool, bool) {
        let vector = self
            .vector_collections
            .keys()
            .any(|(d, t, k)| *d == db && *t == tenant && of_collection(k, collection))
            || self
                .sparse_vector_indexes
                .keys()
                .any(|(d, t, c, _)| *d == db && *t == tenant && c == collection);
        let spatial = self
            .spatial_indexes
            .keys()
            .any(|(d, t, c, _)| *d == db && *t == tenant && c == collection);
        (vector, spatial)
    }

    /// Recharge every standing history charge of this core: columnar
    /// history and every index. A settle point that can move many stores
    /// runs it.
    pub(in crate::data) fn recharge_all_history(&mut self) {
        self.recharge_all_columnar_history();
        self.recharge_all_indexes();
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;

    fn charged(core: &super::super::CoreLoop, key: &IndexChargeKey) -> usize {
        core.maintenance
            .version_purge
            .index_charges
            .get(key)
            .map_or(0, |(bytes, _)| *bytes)
    }

    /// N inserts move the charge by exactly the bytes the index reports,
    /// with no maintenance pass in between, and a drop releases it.
    #[test]
    fn every_insert_moves_the_charge_by_its_exact_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let db = DatabaseId::DEFAULT;
        let tenant = TenantId::new(1);
        let vkey = (db, tenant, "emb".to_string());
        core.vector_collections.insert(
            vkey.clone(),
            VectorCollection::new(4, HnswParams::default()),
        );
        let key = IndexChargeKey::Vector(vkey.clone());
        core.recharge_tenant_indexes(db, tenant);
        let base_allocated = core.governor.budget(EngineId::Vector).allocated();

        for ordinal in 1..=50 {
            let before = charged(&core, &key);
            let resident_before = core.vector_collections[&vkey].resident_bytes();
            core.vector_collections
                .get_mut(&vkey)
                .expect("index")
                .insert_at(
                    vec![ordinal as f32, 0.0, 0.0, 1.0],
                    Surrogate::new(1),
                    ordinal,
                )
                .expect("insert");
            core.recharge_tenant_indexes(db, tenant);
            let resident_after = core.vector_collections[&vkey].resident_bytes();
            assert_eq!(charged(&core, &key), resident_after, "insert {ordinal}");
            assert_eq!(
                charged(&core, &key) - before,
                resident_after - resident_before,
                "insert {ordinal} moves the charge by its own bytes"
            );
        }
        assert_eq!(
            core.governor.budget(EngineId::Vector).allocated() - base_allocated,
            core.vector_collections[&vkey].resident_bytes(),
            "the governor holds exactly the index's bytes"
        );

        core.vector_collections.remove(&vkey);
        core.recharge_tenant_indexes(db, tenant);
        assert_eq!(
            charged(&core, &key),
            0,
            "a dropped index releases its charge"
        );
        assert_eq!(
            core.governor.budget(EngineId::Vector).allocated(),
            base_allocated
        );
    }
}
