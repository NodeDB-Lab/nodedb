// SPDX-License-Identifier: BUSL-1.1

//! The stores one history purge pass visits on a core, and the purge of
//! each.
//!
//! Every engine applies one rule at its horizon: a version goes once a newer
//! version at or below the horizon replaced it, the version each row shows
//! at the horizon stays, and a row deleted at or below the horizon leaves
//! with its history. Each engine's purge function holds the rule for its
//! storage. Arrays keep their audit window and are not visited. Timeseries
//! rows are append-only: the only history they keep is a partition a
//! TRUNCATE ended, which the purge removes once the horizon passes its end.

use std::collections::{BTreeSet, VecDeque};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::index_charge::IndexChargeKey;
use crate::data::executor::core_loop::version_purge::{PurgePass, PurgeUnit};
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Every store of this core a pass purges, in visiting order.
    pub(in crate::data::executor) fn version_purge_units(
        &self,
    ) -> crate::Result<VecDeque<PurgeUnit>> {
        let mut units = VecDeque::new();
        for row in self.sparse.versioned_newest_by_collection()? {
            units.push_back(PurgeUnit::Documents {
                database_id: row.database_id,
                tenant: row.tenant,
                collection: row.collection,
            });
        }
        units.push_back(PurgeUnit::Kv);
        units.extend(
            self.columnar_engines
                .keys()
                .cloned()
                .map(PurgeUnit::Columnar),
        );
        // A collection with both a registry and a memtable purges once.
        let mut timeseries: Vec<(DatabaseId, TenantId, String)> = self
            .ts_registries
            .keys()
            .chain(self.columnar_memtables.keys())
            .cloned()
            .collect();
        timeseries.sort_by(|a, b| {
            (a.0.as_u64(), a.1.as_u64(), a.2.as_str()).cmp(&(
                b.0.as_u64(),
                b.1.as_u64(),
                b.2.as_str(),
            ))
        });
        timeseries.dedup();
        units.extend(timeseries.into_iter().map(PurgeUnit::Timeseries));
        units.extend(
            self.vector_collections
                .keys()
                .cloned()
                .map(PurgeUnit::Vectors),
        );
        units.extend(
            self.sparse_vector_indexes
                .keys()
                .cloned()
                .map(PurgeUnit::SparseVectors),
        );
        units.extend(self.spatial_indexes.keys().cloned().map(PurgeUnit::Spatial));
        units.push_back(PurgeUnit::Text);
        let edge_collections: BTreeSet<(u64, u64, String)> = self
            .edge_store
            .newest_by_home()?
            .into_iter()
            .map(|row| (row.database_id, row.tenant_id, row.collection))
            .collect();
        units.extend(
            edge_collections
                .into_iter()
                .map(|(database_id, tenant, collection)| PurgeUnit::Edges {
                    database_id,
                    tenant,
                    collection,
                }),
        );
        units.push_back(PurgeUnit::NodeLabels);
        Ok(units)
    }

    /// Purge `unit` at its horizon under `pass`. Returns the number of
    /// versions removed. A store gone since the pass started purges nothing.
    pub(in crate::data::executor) fn run_version_purge_unit(
        &mut self,
        pass: &PurgePass,
        unit: &PurgeUnit,
    ) -> crate::Result<usize> {
        match unit {
            PurgeUnit::Documents {
                database_id,
                tenant,
                collection,
            } => {
                let Some(horizon) = pass.horizon_of(*database_id, *tenant, collection) else {
                    return Ok(0);
                };
                let (docs, index) = self.sparse.purge_document_versions_below(
                    *database_id,
                    *tenant,
                    collection,
                    horizon,
                )?;
                if docs + index > 0 {
                    // The purged versions leave the tenant's history charge.
                    self.recharge_document_history(
                        DatabaseId::new(*database_id),
                        TenantId::new(*tenant),
                    )?;
                }
                Ok(docs + index)
            }
            // KV keeps no audit window: every table purges at the pass
            // horizon, and each table's charge follows its bytes.
            PurgeUnit::Kv => Ok(self.kv_engine.purge_all_kv_versions_below(pass.horizon)),
            PurgeUnit::Columnar((db, tenant, collection)) => {
                let Some(horizon) = pass.horizon_of(db.as_u64(), tenant.as_u64(), collection)
                else {
                    return Ok(0);
                };
                Ok(self.purge_columnar_versions_below(*db, *tenant, collection, horizon))
            }
            PurgeUnit::Timeseries((db, tenant, collection)) => {
                let Some(horizon) = pass.horizon_of(db.as_u64(), tenant.as_u64(), collection)
                else {
                    return Ok(0);
                };
                self.purge_timeseries_truncated_below(*db, *tenant, collection, horizon)
            }
            PurgeUnit::Vectors(key) => {
                let Some(horizon) = pass.horizon_of(key.0.as_u64(), key.1.as_u64(), &key.2) else {
                    return Ok(0);
                };
                let Some(collection) = self.vector_collections.get_mut(key) else {
                    return Ok(0);
                };
                let purged = collection.purge_vector_versions_below(horizon);
                if purged > 0 {
                    self.checkpoint_coordinator.mark_dirty("vector", purged);
                    // The purged nodes stay tombstoned in their segments
                    // until compaction removes them and frees their memory.
                    // The next maintenance pass compacts the index.
                    self.maintenance
                        .version_purge
                        .vector_compaction_due
                        .insert(key.clone());
                    self.recharge_index(&IndexChargeKey::Vector(key.clone()));
                }
                Ok(purged)
            }
            PurgeUnit::SparseVectors(key) => {
                let Some(horizon) = pass.horizon_of(key.0.as_u64(), key.1.as_u64(), &key.2) else {
                    return Ok(0);
                };
                let Some(index) = self.sparse_vector_indexes.get_mut(key) else {
                    return Ok(0);
                };
                let purged = index.purge_sparse_versions_below(horizon);
                if purged > 0 {
                    self.checkpoint_coordinator.mark_dirty("vector", purged);
                    self.recharge_index(&IndexChargeKey::Sparse(key.clone()));
                }
                Ok(purged)
            }
            PurgeUnit::Spatial(key) => {
                let Some(horizon) = pass.horizon_of(key.0.as_u64(), key.1.as_u64(), &key.2) else {
                    return Ok(0);
                };
                let purged = self
                    .spatial_indexes
                    .get_mut(key)
                    .map_or(0, |rtree| rtree.purge_versions_below(horizon));
                if purged > 0 {
                    self.recharge_index(&IndexChargeKey::Spatial(key.clone()));
                }
                Ok(purged)
            }
            PurgeUnit::Text => {
                self.inverted
                    .purge_text_versions_below_by(&|db, tenant, collection| {
                        pass.horizon_of(db, tenant, collection)
                    })
            }
            PurgeUnit::Edges {
                database_id,
                tenant,
                collection,
            } => {
                let Some(horizon) = pass.horizon_of(*database_id, *tenant, collection) else {
                    return Ok(0);
                };
                let purged = self.edge_store.purge_edge_versions_below(
                    *database_id,
                    TenantId::new(*tenant),
                    collection,
                    horizon,
                )?;
                Ok(purged.versions + purged.cuts)
            }
            PurgeUnit::NodeLabels => self
                .edge_store
                .purge_node_label_versions_below(&|db, tenant| pass.tenant_horizon(db, tenant)),
        }
    }
}
