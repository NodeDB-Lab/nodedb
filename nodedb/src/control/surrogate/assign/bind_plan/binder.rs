// SPDX-License-Identifier: BUSL-1.1

//! The per-identity resolution rule and the top-level plan walk.

use std::borrow::Cow;
use std::cell::RefCell;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::{CollectionKey, Surrogate};

use super::super::SurrogateAssigner;
use super::carried::CarriedIdentity;
use crate::types::{DatabaseId, TenantId};

/// One surrogate slot of a batched op: the collection it binds in, the key
/// bytes, and the slot holding the carried surrogate. The bind rewrites the
/// slot with the authoritative value.
pub(super) struct BindSlot<'k, 's> {
    pub(super) key: CollectionKey<'k>,
    pub(super) pk: Cow<'s, [u8]>,
    pub(super) surrogate: &'s mut Surrogate,
}

impl<'k, 's> BindSlot<'k, 's> {
    /// A slot keyed by `pk`.
    pub(super) fn keyed(
        key: CollectionKey<'k>,
        pk: &'s [u8],
        surrogate: &'s mut Surrogate,
    ) -> Self {
        Self {
            key,
            pk: Cow::Borrowed(pk),
            surrogate,
        }
    }

    /// A slot for a row with no user key: it self-keys by its carried
    /// surrogate's big-endian bytes, the key `assign_anonymous` binds under.
    pub(super) fn self_keyed(key: CollectionKey<'k>, surrogate: &'s mut Surrogate) -> Self {
        Self {
            key,
            pk: Cow::Owned(surrogate.as_u32().to_be_bytes().to_vec()),
            surrogate,
        }
    }
}

/// Binds carried identities against one node's catalog under one tenancy scope.
pub struct IdentityBinder<'a> {
    assigner: &'a SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    /// `Some` when the walk also lists every identity it binds.
    recorded: Option<RefCell<Vec<CarriedIdentity>>>,
}

impl<'a> IdentityBinder<'a> {
    pub fn new(
        assigner: &'a SurrogateAssigner,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            assigner,
            database_id,
            tenant_id,
            recorded: None,
        }
    }

    /// A binder that also lists every identity it binds, with the
    /// authoritative surrogate.
    pub(super) fn recording(
        assigner: &'a SurrogateAssigner,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            recorded: Some(RefCell::new(Vec::new())),
            ..Self::new(assigner, database_id, tenant_id)
        }
    }

    /// The identities a recording binder bound, in walk order.
    pub(super) fn into_recorded(self) -> Vec<CarriedIdentity> {
        self.recorded.map(RefCell::into_inner).unwrap_or_default()
    }

    /// The canonical key of a collection named on a plan. Plans carry the
    /// database-qualified name, so it is de-qualified here.
    pub(super) fn plan_key<'c>(&self, qualified: &'c str) -> crate::Result<CollectionKey<'c>> {
        Ok(CollectionKey::from_qualified_str(
            self.database_id,
            qualified,
        )?)
    }

    /// The canonical key of a collection named by its bare catalog name, as
    /// an array plan names its array.
    pub(super) fn bare_key<'c>(&self, bare: &'c str) -> CollectionKey<'c> {
        CollectionKey::from_bare(self.database_id, bare)
    }

    fn record(&self, key: CollectionKey<'_>, pk_bytes: &[u8], surrogate: Surrogate) {
        if let Some(recorded) = &self.recorded {
            recorded.borrow_mut().push(CarriedIdentity {
                collection: key.name().to_string(),
                pk_bytes: pk_bytes.to_vec(),
                surrogate,
            });
        }
    }

    /// The authoritative surrogate for `(key, pk_bytes)`.
    ///
    /// `carried` came from the coordinator that planned the write: install it
    /// first-wins and return the bound value, which is the carried one or an
    /// earlier binding. Every writer binds its row's surrogate before the plan
    /// leaves the coordinator, so `Surrogate::ZERO` names no row and the write
    /// is refused.
    pub(super) fn resolve(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        let mut bound = self.resolve_many(&[(key, pk_bytes, carried)])?;
        bound.pop().ok_or_else(|| crate::Error::Internal {
            detail: format!("bind of a key in '{}' returned no surrogate", key.name()),
        })
    }

    /// [`Self::resolve`] for many `(collection, pk_bytes, carried)` identities.
    /// Binds first-wins in one catalog transaction per collection, and returns
    /// the authoritative surrogate per identity in input order. A carried
    /// `Surrogate::ZERO` refuses the whole batch before anything binds.
    pub(super) fn resolve_many(
        &self,
        identities: &[(CollectionKey<'_>, &[u8], Surrogate)],
    ) -> crate::Result<Vec<Surrogate>> {
        if let Some(&(key, pk_bytes, _)) = identities
            .iter()
            .find(|&&(_, _, carried)| carried == Surrogate::ZERO)
        {
            return Err(crate::Error::Internal {
                detail: format!(
                    "write of key '{}' in '{}' carries no surrogate; every write carries the \
                     one its coordinator bound",
                    String::from_utf8_lossy(pk_bytes),
                    key.name()
                ),
            });
        }
        let mut keys: Vec<CollectionKey<'_>> = Vec::new();
        for &(key, _, _) in identities {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        let mut out = vec![Surrogate::ZERO; identities.len()];
        for key in keys {
            let indices: Vec<usize> = (0..identities.len())
                .filter(|&i| identities[i].0 == key)
                .collect();
            let rows: Vec<(&[u8], Surrogate)> = indices
                .iter()
                .map(|&i| (identities[i].1, identities[i].2))
                .collect();
            let bound = self.assigner.bind_many(key, self.tenant_id, &rows)?;
            if bound.len() != indices.len() {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "bind in '{}' answered {} of {} keys",
                        key.name(),
                        bound.len(),
                        indices.len()
                    ),
                });
            }
            for (&i, surrogate) in indices.iter().zip(bound) {
                out[i] = surrogate;
            }
        }
        for (&(key, pk_bytes, _), &bound) in identities.iter().zip(&out) {
            self.record(key, pk_bytes, bound);
        }
        Ok(out)
    }

    /// [`Self::resolve_many`] writing each authoritative value back into its
    /// slot. A slot carries its collection, its key bytes, and the carried
    /// surrogate it holds.
    pub(super) fn resolve_slots_in_place(&self, slots: Vec<BindSlot<'_, '_>>) -> crate::Result<()> {
        let identities: Vec<(CollectionKey<'_>, &[u8], Surrogate)> = slots
            .iter()
            .map(|slot| (slot.key, slot.pk.as_ref(), *slot.surrogate))
            .collect();
        let bound = self.resolve_many(&identities)?;
        drop(identities);
        for (slot, surrogate) in slots.into_iter().zip(bound) {
            *slot.surrogate = surrogate;
        }
        Ok(())
    }

    /// [`Self::resolve`] for a row with no user key: the surrogate self-keys by
    /// its own big-endian bytes, the same key `assign_anonymous` binds under.
    pub(super) fn resolve_self_keyed(
        &self,
        key: CollectionKey<'_>,
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        self.resolve(key, &carried.as_u32().to_be_bytes(), carried)
    }

    /// [`Self::resolve`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_in_place(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve(key, pk_bytes, *slot)?;
        Ok(())
    }

    /// [`Self::resolve_in_place`] for a write that mutates a row it never
    /// creates. A carried surrogate is bound first-wins, and a carried
    /// `Surrogate::ZERO` is refused. `None` names a key the coordinator found
    /// unbound: the catalog is read only, and the slot takes an existing
    /// binding or stays `None`.
    pub(super) fn resolve_existing_in_place(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        slot: &mut Option<Surrogate>,
    ) -> crate::Result<()> {
        *slot = match *slot {
            Some(carried) => Some(self.resolve(key, pk_bytes, carried)?),
            None => self.assigner.lookup_bound(key, self.tenant_id, pk_bytes)?,
        };
        Ok(())
    }

    /// [`Self::resolve_self_keyed`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_self_keyed_in_place(
        &self,
        key: CollectionKey<'_>,
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve_self_keyed(key, *slot)?;
        Ok(())
    }
}

/// Bind every identity `plan` carries and rewrite each surrogate slot with the
/// authoritative value. Exhaustive over every op family that carries a
/// `(collection, key, surrogate)` triple; the rest carry no identity to bind.
pub fn bind_plan_identities(
    assigner: &SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: &mut PhysicalPlan,
) -> crate::Result<()> {
    bind_with(&IdentityBinder::new(assigner, database_id, tenant_id), plan)
}

/// Bind every identity `plan` carries through `binder`.
pub(super) fn bind_with(binder: &IdentityBinder<'_>, plan: &mut PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(op) => super::document::bind(binder, op),
        PhysicalPlan::Kv(op) => super::kv::bind(binder, op),
        PhysicalPlan::Graph(op) => super::graph::bind(binder, op),
        PhysicalPlan::Vector(op) => super::vector::bind(binder, op),
        PhysicalPlan::Crdt(op) => super::crdt::bind(binder, op),
        PhysicalPlan::Array(op) => super::array::bind(binder, op),
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::RestoreRedo(batch)) => {
            super::restore::bind(binder, batch)
        }
        // Columnar-family rows are keyed by the surrogate alone; text, spatial,
        // timeseries, query, other meta and cluster plans carry no pk binding.
        PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_physical::physical_plan::{ArrayOp, GraphOp, KvOp};
    use nodedb_types::{CollectionKey, QualifiedCollection, Surrogate};

    use super::bind_plan_identities;
    use crate::control::planner::sql_plan_convert::test_support::test_assigner;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::{DatabaseId, TenantId};

    const TENANT: TenantId = TenantId::new(1);

    fn kv_put(surrogate: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        nodedb_physical::physical_plan::PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn array_put(surrogate: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(1)],
            attrs: vec![CellValue::Int64(1)],
            surrogate,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        nodedb_physical::physical_plan::PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TENANT, "grid"),
            cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        })
    }

    fn edge_put(src: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        nodedb_physical::physical_plan::PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: "a".into(),
            label: "l".into(),
            dst_id: "b".into(),
            properties: Vec::new(),
            src_surrogate: src,
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        })
    }

    /// `Surrogate::ZERO` names no row on any engine: the binder refuses the
    /// plan and installs nothing in the catalog.
    #[test]
    fn a_carried_zero_is_refused_on_every_engine() {
        let assigner = test_assigner();
        for mut plan in [
            kv_put(Surrogate::ZERO),
            array_put(Surrogate::ZERO),
            edge_put(Surrogate::ZERO),
        ] {
            let result = bind_plan_identities(&assigner, DatabaseId::DEFAULT, TENANT, &mut plan);
            assert!(result.is_err(), "{plan:?} carries ZERO and must be refused");
        }
        let kv_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "kv");
        assert_eq!(
            assigner.lookup_bound(kv_key, TENANT, b"k").expect("lookup"),
            None
        );
        let graph_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "g");
        assert_eq!(
            assigner
                .lookup_bound(graph_key, TENANT, b"a")
                .expect("lookup"),
            None
        );
    }

    /// A bound surrogate installs first-wins and the plan keeps it.
    #[test]
    fn a_carried_surrogate_binds() {
        let assigner = test_assigner();
        let mut plan = kv_put(Surrogate::new(7));
        bind_plan_identities(&assigner, DatabaseId::DEFAULT, TENANT, &mut plan).expect("bind");
        let kv_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "kv");
        assert_eq!(
            assigner.lookup_bound(kv_key, TENANT, b"k").expect("lookup"),
            Some(Surrogate::new(7))
        );
    }
}
