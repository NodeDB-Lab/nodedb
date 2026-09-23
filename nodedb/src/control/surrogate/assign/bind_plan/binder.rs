// SPDX-License-Identifier: BUSL-1.1

//! The per-identity resolution rule and the top-level plan walk.

use std::cell::RefCell;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::Surrogate;

use super::super::SurrogateAssigner;
use super::carried::CarriedIdentity;
use crate::types::{DatabaseId, TenantId};

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

    fn record(&self, collection: &str, pk_bytes: &[u8], surrogate: Surrogate) {
        if let Some(recorded) = &self.recorded {
            recorded.borrow_mut().push(CarriedIdentity {
                collection: collection.to_string(),
                pk_bytes: pk_bytes.to_vec(),
                surrogate,
            });
        }
    }

    /// The authoritative surrogate for `(collection, pk_bytes)`.
    ///
    /// A non-ZERO `carried` value came from the coordinator that planned the
    /// write: install it first-wins and return the bound value, which is the
    /// carried one or an earlier binding. A ZERO `carried` value names no
    /// identity (a coordinator that missed resolution), so the catalog is read
    /// only: an existing binding or ZERO. ZERO is never written.
    pub(super) fn resolve(
        &self,
        collection: &str,
        pk_bytes: &[u8],
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        if carried != Surrogate::ZERO {
            let bound = self.assigner.bind(
                self.database_id,
                self.tenant_id,
                collection,
                pk_bytes,
                carried,
            )?;
            self.record(collection, pk_bytes, bound);
            return Ok(bound);
        }
        Ok(self
            .assigner
            .lookup(self.database_id, self.tenant_id, collection, pk_bytes)?
            .unwrap_or(Surrogate::ZERO))
    }

    /// [`Self::resolve`] for a row with no user key: the surrogate self-keys by
    /// its own big-endian bytes, the same key `assign_anonymous` binds under.
    pub(super) fn resolve_self_keyed(
        &self,
        collection: &str,
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        self.resolve(collection, &carried.as_u32().to_be_bytes(), carried)
    }

    /// [`Self::resolve`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_in_place(
        &self,
        collection: &str,
        pk_bytes: &[u8],
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve(collection, pk_bytes, *slot)?;
        Ok(())
    }

    /// [`Self::resolve_self_keyed`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_self_keyed_in_place(
        &self,
        collection: &str,
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve_self_keyed(collection, *slot)?;
        Ok(())
    }

    /// [`Self::resolve`] for a row-creating CRDT apply whose entry carries no
    /// surrogate: allocate locally, loudly. Only a pre-surrogate entry hits
    /// this; a live apply always carries a non-ZERO value and binds it.
    pub(super) fn resolve_or_assign_in_place(
        &self,
        collection: &str,
        document_id: &str,
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        if *slot != Surrogate::ZERO {
            return self.resolve_in_place(collection, document_id.as_bytes(), slot);
        }
        tracing::warn!(
            database_id = self.database_id.as_u64(),
            tenant_id = self.tenant_id.as_u64(),
            collection,
            document_id,
            "CRDT apply carries no surrogate; allocating locally, which can diverge across replicas"
        );
        *slot = self.assigner.assign(
            self.database_id,
            self.tenant_id,
            collection,
            document_id.as_bytes(),
        )?;
        self.record(collection, document_id.as_bytes(), *slot);
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
        // Columnar-family rows are keyed by the surrogate alone; text, spatial,
        // timeseries, query, meta and cluster plans carry no pk binding.
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
