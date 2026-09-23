// SPDX-License-Identifier: BUSL-1.1

//! Identities listed off a set of plans on one node and bound on another.
//!
//! A committed transaction replicates as resolved post-images, not plans, so
//! the replicas never walk the plans that carried its identities. The
//! proposer lists them with the same per-family rules [`bind_plan_identities`]
//! applies, and every replica binds the list before the post-images apply.
//!
//! [`bind_plan_identities`]: super::bind_plan_identities

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::Surrogate;

use super::super::SurrogateAssigner;
use super::binder::{IdentityBinder, bind_with};
use crate::types::{DatabaseId, TenantId};

/// One `(collection, pk_bytes) → surrogate` identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarriedIdentity {
    pub collection: String,
    pub pk_bytes: Vec<u8>,
    pub surrogate: Surrogate,
}

/// Every identity `plans` carry, with the surrogate this node's catalog binds
/// it to. Binding here is first-wins and returns an existing binding
/// unchanged, so on the node that planned the writes it changes nothing.
pub fn collect_plan_identities(
    assigner: &SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plans: &[PhysicalPlan],
) -> crate::Result<Vec<CarriedIdentity>> {
    let binder = IdentityBinder::recording(assigner, database_id, tenant_id);
    for plan in plans {
        let mut plan = plan.clone();
        bind_with(&binder, &mut plan)?;
    }
    Ok(binder.into_recorded())
}

/// Bind every carried identity into this node's catalog.
///
/// The carried surrogate is the one the rows being applied are stored under.
/// A catalog that already binds the key to a different surrogate has diverged
/// from the proposer's, and applying on top of it would store a row no lookup
/// by key reaches, so that is an error.
pub fn bind_carried_identities(
    assigner: &SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    identities: &[CarriedIdentity],
) -> crate::Result<()> {
    for identity in identities {
        let bound = assigner.bind(
            database_id,
            tenant_id,
            &identity.collection,
            &identity.pk_bytes,
            identity.surrogate,
        )?;
        if bound != identity.surrogate {
            return Err(crate::Error::Internal {
                detail: format!(
                    "surrogate binding diverged on '{}': this node binds the key to {}, the \
                     committed transaction stored its row under {}",
                    identity.collection,
                    bound.as_u32(),
                    identity.surrogate.as_u32()
                ),
            });
        }
    }
    Ok(())
}
