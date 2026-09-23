// SPDX-License-Identifier: BUSL-1.1

//! One committed transaction's redo for one vShard, as a commit hands it to
//! the data-group log and as every replica applies it.

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RedoSumTargets};

use crate::control::state::SharedState;
use crate::control::surrogate::{CarriedIdentity, collect_plan_identities};
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoRecord;

use super::collections::written_collections;
use super::sum_targets::redo_sum_targets;

/// Everything a replica needs to apply one committed transaction's redo.
#[derive(Debug, Clone)]
pub struct TransactionRedoPayload {
    /// The resolved post-images.
    pub redo: RedoRecord,
    /// Every collection the transaction wrote.
    pub collections: Vec<String>,
    /// Materialized-sum resolution the document writes fold into, keyed by
    /// source collection.
    pub sum_targets: Vec<RedoSumTargets>,
    /// Identities every replica binds before the apply.
    pub identities: Vec<CarriedIdentity>,
    /// Event source every replica stamps on the writes.
    pub event_source: EventSource,
}

impl TransactionRedoPayload {
    /// The payload for a commit whose buffered write plans are `plans` and
    /// whose resolved redo is `redo`, built on the node that resolved it.
    pub fn from_commit(
        state: &SharedState,
        database_id: DatabaseId,
        tenant_id: TenantId,
        redo: RedoRecord,
        plans: &[PhysicalPlan],
        event_source: EventSource,
    ) -> crate::Result<Self> {
        Ok(Self {
            redo,
            collections: written_collections(plans),
            sum_targets: redo_sum_targets(plans),
            identities: collect_plan_identities(
                &state.surrogate_assigner,
                database_id,
                tenant_id,
                plans,
            )?,
            event_source,
        })
    }

    /// The Data-Plane plan that applies this redo.
    pub fn apply_plan(&self) -> crate::Result<PhysicalPlan> {
        Ok(PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: self.redo.to_bytes()?,
            collections: self.collections.clone(),
            sum_targets: self.sum_targets.clone(),
        }))
    }
}
