// SPDX-License-Identifier: BUSL-1.1

//! One committed transaction's redo for one vShard, as a commit hands it to
//! the data-group log and as every replica applies it.

use nodedb_physical::physical_plan::{
    CalvinInstall, MetaOp, PhysicalPlan, RedoOrigin, RedoSumTargets,
};

use crate::control::state::SharedState;
use crate::control::surrogate::{CarriedIdentity, collect_plan_identities};
use crate::control::wal_replication::types::CalvinRedoMeta;
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
    /// Which commit-boundary checks every replica's apply runs.
    pub origin: RedoOrigin,
    /// Set when the redo is a committed Calvin slice. The record's
    /// `calvin_stamp` names its `(epoch, position)`.
    pub calvin: Option<CalvinRedoMeta>,
}

/// What a committed Calvin slice's redo carries beside its record, derived
/// from the slice's local plans when it stages.
#[derive(Debug, Clone)]
pub struct CalvinSlice {
    pub collections: Vec<String>,
    pub sum_targets: Vec<RedoSumTargets>,
    pub identities: Vec<CarriedIdentity>,
    pub event_source: EventSource,
    pub origin: RedoOrigin,
    pub meta: CalvinRedoMeta,
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
            origin: RedoOrigin::Commit,
            calvin: None,
        })
    }

    /// The payload of a committed Calvin slice whose resolved redo is `redo`,
    /// built on the vShard's data-group leader. The record's `calvin_stamp`
    /// must name the slice's `(epoch, position)`.
    pub fn from_calvin(redo: RedoRecord, slice: CalvinSlice) -> Self {
        let CalvinSlice {
            collections,
            sum_targets,
            identities,
            event_source,
            origin,
            meta,
        } = slice;
        Self {
            redo,
            collections,
            sum_targets,
            identities,
            event_source,
            origin,
            calvin: Some(meta),
        }
    }

    /// The Data-Plane plan that applies this redo.
    pub fn apply_plan(&self) -> crate::Result<PhysicalPlan> {
        Ok(PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: self.redo.to_bytes()?,
            collections: self.collections.clone(),
            sum_targets: self.sum_targets.clone(),
            origin: self.origin,
            calvin: self.calvin_install()?,
        }))
    }

    /// The Calvin install the apply plan carries: the slice's stamp and the
    /// meta beside it. A Calvin meta on a record with no stamp names no
    /// position, so it is refused.
    fn calvin_install(&self) -> crate::Result<Option<CalvinInstall>> {
        let Some(meta) = &self.calvin else {
            return Ok(None);
        };
        let stamp = self
            .redo
            .calvin_stamp
            .as_ref()
            .ok_or_else(|| crate::Error::Internal {
                detail: "a Calvin redo entry carries no calvin_stamp, so its install names no \
                         (epoch, position); the entry is refused"
                    .into(),
            })?;
        Ok(Some(CalvinInstall {
            epoch: stamp.epoch,
            position: stamp.position,
            epoch_system_ms: meta.epoch_system_ms,
            reply: meta.reply.clone(),
            user_write: meta.user_write,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::CalvinStamp;
    use nodedb_physical::physical_plan::CalvinReplySpec;

    fn payload(
        stamp: Option<CalvinStamp>,
        calvin: Option<CalvinRedoMeta>,
    ) -> TransactionRedoPayload {
        TransactionRedoPayload {
            redo: RedoRecord {
                version: 1,
                ops: Vec::new(),
                calvin_stamp: stamp,
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
            },
            collections: vec!["orders".into()],
            sum_targets: Vec::new(),
            identities: Vec::new(),
            event_source: EventSource::User,
            origin: RedoOrigin::Commit,
            calvin,
        }
    }

    fn meta() -> CalvinRedoMeta {
        CalvinRedoMeta {
            epoch_system_ms: 1_700,
            reply: CalvinReplySpec::Count(vec![7]),
            primary_write: true,
            user_write: true,
            returning: false,
        }
    }

    fn install_of(payload: &TransactionRedoPayload) -> crate::Result<Option<CalvinInstall>> {
        match payload.apply_plan()? {
            PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { calvin, .. }) => Ok(calvin),
            other => panic!("apply plan must be ApplyTransactionRedo, got {other:?}"),
        }
    }

    #[test]
    fn a_session_redo_installs_without_a_calvin_install() {
        let plain = payload(None, None);
        assert_eq!(install_of(&plain).expect("plan"), None);
    }

    #[test]
    fn a_calvin_redo_installs_at_its_stamped_position() {
        let stamp = CalvinStamp {
            epoch: 9,
            position: 4,
            vshard_id: 2,
        };
        let calvin = payload(Some(stamp), Some(meta()));
        assert_eq!(
            install_of(&calvin).expect("plan"),
            Some(CalvinInstall {
                epoch: 9,
                position: 4,
                epoch_system_ms: 1_700,
                reply: CalvinReplySpec::Count(vec![7]),
                user_write: true,
            })
        );
    }

    #[test]
    fn a_calvin_meta_without_a_stamp_is_refused() {
        let unstamped = payload(None, Some(meta()));
        assert!(install_of(&unstamped).is_err());
    }
}
