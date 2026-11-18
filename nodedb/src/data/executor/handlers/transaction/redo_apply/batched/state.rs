// SPDX-License-Identifier: BUSL-1.1

//! The state one batched install of a stream marker keeps across its arm
//! passes and batches.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use nodedb_physical::physical_plan::RedoOrigin;

use crate::data::executor::array_install_flush::ArrayStreamLoads;
use crate::data::executor::handlers::timeseries::TsStreamLoads;
use crate::types::Lsn;
use crate::wal::RedoStreamRef;
use crate::wal::redo::replay_arm::RedoArm;

use super::super::state::CollectionKey;
use super::super::stream_validate::{StreamHeader, StreamLimits};
use super::batch_record::BatchStamps;
use super::space::InstallSpace;
use super::undo::BatchAt;

/// The ops each replay arm claims in one stream, counted by the validate.
/// An arm that claims no op gets no pass over the stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct ActiveArms {
    /// Indexed by the arm's position in [`RedoArm::DISPATCH`].
    claimed: [u64; RedoArm::DISPATCH.len()],
}

impl ActiveArms {
    /// Count `ops` more ops claimed by `arm`. `Reconstitute` is no replay
    /// arm, so a claim for it is an error.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn claim(
        &mut self,
        arm: RedoArm,
        ops: u64,
    ) -> crate::Result<()> {
        let index = dispatch_index(arm).ok_or(crate::Error::Internal {
            detail: format!("the replay step {arm:?} claims {ops} ops of a stream"),
        })?;
        self.claimed[index] = self.claimed[index].saturating_add(ops);
        Ok(())
    }

    /// The ops `arm` claims. `Reconstitute` claims none.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn claimed(
        &self,
        arm: RedoArm,
    ) -> u64 {
        dispatch_index(arm).map_or(0, |index| self.claimed[index])
    }

    /// The arms that claim at least one op, in dispatch order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn active(
        &self,
    ) -> impl Iterator<Item = RedoArm> + '_ {
        RedoArm::DISPATCH
            .into_iter()
            .filter(|arm| self.claimed(*arm) > 0)
    }
}

/// The position of `arm` in [`RedoArm::DISPATCH`].
fn dispatch_index(arm: RedoArm) -> Option<usize> {
    RedoArm::DISPATCH
        .iter()
        .position(|dispatched| *dispatched == arm)
}

/// What a batched install opens with: the marker and the validate's
/// results.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchedInstallOpen {
    pub lsn: Lsn,
    pub tid: u64,
    pub database_id: u64,
    pub origin: RedoOrigin,
    pub reference: RedoStreamRef,
    /// The marker's stamps. Every batch record carries them.
    pub stamps: BatchStamps,
    pub header: StreamHeader,
    pub active_arms: ActiveArms,
    pub limits: StreamLimits,
    /// The spill space the validate's spools spilled into.
    pub space: InstallSpace,
    /// What the validate measured the record writes into each timeseries
    /// collection and array.
    pub loads: InstallLoads,
}

/// What one streamed record writes into the timeseries collections and the
/// arrays of this core, measured by the stream validate. The install
/// flushes by it before and after the batches of those arms.
#[derive(Debug, Default)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct InstallLoads {
    pub ts: TsStreamLoads,
    pub arrays: ArrayStreamLoads,
}

/// One stream marker's batched install. Its spill directory goes when this
/// drops.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchedInstall {
    pub lsn: Lsn,
    pub tid: u64,
    pub database_id: u64,
    pub origin: RedoOrigin,
    pub reference: RedoStreamRef,
    /// The marker's stamps. Every batch record carries them.
    pub stamps: BatchStamps,
    pub header: StreamHeader,
    pub active_arms: ActiveArms,
    /// Bytes of whole framed records one batch holds: the validate's bound,
    /// so the install cuts the same batches.
    pub limits: StreamLimits,
    /// Where the install's spools spill:
    /// `data_dir/txn-spill/core-<core_id>/redo-install-<lsn>/`.
    pub space: InstallSpace,
    /// The fold ledger each durable document batch left.
    pub folded: FoldedThrough,
    /// What the record writes into each timeseries collection and array.
    pub loads: InstallLoads,
    /// The timeseries collections the install flushed between batches.
    pub ts_flushed: HashSet<CollectionKey>,
}

impl BatchedInstall {
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn new(
        open: BatchedInstallOpen,
    ) -> Self {
        let BatchedInstallOpen {
            lsn,
            tid,
            database_id,
            origin,
            reference,
            stamps,
            header,
            active_arms,
            limits,
            space,
            loads,
        } = open;
        Self {
            lsn,
            tid,
            database_id,
            origin,
            reference,
            stamps,
            header,
            active_arms,
            limits,
            space,
            folded: FoldedThrough::default(),
            loads,
            ts_flushed: HashSet::new(),
        }
    }
}

/// The fold ledger each durable batch of the document arm left: the
/// stream's ops through the batch's end. Only the document arm folds, and
/// it folds every source op of a batch, so the forward install through a
/// batch holds every fold below that count and none above.
#[derive(Debug, Default)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct FoldedThrough {
    /// The install writes a fold ledger: under a root journal only.
    ledgered: bool,
    by_batch: BTreeMap<u32, u64>,
}

impl FoldedThrough {
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn new(
        ledgered: bool,
    ) -> Self {
        Self {
            ledgered,
            by_batch: BTreeMap::new(),
        }
    }

    /// Note that document batch `batch` is durable through `ops_through`.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn record(
        &mut self,
        batch: u32,
        ops_through: u64,
    ) {
        if self.ledgered {
            self.by_batch.insert(batch, ops_through);
        }
    }

    /// The fold ledger of the forward install of the record at `lsn`
    /// through batch `kept`. `None` when the ledger stays as it is: the
    /// install writes none, or `kept` follows the document arm, which then
    /// folded every op.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn rewind_to(
        &self,
        lsn: u64,
        kept: BatchAt,
    ) -> crate::Result<Option<u64>> {
        if !self.ledgered {
            return Ok(None);
        }
        match kept.arm.cmp(&RedoArm::Document) {
            Ordering::Less => Ok(Some(0)),
            Ordering::Greater => Ok(None),
            Ordering::Equal => {
                self.by_batch
                    .get(&kept.batch)
                    .copied()
                    .map(Some)
                    .ok_or(crate::Error::Internal {
                        detail: format!(
                            "the rollback of the batched install at lsn {lsn} keeps document batch \
                         {} that never became durable",
                            kept.batch
                        ),
                    })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_arms_follow_dispatch_order() {
        let mut arms = ActiveArms::default();
        assert!(arms.claim(RedoArm::Graph, 2).is_ok());
        assert!(arms.claim(RedoArm::Document, 1).is_ok());
        assert!(arms.claim(RedoArm::Document, 3).is_ok());
        assert_eq!(arms.claimed(RedoArm::Document), 4);
        assert_eq!(arms.claimed(RedoArm::Kv), 0);
        assert_eq!(
            arms.active().collect::<Vec<_>>(),
            vec![RedoArm::Document, RedoArm::Graph]
        );
    }

    #[test]
    fn reconstitution_claims_no_op() {
        let mut arms = ActiveArms::default();
        assert!(arms.claim(RedoArm::Reconstitute, 1).is_err());
        assert_eq!(arms.claimed(RedoArm::Reconstitute), 0);
        assert_eq!(arms.active().count(), 0);
    }
}
