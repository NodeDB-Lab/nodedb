// SPDX-License-Identifier: BUSL-1.1

//! Shared builders for the role and handover tests.

use nodedb_cluster::calvin::CalvinCompletionRegistry;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::scheduler::Scheduler;
use super::test_support::{
    build_test_scheduler_with_data_side, make_validate_only_txn, test_coll_vshard,
};
use crate::bridge::dispatch::CoreChannelDataSide;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// A scheduler on the `test_coll` vShard with its Data Plane side.
pub(super) fn scheduler() -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
    build_test_scheduler_with_data_side(
        test_coll_vshard(),
        CalvinCompletionRegistry::new_detached(),
    )
}

/// A validate-only txn at `(epoch, 0)` that locks `test_coll` row
/// `surrogate`, so txns on distinct rows hold their locks together.
pub(super) fn validate_only_on(epoch: u64, surrogate: u32) -> SequencedTxn {
    let mut txn = make_validate_only_txn(epoch, 0);
    std::sync::Arc::make_mut(&mut txn.tx_class).write_set =
        ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "test_coll".to_string(),
            surrogates: SortedVec::new(vec![surrogate]),
        }]);
    std::sync::Arc::make_mut(&mut txn.tx_class)
        .restore_derived()
        .expect("a derivable class");
    txn
}

/// The `(epoch, position)` of every Calvin stage request on the Data
/// Plane side, in arrival order. Drains the request ring.
pub(super) fn staged_requests(data_side: &mut CoreChannelDataSide) -> Vec<(u64, u32)> {
    let mut staged = Vec::new();
    while let Ok(request) = data_side.request_rx.try_pop() {
        if let PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch, position, ..
        }) = request.inner.plan
        {
            staged.push((epoch, position));
        }
    }
    staged
}

/// Whether a `CalvinDrop` of `txn_id` reached the Data Plane side.
/// Drains the request ring.
pub(super) fn dropped(data_side: &mut CoreChannelDataSide, txn_id: TxnId) -> bool {
    let mut dropped = false;
    while let Ok(request) = data_side.request_rx.try_pop() {
        dropped |= request.inner.plan
            == PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: txn_id.epoch,
                position: txn_id.position,
            });
    }
    dropped
}
