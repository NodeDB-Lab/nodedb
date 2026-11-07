// SPDX-License-Identifier: BUSL-1.1

//! The write gates of a cross-shard Calvin submit: the mirror check and the
//! storage quota.
//!
//! A sequenced transaction applies through a Calvin redo entry, which the
//! leader proposal gate exempts. The submit is the one place the whole
//! transaction's plans are in hand. Both coordinator entry points call this:
//! the submit that awaits completion and the OLLP submit that returns the
//! assignment. The sequencer leader's hooks take what a coordinator admitted.

use nodedb_cluster::calvin::types::TxClass;

use crate::control::server::shared::write_admission::{
    check_mirror_writable, check_storage_quota, plan_adds_storage,
};
use crate::control::state::SharedState;
use crate::event::EventSource;

/// Refuse `tx_class` when its database is a mirror that is not promoted, or
/// when it adds bytes to a database or tenant at its storage limit. A class
/// already split into parts passed the storage check at its coordinator.
pub(super) fn admit_submit(state: &SharedState, tx_class: &TxClass) -> crate::Result<()> {
    // Code `0` is the client default.
    let source = EventSource::from_wal_code(tx_class.event_source).unwrap_or(EventSource::User);
    check_mirror_writable(state, tx_class.database_id, source, || Ok(true))?;
    if tx_class.is_multi_part() {
        return Ok(());
    }
    check_storage_quota(
        state,
        tx_class.database_id,
        tx_class.tenant_id,
        source,
        || {
            let plans = nodedb_physical::physical_plan::wire::decode_batch(&tx_class.plans)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("calvin storage quota: plan decode: {e}"),
                })?;
            Ok(plans.iter().any(plan_adds_storage))
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::planner::calvin::build_single_vshard_tx_class;
    use crate::types::{DatabaseId, TenantId};
    use crate::wal::WalManager;

    use super::super::{submit_calvin_routed, submit_calvin_routed_assign};
    use super::*;

    const DATABASE: DatabaseId = DatabaseId::new(1100);
    const TENANT: TenantId = TenantId::new(1);

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("submit-gate.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, directory)
    }

    fn tx_class() -> TxClass {
        let task = PhysicalTask {
            tenant_id: TENANT,
            database_id: DATABASE,
            vshard_id: nodedb_types::CollectionKey::from_bare(DATABASE, "cache").vshard(),
            plan: PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::new(DATABASE, "cache"),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0,
                surrogate: Surrogate::new(1),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        build_single_vshard_tx_class(&[task], TENANT, &[]).expect("tx class")
    }

    fn over_quota(state: &SharedState) {
        state.database_metrics.ensure_database(DATABASE);
        state.database_metrics.set_storage_limit(DATABASE, 100);
        state
            .cluster_storage_totals
            .publish([((DATABASE, TENANT), 100)].into_iter().collect(), true);
    }

    #[tokio::test]
    async fn the_awaited_submit_refuses_a_mirror() {
        let (state, _directory) = state();
        state
            .mirror
            .fence
            .mark_read_only_for_test(DATABASE, "mirror_db");
        let result = submit_calvin_routed(&state, tx_class()).await;
        assert!(matches!(result, Err(crate::Error::MirrorReadOnly { .. })));
    }

    #[tokio::test]
    async fn the_ollp_submit_refuses_a_mirror() {
        let (state, _directory) = state();
        state
            .mirror
            .fence
            .mark_read_only_for_test(DATABASE, "mirror_db");
        let result = submit_calvin_routed_assign(&state, tx_class()).await;
        assert!(matches!(result, Err(crate::Error::MirrorReadOnly { .. })));
    }

    #[tokio::test]
    async fn the_ollp_submit_refuses_a_full_database() {
        let (state, _directory) = state();
        over_quota(&state);
        let result = submit_calvin_routed_assign(&state, tx_class()).await;
        assert!(matches!(
            result,
            Err(crate::Error::StorageQuotaExceeded { .. })
        ));
    }
}
