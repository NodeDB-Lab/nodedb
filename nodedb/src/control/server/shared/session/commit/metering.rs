// SPDX-License-Identifier: BUSL-1.1

//! Usage metering for the writes a COMMIT replayed from its durable batch.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::metering::meter_buffered_write;
use crate::control::server::shared::sql::staging_predicates::is_stageable_write;
use crate::control::state::SharedState;

/// Meter every non-stageable ("Buffered") write in `buffered`, once its
/// COMMIT-time durable replay has already succeeded.
///
/// Skips any task `is_stageable_write` still classifies as stageable: that
/// task was already metered at STATEMENT time when it staged into the
/// per-transaction overlay (`staging_gate::stage_write`) — it is buffered
/// here too (COMMIT replays every write, staged or not, from the one durable
/// batch), but billing it again here would double-count it. Re-deriving the
/// predicate here, rather than carrying a "was this staged" flag on
/// `PhysicalTask`, keeps this in lockstep with `route_in_tx_write`'s own
/// routing decision by construction — the two can never independently drift.
///
/// Each task metered independently (its own collection/engine, `rows: None`
/// — one unit per write, matching every other door's convention for a
/// dispatch whose response carries no row payload to count).
pub(super) fn meter_committed_buffered_writes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    buffered: &[PhysicalTask],
) {
    if !state.metering_config.enabled {
        return;
    }
    for task in buffered {
        if is_stageable_write(&task.plan) {
            continue;
        }
        let scope = RequestAuthScope::builder(identity, state.auth_stores())
            .with_session_database(Some(task.database_id))
            .build();
        meter_buffered_write(state, &scope, &task.plan);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;
    use nodedb_physical::physical_task::PostSetOp;

    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::WalManager;

    use super::*;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            1,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    fn buffered_task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::from_collection_in_database(DatabaseId::DEFAULT, "widgets"),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// `KvOp::Put` is on `is_stageable_write`'s allow-list — a real
    /// `Staged` route would already have billed it at STATEMENT time
    /// (`staging_gate::stage_write`), so a task shaped like this must be
    /// skipped here or COMMIT would double-bill it.
    fn stageable_task() -> PhysicalTask {
        buffered_task(PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "widgets"),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        }))
    }

    /// `KvOp::Get` is not on `is_stageable_write`'s allow-list, so this
    /// stands in for the non-stageable ("Buffered") route's shape — the one
    /// `meter_committed_buffered_writes` must bill.
    fn non_stageable_task() -> PhysicalTask {
        buffered_task(PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "widgets"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }))
    }

    #[test]
    fn meters_only_non_stageable_tasks() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = identity();
        let buffered = vec![stageable_task(), non_stageable_task()];

        meter_committed_buffered_writes(&state, &identity, &buffered);

        let events = state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "the stageable task was already billed at statement time and must be skipped here"
        );
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    #[test]
    fn records_nothing_when_metering_disabled() {
        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = identity();
        let buffered = vec![non_stageable_task()];

        meter_committed_buffered_writes(&state, &identity, &buffered);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn records_nothing_for_an_empty_buffer() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = identity();

        meter_committed_buffered_writes(&state, &identity, &[]);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }
}
