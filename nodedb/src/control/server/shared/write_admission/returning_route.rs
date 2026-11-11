// SPDX-License-Identifier: BUSL-1.1

//! Whether a data-group leader's write gate sends an entry to the Calvin
//! sequencer because its `RETURNING` list can fail.
//!
//! The direct path commits a write first and evaluates its list after, so an
//! error in the list arrives after the write is stored. Every protocol sends
//! a client write whose list can fail through Calvin before it proposes. The
//! gate is the backstop for every other path: a client entry whose contract
//! can fail routes, whatever proposed it, and its participant evaluates the
//! list before it votes. A list that cannot fail proposes as sent.
//!
//! The decision is the protocols' own (`plan_returning_can_fail`), under
//! this leader's response limits. A proposer on this node runs the gate in
//! its own task, so the columns its statement announced count. A forwarded
//! entry carries no announced columns, and the list's own names stand in.
//!
//! An entry a mirror's applier proposed never routes: its source already
//! committed it. An entry of any other source carries no client contract.
//! No route carries a restore id, so a restore entry never routes.

use crate::control::server::shared::statement_exec::fallible_returning::plan_returning_can_fail;
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::event::EventSource;
use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;

/// Whether `entry`, whose decoded write is `plan` of `tenant_id`, routes to
/// the Calvin sequencer because its `RETURNING` contract can fail.
pub(super) fn routes_for_returning(
    state: &SharedState,
    entry: &ReplicatedEntry,
    tenant_id: TenantId,
    plan: &PhysicalPlan,
) -> bool {
    entry.mirror.is_none()
        && entry.restore_id == 0
        && matches!(EventSource::from(entry.event_source), EventSource::User)
        && plan_returning_can_fail(state, tenant_id, plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_query::expr::{BinaryOp, SqlExpr};
    use nodedb_types::{
        DatabaseId, MirrorEntryStamp, QualifiedCollection, ReturningColumns, ReturningItem,
        ReturningSpec, RlsWriteCheck, Surrogate, Value,
    };

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::shared::write_admission::WriteAdmission;
    use crate::control::server::shared::write_admission::leader_gate::admit_entry;
    use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
    use crate::types::VShardId;
    use crate::wal::WalManager;

    const TENANT: u64 = 1;
    const VSHARD: u32 = 0;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        (state, dir)
    }

    fn column(name: &str) -> ReturningItem {
        ReturningItem::Column {
            name: name.into(),
            alias: None,
        }
    }

    /// `1 / n`: a division can fail.
    fn division() -> ReturningItem {
        let expression = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(1))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Column("n".into())),
        };
        ReturningItem::Computed {
            expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
            name: "q".into(),
        }
    }

    /// A contract over `items`: the full list rides the precommit slot.
    fn contract(items: Vec<ReturningItem>) -> Option<ReturningSpec> {
        let list = ReturningColumns::Named(items);
        Some(ReturningSpec {
            columns: list.clone(),
            precommit: Some(list),
        })
    }

    /// A point insert answering `returning`.
    fn insert(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// The client entry a proposer builds for `plan`.
    fn entry(plan: &PhysicalPlan) -> ReplicatedEntry {
        let write = ReplicableWrite::decide_for_replication(plan).expect("a replicable write");
        to_replicated_entry(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(VSHARD),
            &write,
        )
        .expect("encode")
        .expect("a replicated write")
    }

    /// Whether the gate routes `entry` to the sequencer.
    fn routes(state: &SharedState, entry: &ReplicatedEntry) -> bool {
        matches!(
            admit_entry(state, VShardId::new(VSHARD), entry).expect("the gate admits"),
            WriteAdmission::RouteToCalvin
        )
    }

    #[test]
    fn a_client_entry_whose_list_can_fail_routes() {
        let (state, _dir) = state();
        let entry = entry(&insert(contract(vec![column("id"), division()])));
        assert!(
            routes(&state, &entry),
            "the decoded entry keeps its contract, and the gate routes it"
        );
    }

    #[test]
    fn a_list_that_cannot_fail_or_no_contract_proposes_as_sent() {
        let (state, _dir) = state();
        let cannot_fail = entry(&insert(contract(vec![column("id")])));
        assert!(!routes(&state, &cannot_fail), "`RETURNING id` stays direct");
        let no_contract = entry(&insert(None));
        assert!(!routes(&state, &no_contract));
        let stored_only = entry(&insert(Some(ReturningSpec {
            columns: ReturningColumns::Named(vec![column("id")]),
            precommit: None,
        })));
        assert!(
            !routes(&state, &stored_only),
            "a stored list is no contract"
        );
    }

    #[test]
    fn a_mirror_or_non_client_entry_never_routes() {
        let (state, _dir) = state();
        let plan = insert(contract(vec![column("id"), division()]));
        let tenant = TenantId::new(TENANT);

        let mut mirrored = entry(&plan);
        mirrored.mirror = Some(MirrorEntryStamp {
            source_group: 1,
            source_index: 9,
            cut: 1,
            epoch: 1,
        });
        assert!(!routes_for_returning(&state, &mirrored, tenant, &plan));

        let triggered = entry(&plan).with_event_source(EventSource::Trigger);
        assert!(!routes_for_returning(&state, &triggered, tenant, &plan));

        let mut restored = entry(&plan);
        restored.restore_id = 7;
        assert!(!routes_for_returning(&state, &restored, tenant, &plan));

        assert!(routes_for_returning(&state, &entry(&plan), tenant, &plan));
    }
}
