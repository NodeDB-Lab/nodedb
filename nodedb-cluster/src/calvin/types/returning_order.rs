// SPDX-License-Identifier: BUSL-1.1

//! The order the participants of a Calvin txn evaluate their `RETURNING`
//! lists in.
//!
//! The plans of one request run in statement order, so a later plan's list
//! sees the sequence changes of every earlier plan. Each participant
//! evaluates its own plans. A plan's evaluation on one vShard follows the
//! evaluation of an earlier plan on another vShard when the contract names
//! the earlier plan in the later plan's `sequence_after`. The earlier
//! participant sends its plan's sequence changes to the later one before it
//! votes, and the later one seeds its session with them before it
//! evaluates.
//!
//! The order is `(plan index, vShard)`: a plan's participants follow one
//! another in vShard order when the plan names itself.

use super::returning_contract::{PlanReturning, ReturningContract};

impl ReturningContract {
    /// The entry of the plan at `plan_index`.
    pub fn plan(&self, plan_index: u32) -> Option<&PlanReturning> {
        self.plans.iter().find(|plan| plan.plan_index == plan_index)
    }

    /// The `(plan index, vShard)` participants whose sequence changes the
    /// evaluation of plan `plan_index` on `vshard` follows, ascending. Never
    /// `vshard` itself: its own plans evaluate in order in one session.
    pub fn sequence_sources(&self, plan_index: u32, vshard: u32) -> Vec<(u32, u32)> {
        let Some(plan) = self.plan(plan_index) else {
            return Vec::new();
        };
        let mut sources: Vec<(u32, u32)> = plan
            .sequence_after
            .iter()
            .filter_map(|earlier| self.plan(*earlier))
            .flat_map(|earlier| {
                earlier
                    .vshards
                    .iter()
                    .map(move |source| (earlier.plan_index, *source))
            })
            .filter(|key| key.1 != vshard && *key < (plan_index, vshard))
            .collect();
        sources.sort_unstable();
        sources.dedup();
        sources
    }

    /// The vShards whose evaluation follows the evaluation of plan
    /// `plan_index` on `vshard`, ascending. Never `vshard` itself.
    pub fn sequence_targets(&self, plan_index: u32, vshard: u32) -> Vec<u32> {
        let mut targets: Vec<u32> = self
            .plans
            .iter()
            .filter(|later| later.sequence_after.contains(&plan_index))
            .flat_map(|later| {
                later
                    .vshards
                    .iter()
                    .map(move |target| (later.plan_index, *target))
            })
            .filter(|key| key.1 != vshard && (plan_index, vshard) < *key)
            .map(|(_, target)| target)
            .collect();
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    /// Whether any plan's evaluation follows another plan's sequence
    /// changes.
    pub fn orders_sequences(&self) -> bool {
        self.plans
            .iter()
            .any(|plan| !plan.sequence_after.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{ReturningColumns, ReturningSpec};

    use super::super::returning_contract::ReturningContractLimits;
    use super::*;

    fn entry(plan_index: u32, vshards: &[u32], sequence_after: &[u32]) -> PlanReturning {
        PlanReturning {
            plan_index,
            collection: "c".into(),
            spec: ReturningSpec {
                columns: ReturningColumns::Star,
                precommit: None,
            },
            source_columns: Vec::new(),
            limits: ReturningContractLimits {
                rows: 1,
                columns: 1,
                cells: 1,
                payload_bytes: 1,
            },
            announced: None,
            vshards: vshards.to_vec(),
            sequence_after: sequence_after.to_vec(),
        }
    }

    fn contract(plans: Vec<PlanReturning>) -> ReturningContract {
        ReturningContract {
            plans,
            roles: Vec::new(),
            sequence_values: Vec::new(),
        }
    }

    /// Plan 1 on vShard 5 follows plan 0 on vShard 3: it waits for vShard
    /// 3's changes, and vShard 3 sends them to it.
    #[test]
    fn a_later_plan_on_another_vshard_follows_the_earlier_one() {
        let contract = contract(vec![entry(0, &[3], &[]), entry(1, &[5], &[0])]);
        assert_eq!(contract.sequence_sources(1, 5), vec![(0, 3)]);
        assert_eq!(contract.sequence_targets(0, 3), vec![5]);
        assert!(contract.sequence_sources(0, 3).is_empty());
        assert!(contract.sequence_targets(1, 5).is_empty());
        assert!(contract.orders_sequences());
    }

    /// Plans on one vShard evaluate in order in one session: no plan of it
    /// waits for another of its own.
    #[test]
    fn plans_on_one_vshard_wait_for_nothing() {
        let contract = contract(vec![entry(0, &[3], &[]), entry(1, &[3], &[0])]);
        assert!(contract.sequence_sources(1, 3).is_empty());
        assert!(contract.sequence_targets(0, 3).is_empty());
    }

    /// Plans 0 and 2 on vShard 3 and plan 1 on vShard 5, each after the
    /// last. vShard 3 evaluates plan 0, sends it, and evaluates plan 2 once
    /// plan 1's changes arrive.
    #[test]
    fn interleaved_plans_wait_plan_by_plan() {
        let contract = contract(vec![
            entry(0, &[3], &[]),
            entry(1, &[5], &[0]),
            entry(2, &[3], &[0, 1]),
        ]);
        assert_eq!(contract.sequence_sources(1, 5), vec![(0, 3)]);
        assert_eq!(contract.sequence_sources(2, 3), vec![(1, 5)]);
        assert_eq!(contract.sequence_targets(0, 3), vec![5]);
        assert_eq!(contract.sequence_targets(1, 5), vec![3]);
        assert!(contract.sequence_targets(2, 3).is_empty());
    }

    /// A plan that names itself orders its own participants by vShard.
    #[test]
    fn a_plan_that_follows_itself_orders_its_participants() {
        let contract = contract(vec![entry(0, &[3, 5, 8], &[0])]);
        assert!(contract.sequence_sources(0, 3).is_empty());
        assert_eq!(contract.sequence_sources(0, 8), vec![(0, 3), (0, 5)]);
        assert_eq!(contract.sequence_targets(0, 3), vec![5, 8]);
        assert_eq!(contract.sequence_targets(0, 8), Vec::<u32>::new());
    }

    /// Independent plans order nothing.
    #[test]
    fn independent_plans_order_nothing() {
        let contract = contract(vec![entry(0, &[3], &[]), entry(1, &[5], &[])]);
        assert!(!contract.orders_sequences());
        assert!(contract.sequence_sources(1, 5).is_empty());
        assert!(contract.sequence_targets(0, 3).is_empty());
    }
}
