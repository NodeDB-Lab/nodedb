// SPDX-License-Identifier: BUSL-1.1

//! The rows each plan of a committed Calvin write meters.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::Response;
use crate::control::server::response_shape::types::{
    PlanKind, describe_plan, plan_count_reads_reply,
};
use crate::control::server::shared::sql::staging_predicates::extract_affected_count;

/// The rows each of `plans` meters, in plan order.
///
/// Calvin deposits one reply that holds the total count of the batch. The
/// first plan that reads its count from that reply meters the total. Every
/// other plan meters `None`, which `meter_dispatch` charges as one unit. A
/// batch that answers RETURNING rows has no count to meter.
pub(crate) fn calvin_metered_rows(
    plans: &[&PhysicalPlan],
    apply_result: Option<&Response>,
) -> Vec<Option<u64>> {
    let reads_total: Vec<bool> = plans
        .iter()
        .map(|plan| plan_count_reads_reply(plan))
        .collect();
    let rows_answer = plans
        .iter()
        .any(|plan| matches!(describe_plan(plan), PlanKind::ReturningRows));
    let total =
        apply_result.and_then(|response| extract_affected_count(response.payload.as_bytes()));
    metered_rows(&reads_total, rows_answer, total)
}

/// `calvin_metered_rows` over the classified plans.
fn metered_rows(reads_total: &[bool], rows_answer: bool, total: Option<u64>) -> Vec<Option<u64>> {
    let mut total = if rows_answer { None } else { total };
    reads_total
        .iter()
        .map(|reads| if *reads { total.take() } else { None })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_plan_that_reads_the_total_meters_it() {
        assert_eq!(
            metered_rows(&[false, true, true], false, Some(4)),
            vec![None, Some(4), None]
        );
    }

    #[test]
    fn a_rows_answer_meters_no_count() {
        assert_eq!(metered_rows(&[true], true, Some(4)), vec![None]);
    }

    #[test]
    fn a_batch_with_no_deposited_count_meters_one_unit_per_plan() {
        assert_eq!(metered_rows(&[true, true], false, None), vec![None, None]);
    }

    #[test]
    fn no_plans_meter_nothing() {
        assert!(metered_rows(&[], false, Some(1)).is_empty());
    }
}
