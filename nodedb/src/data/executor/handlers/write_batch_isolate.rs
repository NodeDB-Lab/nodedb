// SPDX-License-Identifier: BUSL-1.1

//! A coalesced batch whose only failures are row-limit refusals.
//!
//! Every put of a coalesced batch is its own autocommit statement, so one
//! row past the row image limit fails that statement alone. The batch's
//! shared transaction is dropped first. Each refused put answers its
//! SQLSTATE 54000 and journals no row. Every other put goes back to the
//! front of the queue, in order, with its record group, and runs again
//! without the refused ones.

use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::data::executor::task::ExecutionTask;

/// Whether `response` refuses a row past a size limit.
fn is_size_refusal(response: &Response) -> bool {
    matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::ProgramLimitExceeded { .. })
    )
}

/// Whether every failure of a batch is a row-limit refusal, and there is at
/// least one.
pub(super) fn size_refusals_only(
    results: &[Result<PointPutOutcome, Response>],
    preparation_error: Option<&(usize, Response)>,
) -> bool {
    let applied = results.iter().all(|result| match result {
        Ok(_) => true,
        Err(response) => is_size_refusal(response),
    });
    let prepared = preparation_error.is_none_or(|(_, response)| is_size_refusal(response));
    let any = results.iter().any(Result::is_err) || preparation_error.is_some();
    applied && prepared && any
}

impl CoreLoop {
    /// Answer each refused put of a batch whose shared transaction is
    /// dropped and whose in-memory effects are reversed, and return every
    /// other put to the queue. Returns the number of puts answered.
    pub(super) fn isolate_size_refusals(
        &mut self,
        batch: Vec<ExecutionTask>,
        groups: Vec<Option<JournalGroup>>,
        results: Vec<Result<PointPutOutcome, Response>>,
        preparation_error: Option<(usize, Response)>,
        journalled: bool,
    ) -> usize {
        let (prepared_index, mut prepared_refusal) = match preparation_error {
            Some((index, response)) => (Some(index), Some(response)),
            None => (None, None),
        };
        let mut refused = Vec::new();
        let mut refused_groups = Vec::new();
        let mut responses = Vec::new();
        let mut again = Vec::new();
        for (index, ((task, group), result)) in
            batch.into_iter().zip(groups).zip(results).enumerate()
        {
            let refusal = match result {
                Err(response) => Some(response),
                Ok(_) if prepared_index == Some(index) => prepared_refusal.take(),
                Ok(_) => None,
            };
            match refusal {
                Some(response) => {
                    // The shared transaction never committed.
                    responses.push((response, crate::diag::LostResponseWrite::RolledBack));
                    refused.push(task);
                    refused_groups.push(group);
                }
                None => {
                    if let Some(group) = group {
                        self.restore_journal_group(&task, group);
                    }
                    again.push(task);
                }
            }
        }
        // A batch journals as one run. When no refused put holds a group,
        // the run ends here with nothing stored, and each put that runs
        // again opens its own.
        if journalled && refused_groups.iter().all(Option::is_none) {
            self.end_empty_journalled();
        }
        let answered = refused.len();
        self.finish_journalled_batch(&refused, refused_groups, responses);
        for task in again.into_iter().rev() {
            self.task_queue.push_front(task);
        }
        answered
    }
}
