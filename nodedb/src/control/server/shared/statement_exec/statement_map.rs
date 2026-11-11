// SPDX-License-Identifier: BUSL-1.1

//! The statement each planned task belongs to.
//!
//! A request plans as one task set. The map holds one statement index per
//! task, in task order. A derived task (an implicit edge, a balance move)
//! takes the statement of the task whose processing appended it.

use nodedb_physical::physical_task::PhysicalTask;

/// One statement index per task, in task order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementMap(Vec<u32>);

impl StatementMap {
    /// The map of a request of one statement that plans `tasks` tasks.
    pub fn single(tasks: usize) -> Self {
        Self(vec![0; tasks])
    }

    /// Add one task of `statement` at the end.
    pub fn push(&mut self, statement: u32) {
        self.0.push(statement);
    }

    /// The number of tasks the map covers.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map covers no task.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The statement of `task`, or `None` for an index past the end.
    pub fn statement_of(&self, task: usize) -> Option<u32> {
        self.0.get(task).copied()
    }

    /// One past the highest statement index, or 0 for an empty map.
    /// A trailing statement that plans no task is not counted.
    pub fn statement_count(&self) -> usize {
        self.0
            .iter()
            .max()
            .map_or(0, |highest| *highest as usize + 1)
    }

    /// Add one entry per derived task, in append order.
    ///
    /// `parents[i]` is the index of the task whose processing appended
    /// derived task `i`. Each derived task takes its parent's statement.
    /// A parent must sit before its child, so it has an entry when the
    /// child is pushed. A parent with no entry returns an internal error.
    pub fn push_derived(&mut self, parents: &[usize]) -> crate::Result<()> {
        for &parent in parents {
            let Some(statement) = self.statement_of(parent) else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "derived task {} names parent task {parent}, which has no statement",
                        self.len()
                    ),
                });
            };
            self.push(statement);
        }
        Ok(())
    }

    /// Debug check: the map holds one entry per task.
    pub fn debug_check_covers(&self, tasks: &[PhysicalTask]) {
        debug_assert_eq!(
            self.len(),
            tasks.len(),
            "the statement map must hold one entry per task"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_gives_every_task_statement_zero() {
        let map = StatementMap::single(3);
        assert_eq!(map.len(), 3);
        assert_eq!(map, StatementMap(vec![0, 0, 0]));
        assert_eq!(map.statement_count(), 1);
        assert!(StatementMap::single(0).is_empty());
        assert_eq!(StatementMap::single(0).statement_count(), 0);
    }

    #[test]
    fn a_derived_task_takes_its_parent_statement() {
        // Statement 0 is task 0, statement 1 is task 1.
        let mut map = StatementMap::default();
        map.push(0);
        map.push(1);
        // Task 2 derives from task 1, task 3 from task 0, task 4 from task 2.
        map.push_derived(&[1, 0, 2])
            .expect("every parent precedes its child");
        assert_eq!(map, StatementMap(vec![0, 1, 1, 0, 1]));
        assert_eq!(map.statement_of(2), Some(1));
        assert_eq!(map.statement_of(5), None);
        assert_eq!(map.statement_count(), 2);
    }

    #[test]
    fn a_parent_after_its_child_is_an_error() {
        let mut map = StatementMap::single(1);
        let error = map
            .push_derived(&[1])
            .expect_err("task 1 has no entry when it is pushed");
        assert!(
            matches!(error, crate::Error::Internal { .. }),
            "unexpected error: {error:?}"
        );
    }
}
