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

    /// Put each statement's tasks next to each other, in statement order.
    ///
    /// `items` holds one item per task, in task order. Items of one statement
    /// keep their order. A derived task sits at the end of the set until this
    /// runs, so it follows the tasks of its own statement and precedes every
    /// later statement's tasks afterwards. The map is reordered with them.
    pub fn group_by_statement<T>(&mut self, items: Vec<T>) -> Vec<T> {
        debug_assert_eq!(self.len(), items.len());
        let mut keyed: Vec<(u32, T)> = self.0.iter().copied().zip(items).collect();
        keyed.sort_by_key(|(statement, _)| *statement);
        self.0 = keyed.iter().map(|(statement, _)| *statement).collect();
        keyed.into_iter().map(|(_, item)| item).collect()
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

    /// Group `items`, one per task in task order, by statement.
    ///
    /// The result holds one list per statement: at least `statement_count`
    /// lists, and one more than the highest statement the map names. A task
    /// with no entry in the map returns an internal error.
    pub fn group<T>(
        &self,
        items: impl IntoIterator<Item = T>,
        statement_count: usize,
    ) -> crate::Result<StatementGroups<T>> {
        let count = statement_count.max(self.statement_count());
        let mut by_statement: Vec<Vec<T>> = (0..count).map(|_| Vec::new()).collect();
        let mut task_statements = Vec::with_capacity(self.len());
        for (task, item) in items.into_iter().enumerate() {
            let statement = self.statement_of(task).map(|statement| statement as usize);
            // Every entry is below `count`, so a mapped task finds its list.
            let Some((statement, list)) = statement
                .and_then(|statement| by_statement.get_mut(statement).map(|l| (statement, l)))
            else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "task {task} has no statement: the statement map holds {} entries",
                        self.len()
                    ),
                });
            };
            list.push(item);
            task_statements.push(statement);
        }
        Ok(StatementGroups {
            by_statement,
            task_statements,
        })
    }
}

/// Per-task items grouped by statement. See [`StatementMap::group`].
#[derive(Debug)]
pub struct StatementGroups<T> {
    /// One list per statement, in statement order. A statement that planned
    /// no task has an empty list.
    pub by_statement: Vec<Vec<T>>,
    /// The statement of each task, in task order.
    pub task_statements: Vec<usize>,
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
    fn a_derived_task_moves_next_to_its_statement() {
        // Tasks 0 and 1 are statements 0 and 1. Task 2 derives from task 0.
        let mut map = StatementMap::default();
        map.push(0);
        map.push(1);
        map.push_derived(&[0])
            .expect("the parent precedes its child");
        let tasks = map.group_by_statement(vec!["write0", "write1", "derived0"]);
        assert_eq!(tasks, ["write0", "derived0", "write1"]);
        assert_eq!(map, StatementMap(vec![0, 0, 1]));
    }

    #[test]
    fn group_gives_each_statement_its_own_tasks() {
        let map = StatementMap(vec![0, 1, 0]);
        let groups = map
            .group(["a", "b", "c"], 3)
            .expect("every task has a statement");
        assert_eq!(groups.by_statement, [vec!["a", "c"], vec!["b"], vec![]]);
        assert_eq!(groups.task_statements, [0, 1, 0]);
    }

    #[test]
    fn group_refuses_a_task_with_no_statement() {
        let error = StatementMap::single(1)
            .group(["a", "b"], 1)
            .expect_err("task 1 has no entry");
        assert!(
            matches!(error, crate::Error::Internal { .. }),
            "unexpected error: {error:?}"
        );
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
