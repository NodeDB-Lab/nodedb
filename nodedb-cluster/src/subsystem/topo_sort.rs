//! Topological sort for the subsystem dependency graph.
//!
//! Uses Kahn's algorithm (BFS-based). Returns indices into the original
//! slice in a valid start order, or a `TopoError` describing the problem.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use super::errors::TopoError;
use super::r#trait::ClusterSubsystem;

/// Topo-sort `subsystems` by their declared dependencies.
///
/// Returns a `Vec<usize>` where each element is an index into `subsystems`
/// and the order represents a valid start sequence (dependencies before
/// dependents).
///
/// # Errors
///
/// - `TopoError::UnknownDependency` — a subsystem lists a dependency name
///   that is not present in `subsystems`.
/// - `TopoError::Cycle` — a circular dependency was detected.
pub fn topo_sort(subsystems: &[Arc<dyn ClusterSubsystem>]) -> Result<Vec<usize>, TopoError> {
    // Build name → index map.
    let name_to_idx: HashMap<&'static str, usize> = subsystems
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name(), i))
        .collect();

    // Validate all dependency names and build adjacency (dep → dependent)
    // plus in-degree per node.
    let n = subsystems.len();
    let mut in_degree = vec![0usize; n];
    // adjacency[i] = list of indices that depend on i (i must start before them)
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (i, subsystem) in subsystems.iter().enumerate() {
        for &dep_name in subsystem.dependencies() {
            let dep_idx =
                name_to_idx
                    .get(dep_name)
                    .copied()
                    .ok_or(TopoError::UnknownDependency {
                        subsystem: subsystem.name(),
                        dependency: dep_name,
                    })?;
            // dep_idx must come before i
            adjacency[dep_idx].push(i);
            in_degree[i] += 1;
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();

    let mut order = Vec::with_capacity(n);

    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        for &dependent in &adjacency[idx] {
            in_degree[dependent] -= 1;
            if in_degree[dependent] == 0 {
                queue.push_back(dependent);
            }
        }
    }

    if order.len() != n {
        // Some nodes still have non-zero in-degree — there is a cycle.
        // Collect the names of nodes still in the cycle for diagnostics.
        let cycle_names: Vec<&'static str> = in_degree
            .iter()
            .enumerate()
            .filter_map(|(i, &deg)| {
                if deg > 0 {
                    Some(subsystems[i].name())
                } else {
                    None
                }
            })
            .collect();
        return Err(TopoError::Cycle { cycle: cycle_names });
    }

    Ok(order)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use async_trait::async_trait;
    use tokio::sync::watch;

    use super::*;
    use crate::subsystem::context::BootstrapCtx;
    use crate::subsystem::errors::{BootstrapError, ShutdownError};
    use crate::subsystem::health::SubsystemHealth;
    use crate::subsystem::r#trait::{ClusterSubsystem, SubsystemHandle};

    struct MockSubsystem {
        name: &'static str,
        deps: &'static [&'static str],
    }

    #[async_trait]
    impl ClusterSubsystem for MockSubsystem {
        fn name(&self) -> &'static str {
            self.name
        }

        fn dependencies(&self) -> &'static [&'static str] {
            self.deps
        }

        async fn start(&self, _ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
            let (tx, _rx) = watch::channel(false);
            let handle = tokio::spawn(async {});
            Ok(SubsystemHandle::new(self.name, handle, tx))
        }

        async fn shutdown(&self, _deadline: Instant) -> Result<(), ShutdownError> {
            Ok(())
        }

        fn health(&self) -> SubsystemHealth {
            SubsystemHealth::Running
        }
    }

    fn mock(name: &'static str, deps: &'static [&'static str]) -> Arc<dyn ClusterSubsystem> {
        Arc::new(MockSubsystem { name, deps })
    }

    #[test]
    fn empty_list() {
        let result = topo_sort(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn single_no_deps() {
        let subsystems = vec![mock("alpha", &[])];
        let order = topo_sort(&subsystems).unwrap();
        assert_eq!(order, vec![0]);
    }

    #[test]
    fn linear_chain() {
        // a -> b -> c  (b depends on a, c depends on b)
        let subsystems = vec![mock("a", &[]), mock("b", &["a"]), mock("c", &["b"])];
        let order = topo_sort(&subsystems).unwrap();
        // a must come before b, b before c
        let pos: Vec<usize> = ["a", "b", "c"]
            .iter()
            .map(|name| {
                order
                    .iter()
                    .position(|&i| subsystems[i].name() == *name)
                    .unwrap()
            })
            .collect();
        assert!(pos[0] < pos[1], "a must precede b");
        assert!(pos[1] < pos[2], "b must precede c");
    }

    #[test]
    fn two_independent_roots() {
        let subsystems = vec![mock("x", &[]), mock("y", &[])];
        let order = topo_sort(&subsystems).unwrap();
        // Both must appear, order between them is undefined.
        assert_eq!(order.len(), 2);
        assert!(order.contains(&0));
        assert!(order.contains(&1));
    }

    #[test]
    fn diamond_dependency() {
        // a -> b -> d
        // a -> c -> d
        let subsystems = vec![
            mock("a", &[]),
            mock("b", &["a"]),
            mock("c", &["a"]),
            mock("d", &["b", "c"]),
        ];
        let order = topo_sort(&subsystems).unwrap();
        assert_eq!(order.len(), 4);
        let pos = |name: &str| {
            order
                .iter()
                .position(|&i| subsystems[i].name() == name)
                .unwrap()
        };
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
    }

    #[test]
    fn cycle_detected() {
        let subsystems = vec![mock("x", &["y"]), mock("y", &["x"])];
        let err = topo_sort(&subsystems).unwrap_err();
        assert!(matches!(err, TopoError::Cycle { .. }));
    }

    #[test]
    fn missing_dependency() {
        let subsystems = vec![mock("a", &["nonexistent"])];
        let err = topo_sort(&subsystems).unwrap_err();
        assert!(
            matches!(err, TopoError::UnknownDependency { dependency, .. } if dependency == "nonexistent")
        );
    }
}
