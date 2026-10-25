// SPDX-License-Identifier: BUSL-1.1

//! The bitemporal cut of a graph read statement.

use nodedb_physical::physical_plan::{ALL_VERSIONS_REFUSED, GraphReadScope, GraphSystemCut};
use nodedb_sql::temporal::TemporalScope;
use nodedb_types::SystemTimeScope;

use crate::control::planner::sql_plan_convert::valid_time_point;
use crate::control::state::SharedState;

use crate::control::read_cut::pin_historical_cut;

/// The `GraphReadScope` a statement's `AS OF` clauses select. A system time
/// pins one ordinal for the whole statement ([`pin_historical_cut`]), and
/// the scope carries the cut marker every serving node waits for
/// (`read_cut::wait`).
///
/// `AS OF SYSTEM TIME NULL` is refused: a graph walk reads one graph, and
/// every system-time version of every edge is no graph.
pub async fn graph_read_scope(
    state: &SharedState,
    temporal: &TemporalScope,
) -> crate::Result<GraphReadScope> {
    let valid_at_ms = valid_time_point(temporal.valid_time, "graph")?;
    let (system, cut_marker) = match temporal.system_time {
        SystemTimeScope::Current => (GraphSystemCut::Live, 0),
        SystemTimeScope::AsOf(ms) => {
            let cut = pin_historical_cut(state, ms).await?;
            (GraphSystemCut::AtOrdinal(cut.ordinal), cut.marker)
        }
        // A cut the statement already pinned reads as pinned.
        SystemTimeScope::Pinned {
            ordinal,
            cut_marker,
        } => (GraphSystemCut::AtOrdinal(ordinal), cut_marker),
        SystemTimeScope::AllVersions => return Err(all_versions_refused()),
    };
    Ok(GraphReadScope {
        system,
        valid_at_ms,
        cut_marker,
    })
}

/// Refuse a plan scope a graph read cannot run at.
pub fn check_walk_scope(scope: &GraphReadScope) -> crate::Result<()> {
    match scope.system {
        GraphSystemCut::AllVersions => Err(all_versions_refused()),
        GraphSystemCut::Live | GraphSystemCut::AtOrdinal(_) => Ok(()),
    }
}

fn all_versions_refused() -> crate::Error {
    crate::Error::BadRequest {
        detail: ALL_VERSIONS_REFUSED.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_versions_is_refused_on_a_walk() {
        let scope = GraphReadScope {
            system: GraphSystemCut::AllVersions,
            valid_at_ms: None,
            cut_marker: 0,
        };
        assert!(matches!(
            check_walk_scope(&scope),
            Err(crate::Error::BadRequest { .. })
        ));
        assert!(check_walk_scope(&GraphReadScope::default()).is_ok());
    }
}
