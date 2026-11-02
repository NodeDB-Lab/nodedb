// SPDX-License-Identifier: BUSL-1.1

//! The bitemporal cut of a graph read statement.

use nodedb_physical::physical_plan::{ALL_VERSIONS_REFUSED, GraphReadScope, GraphSystemCut};
use nodedb_sql::temporal::TemporalScope;
use nodedb_types::SystemTimeScope;

use crate::control::planner::sql_plan_convert::valid_time_point;
use crate::control::server::shared::session::{DmlTxnCtx, TxnSnapshotCut, ensure_txn_snapshot};
use crate::control::state::SharedState;

use crate::control::read_cut::pin_historical_cut;
use crate::control::version_cleanup::history_window::{GraphHistory, check_graph_history_window};

/// The `GraphReadScope` a statement's `AS OF` clauses select. A system time
/// pins one ordinal for the whole statement ([`pin_historical_cut`]), and
/// the scope carries the cut marker every serving node waits for
/// (`read_cut::wait`).
///
/// `AS OF SYSTEM TIME NULL` is refused: a graph walk reads one graph, and
/// every system-time version of every edge is no graph.
///
/// A cut older than the snapshot age limit on an edge collection that is not
/// bitemporal is refused: the cleanup horizon purges its history
/// (`version_cleanup::check_graph_history_window`). `history` names the
/// collections the walk reads.
pub async fn graph_read_scope(
    state: &SharedState,
    temporal: &TemporalScope,
    history: GraphHistory<'_>,
) -> crate::Result<GraphReadScope> {
    let valid_at_ms = valid_time_point(temporal.valid_time, "graph")?;
    let (system, cut_marker) = match temporal.system_time {
        SystemTimeScope::Current => (GraphSystemCut::Live, 0),
        SystemTimeScope::AsOf(ms) => {
            let cut = pin_historical_cut(state, ms).await?;
            check_graph_history_window(state, history, cut.ordinal)?;
            (GraphSystemCut::AtOrdinal(cut.ordinal), cut.marker)
        }
        // A cut the statement already pinned reads as pinned.
        SystemTimeScope::Pinned {
            ordinal,
            cut_marker,
        } => {
            check_graph_history_window(state, history, ordinal)?;
            (GraphSystemCut::AtOrdinal(ordinal), cut_marker)
        }
        // A transaction block's snapshot reads the edges at its ordinal with
        // the transaction's staged edge writes merged on top.
        SystemTimeScope::TxnSnapshot {
            ordinal,
            cut_marker,
        } => (GraphSystemCut::TxnSnapshot(ordinal), cut_marker),
        SystemTimeScope::AllVersions => return Err(all_versions_refused()),
    };
    Ok(GraphReadScope {
        system,
        valid_at_ms,
        cut_marker,
    })
}

/// The temporal scope a graph read statement of a transaction block reads
/// at. Outside a block `temporal` is returned as is. Inside one, a statement
/// that names no system time reads the block's snapshot, and an `AS OF
/// SYSTEM TIME` pins under the snapshot, never past it. The block's first
/// statement takes the snapshot ([`ensure_txn_snapshot`]).
pub async fn block_graph_temporal(
    state: &SharedState,
    txn_ctx: &DmlTxnCtx<'_>,
    temporal: TemporalScope,
) -> crate::Result<TemporalScope> {
    let snapshot = ensure_txn_snapshot(txn_ctx.sessions, txn_ctx.session_id, state).await?;
    Ok(in_block_temporal(temporal, snapshot))
}

/// `temporal` read inside a block whose snapshot is `snapshot`. `None` reads
/// outside a block.
fn in_block_temporal(temporal: TemporalScope, snapshot: Option<TxnSnapshotCut>) -> TemporalScope {
    let Some(cut) = snapshot else {
        return temporal;
    };
    let system_time = match temporal.system_time {
        SystemTimeScope::Current => cut.scope(),
        SystemTimeScope::AsOf(ms) => {
            let pinned = cut.statement_cap().pin(ms);
            SystemTimeScope::Pinned {
                ordinal: pinned.ordinal,
                cut_marker: pinned.marker,
            }
        }
        SystemTimeScope::Pinned { ordinal, .. } if ordinal > cut.ordinal => {
            SystemTimeScope::Pinned {
                ordinal: cut.ordinal,
                cut_marker: cut.cut_marker,
            }
        }
        scope @ (SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. }
        | SystemTimeScope::AllVersions) => scope,
    };
    TemporalScope {
        system_time,
        ..temporal
    }
}

/// Refuse a plan scope a graph read cannot run at.
pub fn check_walk_scope(scope: &GraphReadScope) -> crate::Result<()> {
    match scope.system {
        GraphSystemCut::AllVersions => Err(all_versions_refused()),
        GraphSystemCut::Live | GraphSystemCut::AtOrdinal(_) | GraphSystemCut::TxnSnapshot(_) => {
            Ok(())
        }
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
    fn a_block_statement_reads_the_snapshot_and_pins_under_it() {
        let cut = TxnSnapshotCut {
            ordinal: 1_700_000_000_500 * nodedb_types::NANOS_PER_MS,
            cut_marker: 4,
        };
        let temporal = |system_time| TemporalScope {
            system_time,
            valid_time: nodedb_sql::temporal::ValidTime::Any,
        };
        let live = in_block_temporal(temporal(SystemTimeScope::Current), Some(cut));
        assert_eq!(live.system_time, cut.scope());

        let future = in_block_temporal(
            temporal(SystemTimeScope::AsOf(1_800_000_000_000)),
            Some(cut),
        );
        assert_eq!(
            future.system_time,
            SystemTimeScope::Pinned {
                ordinal: cut.ordinal,
                cut_marker: 4,
            },
            "a time past the snapshot reads at the snapshot"
        );

        let outside = in_block_temporal(temporal(SystemTimeScope::Current), None);
        assert_eq!(outside.system_time, SystemTimeScope::Current);
    }

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
