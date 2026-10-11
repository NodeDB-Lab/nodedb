// SPDX-License-Identifier: BUSL-1.1

//! Re-issue every restored document row and graph edge as Calvin
//! transactions.
//!
//! Rows go first, then edges, so an edge's endpoints are in place when it
//! installs. Each row goes to its collection's home, each edge to both of its
//! endpoint homes. Every one of them runs in the Calvin sequence, so one
//! RESTORE has one ordering domain. A backup's index entries are not
//! re-issued: every replica derives a row's secondary index entries as it
//! installs the row, exactly as for a committed transaction's row.

use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::target::DatabaseTarget;
use super::commit::{commit_collection, commit_edges};
use super::edges::edge_units;
use super::prepared::{PreparedRows, document_units};

/// The backup sections this re-issue consumes.
pub(in crate::control::backup::restore) struct RestoredRows {
    /// The document rows, prepared and checked against their homes.
    pub documents: Vec<PreparedRows>,
    pub edges: Vec<(String, Vec<u8>)>,
}

/// What the re-issue committed.
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::control::backup::restore) struct RedoReissueStats {
    /// Document sub-records: one per current row, one per version.
    pub documents: usize,
    /// Edge versions, each counted once whatever the number of homes it
    /// re-issues to.
    pub edges: usize,
    /// Calvin transactions committed.
    pub records: usize,
}

/// Re-issue `rows` of `tenant_id`, backed up from `target.source`, durably
/// into `target.dest`. The first error fails the restore.
pub(in crate::control::backup::restore) async fn reissue_rows_and_edges(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    rows: RestoredRows,
) -> crate::Result<RedoReissueStats> {
    let tenant = TenantId::new(tenant_id);
    let mut stats = RedoReissueStats::default();
    let documents = document_units(state, rows.documents)?;
    for collection in documents {
        stats.documents += collection.units.iter().map(|u| u.ops.len()).sum::<usize>();
        stats.records += commit_collection(state, tenant, target.restore_id, collection).await?;
    }
    // Fails the re-issue after the rows committed and before any edge.
    crate::fail_point_err!(
        crate::fail_point::FailScope::Node(state.node_id),
        "restore::reissue::before_edges",
        |detail: String| {
            crate::Error::Internal {
                detail: format!("fail point: {detail}"),
            }
        }
    );
    let (versions, edges) = edge_units(state, tenant_id, target, rows.edges).await?;
    stats.edges = versions;
    // Both homes of an edge commit in one transaction. Each version is
    // absolute per home (a versioned edge key, a first-wins bind), so a
    // retry rewrites both homes to the same state.
    for collection in edges {
        stats.records += commit_edges(state, tenant, target.restore_id, collection).await?;
    }
    Ok(stats)
}
