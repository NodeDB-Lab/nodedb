// SPDX-License-Identifier: BUSL-1.1

//! Re-issue every restored document row and graph edge as committed redo.
//!
//! Rows go first, then edges, so an edge's endpoints are in place when it
//! installs. A backup's index entries are not re-issued: every replica
//! derives a row's secondary index entries as it installs the row, exactly as
//! for a committed transaction's row.

use crate::control::state::SharedState;
use crate::types::{SurrogateBindEntry, TenantId};

use super::commit::commit_collection;
use super::documents::document_units;
use super::edges::edge_units;

/// The backup sections this re-issue consumes.
pub(in crate::control::backup::restore) struct RestoredRows<'a> {
    pub documents: Vec<(String, Vec<u8>)>,
    pub documents_versioned: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, Vec<u8>)>,
    /// The backup's primary-key section: each row's client identity.
    pub binds: &'a [SurrogateBindEntry],
}

/// What the re-issue committed.
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::control::backup::restore) struct RedoReissueStats {
    /// Document sub-records: one per current row, one per version.
    pub documents: usize,
    /// Edge sub-records: one per edge version.
    pub edges: usize,
    /// Redo records committed.
    pub records: usize,
}

/// Re-issue `rows` of `tenant_id` durably. The first error fails the restore.
pub(in crate::control::backup::restore) async fn reissue_rows_and_edges(
    state: &SharedState,
    tenant_id: u64,
    rows: RestoredRows<'_>,
) -> crate::Result<RedoReissueStats> {
    let tenant = TenantId::new(tenant_id);
    let mut stats = RedoReissueStats::default();
    let documents = document_units(
        state,
        tenant_id,
        rows.documents,
        rows.documents_versioned,
        rows.binds,
    )?;
    for collection in documents {
        stats.documents += collection.units.iter().map(|u| u.ops.len()).sum::<usize>();
        stats.records += commit_collection(state, tenant, collection).await?;
    }
    for collection in edge_units(state, tenant_id, rows.edges)? {
        stats.edges += collection.units.iter().map(|u| u.ops.len()).sum::<usize>();
        stats.records += commit_collection(state, tenant, collection).await?;
    }
    Ok(stats)
}
