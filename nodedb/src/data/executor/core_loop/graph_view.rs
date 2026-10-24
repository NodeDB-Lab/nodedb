// SPDX-License-Identifier: BUSL-1.1

//! The CSR a graph read walks: the live partition, or a historical CSR of
//! one collection at the read's bitemporal cut.
//!
//! A read handler borrows the core shared, so it cannot build a CSR. The
//! dispatcher calls [`CoreLoop::prepare_csr_view`] first, which builds or
//! reuses the historical CSR. The handler then reads it through
//! [`CoreLoop::csr_view`].

use nodedb_graph::CsrIndex;
use nodedb_physical::physical_plan::{ALL_VERSIONS_REFUSED, GraphReadScope};
use nodedb_types::DatabaseId;

use crate::engine::graph::csr::{HistoricalCsrKey, HistoricalCsrSource};
use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Build or reuse the CSR a read at `scope` walks. A live scope needs
    /// nothing. A historical scope builds the CSR of `collection` at the cut
    /// from the versioned edge store, unless this core's cache holds it.
    pub(in crate::data::executor) fn prepare_csr_view(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: Option<&str>,
        scope: &GraphReadScope,
    ) -> crate::Result<()> {
        let Some(key) = historical_key(database_id, tid, collection, scope)? else {
            return Ok(());
        };
        self.historical_csr.get_or_build(
            key,
            HistoricalCsrSource {
                store: &self.edge_store,
                governor: &self.governor,
                applied_ordinal: self.hlc.peek(),
                max_entries: self.graph_tuning.max_historical_csr_cache_entries,
            },
        )?;
        Ok(())
    }

    /// The CSR a read at `scope` walks. `None` when the tenant has no live
    /// graph state on this core: a read treats that as empty. A historical
    /// scope reads the CSR [`Self::prepare_csr_view`] built.
    pub(in crate::data::executor) fn csr_view(
        &self,
        database_id: u64,
        tid: u64,
        collection: Option<&str>,
        scope: &GraphReadScope,
    ) -> crate::Result<Option<&CsrIndex>> {
        match historical_key(database_id, tid, collection, scope)? {
            None => Ok(self.csr_partition(database_id, tid)),
            Some(key) => {
                self.historical_csr
                    .get(&key)
                    .map(Some)
                    .ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "historical CSR of '{}' at ordinal {} was not prepared before the read",
                            key.collection, key.cutoff_ordinal
                        ),
                    })
            }
        }
    }
}

/// The historical CSR a read at `scope` walks, or `None` for the live CSR.
fn historical_key(
    database_id: u64,
    tid: u64,
    collection: Option<&str>,
    scope: &GraphReadScope,
) -> crate::Result<Option<HistoricalCsrKey>> {
    if scope.is_live() {
        return Ok(None);
    }
    let Some(cutoff_ordinal) = scope.cutoff_ordinal() else {
        return Err(crate::Error::BadRequest {
            detail: ALL_VERSIONS_REFUSED.into(),
        });
    };
    let Some(collection) = collection else {
        return Err(crate::Error::BadRequest {
            detail: "a graph read AS OF a system or valid time must name its collection: \
                     edge versions are stored per collection"
                .into(),
        });
    };
    Ok(Some(HistoricalCsrKey {
        db: DatabaseId::new(database_id),
        tid: TenantId::new(tid),
        collection: collection.to_string(),
        cutoff_ordinal,
        valid_at_ms: scope.valid_at_ms,
    }))
}
