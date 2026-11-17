// SPDX-License-Identifier: BUSL-1.1

//! Retention prune of the collection purge log.
//!
//! Each sweep tick computes a cutoff from this node's
//! `collection_purge_log_retention_secs` and, when a row on record is older
//! than it, proposes `CatalogEntry::PruneCollectionPurgeLog` carrying the
//! cutoff. Every node prunes at the carried cutoff, so every node keeps the
//! same rows and floors whatever its own retention setting reads.

use tracing::debug;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

/// The prune this node proposes at `now_ms`, if a row is past retention.
pub(super) fn prune_cutoff(oldest_ms: Option<u64>, now_ms: u64, retention_ms: u64) -> Option<u64> {
    let cutoff_ms = now_ms.saturating_sub(retention_ms);
    oldest_ms
        .is_some_and(|oldest| oldest < cutoff_ms)
        .then_some(cutoff_ms)
}

/// Propose a prune when a row on record is past retention. Proposing from
/// every node is idempotent: a prune at a passed cutoff removes nothing.
pub(super) async fn propose_purge_log_prune(shared: &SharedState) {
    let oldest = match shared.credentials.catalog().oldest_collection_purge_ms() {
        Ok(oldest) => oldest,
        Err(error) => {
            crate::diag::purge_log_prune_not_proposed(&error);
            tracing::warn!(%error, "collection-gc: purge log read failed; no prune proposed");
            return;
        }
    };
    let now_ms = crate::control::security::time::now_ms();
    let retention_ms = shared
        .tuning
        .maintenance
        .collection_purge_log_retention_secs
        .saturating_mul(1_000);
    let Some(cutoff_ms) = prune_cutoff(oldest, now_ms, retention_ms) else {
        return;
    };
    let entry = CatalogEntry::PruneCollectionPurgeLog { cutoff_ms };
    match crate::control::metadata_proposer::propose_catalog_entry_async(shared, &entry).await {
        Ok(_) => debug!(cutoff_ms, "collection-gc: proposed purge-log prune"),
        // Another node's sweep proposes the same prune.
        Err(error) => debug!(cutoff_ms, %error, "collection-gc: purge-log prune propose failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prune_is_proposed_only_past_retention() {
        assert_eq!(prune_cutoff(None, 1_000, 100), None);
        assert_eq!(prune_cutoff(Some(950), 1_000, 100), None);
        assert_eq!(prune_cutoff(Some(899), 1_000, 100), Some(900));
        assert_eq!(prune_cutoff(Some(0), 50, 100), None);
    }
}
