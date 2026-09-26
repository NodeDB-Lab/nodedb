// SPDX-License-Identifier: BUSL-1.1

//! The per-tenant observed write-HLC high-water that RESTORE's staleness gate
//! compares a backup envelope's watermark against.
//!
//! Each mark keeps the write that last raised it: the dispatch site and the
//! collection the plan wrote. A refused restore names that write, so an
//! operator sees which newer data the restore would overwrite.

use std::collections::HashMap;

use super::SharedState;

/// The write that last raised a tenant's high-water.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantWriteOrigin {
    /// The dispatch site that recorded the write.
    pub site: &'static str,
    /// The collection the write's plan named, when it named one.
    pub collection: Option<String>,
}

/// One tenant's observed write high-water.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantWriteMark {
    /// HLC wall time, in nanoseconds, of the newest recorded write.
    pub hlc: u64,
    /// The write that recorded `hlc`.
    pub origin: TenantWriteOrigin,
}

/// Per-tenant marks, keyed by tenant id.
pub type TenantWriteMarks = HashMap<u64, TenantWriteMark>;

impl SharedState {
    /// Raise `tenant_id`'s observed write high-water to `commit_hlc`, the HLC
    /// wall time in nanoseconds at which a user data write committed, and
    /// record `site` and `collection` as its origin. Monotonic: a mark already
    /// at or above `commit_hlc` stays.
    ///
    /// Callers record a write before its ack returns, with the instant the
    /// write committed. A backup taken after the ack then always carries a
    /// watermark at or above the mark, and a write committed after the backup
    /// always carries a newer one.
    ///
    /// A mark carried from another node folds into this node's clock, so a
    /// backup taken here after the write never stamps an older watermark.
    pub fn advance_tenant_write_hlc(
        &self,
        tenant_id: u64,
        commit_hlc: u64,
        site: &'static str,
        collection: Option<&str>,
    ) {
        if let Err(skew) = self
            .hlc_clock
            .update_checked(nodedb_types::Hlc::new(commit_hlc, 0))
        {
            tracing::warn!(
                tenant_id,
                commit_hlc,
                site,
                skew_ns = skew.skew_ns,
                "a write's commit HLC runs ahead of this node's clock past the skew bound; \
                 RESTORE refuses envelopes older than it until the clock catches up"
            );
        }
        // Recover a poisoned lock rather than skipping the advance. The map
        // is a plain `HashMap` that a panic elsewhere cannot corrupt, and a
        // dropped write would leave the staleness gate reading a stale mark.
        let mut marks = self
            .tenant_write_hlc
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if marks
            .get(&tenant_id)
            .is_some_and(|mark| mark.hlc >= commit_hlc)
        {
            return;
        }
        marks.insert(
            tenant_id,
            TenantWriteMark {
                hlc: commit_hlc,
                origin: TenantWriteOrigin {
                    site,
                    collection: collection.map(str::to_owned),
                },
            },
        );
    }

    /// The observed write mark of `tenant_id`, when one is recorded.
    ///
    /// Recovers a poisoned lock. Reporting no mark because the mutex is
    /// poisoned would silently disable the restore staleness gate.
    pub fn tenant_write_mark(&self, tenant_id: u64) -> Option<TenantWriteMark> {
        self.tenant_write_hlc
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&tenant_id)
            .cloned()
    }
}
