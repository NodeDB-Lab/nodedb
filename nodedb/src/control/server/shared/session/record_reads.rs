// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral post-dispatch read-set recording.
//!
//! Every transport that dispatches a read makes the same captures-aware-or-plain
//! decision once the response returns: a distributed read that materialized on
//! the coordinator (a gathered `HashJoin`, a multi-collection gather, or a
//! shuffle JOIN) records ONE read-set entry per per-collection capture — each
//! from its own single-collection scan plan and REAL observed read-version, so
//! the commit-time OCC validator re-homes and revalidates each collection's
//! vshard independently — while every other read records a single
//! collection-scoped entry from the executed plan and the versions its cores
//! reported. When captures are present the default single-collection entry is
//! SKIPPED, because a `HashJoin` plan collapses to the left collection via
//! `extract_collection` and would miss the build side entirely. This module
//! hosts that decision so all transports funnel through one implementation
//! instead of divergent copies.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::DistributedReadCapture;
use crate::control::state::SharedState;
use crate::types::{ReadVersions, TenantId};

use super::connection::SessionId;
use super::read_set::{ReadCapture, record_read_set};
use super::store::SessionStore;

/// The observed reads produced by one dispatched response.
///
/// `plan` / `read_versions` / `found` describe the plain single-collection
/// observation (used when `distributed_reads` is empty).
/// `distributed_reads`, when non-empty, carries the per-collection captures of a
/// distributed gather/shuffle read and takes precedence over the plain fields.
pub struct ResponseReads<'a> {
    pub plan: &'a PhysicalPlan,
    pub read_versions: &'a ReadVersions,
    pub found: bool,
    pub distributed_reads: &'a [DistributedReadCapture],
}

/// Record a dispatched response's reads into the session transaction read-set.
///
/// Protocol-neutral: pgwire and native direct-ops both call this after a read
/// returns. With distributed captures present, records one entry per capture
/// from its own scan plan and read-version; otherwise records the single plain
/// entry. Delegates every entry to [`record_read_set`], which still applies the
/// session's own-write floor and drops the entry outside a transaction block.
pub async fn record_reads_for_response(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    reads: ResponseReads<'_>,
) {
    if !reads.distributed_reads.is_empty() {
        for cap in reads.distributed_reads {
            record_read_set(
                state,
                sessions,
                session_id,
                tenant_id,
                ReadCapture {
                    plan: &cap.scan_plan,
                    read_versions: &cap.read_versions,
                    found: false,
                },
            )
            .await;
        }
    } else {
        record_read_set(
            state,
            sessions,
            session_id,
            tenant_id,
            ReadCapture {
                plan: reads.plan,
                read_versions: reads.read_versions,
                found: reads.found,
            },
        )
        .await;
    }
}
