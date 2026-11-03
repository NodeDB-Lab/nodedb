// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral post-dispatch read-set recording.
//!
//! Every transport that dispatches a read makes the same captures-aware-or-plain
//! decision once the response returns: a distributed read that materialized on
//! the coordinator (a gathered `HashJoin`, a multi-collection gather, or a
//! shuffle JOIN) records ONE read-set entry per per-collection capture — each
//! from its own single-collection scan plan, so the commit enlists and
//! validates each collection's vShard independently — while every other read
//! records a single collection-scoped entry from the executed plan and the
//! vShards its cores reported. When captures are present the default single-collection entry is
//! SKIPPED, because a `HashJoin` plan collapses to the left collection via
//! `extract_collection` and would miss the build side entirely. This module
//! hosts that decision so all transports funnel through one implementation
//! instead of divergent copies.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::DistributedReadCapture;
use crate::control::state::SharedState;
use crate::types::{ReadShards, TenantId};

use super::connection::SessionId;
use super::read_set::{ReadCapture, record_nested_read_set, record_read_set};
use super::store::SessionStore;

/// The observed reads produced by one dispatched response.
///
/// `plan` / `read_vshards` / `found` describe the plain single-collection
/// observation (used when `distributed_reads` is empty). `read_vshards`
/// names the vShards the read's cores reported.
/// `distributed_reads`, when non-empty, carries the per-collection captures of a
/// distributed gather/shuffle read and takes precedence over the plain fields.
pub struct ResponseReads<'a> {
    pub plan: &'a PhysicalPlan,
    pub read_vshards: &'a ReadShards,
    pub found: bool,
    pub distributed_reads: &'a [DistributedReadCapture],
}

/// Record a dispatched response's reads into the session transaction read-set.
///
/// Protocol-neutral: pgwire and native direct-ops both call this after a read
/// returns. With distributed captures present, records one entry per capture
/// from its own scan plan, plus the reads nested in the executed plan.
/// Otherwise records the executed plan and every read nested in it. Delegates
/// every entry to [`record_read_set`], which drops the entry outside a
/// transaction block.
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
                    read_vshards: &cap.read_vshards,
                    found: false,
                },
            )
            .await;
        }
        record_nested_read_set(
            sessions,
            session_id,
            tenant_id,
            ReadCapture {
                plan: reads.plan,
                read_vshards: reads.read_vshards,
                found: false,
            },
        );
    } else {
        record_read_set(
            state,
            sessions,
            session_id,
            tenant_id,
            ReadCapture {
                plan: reads.plan,
                read_vshards: reads.read_vshards,
                found: reads.found,
            },
        )
        .await;
    }
}
