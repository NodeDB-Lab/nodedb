// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns raw payload bytes, used by the CRDT delta path.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{MintedRecords, RecordOwner};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use crate::event::EventSource;
use crate::types::VShardId;

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::outcome::SyncDispatchOutcome;
use super::propose::propose_sync_write;

/// Dispatch a sync write and return the apply payload plus what CRDT admission
/// measured about the delta. Cluster path proposes through Raft; single-node
/// falls through to `sync_dispatch::dispatch_system_with_source`.
pub async fn dispatch_sync_bytes(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
    policy: &dyn crate::control::crdt_admission::CrdtPostImagePolicy,
) -> crate::Result<SyncDispatchOutcome> {
    // Sync inbound envelope carries no session database, so scoped to the default database.
    if matches!(
        authorized.plan(),
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
        )
    ) {
        let outcome =
            crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted_outcome(
                state,
                crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
                    authorized,
                    collection,
                    timeout,
                    event_source,
                    policy,
                },
            )
            .await?;
        return Ok(SyncDispatchOutcome {
            payload: outcome.payload,
            trimmed_ops: outcome.trimmed_ops,
        });
    }
    // Mints no redo of its own — the admitted-apply path and Raft entry own durability.
    dispatch_write_replicated(state, collection, authorized, timeout, event_source, None)
        .await
        .map(SyncDispatchOutcome::untrimmed)
}

/// Dispatch a write so it is quorum-durable when the node is clustered.
///
/// `minted` holds the redo records the caller already appended, under their
/// outcome-floor window. Cluster path proposes through Raft and blocks until
/// applied locally; the Raft entry's apply appends its own records, so the
/// caller's records are cancelled. Single-node path installs the write,
/// closes the records from its outcome, and waits until they are durable.
pub(crate) async fn dispatch_write_replicated(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
    minted: Option<MintedRecords>,
) -> crate::Result<Vec<u8>> {
    let task = authorized.into_physical_task();
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
    let vshard_id = task.vshard_id;
    let plan = task.plan;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    let refused = reject_unadmitted_crdt_apply(&plan).and_then(|()| {
        if vshard_id == VShardId::from_collection_in_database(database_id, collection) {
            Ok(())
        } else {
            Err(crate::Error::Internal {
                detail: "authorized sync task vShard does not match collection".into(),
            })
        }
    });
    if let Err(error) = refused {
        // Nothing was dispatched.
        if let Some(minted) = minted {
            minted.cancel(&state.wal, owner, 0).await?;
        }
        return Err(error);
    }
    let local_frontier_mutation = matches!(
        &plan,
        PhysicalPlan::Crdt(op) if crate::control::crdt_admission::changes_crdt_frontier(op)
    );

    if let Some(proposer) = state.async_raft_proposer() {
        let entry = ReplicableWrite::decide_for_replication(&plan).and_then(|replicable| {
            to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)
        });
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                if let Some(minted) = minted {
                    minted.cancel(&state.wal, owner, 0).await?;
                }
                return Err(error);
            }
        };
        if let Some(entry) = entry {
            // The Raft entry's apply appends its own records.
            let superseded = minted.map(|minted| {
                minted.supersede(std::sync::Arc::clone(&state.wal), owner, "raft_proposal")
            });
            let proposed = propose_sync_write(state, entry, proposer).await;
            if let Some(superseded) = superseded {
                superseded.finish().await;
            }
            return proposed;
        }
    }

    let wal_lsn = minted.as_ref().and_then(MintedRecords::highest);
    let task = crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
        crate::control::server::shared::ddl::sync_dispatch::SystemReason::AdmittedContinuation,
        tenant_id,
        database_id,
        collection,
        plan,
    );
    let resp = if local_frontier_mutation {
        // The sequencer can refuse before it runs the dispatch. The records
        // stay here until the dispatch takes them, so such a refusal cancels
        // them: nothing reached a core.
        let unsent = std::sync::Mutex::new(minted);
        let run = state
            .vshard_admission_sequencer
            .run(vshard_id, || async {
                let minted = unsent.lock().unwrap_or_else(|p| p.into_inner()).take();
                let task = match minted {
                    Some(minted) => task.with_minted(minted),
                    None => task,
                };
                crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
                    state,
                    task,
                    timeout,
                    event_source,
                )
                .await
            })
            .await;
        match run {
            Ok(resp) => resp,
            Err(error) => {
                let never_sent = unsent.into_inner().unwrap_or_else(|p| p.into_inner());
                if let Some(minted) = never_sent {
                    minted.cancel(&state.wal, owner, 0).await?;
                }
                return Err(error);
            }
        }
    } else {
        let task = match minted {
            Some(minted) => task.with_minted(minted),
            None => task,
        };
        crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
            state,
            task,
            timeout,
            event_source,
        )
        .await?
    };

    // Rejection short-circuits here with its typed error code preserved, so the
    // CRDT delta path builds a precise compensation hint instead of
    // substring-matching a message.
    let payload = payload_or_typed_error(resp)?;

    // System-task dispatch bypasses the write funnel's own durable-at-ack barrier —
    // without this fsync, `kill -9` erases an acked write.
    if let Some(lsn) = wal_lsn {
        state.wal.wait_durable(lsn).await?;
    }

    // Mirrors `dispatch_system_with_source`'s success-path write-HLC advance.
    state.advance_tenant_write_hlc(tenant_id.as_u64());
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::super::durability_test_support::{
        COLLECTION, append_buffered_record, authorized_write, fixture, minted_buffered_record,
        respond_once,
    };
    use super::dispatch_write_replicated;
    use crate::event::EventSource;

    /// Guards the single-node branch's durable-at-ack barrier: without the wait
    /// below, the record is still only buffered when the peer hears "applied".
    #[tokio::test]
    async fn a_supplied_lsn_is_fsync_durable_before_the_payload_returns() {
        let (state, side, _directory) = fixture();
        let (minted, lsn) = minted_buffered_record(&state);
        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "the append must only buffer, or this test proves nothing"
        );
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            Duration::from_secs(5),
            EventSource::CrdtSync,
            Some(minted),
        )
        .await
        .expect("replicated sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() >= lsn.as_u64(),
            "the supplied redo must be fsync-durable before the peer is acked"
        );
    }

    /// A caller that appended nothing has nothing to wait on, which is what
    /// makes the assertion above a statement about the threaded LSN.
    #[tokio::test]
    async fn no_supplied_lsn_leaves_an_unrelated_buffered_record_alone() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            Duration::from_secs(5),
            EventSource::CrdtSync,
            None,
        )
        .await
        .expect("replicated sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "nothing appended by this dispatch means nothing to fsync"
        );
    }

    /// The admission sequencer refuses a frontier write before it runs the
    /// dispatch. Nothing reached a core, so the caller's records are
    /// cancelled and their window settles.
    #[tokio::test]
    async fn a_sequencer_refusal_cancels_the_unsent_records() {
        use super::super::durability_test_support::{authorized_plan, vshard};

        let (state, _side, _directory) = fixture();
        let (minted, lsn) = minted_buffered_record(&state);
        let sequencer = Arc::clone(&state.vshard_admission_sequencer);
        let holders: Vec<_> = (0..crate::control::vshard_admission::VSHARD_ADMISSION_CAPACITY)
            .map(|_| {
                let sequencer = Arc::clone(&sequencer);
                tokio::spawn(async move {
                    sequencer
                        .run(vshard(), std::future::pending::<crate::Result<()>>)
                        .await
                })
            })
            .collect();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let authorized = authorized_plan(
            &state,
            crate::bridge::envelope::PhysicalPlan::Crdt(
                nodedb_physical::physical_plan::CrdtOp::DocDelete {
                    collection: nodedb_types::QualifiedCollection::new(
                        crate::types::DatabaseId::DEFAULT,
                        COLLECTION,
                    ),
                    document_id: "d1".into(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    returning: None,
                    rls_filters: Vec::new(),
                },
            ),
        );

        let result = dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            Duration::from_secs(5),
            EventSource::CrdtSync,
            Some(minted),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(crate::Error::VShardAdmissionCapacityExceeded { .. })
            ),
            "got {result:?}"
        );
        assert!(state.outcome_floor.floor() >= lsn);
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
        for holder in holders {
            holder.abort();
        }
    }
}
