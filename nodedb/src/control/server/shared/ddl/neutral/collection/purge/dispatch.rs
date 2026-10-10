// SPDX-License-Identifier: BUSL-1.1

//! Result-checked `UnregisterCollection` fan-out to every local Data Plane core.
//!
//! Collection-scoped state is present on multiple cores (including aggregate
//! caches and engine-local segments), so purge completion means every core has
//! acknowledged reclaim. All requests are dispatched before responses are
//! awaited; any send, timeout, channel, or handler failure aborts the barrier.

use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

/// Dispatch `MetaOp::UnregisterCollection` to every local core and require an
/// `Ok` acknowledgement from each one.
///
/// The catalog row has already been removed when this runs. Returning success
/// after only a subset of cores reclaimed will allow a same-name re-CREATE to
/// observe predecessor state, so partial success is always an error. The caller
/// records a durable pending-reclaim entry and the applied-index barrier fails
/// closed.
pub async fn dispatch_unregister_collection(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
) -> crate::Result<()> {
    let tenant = TenantId::new(tenant_id);
    let database = DatabaseId::new(database_id);
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .num_cores();
    let mut receivers = Vec::with_capacity(num_cores);

    {
        let mut dispatcher = state
            .dispatcher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The shared on-disk L1 files are keyed by (database, tenant,
        // collection), so exactly one core reclaims them. Route to the
        // collection's homing vshard; fall back to core 0 if the router
        // cannot resolve it, so the files are never orphaned.
        let homing_core = dispatcher
            .router()
            .resolve(nodedb_types::CollectionKey::from_bare(database, name).vshard())
            .unwrap_or(0);
        for core_id in 0..num_cores {
            let request_id = state.next_request_id();
            let request = Request {
                request_id,
                tenant_id: tenant,
                database_id: database,
                vshard_id: VShardId::new(core_id as u32),
                plan: PhysicalPlan::Meta(MetaOp::UnregisterCollection {
                    tenant_id,
                    // The Data Plane keys the collection's state by its
                    // database-qualified name outside the default database.
                    name: nodedb_types::QualifiedCollection::new(database, name)
                        .as_str()
                        .to_string(),
                    purge_lsn,
                    reclaim_l1_files: core_id == homing_core,
                }),
                deadline: Instant::now() + timeout,
                priority: Priority::Background,
                trace_id: TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                commit_hlc: None,
                entry_version: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            };
            let receiver = state.tracker.register(request_id);
            if let Err(error) = dispatcher.dispatch_to_core(core_id, request) {
                state.tracker.cancel(&request_id);
                for (registered_id, _, _) in &receivers {
                    state.tracker.cancel(registered_id);
                }
                return Err(error);
            }
            receivers.push((request_id, core_id, receiver));
        }
    }

    // Each core's acknowledgement counts as apply progress as it arrives, so
    // a proposer waiting on this purge sees a slow reclaim move.
    let mut pending: FuturesUnordered<_> = receivers
        .into_iter()
        .map(|(_request_id, core_id, mut receiver)| async move {
            let response = tokio::time::timeout(timeout, receiver.recv())
                .await
                .map_err(|_| crate::Error::Dispatch {
                    detail: format!("collection reclaim timed out on core {core_id}"),
                })?
                .ok_or_else(|| crate::Error::Dispatch {
                    detail: format!("collection reclaim channel closed on core {core_id}"),
                })?;
            // A coded refusal keeps its Data-Plane code.
            if response.status != Status::Ok {
                return Err(match response.error_code {
                    Some(code) => crate::Error::DataPlane(*code),
                    None => crate::Error::Storage {
                        engine: "collection-purge".into(),
                        detail: format!(
                            "UnregisterCollection for tenant {tenant_id} collection '{name}' \
                             failed on core {core_id} with no error code"
                        ),
                    },
                });
            }
            Ok(())
        })
        .collect();

    let mut first_error = None;
    while let Some(response) = pending.next().await {
        match response {
            Ok(()) => {
                state
                    .metadata_apply_progress
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}
