// SPDX-License-Identifier: BUSL-1.1

//! Single hooked-in clone CoW read-interception entry point. For a
//! `Shadowed`/`Materializing` clone, walks the clone chain for one task,
//! dispatches target + source, and merges tombstone-filtered rows into one
//! `Response`.
//!
//! Called once per `Read` task from
//! [`super::super::clone_write::gate::intercept_and_authorize`] — never
//! called directly by a protocol handler, so every dispatch entry point
//! (pgwire, native, HTTP, RESP, WS-RPC, internal DDL scans) inherits it
//! without a call-site edit.

use nodedb_types::TenantId;

use crate::bridge::envelope::Response;
use crate::control::clone::resolver::{CloneReadParams, ResolveOutcome, resolve_read};
use crate::control::security::audit::AuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::dispatch::{DispatchAugmentedParams, dispatch_augmented, empty_response};
use super::temporal::extract_system_as_of_ms;

/// Outcome of read-path clone interception.
pub(in crate::control::server) enum CloneReadOutcome {
    /// No interception needed — caller must dispatch normally.
    Passthrough,
    /// The read was fully handled by the clone path. Caller uses this response.
    Handled(Response),
}

/// Inputs for [`maybe_intercept_clone_read`] — one struct because the gate
/// needs everything the resolver, the authorizer, and the dispatcher each
/// need, which exceeds a readable positional argument count.
pub(in crate::control::server) struct CloneReadInterceptParams<'a> {
    pub state: &'a SharedState,
    pub task: &'a PhysicalTask,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    pub emitter: &'a dyn AuditEmitter,
}

/// Intercept a single `Read` task for a cloned collection.
pub(in crate::control::server) async fn maybe_intercept_clone_read(
    params: CloneReadInterceptParams<'_>,
) -> crate::Result<CloneReadOutcome> {
    let CloneReadInterceptParams {
        state,
        task,
        identity,
        tenant_id,
        permissions,
        roles,
        emitter,
    } = params;
    // The resolver maps the `AS OF` time to an LSN only for a live clone.
    let resolve_params = CloneReadParams {
        as_of_ms: extract_system_as_of_ms(Some(&task.plan)),
    };

    let Some(outcome) = resolve_read(state, task.clone(), tenant_id, &resolve_params).await? else {
        return Ok(CloneReadOutcome::Passthrough);
    };

    match outcome {
        ResolveOutcome::PreDatesClone(note) => {
            tracing::debug!(
                message = note.message,
                query_lsn = %note.query_lsn,
                clone_created_at = %note.clone_created_at,
                "clone read predates clone creation — returning empty result"
            );
            Ok(CloneReadOutcome::Handled(empty_response(state)))
        }
        ResolveOutcome::Augmented {
            target_task,
            source_tasks,
            target_collection_key,
            note,
        } => {
            if let Some(note) = note {
                tracing::debug!(
                    message = note.message,
                    "clone read: T_lsn < clone_created_at (note attached)"
                );
            }
            let response = dispatch_augmented(DispatchAugmentedParams {
                state,
                identity,
                permissions,
                roles,
                emitter,
                target_task: *target_task,
                source_tasks,
                target_collection_key: &target_collection_key,
            })
            .await?;
            Ok(CloneReadOutcome::Handled(response))
        }
    }
}
