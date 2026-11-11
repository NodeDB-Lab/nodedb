// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch quota admission: refuse a caller who has already spent the
//! cap on an entitlement that covers this request. Asks "already over the
//! cap?", not the exact token cost, since row count isn't known yet.

use crate::control::security::metering::quota::QuotaStatus;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::state::SharedState;

// Shared with the charging path — divergence would bill against a ledger that
// never refuses, or refuse via one that never bills.
use super::metering::{PlanMeteringInfo, scope_covers_request};

/// Refuse the request when a covering scope's `Hard` quota is already spent.
/// `Ok(())` when disabled, internal-service, no collection, or nothing exhausted.
/// `Soft`/`Throttle`/`Overage` never refuse here — `check_quota` handles those.
pub(crate) fn admit_quota_for_dispatch(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &PlanMeteringInfo,
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    if scope.identity().is_internal_service() {
        return Ok(());
    }
    let collections = info.collections();
    if collections.is_empty() {
        return Ok(());
    }

    let auth = scope.auth();
    let now_secs = crate::control::security::time::now_secs();
    let effective = state.scope_grants.effective_scopes(&auth.id, &auth.org_ids);

    // Every collection the plan writes into: an exhausted cap on any refuses the write.
    for collection in collections {
        for scope_name in &effective {
            if !scope_covers_request(state, scope_name, info.permission(), collection) {
                continue;
            }
            // A scope with no definition returns `Ok` — costs nothing for the common uncapped scope.
            if let Err(status) = state
                .quota_manager
                .check_quota(scope_name, &auth.id, 0, now_secs)
            {
                return Err(quota_exceeded(&status));
            }
        }
    }

    Ok(())
}

/// [`admit_quota_for_dispatch`] for every task of one statement. A route that
/// commits the tasks together, such as a Calvin transaction, takes no
/// per-task dispatch step, so it admits the whole set before it submits.
pub(crate) fn admit_quota_for_tasks(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    tasks.iter().try_for_each(|task| {
        admit_quota_for_dispatch(state, scope, &PlanMeteringInfo::extract(&task.plan))
    })
}

/// Build the refusal for an exhausted hard quota, naming the scope, cap, and
/// consumption so the operator knows which entitlement ran out.
fn quota_exceeded(status: &QuotaStatus) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "quota exceeded on scope '{}': {} of {} tokens used this period",
            status.scope_name, status.used_tokens, status.max_tokens
        ),
    }
}
