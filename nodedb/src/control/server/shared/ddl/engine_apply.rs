// SPDX-License-Identifier: BUSL-1.1

//! Apply one DDL side-effect in the Data Plane and surface its verdict.
//!
//! A Data-Plane rejection arrives as an `Ok(Response)` carrying a non-`Ok`
//! status, so a caller that only handles the transport `Result` reports a
//! refused configuration as a successful statement. Index DDL routes its
//! engine registration through here so the refusal always reaches the client.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::result::DdlError;
use super::sync_dispatch::{SystemReason, SystemTask, dispatch_system};

/// Dispatch `plan` for `collection` and translate any Data-Plane refusal into
/// a [`DdlError`] carrying `sqlstate` and `context`.
pub(crate) async fn apply_in_engine(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    sqlstate: &str,
    context: &str,
) -> Result<(), DdlError> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_system(
        state,
        SystemTask::new(
            SystemReason::DdlApply,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        timeout,
    )
    .await
    .map(|_| ())
    .map_err(|e| DdlError::new(sqlstate, format!("{context}: {e}")))
}

/// Refuse a vector index definition the engine would refuse, without
/// changing engine state.
///
/// `VectorOp::SetParams` refuses a core whose index already materialized.
/// Inside an explicit transaction the parameters install at COMMIT, so the
/// statement probes with the read-only `VectorOp::QueryStats` instead: an
/// index that answers has materialized, and `NotFound` means the parameters
/// will install.
pub(crate) async fn refuse_materialized_vector_index(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    field_name: &str,
    sqlstate: &str,
    context: &str,
) -> Result<(), DdlError> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let plan = PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::QueryStats {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        field_name: field_name.to_string(),
    });
    let response = super::sync_dispatch::dispatch_system_response_with_source(
        state,
        SystemTask::new(
            SystemReason::DdlApply,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        timeout,
        crate::event::EventSource::User,
    )
    .await
    .map_err(|e| DdlError::new("XX000", format!("{context}: {e}")))?;
    match (response.status, response.error_code.as_deref()) {
        (crate::bridge::envelope::Status::Ok, _) => Err(DdlError::new(
            sqlstate,
            format!(
                "{context}: cannot change index params after creation; drop and recreate \
                 the collection"
            ),
        )),
        (_, Some(crate::bridge::envelope::ErrorCode::NotFound)) => Ok(()),
        (_, code) => Err(DdlError::new(
            "XX000",
            format!("{context}: vector index probe failed: {code:?}"),
        )),
    }
}
