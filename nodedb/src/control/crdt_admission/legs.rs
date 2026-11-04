// SPDX-License-Identifier: BUSL-1.1

//! The Data-Plane reads an admission runs before it proposes: the preview of
//! a delta, and the generation of a restore delta.
//!
//! Both are legs of a request whose capability was consumed at its entry
//! point, so they go through the system door.

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::CrdtPreviewResult;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system_response_with_source,
};

use super::workflow::CrdtAdmissionWorkflow;

/// The post-image and frontier digest `delta` will produce on the document.
pub(super) async fn preview(
    workflow: &CrdtAdmissionWorkflow<'_>,
    document_id: &str,
    delta: &[u8],
) -> crate::Result<CrdtPreviewResult> {
    let response = dispatch_system_response_with_source(
        workflow.state,
        SystemTask::new(
            SystemReason::AdmittedContinuation,
            workflow.tenant_id,
            nodedb_types::CollectionKey::from_qualified_str(
                workflow.database_id,
                workflow.collection,
            )?,
            PhysicalPlan::Crdt(CrdtOp::PreviewApply {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    workflow.database_id,
                    workflow.collection.to_owned(),
                )?,
                document_id: document_id.to_owned(),
                delta: delta.to_vec(),
            }),
        ),
        workflow.deadline,
        workflow.event_source,
    )
    .await
    .map_err(|error| workflow.before_propose(error))?;
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    zerompk::from_msgpack(response.payload.as_bytes()).map_err(|error| crate::Error::Internal {
        detail: format!("CRDT preview payload decode: {error}"),
    })
}

/// The delta that takes the document back to `target_version_json`.
pub(super) async fn generate_restore_delta(
    workflow: &CrdtAdmissionWorkflow<'_>,
    document_id: &str,
    target_version_json: &str,
    surrogate: nodedb_types::Surrogate,
) -> crate::Result<Vec<u8>> {
    let response = dispatch_system_response_with_source(
        workflow.state,
        SystemTask::new(
            SystemReason::AdmittedContinuation,
            workflow.tenant_id,
            nodedb_types::CollectionKey::from_qualified_str(
                workflow.database_id,
                workflow.collection,
            )?,
            PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    workflow.database_id,
                    workflow.collection.to_owned(),
                )?,
                document_id: document_id.to_owned(),
                target_version_json: target_version_json.to_owned(),
                surrogate,
            }),
        ),
        workflow.deadline,
        workflow.event_source,
    )
    .await
    .map_err(|error| workflow.before_propose(error))?;
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    Ok(response.payload.to_vec())
}

fn response_error(response: &crate::bridge::envelope::Response) -> crate::Error {
    match response.error_code.as_deref() {
        Some(code) => crate::Error::DataPlane(code.clone()),
        None => crate::Error::Internal {
            detail: String::from_utf8_lossy(&response.payload).into_owned(),
        },
    }
}
