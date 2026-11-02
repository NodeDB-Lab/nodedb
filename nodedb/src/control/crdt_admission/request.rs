// SPDX-License-Identifier: BUSL-1.1

//! What a caller hands a CRDT admission, and what it gets back.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::server::shared::session::StatementDeadline;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};

use super::policy::CrdtPostImagePolicy;

pub struct AuthorizedCrdtApplyAdmissionRequest<'a> {
    pub authorized: AuthorizedTask,
    /// The db-qualified collection (`QualifiedCollection::as_str`), the same
    /// string the plan's `CrdtOp::Apply` carries. The preview and the apply
    /// address the Data Plane by it, and the catalog lookup de-qualifies it.
    pub collection: &'a str,
    /// The deadline every phase of the admission ends at.
    pub deadline: StatementDeadline,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}

pub struct CrdtApplyAdmissionRequest<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The db-qualified collection, equal to the plan's `CrdtOp::Apply`
    /// collection.
    pub collection: &'a str,
    pub plan: PhysicalPlan,
    /// The deadline every phase of the admission ends at.
    pub deadline: StatementDeadline,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}

pub struct CrdtAdmissionOutcome {
    pub payload: Vec<u8>,
    /// Operations the admitted delta encoded that the target document already
    /// knew, measured by the preview that fenced this apply. Distinguishes a
    /// client whose writes land from one whose writes are being absorbed —
    /// both otherwise produce the same successful payload.
    pub trimmed_ops: u64,
}

impl CrdtAdmissionOutcome {
    /// Attach the trim count measured by the preview that fenced this apply.
    pub(super) fn with_trimmed_ops(self, trimmed_ops: u64) -> Self {
        Self {
            trimmed_ops,
            ..self
        }
    }
}

pub struct CrdtRestoreAdmissionRequest<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The db-qualified collection the generated restore ops address.
    pub collection: &'a str,
    pub document_id: &'a str,
    pub target_version_json: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub peer_id: u64,
    /// The deadline every phase of the admission ends at.
    pub deadline: StatementDeadline,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}
