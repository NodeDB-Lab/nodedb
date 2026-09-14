// SPDX-License-Identifier: BUSL-1.1

//! Inputs for one materialized response-shaping call.
//!
//! Grouped into a struct rather than passed positionally: the shaping entry
//! point takes payload, plan, plan kind, projection, shared state, database,
//! tenant and redaction context together, and a positional list that long is
//! both unreadable and easy to transpose at a call site.

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_types::{DatabaseId, TenantId};

use super::redaction::RedactionCtx;
use super::schema::OutputSchema;
use super::types::PlanKind;

/// One Data-Plane payload plus everything needed to shape it into rows.
pub struct MaterializedShapeRequest<'a> {
    /// The raw Data-Plane response payload.
    pub payload: &'a [u8],
    /// The plan that produced `payload`.
    pub plan: &'a PhysicalPlan,
    /// The plan's response classification.
    pub plan_kind: PlanKind,
    /// The statement's resolved SELECT list, when it names columns.
    pub projection: Option<&'a OutputSchema>,
    pub state: &'a SharedState,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    /// Column-level redaction for this statement, resolved once per query.
    /// `None` only where the producer has no requester identity at all.
    pub redaction: Option<RedactionCtx<'a>>,
    /// The calling session's `currval` map, so a per-row `nextval` stamp is
    /// visible to a later `currval` in the same session. `None` where the
    /// producer has no session (HTTP, internal merges).
    pub session_sequences: Option<Arc<crate::control::sequence::SessionSequenceValues>>,
}
