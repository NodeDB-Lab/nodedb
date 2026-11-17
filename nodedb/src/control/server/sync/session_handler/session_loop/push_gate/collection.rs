// SPDX-License-Identifier: BUSL-1.1

//! The read checks one collection's row pushes pass, resolved once per drain.
//!
//! A row push is a read on the subscriber's behalf, so it passes the checks
//! the shape snapshot passes, in the same order:
//!
//! 1. The principal holds a read grant on the collection, as of this drain.
//! 2. The row passes the collection's RLS read policy for the principal. The
//!    filters are the ones the read path injects into a scan, evaluated by
//!    the same evaluator the Data Plane runs on stored rows.
//! 3. The redacted row matches the predicate of a shape on the collection. A
//!    shape whose predicate names a redacted column matches nothing.
//!
//! Every check here reads in-memory stores and the delta's own images. None
//! reads storage.

use std::sync::Arc;

use nodedb_crs::CrsContext;
use nodedb_query::metadata_filter::matches_metadata_filter;
use nodedb_types::filter::MetadataFilter;

use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::redaction::RedactionStore;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::event::crdt_sync::subscriptions::SharedSubscriptions;
use crate::event::crdt_sync::types::{DeltaOp, OutboundDelta};
use crate::types::DatabaseId;

use super::super::row_redaction::RowPushRedaction;
use super::gate::PushDecision;

/// The collection's RLS read policy, as it applies to the principal.
enum RowPolicy {
    /// No read policy restricts the principal here.
    Unrestricted,
    /// A row must pass every filter.
    Filters(Vec<ScanFilter>),
    /// No row passes: a policy names an `$auth` field the principal lacks, or
    /// its filter set does not encode or decode. The read path refuses the
    /// statement in the same cases.
    DenyAll,
}

/// The principal and session inputs a collection gate resolves against.
pub(super) struct GateInputs<'a> {
    pub shared: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    pub auth: &'a AuthContext,
    pub subscriptions: &'a SharedSubscriptions,
}

/// One collection's resolved read checks.
pub(super) struct CollectionGate {
    /// The principal holds a read grant on the collection.
    readable: bool,
    policy: RowPolicy,
    /// One entry per shape on the collection that can match rows. `None`
    /// matches every row. Each predicate is the one decoded at subscribe.
    shapes: Vec<Option<Arc<MetadataFilter>>>,
    redaction: RowPushRedaction,
}

impl CollectionGate {
    /// Resolve the checks for `collection` against the stores as they are now.
    pub(super) fn resolve(inputs: &GateInputs<'_>, collection: &str) -> Self {
        let redaction = RowPushRedaction::new(
            inputs.identity.tenant_id,
            inputs.auth.roles.clone(),
            inputs.database_id,
            collection,
        );
        // A REVOKE after subscribe takes effect here, at the next drain.
        let readable = authorize_collection(
            inputs.identity,
            inputs.database_id,
            collection,
            Permission::Read,
            &inputs.shared.permissions,
            &inputs.shared.roles,
            &NoopAuditEmitter,
        )
        .is_ok();
        if !readable {
            return Self {
                readable,
                policy: RowPolicy::DenyAll,
                shapes: Vec::new(),
                redaction,
            };
        }

        let shapes = inputs
            .subscriptions
            .read()
            .shapes_on(collection)
            .filter(|shape| {
                redaction
                    .predicate_redacted_field(&inputs.shared.redaction, shape.predicate_bytes())
                    .is_none()
            })
            .map(|shape| shape.predicate().cloned())
            .collect();

        Self {
            readable,
            policy: row_policy(inputs, collection),
            shapes,
            redaction,
        }
    }

    /// Decide what the session sends for one delta of this collection.
    ///
    /// An upsert whose post-image the session can see goes out redacted. Any
    /// other delta goes out as a delete only when the session saw the
    /// row's prior image: the row left what it sees. Otherwise nothing goes
    /// out, because nothing proves the session ever held the row.
    pub(super) fn decide(
        &self,
        delta: &mut OutboundDelta,
        store: &RedactionStore,
        crs: &CrsContext<'_>,
    ) -> PushDecision {
        if !self.readable {
            return PushDecision::Drop;
        }
        if delta.op == DeltaOp::Upsert && self.visible(Some(delta.payload.as_slice()), store, crs) {
            // A payload a rule covers but that cannot be rewritten is dropped
            // rather than delivered.
            if !self.redaction.redact(store, &mut delta.payload) {
                return PushDecision::DropUnredactable;
            }
            return PushDecision::Upsert;
        }
        if self.visible(delta.prior_payload.as_deref(), store, crs) {
            PushDecision::Delete
        } else {
            PushDecision::Drop
        }
    }

    /// Whether every row of the collection is visible: no read policy
    /// restricts the principal and a shape matches every row.
    fn unconditional(&self) -> bool {
        matches!(self.policy, RowPolicy::Unrestricted) && self.shapes.iter().any(Option::is_none)
    }

    /// Whether the session can see a row with `image`. An absent image is
    /// visible only when every row is. An image that fails to evaluate is
    /// not visible.
    fn visible(&self, image: Option<&[u8]>, store: &RedactionStore, crs: &CrsContext<'_>) -> bool {
        if !self.readable || self.shapes.is_empty() {
            return false;
        }
        if self.unconditional() {
            return true;
        }
        let Some(image) = image else {
            return false;
        };
        match &self.policy {
            RowPolicy::DenyAll => return false,
            RowPolicy::Unrestricted => {}
            RowPolicy::Filters(filters) => {
                if !matches!(ScanFilter::all_match_binary(filters, image, crs), Ok(true)) {
                    return false;
                }
            }
        }
        if self.shapes.iter().any(Option::is_none) {
            return true;
        }
        // The snapshot tests a shape predicate on the redacted row, so the
        // push does too.
        let mut redacted = image.to_vec();
        if !self.redaction.redact(store, &mut redacted) {
            return false;
        }
        // A row that does not decode is not visible. An empty stand-in row
        // matches a negated predicate, so it is never used.
        let Some(row) = decode_row_strict(&redacted) else {
            return false;
        };
        self.shapes
            .iter()
            .flatten()
            .any(|predicate| matches_metadata_filter(&row, predicate))
    }
}

/// Decode a row image to the JSON object a shape predicate is tested on.
/// `None` when the bytes are not a MessagePack map.
fn decode_row_strict(bytes: &[u8]) -> Option<serde_json::Value> {
    let json = nodedb_types::json_msgpack::msgpack_to_json_string(bytes).ok()?;
    let row: serde_json::Value = sonic_rs::from_str(&json).ok()?;
    row.is_object().then_some(row)
}

/// The collection's read policy for the principal, keyed as the read path
/// keys it: by the database-qualified collection name.
fn row_policy(inputs: &GateInputs<'_>, collection: &str) -> RowPolicy {
    let qualified = nodedb_types::QualifiedCollection::new(inputs.database_id, collection);
    let bytes = match inputs.shared.rls.combined_read_predicate_with_auth(
        inputs.identity.tenant_id.as_u64(),
        qualified.as_str(),
        inputs.auth,
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return RowPolicy::DenyAll,
        Err(error) => {
            tracing::warn!(
                collection,
                %error,
                "sync: row pushes of the collection suppressed; its read policy does not encode"
            );
            return RowPolicy::DenyAll;
        }
    };
    match decode_scan_filters(&bytes, "RLS read filter") {
        Ok(filters) if filters.is_empty() => RowPolicy::Unrestricted,
        Ok(filters) => RowPolicy::Filters(filters),
        Err(error) => {
            tracing::warn!(
                collection,
                %error,
                "sync: row pushes of the collection suppressed; its read policy does not decode"
            );
            RowPolicy::DenyAll
        }
    }
}

#[cfg(test)]
impl CollectionGate {
    /// A gate built from parts, for the decision tests.
    pub(super) fn from_parts(
        readable: bool,
        filters: Option<Vec<ScanFilter>>,
        shapes: Vec<Option<Arc<MetadataFilter>>>,
        redaction: RowPushRedaction,
    ) -> Self {
        let policy = match filters {
            None => RowPolicy::Unrestricted,
            Some(filters) => RowPolicy::Filters(filters),
        };
        Self {
            readable,
            policy,
            shapes,
            redaction,
        }
    }
}
