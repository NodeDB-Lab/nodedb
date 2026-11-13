// SPDX-License-Identifier: BUSL-1.1

//! Async Data Plane dispatch helpers for the sync WebSocket listener.
//!
//! Contains async functions that cross the Control Plane / Data Plane boundary
//! via the SPSC bridge: shape-subscription snapshot queries and CRDT delta
//! constraint validation.

mod delta;
mod shape;

pub(super) use delta::{
    DeltaDispatchOutcome, DeltaSessionContext, apply_delta_and_finalize, authorize_delta_write,
    permission_denied_delta_reject,
};
pub(super) use shape::{
    CollectionResyncFailure, collection_resync, handle_resync_request_async,
    handle_shape_subscribe_async, predicate_redacted_field, snapshot_rate,
};
