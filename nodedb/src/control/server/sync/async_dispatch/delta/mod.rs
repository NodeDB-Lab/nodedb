// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta apply dispatch and client-frame construction.

mod apply;
mod authorize;
mod compensation;
mod outcome;
mod peer_identity;
mod refusal;
mod signature;

pub(crate) use apply::{DeltaDispatchOutcome, DeltaSessionContext, apply_delta_and_finalize};
pub(in crate::control::server::sync) use authorize::{
    authorize_delta_write, permission_denied_delta_reject,
};
