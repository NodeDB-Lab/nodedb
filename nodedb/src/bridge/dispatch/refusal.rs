// SPDX-License-Identifier: BUSL-1.1

//! A refused dispatch that hands the unsent request back to the caller.

use crate::bridge::envelope;

/// A request the dispatcher refused, returned unsent with the reason.
///
/// A caller that retries a capacity refusal re-dispatches `request` as is,
/// with no clone and no rebuild.
#[derive(Debug)]
pub struct DispatchRefusal {
    /// Why the dispatcher refused the request.
    pub error: crate::Error,
    /// The refused request. The dispatcher tracked nothing for it.
    pub request: envelope::Request,
}

impl DispatchRefusal {
    /// Box a refusal of `request` for `error`.
    pub(super) fn boxed(error: crate::Error, request: envelope::Request) -> Box<Self> {
        Box::new(Self { error, request })
    }
}
