// SPDX-License-Identifier: BUSL-1.1

//! What a committed entry produced, as its propose waiter receives it.

use crate::bridge::envelope::Response;

/// What a committed entry produced on the replica that applied it.
#[derive(Debug, Clone)]
pub struct AppliedWrite {
    /// The Data Plane's response payload, verbatim.
    pub payload: Vec<u8>,
}

impl AppliedWrite {
    /// An applied entry that answered `payload`.
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Take the payload off the Data Plane's response to a committed write.
    pub fn from_response(response: &Response) -> Self {
        Self::new(response.payload.to_vec())
    }
}

/// Result sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<AppliedWrite, crate::Error>;
