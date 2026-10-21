// SPDX-License-Identifier: Apache-2.0

//! Incoming delta parameters for policy-aware validation.

use crate::CrdtAuthContext;

/// The delta a peer sends, with who sent it and when.
pub struct IncomingDelta {
    pub peer_id: u64,
    pub auth: CrdtAuthContext,
    pub delta_bytes: Vec<u8>,
    /// Hybrid Logical Clock timestamp of the incoming write.
    pub hlc_timestamp: u64,
}
