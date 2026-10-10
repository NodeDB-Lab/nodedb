// SPDX-License-Identifier: BUSL-1.1

//! What a committed entry produced, as its propose waiter receives it.

use crate::bridge::envelope::Response;
use crate::types::ReadVersions;

/// What a committed entry produced on the replica that applied it.
///
/// Carries the write's collection versions alongside the payload because the
/// proposer has no other way to learn them: the versions are recorded inside
/// the apply path, never sent back on the wire, and the propose tracker
/// resolves on the very node that applied locally.
#[derive(Debug, Clone)]
pub struct AppliedWrite {
    /// The Data Plane's response payload, verbatim.
    pub payload: Vec<u8>,
    /// The written collection's version on the write's vShard AFTER this
    /// write: the log position of the entry, the same on every replica.
    pub write_versions: ReadVersions,
}

impl AppliedWrite {
    /// Take both fields off the Data Plane's response to a committed write.
    ///
    /// `Response::read_versions` is stamped by the core loop from the written
    /// collection's version, read AFTER the handler recorded this write into
    /// the version index, so on a write response it is the post-write
    /// version. It is empty for a plan that maps to no user collection (see
    /// [`AppliedWrite::unversioned`]).
    pub fn from_response(response: &Response) -> Self {
        Self {
            payload: response.payload.to_vec(),
            write_versions: response.read_versions.clone(),
        }
    }

    /// An applied entry that publishes no collection version: it wrote no
    /// Data-Plane collection state (a decode skip, a forwarded read result, a
    /// schema snapshot), or it was deduplicated before reaching the funnel.
    /// There is no version to floor a later read at, and empty versions are
    /// the read-set capture's "no own-write floor" value: never a fabricated
    /// stand-in for a version that exists but was not read back.
    pub fn unversioned(payload: Vec<u8>) -> Self {
        Self {
            payload,
            write_versions: ReadVersions::new(),
        }
    }
}

/// Result sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<AppliedWrite, crate::Error>;
