// SPDX-License-Identifier: BUSL-1.1

//! Wire protocol version newtype.

/// Opaque wrapper around a `u16` wire-protocol version number.
///
/// [`crate::wire_version::envelope::encode_versioned`] stamps
/// [`WireVersion::CURRENT`] on every envelope it emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WireVersion(pub u16);

impl WireVersion {
    /// The one wire version this build emits and accepts.
    pub const CURRENT: WireVersion = WireVersion(1);
}

impl std::fmt::Display for WireVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}
