// SPDX-License-Identifier: BUSL-1.1

//! Wire protocol version newtype.

/// Opaque wrapper around a `u16` wire-protocol version number.
///
/// v1 is the implicit "no envelope" world — messages serialized directly
/// without any outer `Versioned<T>` wrapper. v2 is the first explicit version
/// emitted by [`crate::wire_version::envelope::encode_versioned`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WireVersion(pub u16);

impl WireVersion {
    /// v1: legacy — no `Versioned<T>` envelope. Raw inner type bytes.
    pub const V1: WireVersion = WireVersion(1);

    /// The current envelope version this build emits. `encode_versioned`
    /// always stamps `CURRENT`, and `decode_versioned` rejects any envelope
    /// whose version exceeds it — so a newer peer's frames fail loudly on an
    /// older node rather than being silently misdecoded.
    ///
    /// - v2: first explicit envelope version, introduced alongside this module.
    /// - v3: bumped alongside `crate::wire::WIRE_VERSION` for the
    ///   `ExecuteRequest.txn_id` rkyv layout change (cross-node in-transaction
    ///   read-your-own-writes).
    pub const CURRENT: WireVersion = WireVersion(3);
}

impl std::fmt::Display for WireVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}
