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

    /// v2: first explicit envelope version. Introduced alongside this module.
    /// `encode_versioned` always emits v2; `decode_versioned` falls back to v1
    /// if the outer envelope is absent or unparseable.
    pub const CURRENT: WireVersion = WireVersion(2);
}

impl std::fmt::Display for WireVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}
