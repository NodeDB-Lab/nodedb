// SPDX-License-Identifier: BUSL-1.1

//! Wire-version range negotiation.

use super::error::WireVersionError;
use super::types::WireVersion;

/// A contiguous inclusive range of wire-protocol versions supported by one
/// endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRange {
    pub min: WireVersion,
    pub max: WireVersion,
}

impl VersionRange {
    /// Construct a new `VersionRange`. Panics in debug mode if `min > max`.
    pub fn new(min: WireVersion, max: WireVersion) -> Self {
        debug_assert!(
            min <= max,
            "VersionRange: min ({min}) must be <= max ({max})"
        );
        Self { min, max }
    }

    /// Returns `true` if `version` falls within `[min, max]`.
    pub fn contains(&self, version: WireVersion) -> bool {
        version >= self.min && version <= self.max
    }
}

/// Negotiate the highest wire version both sides support.
///
/// Returns the highest `WireVersion` in the intersection of `local` and
/// `remote`. If the ranges are disjoint, returns
/// `WireVersionError::NegotiationFailed` with both ranges captured for
/// operator diagnostics.
pub fn negotiate(
    local: VersionRange,
    remote: VersionRange,
) -> Result<WireVersion, WireVersionError> {
    // Intersection: [max(local.min, remote.min), min(local.max, remote.max)]
    let intersect_min = local.min.max(remote.min);
    let intersect_max = local.max.min(remote.max);

    if intersect_min > intersect_max {
        return Err(WireVersionError::NegotiationFailed {
            local_min: local.min,
            local_max: local.max,
            remote_min: remote.min,
            remote_max: remote.max,
        });
    }

    // Highest common version — prefer new capabilities over old.
    Ok(intersect_max)
}

/// Wire handshake types. Both sides exchange their `VersionRange` at the start
/// of every nexar/QUIC connection. The receiving side calls [`negotiate`] to
/// compute the agreed version and rejects the connection if ranges are
/// disjoint.
///
/// # Transport wiring
///
/// The actual injection into the QUIC connection accept loop lives in
/// `transport/client` (outbound) and `transport/server` (inbound).
/// On connect, the client opens a dedicated bidi stream and sends
/// `VersionHandshake { range: local_range }` before any RPC frames.
/// The server reads the handshake, negotiates, and replies with
/// `VersionHandshakeAck { agreed }`. If ranges are disjoint the server
/// closes the QUIC connection with application error code 0x01.
/// See `handshake_io` and `negotiation::negotiate` for implementation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VersionHandshake {
    pub range: (u16, u16),
    /// Capability bitmask for feature advertisement. `0` means no extra
    /// capabilities.
    pub capabilities: u64,
    /// Sender's `nodedb_types::wire_version::WIRE_BUILD_ID`. Compared for
    /// exact equality by the receiver — see `handshake_io::perform_version_handshake_server`.
    pub build_id: String,
}

/// Server-side acknowledgement returned after negotiation succeeds.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VersionHandshakeAck {
    pub agreed: u16,
    /// Capability bitmask echoed (or narrowed) by the server.
    /// Unknown bits are ignored by the receiver.  Defaults to `0`.
    #[serde(default)]
    pub capabilities: u64,
    /// Server's `nodedb_types::wire_version::WIRE_BUILD_ID`, echoed so the
    /// client can also refuse a build mismatch the server failed to catch.
    pub build_id: String,
}

impl VersionHandshake {
    /// Build a handshake from a [`VersionRange`] and this build's identity.
    pub fn from_range(range: VersionRange, build_id: String) -> Self {
        Self {
            range: (range.min.0, range.max.0),
            capabilities: 0,
            build_id,
        }
    }

    /// Recover the [`VersionRange`] from wire fields.
    pub fn to_range(&self) -> VersionRange {
        VersionRange::new(WireVersion(self.range.0), WireVersion(self.range.1))
    }
}

impl VersionHandshakeAck {
    /// Construct an ack for the given agreed wire version and build identity.
    pub fn new(agreed: WireVersion, build_id: String) -> Self {
        Self {
            agreed: agreed.0,
            capabilities: 0,
            build_id,
        }
    }

    /// The agreed wire version as a typed [`WireVersion`].
    pub fn agreed_version(&self) -> WireVersion {
        WireVersion(self.agreed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(n: u16) -> WireVersion {
        WireVersion(n)
    }

    fn range(min: u16, max: u16) -> VersionRange {
        VersionRange::new(v(min), v(max))
    }

    #[test]
    fn overlapping_ranges_return_highest_common() {
        // local: [1,3]  remote: [2,5]  → intersection [2,3] → pick 3
        let result = negotiate(range(1, 3), range(2, 5)).unwrap();
        assert_eq!(result, v(3));
    }

    #[test]
    fn disjoint_ranges_return_negotiation_failed() {
        // local: [1,2]  remote: [3,5]  → no overlap
        let err = negotiate(range(1, 2), range(3, 5)).unwrap_err();
        assert!(
            matches!(err, WireVersionError::NegotiationFailed { .. }),
            "expected NegotiationFailed, got: {err}"
        );
    }

    #[test]
    fn equal_single_version_succeeds() {
        let result = negotiate(range(2, 2), range(2, 2)).unwrap();
        assert_eq!(result, v(2));
    }

    #[test]
    fn equal_min_equal_max_succeeds_with_that_version() {
        let result = negotiate(range(1, 4), range(4, 6)).unwrap();
        assert_eq!(result, v(4));
    }

    #[test]
    fn handshake_roundtrip() {
        let r = range(1, 2);
        let hs = VersionHandshake::from_range(r, "test-build".to_owned());
        let bytes = zerompk::to_msgpack_vec(&hs).unwrap();
        let decoded: VersionHandshake = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.to_range(), r);
        assert_eq!(decoded.build_id, "test-build");
    }
}
