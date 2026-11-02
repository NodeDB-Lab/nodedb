// SPDX-License-Identifier: Apache-2.0

//! Wire result for a bounded, non-mutating CRDT delta preview.

/// Typed result returned by a Data-Plane CRDT preview.
///
/// `post_image_msgpack` encodes `Option<Value>` with zerompk. `None` represents
/// a target row absent after a valid delete; it is distinct from `Some(Null)`.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct CrdtPreviewResult {
    /// Canonical zerompk encoding of the validated target post-image.
    pub post_image_msgpack: Vec<u8>,
    /// The target row before the delta, encoded as `post_image_msgpack` is.
    /// Encodes `None` when the row is absent. Admission judges the
    /// collection's transition rules on this image and the post-image.
    pub pre_image_msgpack: Vec<u8>,
    /// Number of newly imported operations represented by this delta.
    pub imported_ops: u64,
    /// Domain-bound current frontier digest used to fence the subsequent apply.
    pub frontier_digest: [u8; 32],
    /// Operations the delta encoded that the target document already knew and
    /// the CRDT merge therefore discards.
    ///
    /// A resync trims its replay prefix and still advances (`imported_ops > 0`).
    /// A delta that trims entirely contributed nothing — the shape both an
    /// idempotent replay and a peer-id collision take — and without this count
    /// the second is indistinguishable from the first at the session boundary.
    pub trimmed_ops: u64,
}
