// SPDX-License-Identifier: Apache-2.0

//! Error type for the shared SqlPlan → PhysicalPlan converter helpers.
//!
//! The variant set matches what the converter actually produces; deeper
//! engine-specific failures are re-wrapped at the deployment boundary.
//! Origin maps `ConvertError → nodedb::Error`; Lite will map it to its
//! own error type.

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// The plan shape is invalid (unsupported combination, missing field, etc.).
    #[error("plan error: {0}")]
    PlanError(String),

    /// A client-facing request is malformed (bad cast, wrong literal type, etc.).
    #[error("bad request: {0}")]
    BadRequest(String),

    /// A defensive cap was exceeded (max fan-out, depth, columns, etc.).
    #[error("{limit_name} exceeded: {value} > {max}")]
    LimitExceeded {
        limit_name: &'static str,
        value: u64,
        max: u64,
    },

    /// Serialization failure (msgpack encoding of filters, projections, etc.).
    #[error("serialization: {0}")]
    Serialization(String),

    /// Catch-all for converter-internal failures that don't fit the above
    /// and that we don't want to leak from the shared crate as untyped strings.
    /// Carries a `'static` short reason; full detail propagates via `cause`.
    #[error("converter internal: {0}")]
    Other(String),
}

/// A CRDT fields body that does not encode or decode.
#[derive(Debug, thiserror::Error)]
pub enum FieldsBodyError {
    /// The fields do not encode as a native MessagePack object.
    #[error("CRDT fields do not encode: {0}")]
    Encode(#[source] zerompk::Error),

    /// The body is not a MessagePack object.
    #[error("CRDT fields are not a MessagePack object: {0}")]
    NotAnObject(#[source] nodedb_types::MsgpackError),

    /// The body is a plain MessagePack map, not a native object.
    #[error(
        "CRDT fields are a plain MessagePack map; a fields body is a native object of tagged values"
    )]
    PlainMap,

    /// The native object body does not decode.
    #[error("CRDT fields do not decode: {0}")]
    Decode(#[source] nodedb_types::MsgpackError),

    /// The body decodes to a value that is not an object.
    #[error("CRDT fields decode as {found}, expected an object")]
    NotObject { found: &'static str },
}

impl From<FieldsBodyError> for ConvertError {
    fn from(e: FieldsBodyError) -> Self {
        ConvertError::Serialization(e.to_string())
    }
}
