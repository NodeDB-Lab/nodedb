// SPDX-License-Identifier: Apache-2.0

//! Graph edge identifier.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::error::{IdError, validate};
use super::node::NodeId;

/// Identifies a graph edge. Returned by `graph_insert_edge`.
///
/// An edge is uniquely identified by `(src, dst, label, seq)`. The `seq`
/// field disambiguates parallel edges — multiple edges that share the same
/// `(src, dst, label)` bucket. `seq = 0` means "the first (or only) edge
/// in this bucket". The engine-side allocator that assigns monotonically
/// increasing `seq` values is a follow-up TODO; until it is wired in,
/// callers use `seq = 0` via `EdgeId::try_first`.
///
/// # Wire format
/// Serialization uses the **structured fields** (serde / zerompk).
/// The `Display` / `FromStr` impl uses the **length-prefixed** text form
/// `"{src_len}:{src}|{label_len}:{label}|{dst_len}:{dst}|{seq}"` — unambiguous
/// even when node IDs or labels contain `|`, `:`, or `--` characters.
/// The Display form is used only for logging and the pgwire DELETE path.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct EdgeId {
    /// Source node identifier.
    pub src: NodeId,
    /// Destination node identifier.
    pub dst: NodeId,
    /// Edge label / relationship type (e.g. `"KNOWS"`, `"RELATES_TO"`).
    pub label: String,
    /// Sequence number within the `(src, dst, label)` bucket. Always `0`.
    ///
    /// An edge is identified by `(src, dst, label)` alone. A second insert of
    /// the same triple writes a new version of that edge and replaces its
    /// properties. It never adds a parallel edge.
    pub seq: u64,
}

impl EdgeId {
    /// Create an `EdgeId` for the first (or only) edge in a `(src, dst, label)` bucket.
    ///
    /// Validates the label against the same rules as `NodeId::try_new`. Returns
    /// `Err(IdError)` if the label is empty, too long, or contains a NUL byte.
    /// Node IDs are accepted pre-validated (callers hold `NodeId` values which
    /// were already validated at construction time).
    pub fn try_first(src: NodeId, dst: NodeId, label: impl Into<String>) -> Result<Self, IdError> {
        let label = label.into();
        validate(&label)?;
        Ok(Self {
            src,
            dst,
            label,
            seq: 0,
        })
    }

    /// Create an `EdgeId` with an explicit sequence number.
    ///
    /// Validates the label against the same rules as `NodeId::try_new`. Returns
    /// `Err(IdError)` if the label is empty, too long, or contains a NUL byte.
    pub fn try_with_seq(
        src: NodeId,
        dst: NodeId,
        label: impl Into<String>,
        seq: u64,
    ) -> Result<Self, IdError> {
        let label = label.into();
        validate(&label)?;
        Ok(Self {
            src,
            dst,
            label,
            seq,
        })
    }
}

/// Error returned when parsing an `EdgeId` from its Display string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EdgeIdParseError {
    #[error(
        "missing segment: expected format '{{src_len}}:{{src}}|{{label_len}}:{{label}}|{{dst_len}}:{{dst}}|{{seq}}'"
    )]
    MissingSegment,
    #[error("invalid length prefix in segment '{segment}': {reason}")]
    InvalidLengthPrefix { segment: String, reason: String },
    #[error("invalid seq value '{value}': {reason}")]
    InvalidSeq { value: String, reason: String },
    /// The label embedded in the wire string failed ID validation.
    #[error("invalid label: {0}")]
    InvalidLabel(IdError),
}

impl fmt::Display for EdgeId {
    /// Length-prefixed format: `"{src_len}:{src}|{label_len}:{label}|{dst_len}:{dst}|{seq}"`.
    ///
    /// Unambiguous regardless of what characters appear in node IDs or labels.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let src = self.src.as_str();
        let label = &self.label;
        let dst = self.dst.as_str();
        write!(
            f,
            "{}:{}|{}:{}|{}:{}|{}",
            src.len(),
            src,
            label.len(),
            label,
            dst.len(),
            dst,
            self.seq
        )
    }
}

impl FromStr for EdgeId {
    type Err = EdgeIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse one `"{len}:{value}"` segment from the front of `input`.
        // Returns `(value, rest_after_separator)` where the separator is `|`.
        fn take_segment(input: &str, separator: char) -> Result<(&str, &str), EdgeIdParseError> {
            let colon = input.find(':').ok_or(EdgeIdParseError::MissingSegment)?;
            let len_str = &input[..colon];
            let len: usize =
                len_str
                    .parse()
                    .map_err(|e| EdgeIdParseError::InvalidLengthPrefix {
                        segment: len_str.to_owned(),
                        reason: format!("{e}"),
                    })?;
            let after_colon = &input[colon + 1..];
            if after_colon.len() < len {
                return Err(EdgeIdParseError::MissingSegment);
            }
            let value = &after_colon[..len];
            let rest = &after_colon[len..];
            // Consume the expected separator (or allow empty for the last segment).
            let rest = if rest.starts_with(separator) {
                &rest[1..]
            } else if rest.is_empty() {
                rest
            } else {
                return Err(EdgeIdParseError::MissingSegment);
            };
            Ok((value, rest))
        }

        let (src, rest) = take_segment(s, '|')?;
        let (label, rest) = take_segment(rest, '|')?;
        let (dst, rest) = take_segment(rest, '|')?;

        // Remaining `rest` is the seq value.
        let seq: u64 = rest.parse().map_err(|e| EdgeIdParseError::InvalidSeq {
            value: rest.to_owned(),
            reason: format!("{e}"),
        })?;

        // Validate the label parsed from the wire string.
        validate(label).map_err(EdgeIdParseError::InvalidLabel)?;

        Ok(EdgeId {
            // src and dst come from previously-validated wire bytes (NodeDB server output).
            src: NodeId::from_validated(src.to_owned()),
            dst: NodeId::from_validated(dst.to_owned()),
            label: label.to_owned(),
            seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::ID_MAX_LEN;
    use super::*;

    #[test]
    fn try_first_accepts_valid() {
        let src = NodeId::try_new("alice").expect("valid");
        let dst = NodeId::try_new("bob").expect("valid");
        let e = EdgeId::try_first(src, dst, "KNOWS").expect("valid label");
        assert_eq!(e.seq, 0);
        assert_eq!(e.label, "KNOWS");
    }

    #[test]
    fn edge_id_label_validation_propagates_empty() {
        let src = NodeId::try_new("x").expect("valid");
        let dst = NodeId::try_new("y").expect("valid");
        assert_eq!(EdgeId::try_first(src, dst, ""), Err(IdError::Empty));
    }

    #[test]
    fn edge_id_label_validation_propagates_too_long() {
        let src = NodeId::try_new("x").expect("valid");
        let dst = NodeId::try_new("y").expect("valid");
        let long = "L".repeat(ID_MAX_LEN + 1);
        assert!(matches!(
            EdgeId::try_first(src, dst, long),
            Err(IdError::TooLong { .. })
        ));
    }

    #[test]
    fn edge_id_label_validation_propagates_nul() {
        let src = NodeId::try_new("x").expect("valid");
        let dst = NodeId::try_new("y").expect("valid");
        assert_eq!(
            EdgeId::try_first(src, dst, "la\0bel"),
            Err(IdError::ContainsNul)
        );
    }

    #[test]
    fn edge_id_no_collision_with_dashes_in_label() {
        let a = EdgeId::try_first(
            NodeId::try_new("alice").expect("v"),
            NodeId::try_new("bob").expect("v"),
            "--",
        )
        .expect("valid");
        let b = EdgeId::try_first(
            NodeId::try_new("alice").expect("v"),
            NodeId::try_new("bob").expect("v"),
            "-->",
        )
        .expect("valid");
        assert_ne!(
            a, b,
            "labels '--' and '-->' must not produce the same EdgeId"
        );
    }

    #[test]
    fn edge_id_parallel_edges_distinguished_by_seq() {
        let a = EdgeId::try_first(
            NodeId::try_new("x").expect("v"),
            NodeId::try_new("y").expect("v"),
            "KNOWS",
        )
        .expect("valid");
        let b = EdgeId::try_with_seq(
            NodeId::try_new("x").expect("v"),
            NodeId::try_new("y").expect("v"),
            "KNOWS",
            1,
        )
        .expect("valid");
        assert_ne!(a, b, "different seq values must produce distinct EdgeIds");
        assert_eq!(a.seq, 0);
        assert_eq!(b.seq, 1);
    }

    #[test]
    fn edge_id_display_fromstr_roundtrip() {
        let e = EdgeId::try_first(
            NodeId::try_new("alice").expect("v"),
            NodeId::try_new("bob").expect("v"),
            "KNOWS",
        )
        .expect("valid");
        let s = e.to_string();
        let parsed: EdgeId = s.parse().expect("round-trip must succeed");
        assert_eq!(e, parsed);

        let e2 = EdgeId::try_with_seq(
            NodeId::try_new("a|b:c").expect("v"),
            NodeId::try_new("d-->e").expect("v"),
            "label--weird-->one",
            42,
        )
        .expect("valid");
        let s2 = e2.to_string();
        let parsed2: EdgeId = s2
            .parse()
            .expect("round-trip with weird chars must succeed");
        assert_eq!(e2, parsed2);

        let e3 = EdgeId::try_first(
            NodeId::try_new("n1").expect("v"),
            NodeId::try_new("n2").expect("v"),
            "REL",
        )
        .expect("valid");
        let s3 = e3.to_string();
        let parsed3: EdgeId = s3.parse().expect("seq=0 round-trip must succeed");
        assert_eq!(e3, parsed3);
    }

    #[test]
    fn edge_id_serde_roundtrip() {
        let e = EdgeId::try_with_seq(
            NodeId::try_new("src").expect("v"),
            NodeId::try_new("dst").expect("v"),
            "EDGE",
            7,
        )
        .expect("valid");
        let bytes = zerompk::to_msgpack_vec(&e).expect("msgpack serialization must succeed");
        let decoded: EdgeId =
            zerompk::from_msgpack(&bytes).expect("msgpack deserialization must succeed");
        assert_eq!(e, decoded);
    }

    #[test]
    fn from_str_rejects_empty_label() {
        // An EdgeId whose Display has an empty label segment should fail FromStr
        // because validate("") returns IdError::Empty.
        // We construct one using from_validated (bypassing validation) and
        // then parse the Display string — expecting InvalidLabel(Empty).
        let e = EdgeId {
            src: NodeId::from_validated("a".to_owned()),
            dst: NodeId::from_validated("b".to_owned()),
            label: String::new(),
            seq: 0,
        };
        let s = e.to_string();
        let err = s
            .parse::<EdgeId>()
            .expect_err("empty label must fail parse");
        assert!(matches!(
            err,
            EdgeIdParseError::InvalidLabel(IdError::Empty)
        ));
    }
}
