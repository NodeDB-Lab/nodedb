// SPDX-License-Identifier: Apache-2.0

//! The answer of `MetaOp::ResolveTxnPart`: one part of a transaction's
//! resolved redo, as framed redo bytes.

/// One part of a resolved redo.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ResolvedTxnPart {
    /// The part this answer carries, counted from `0`.
    pub index: u32,
    /// Whole redo frames. Empty only when the resolve holds no frame.
    pub bytes: Vec<u8>,
    /// Whether this is the last part. The cursor is closed once it is.
    pub done: bool,
    /// What the resolve found. Set on part `0` alone.
    pub opened: Option<ResolvedTxnOpen>,
}

/// What a resolve found, as part `0` reports it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ResolvedTxnOpen {
    /// The byte length of every part the cursor returns, part `0` included.
    pub len: u64,
    /// How many engine sub-records the redo holds.
    pub op_count: u64,
    /// The timeseries lines the resolve rejected, by collection.
    pub ts_rejected: Vec<RejectedLines>,
}

/// The lines a resolve rejected for one collection.
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
pub struct RejectedLines {
    pub collection: String,
    pub lines: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resolved_part_round_trips() {
        let part = ResolvedTxnPart {
            index: 0,
            bytes: vec![1, 2, 3],
            done: false,
            opened: Some(ResolvedTxnOpen {
                len: 4096,
                op_count: 12,
                ts_rejected: vec![RejectedLines {
                    collection: "metrics".into(),
                    lines: 3,
                }],
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&part).expect("encode");
        let decoded: ResolvedTxnPart = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, part);
    }
}
