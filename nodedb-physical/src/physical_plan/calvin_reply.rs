// SPDX-License-Identifier: Apache-2.0

//! The reply a committed Calvin slice answers with, in the form a redo
//! entry carries to every replica.
//!
//! The leader's stage decides the reply. Every replica answers it after its
//! own install of the slice's redo, so every replica answers the same bytes.

/// What a Calvin slice answers once its redo installs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CalvinReplySpec {
    /// An affected count, decided when the plan staged.
    Count(Vec<u8>),
    /// `RETURNING` rows, decided when the plan staged.
    Rows(Vec<u8>),
    /// The frozen `RETURNING` replies of the slice's txn, which its
    /// data-group log stored before the slice's vote. The install renders
    /// nothing. Every replica answers the stored reply.
    Frozen,
}

/// What a stamped redo install of a Calvin slice needs beyond the redo
/// record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinInstall {
    pub epoch: u64,
    pub position: u32,
    /// The epoch's deterministic instant. The install stamps graph versions
    /// at the transaction's ordinal.
    pub epoch_system_ms: i64,
    /// The reply the install answers with.
    pub reply: CalvinReplySpec,
    /// Whether the install raises the tenant's write mark. A slice that only
    /// installs schema raises none.
    pub user_write: bool,
    /// Whether the slice's position is a mirror source's. It names no staged
    /// entry on this cluster.
    pub mirror: bool,
}

/// The answer of `MetaOp::CalvinResolve`: one part of the slice's resolved
/// redo, and on part `0` the reply its stage decided.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinResolvedPart {
    pub part: super::ResolvedTxnPart,
    /// Set on part `0` alone.
    pub reply: Option<CalvinReplySpec>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resolved_answer_round_trips() {
        for reply in [
            CalvinReplySpec::Count(vec![4]),
            CalvinReplySpec::Rows(vec![0x90]),
            CalvinReplySpec::Frozen,
        ] {
            let resolved = CalvinResolvedPart {
                part: super::super::ResolvedTxnPart {
                    index: 0,
                    bytes: vec![1, 2, 3],
                    done: true,
                    opened: None,
                },
                reply: Some(reply),
            };
            let bytes = zerompk::to_msgpack_vec(&resolved).expect("encode");
            let decoded: CalvinResolvedPart = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, resolved);
        }
    }
}
