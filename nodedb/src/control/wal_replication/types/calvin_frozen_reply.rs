// SPDX-License-Identifier: BUSL-1.1

//! The frozen `RETURNING` replies of one Calvin slice, as its data-group log
//! carries them. A slice that holds a `RETURNING` contract answers its
//! coordinator with the encoded [`CalvinFrozenReply`].
//!
//! The slice's leader evaluates the contract's lists before it votes, and
//! proposes the result as a `ReplicatedWrite::CalvinFrozenReply` entry. It
//! votes only once the entry applied. Every replica stores the first copy
//! of the entry, keyed by the txn, and the slice's install answers it. So
//! every replica answers the same bytes, and no replica evaluates again.

/// One sequence change a frozen evaluation made, and the value it returned.
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
pub enum FrozenSequenceEffect {
    /// `nextval(name)` returned `value`. The writer's session takes it as
    /// its `currval` of `name`.
    Nextval { name: String, value: i64 },
    /// `setval(name, ..)` returned `value`.
    Setval { name: String, value: i64 },
}

/// The lines a timeseries ingest plan rejected, which its reply reports
/// beside its rows.
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
#[msgpack(map)]
pub struct FrozenRejection {
    pub collection: String,
    pub lines: u64,
}

/// The frozen reply of one `RETURNING` plan.
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
#[msgpack(map)]
pub struct FrozenPlanReply {
    /// The plan's index in the class's plan batch.
    pub plan_index: u32,
    /// The final rows, a `RowsPayload` of typed cells.
    pub payload: Vec<u8>,
    /// The wire name of each column's type, parallel to the payload's
    /// columns. See `DdlColType::wire_name`.
    pub column_types: Vec<String>,
    /// The `nextval` and `setval` results of the evaluation, in invocation
    /// order.
    pub sequence_effects: Vec<FrozenSequenceEffect>,
    /// The lines the plan rejected, for a timeseries ingest that rejected
    /// any.
    pub rejected: Option<FrozenRejection>,
}

/// The frozen replies of every local `RETURNING` plan of Calvin txn
/// `(epoch, position)` on `vshard`, ascending by plan index.
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
#[msgpack(map)]
pub struct CalvinFrozenReply {
    pub epoch: u64,
    pub position: u32,
    pub vshard: u32,
    pub plans: Vec<FrozenPlanReply>,
}

impl CalvinFrozenReply {
    /// The reply as the bytes a stored row holds.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("calvin frozen reply: {e}"),
        })
    }

    /// The reply `bytes` encode.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("calvin frozen reply: {e}"),
        })
    }

    /// The plan whose rows the slice answers: the last plan, as a slice
    /// answers its last `RETURNING` plan's rows.
    pub fn answered_plan(&self) -> Option<&FrozenPlanReply> {
        self.plans.iter().max_by_key(|plan| plan.plan_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply() -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch: 4,
            position: 2,
            vshard: 9,
            plans: vec![
                FrozenPlanReply {
                    plan_index: 0,
                    payload: vec![0x90],
                    column_types: vec!["int8".into()],
                    sequence_effects: vec![FrozenSequenceEffect::Nextval {
                        name: "s".into(),
                        value: 3,
                    }],
                    rejected: None,
                },
                FrozenPlanReply {
                    plan_index: 3,
                    payload: vec![0x91],
                    column_types: Vec::new(),
                    sequence_effects: vec![FrozenSequenceEffect::Setval {
                        name: "s".into(),
                        value: 10,
                    }],
                    rejected: Some(FrozenRejection {
                        collection: "metrics".into(),
                        lines: 2,
                    }),
                },
            ],
        }
    }

    #[test]
    fn a_frozen_reply_round_trips() {
        let stored = reply().to_bytes().expect("encode");
        assert_eq!(
            CalvinFrozenReply::from_bytes(&stored).expect("decode"),
            reply()
        );
    }

    #[test]
    fn the_last_plan_answers() {
        assert_eq!(reply().answered_plan().map(|plan| plan.plan_index), Some(3));
    }
}
