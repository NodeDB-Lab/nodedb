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

impl FrozenSequenceEffect {
    /// The sequence the effect changed, and the value it left as `currval`.
    pub fn name_value(&self) -> (&str, i64) {
        match self {
            Self::Nextval { name, value } | Self::Setval { name, value } => (name, *value),
        }
    }
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

/// The sequence changes one frozen plan made, or its refusal, as a
/// participant sends them to a participant whose evaluation follows the
/// plan.
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
pub struct PlanSequenceEffects {
    /// The plan's index in the class's plan batch.
    pub plan_index: u32,
    /// The plan's `nextval` and `setval` results, in invocation order.
    /// Empty when `refused`.
    pub effects: Vec<FrozenSequenceEffect>,
    /// Whether the participant voted abort before the plan was stored. A
    /// participant that follows the plan then votes abort at once: the txn
    /// cannot commit.
    pub refused: bool,
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

    /// Whether this reply holds a plan at `plan_index`.
    pub fn holds_plan(&self, plan_index: u32) -> bool {
        self.plans.iter().any(|plan| plan.plan_index == plan_index)
    }

    /// The reply a stored row holds once `later`, another copy of this
    /// txn's reply on this vShard, applies over this one. A leader that
    /// evaluates its plans in rounds proposes each round's copy with every
    /// stored plan in it. `later` extends the row when it holds every plan
    /// the row holds, unchanged, and more. Any other copy changes nothing:
    /// `None`. Every replica applies the copies in log order, so every
    /// replica keeps the same row.
    pub fn extended_by(&self, later: &Self) -> Option<Self> {
        let same_txn =
            (self.epoch, self.position, self.vshard) == (later.epoch, later.position, later.vshard);
        let keeps_every_plan = self
            .plans
            .iter()
            .all(|held| later.plans.iter().any(|plan| plan == held));
        (same_txn && keeps_every_plan && later.plans.len() > self.plans.len())
            .then(|| later.clone())
    }

    /// The union of `replies`, the frozen replies of one txn's participants:
    /// every plan of each, in statement order. That is ascending by plan
    /// index, and ascending by vShard within one plan index, as a plan that
    /// spans vShards answers each one's rows. The union's `vshard` is its
    /// lowest participant. `None` for no reply. Replies of two txns fail.
    pub fn union(mut replies: Vec<Self>) -> crate::Result<Option<Self>> {
        replies.sort_by_key(|reply| reply.vshard);
        let mut replies = replies.into_iter();
        let Some(mut union) = replies.next() else {
            return Ok(None);
        };
        for reply in replies {
            if (reply.epoch, reply.position) != (union.epoch, union.position) {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "calvin RETURNING: the frozen replies of txns ({}, {}) and ({}, {}) \
                         answer one statement",
                        union.epoch, union.position, reply.epoch, reply.position
                    ),
                });
            }
            union.plans.extend(reply.plans);
        }
        // A stable sort keeps the vShard order within one plan index.
        union.plans.sort_by_key(|plan| plan.plan_index);
        Ok(Some(union))
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

    /// A copy that keeps every stored plan and adds one extends the row. A
    /// copy that changes a stored plan, adds none, or names another txn
    /// does not.
    #[test]
    fn a_later_round_extends_the_stored_reply() {
        let full = reply();
        let first = CalvinFrozenReply {
            plans: vec![full.plans[0].clone()],
            ..full.clone()
        };
        assert_eq!(first.extended_by(&full), Some(full.clone()));
        assert_eq!(full.extended_by(&full), None, "no new plan");
        assert_eq!(full.extended_by(&first), None, "a shorter copy");
        let mut changed = full.clone();
        changed.plans[0].payload = vec![0x91];
        assert_eq!(first.extended_by(&changed), None, "a changed plan");
        let other = CalvinFrozenReply {
            position: 9,
            ..full.clone()
        };
        assert_eq!(first.extended_by(&other), None, "another txn");
        assert!(full.holds_plan(3) && !first.holds_plan(3));
    }

    #[test]
    fn a_frozen_reply_round_trips() {
        let stored = reply().to_bytes().expect("encode");
        assert_eq!(
            CalvinFrozenReply::from_bytes(&stored).expect("decode"),
            reply()
        );
    }

    /// The union holds every plan of every participant, by plan index, and
    /// by vShard within one plan index.
    #[test]
    fn the_union_holds_every_plan_in_statement_order() {
        let at = |vshard: u32, indexes: &[u32]| CalvinFrozenReply {
            vshard,
            plans: indexes
                .iter()
                .map(|index| FrozenPlanReply {
                    plan_index: *index,
                    payload: vec![u8::try_from(vshard).expect("small vshard")],
                    ..reply().plans[0].clone()
                })
                .collect(),
            ..reply()
        };
        let union = CalvinFrozenReply::union(vec![at(7, &[1, 2]), at(3, &[0, 2])])
            .expect("one txn")
            .expect("a union");
        assert_eq!(union.vshard, 3);
        let order: Vec<(u32, u8)> = union
            .plans
            .iter()
            .map(|plan| (plan.plan_index, plan.payload[0]))
            .collect();
        assert_eq!(order, vec![(0, 3), (1, 7), (2, 3), (2, 7)]);

        assert!(
            CalvinFrozenReply::union(Vec::new())
                .expect("none")
                .is_none()
        );
        let other_txn = CalvinFrozenReply {
            position: 5,
            ..at(4, &[0])
        };
        assert!(CalvinFrozenReply::union(vec![at(3, &[0]), other_txn]).is_err());
    }
}
