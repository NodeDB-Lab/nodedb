// SPDX-License-Identifier: BUSL-1.1

//! The response contract of a Calvin write that carries `RETURNING`.
//!
//! The coordinator fixes everything the response evaluation reads, so every
//! replica that evaluates it reads the same inputs: the full item list, the
//! source field order, the roles of the writer, the `currval` of the
//! sequences the list names, and the response limits.

use nodedb_types::ReturningSpec;
use serde::{Deserialize, Serialize};

/// One sequence and the last value the writer's session obtained from it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SequenceValue {
    /// The bare sequence name.
    pub name: String,
    /// The value `nextval` last handed the writer's session.
    pub value: i64,
}

/// The hard response limits of one `RETURNING` evaluation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReturningContractLimits {
    /// Most rows a response holds.
    pub rows: u64,
    /// Most columns a response holds.
    pub columns: u64,
    /// Most cells (rows times columns) a response holds.
    pub cells: u64,
    /// Most bytes the encoded response payload holds.
    pub payload_bytes: u64,
}

/// One column the statement announced to its client, before any row
/// existed.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct AnnouncedColumn {
    /// The column's client-facing name.
    pub name: String,
    /// The wire name of the column's type.
    pub ty: String,
}

/// The `RETURNING` list of one plan of the transaction.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct PlanReturning {
    /// Index of the plan in the class's plan batch.
    pub plan_index: u32,
    /// The plan's collection in its stored qualified form, as
    /// `QualifiedCollection::as_str` writes it. The redaction snapshot and
    /// the staged images name the collection in this form.
    pub collection: String,
    /// The full ordered response list, with aliases and computed items.
    pub spec: ReturningSpec,
    /// The declared source field order of the plan's collection. It names
    /// the columns of a response with no rows.
    pub source_columns: Vec<String>,
    /// The response limits.
    pub limits: ReturningContractLimits,
    /// The columns the statement announced, one per list item, in list
    /// order. The evaluation answers each cell under its column's name and
    /// type. `None` when the statement announced none: a star, or a
    /// statement with no announced columns.
    pub announced: Option<Vec<AnnouncedColumn>>,
}

/// What a participant needs to evaluate the `RETURNING` lists of a
/// transaction.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ReturningContract {
    /// One entry per plan that carries `RETURNING`, ascending by plan index.
    pub plans: Vec<PlanReturning>,
    /// The roles of the writer, in the order the redaction snapshot reads
    /// them. Shared by every plan.
    pub roles: Vec<String>,
    /// The `currval` of every sequence any list names, ascending by name. A
    /// list that names a sequence at run time holds every sequence the
    /// writer's session has a value for. Shared by every plan.
    pub sequence_values: Vec<SequenceValue>,
}

#[cfg(test)]
use nodedb_types::{ReturningColumns, ReturningItem};

/// A contract that sets every field, for round-trip tests.
#[cfg(test)]
pub(crate) fn sample_contract() -> ReturningContract {
    let limits = ReturningContractLimits {
        rows: 10_000,
        columns: 256,
        cells: 1_000_000,
        payload_bytes: 33_554_432,
    };
    ReturningContract {
        plans: vec![
            PlanReturning {
                plan_index: 0,
                collection: "orders".into(),
                spec: ReturningSpec {
                    columns: ReturningColumns::Named(vec![
                        ReturningItem::Column {
                            name: "id".into(),
                            alias: Some("key".into()),
                        },
                        ReturningItem::Computed {
                            expression: vec![0x91, 0x01],
                            name: "next".into(),
                        },
                        ReturningItem::Star,
                    ]),
                    precommit: None,
                },
                source_columns: vec!["id".into(), "balance".into()],
                limits,
                announced: Some(vec![
                    AnnouncedColumn {
                        name: "key".into(),
                        ty: "text".into(),
                    },
                    AnnouncedColumn {
                        name: "next".into(),
                        ty: "int8".into(),
                    },
                ]),
            },
            PlanReturning {
                plan_index: 2,
                collection: "7/orders".into(),
                spec: ReturningSpec {
                    columns: ReturningColumns::Star,
                    precommit: None,
                },
                source_columns: vec!["id".into()],
                limits,
                announced: None,
            },
        ],
        roles: vec!["reader".into(), "writer".into()],
        sequence_values: vec![SequenceValue {
            name: "orders_seq".into(),
            value: 41,
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_contract_survives_a_msgpack_round_trip() {
        let contract = sample_contract();
        let bytes = zerompk::to_msgpack_vec(&contract).expect("encode contract");
        let decoded: ReturningContract = zerompk::from_msgpack(&bytes).expect("decode contract");
        assert_eq!(decoded, contract);
    }
}
