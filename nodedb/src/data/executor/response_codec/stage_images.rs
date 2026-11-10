// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` images a Calvin stage answers with.
//!
//! A stage response carries one entry per local plan that carries
//! `RETURNING`. Each row is the full source image the plan staged, before
//! projection, with the caller's read policy applied. The participant leader
//! keys the entries by plan index and evaluates the response from them.

use nodedb_types::Value;

/// Every `RETURNING` plan's images of one stage, in plan order.
#[derive(Debug, Clone, Default, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct StagedReturningImages {
    pub plans: Vec<StagedPlanImages>,
}

/// The images one `RETURNING` plan staged.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct StagedPlanImages {
    /// Index of the plan in the stage's local plan slice.
    pub plan: u32,
    /// The collection the plan writes, as the Data Plane keys it.
    pub collection: String,
    /// The rows the read policy shows, in the order the plan wrote them.
    pub rows: Vec<StagedImageRow>,
}

/// One row image of a `RETURNING` plan.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct StagedImageRow {
    /// The row's stable identity bytes. Never a user-visible field.
    pub key: Vec<u8>,
    /// The row's position among every row the plan wrote, counted before the
    /// read policy hides any.
    pub ordinal: u32,
    /// The full source row: a `Value::Object` in its tagged encoding, so
    /// every value keeps its type.
    pub fields: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn images_round_trip_with_typed_values() {
        let decimal = Value::Decimal(rust_decimal::Decimal::new(1234, 2));
        let fields = Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::String("r1".into())),
            ("price".to_string(), decimal),
        ]));
        let images = StagedReturningImages {
            plans: vec![StagedPlanImages {
                plan: 2,
                collection: "orders".into(),
                rows: vec![StagedImageRow {
                    key: b"r1".to_vec(),
                    ordinal: 0,
                    fields,
                }],
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&images).expect("encode images");
        let back: StagedReturningImages = zerompk::from_msgpack(&bytes).expect("decode images");
        assert_eq!(back, images);
    }
}
