// SPDX-License-Identifier: Apache-2.0

//! Columnar row-image WAL record payload.
//!
//! A committed transaction's columnar writes travel as the final row images
//! the transaction staged and showed its own reads. Each entry names one row
//! by its cross-engine surrogate. It carries the primary key of the base row
//! the transaction replaced or removed, and the image the row holds after the
//! commit. Replay installs the image verbatim. It never re-runs an
//! `ON CONFLICT` merge, a SET list or a predicate against the replaying
//! node's state.
//!
//! Rides `RecordType::TimeseriesBatch`, disambiguated from the other columnar
//! record shapes by `kind = "columnar_image"`.

use serde::{Deserialize, Serialize};

/// The `kind` tag every [`ColumnarImageWalRecord`] carries.
pub const COLUMNAR_IMAGE_KIND: &str = "columnar_image";

/// One row inside a [`ColumnarImageWalRecord`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ColumnarImageWalRow {
    /// The row's cross-engine surrogate.
    pub surrogate: u32,
    /// MessagePack-encoded primary key of the base row this write replaces or
    /// removes. Empty when the row had no base row the write displaced by key
    /// (an insert or an `ON CONFLICT` upsert, which the image's own key
    /// overwrites).
    pub prior_pk_msgpack: Vec<u8>,
    /// MessagePack-encoded post-image (`Value::Object`, column name to value,
    /// bitemporal columns included). Empty for a delete.
    pub image_msgpack: Vec<u8>,
}

/// Map-encoded columnar row-image WAL record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ColumnarImageWalRecord {
    /// Record kind tag. Always [`COLUMNAR_IMAGE_KIND`].
    pub kind: String,
    /// Target collection name.
    pub collection: String,
    /// The catalog schema the writing plan carried (`ColumnarSchema`,
    /// MessagePack). Empty when the plan carried none. Replay uses it to
    /// create the engine on a node that holds no row of the collection yet.
    pub schema_bytes: Vec<u8>,
    /// Every row the transaction wrote, in surrogate order.
    pub rows: Vec<ColumnarImageWalRow>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnarDmlWalRecord, ColumnarResolvedDmlWalRecord};

    fn record() -> ColumnarImageWalRecord {
        ColumnarImageWalRecord {
            kind: COLUMNAR_IMAGE_KIND.to_string(),
            collection: "events".to_string(),
            schema_bytes: vec![9],
            rows: vec![
                ColumnarImageWalRow {
                    surrogate: 7,
                    prior_pk_msgpack: vec![1],
                    image_msgpack: vec![2, 3],
                },
                ColumnarImageWalRow {
                    surrogate: 8,
                    prior_pk_msgpack: vec![4],
                    image_msgpack: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn round_trips_every_row() {
        let rec = record();
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarImageWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn does_not_decode_as_the_dml_record_shapes() {
        let bytes = zerompk::to_msgpack_vec(&record()).expect("encode");
        let as_dml = zerompk::from_msgpack::<ColumnarDmlWalRecord>(&bytes);
        assert!(as_dml.map(|r| r.kind != "columnar_dml").unwrap_or(true));
        let as_resolved = zerompk::from_msgpack::<ColumnarResolvedDmlWalRecord>(&bytes);
        assert!(
            as_resolved
                .map(|r| r.kind != "columnar_resolved_dml")
                .unwrap_or(true)
        );
    }
}
