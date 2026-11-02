// SPDX-License-Identifier: BUSL-1.1

//! Payload of one restored columnar key: a `RecordType::TimeseriesBatch`
//! sub-record of a RESTORE batch's redo.
//!
//! A backup holds every version of a columnar primary key with the interval
//! the source stamped. The sub-record carries every version of one key, so
//! every replica and every replay installs each version at exactly its
//! interval, and a read at any cut answers as the source answered.
//!
//! A RESTORE is a write in the Calvin sequence. The resolve reads the
//! destination's newest ordinal for the key and rewrites `versions` to the
//! exact versions the install writes (`restore_key_plan`). The install never
//! reads the destination to choose, so every replica writes alike.
//!
//! The payload is the pair `("columnar_restore", ColumnarRestoreRedo)`. The
//! other `TimeseriesBatch` shapes are maps or tuples that never open with a
//! two-element array holding this discriminator.

use serde::{Deserialize, Serialize};

/// The discriminator a restored columnar key's payload opens with.
pub const COLUMNAR_RESTORE: &str = "columnar_restore";

/// One version of a restored columnar key.
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
pub struct ColumnarRestoreVersion {
    /// The row as a native-`Value` msgpack object keyed by column name.
    pub row: Vec<u8>,
    /// The ordinal the version starts at.
    pub sys_from: i64,
    /// The ordinal the version ends at. `None` for the open version.
    pub sys_to: Option<i64>,
}

/// Every version of one restored columnar key.
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
pub struct ColumnarRestoreRedo {
    /// The collection, as the destination Data Plane stores it.
    pub collection: String,
    /// The catalog schema (`ColumnarSchema`, msgpack). The install creates
    /// the engine from it on a core that holds no row of the collection.
    pub schema_bytes: Vec<u8>,
    /// The key's identity in the destination database. Every version
    /// installs under it.
    pub surrogate: u32,
    /// The versions to install, ascending by `sys_from`.
    pub versions: Vec<ColumnarRestoreVersion>,
}

impl ColumnarRestoreRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(COLUMNAR_RESTORE, self)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal columnar restore of '{}': {e}", self.collection),
            }
        })
    }

    /// Decode a sub-record payload. `None` when it is no restored columnar
    /// key.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == COLUMNAR_RESTORE).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ColumnarRestoreRedo {
        ColumnarRestoreRedo {
            collection: "metrics".into(),
            schema_bytes: vec![1, 2],
            surrogate: 9,
            versions: vec![
                ColumnarRestoreVersion {
                    row: vec![3],
                    sys_from: 10,
                    sys_to: Some(20),
                },
                ColumnarRestoreVersion {
                    row: vec![4],
                    sys_from: 20,
                    sys_to: None,
                },
            ],
        }
    }

    #[test]
    fn a_restored_key_round_trips() {
        let bytes = sample().encode().expect("encode");
        assert_eq!(ColumnarRestoreRedo::decode(&bytes), Some(sample()));
    }

    #[test]
    fn another_pair_is_no_restored_key() {
        let other = zerompk::to_msgpack_vec(&("kv_restore", sample())).expect("encode");
        assert!(ColumnarRestoreRedo::decode(&other).is_none());
    }
}
