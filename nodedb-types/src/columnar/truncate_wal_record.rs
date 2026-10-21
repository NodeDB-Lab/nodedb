// SPDX-License-Identifier: Apache-2.0

//! Columnar-family `TRUNCATE` WAL record payload.
//!
//! Rides `RecordType::ColumnarTruncate` (columnar and spatial collections)
//! and `RecordType::TimeseriesTruncate` (timeseries collections). The record
//! type says which engine clears the collection. The payload carries the
//! collection name and, for a bitemporal columnar collection, the ordinal its
//! tombstones stamp. `restart_identity` is a Control-Plane sequence concern
//! applied after dispatch and never enters the Data-Plane record.

use serde::{Deserialize, Serialize};

/// Map-encoded columnar-family truncate WAL record.
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
pub struct ColumnarTruncateWalRecord {
    /// Target collection name.
    pub collection: String,
    /// The system-time ordinal at which a truncate of a bitemporal columnar
    /// collection closes every current key, raised per key above its bound
    /// row. `None` for any other truncate.
    #[msgpack(default)]
    #[serde(default)]
    pub system_from: Option<i64>,
    /// Erase every row and version even on a bitemporal collection: the
    /// clear a backup restore runs before it reinstalls every version.
    #[msgpack(default)]
    #[serde(default)]
    pub erase_history: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let rec = ColumnarTruncateWalRecord {
            collection: "metrics".to_string(),
            system_from: Some(42),
            erase_history: true,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let back: ColumnarTruncateWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, rec);
    }
}
