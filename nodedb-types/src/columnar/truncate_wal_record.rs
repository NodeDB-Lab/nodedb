// SPDX-License-Identifier: Apache-2.0

//! Columnar-family `TRUNCATE` WAL record payload.
//!
//! Rides `RecordType::ColumnarTruncate` (columnar and spatial collections)
//! and `RecordType::TimeseriesTruncate` (timeseries collections). The record
//! type says which engine clears the collection, so the payload carries the
//! collection name only; `restart_identity` is a Control-Plane sequence
//! concern applied after dispatch and never enters the Data-Plane record.

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let rec = ColumnarTruncateWalRecord {
            collection: "metrics".to_string(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let back: ColumnarTruncateWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, rec);
    }
}
