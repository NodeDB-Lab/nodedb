// SPDX-License-Identifier: BUSL-1.1

//! The restored rows of one timeseries collection in a RESTORE batch: a
//! `RecordType::TimeseriesBatch` sub-record its resolve replaces.
//!
//! A timeseries row has no key. An ingest appends its rows and never matches
//! them against stored ones, and every writer of a timeseries collection
//! locks the collection whole. So the unit a RESTORE orders against is the
//! collection. The batch carries the backup's current rows: the memtable and
//! every partition no TRUNCATE ended. A row of an ended partition is no
//! current row, and the RESTORE never makes it current again.
//!
//! The resolve reads the destination's partition ends at the transaction's
//! turn. A partition ended above the RESTORE's cut names a TRUNCATE after the
//! RESTORE began: that TRUNCATE ends every row the RESTORE wrote, so nothing
//! installs. Otherwise the resolve turns the rows into the `ts-resolved`
//! ingest sub-record a live ingest writes, and the install appends them at
//! the transaction's ordinal. This sub-record never reaches an install.
//!
//! The payload is the pair `("timeseries_restore", TimeseriesRestoreRedo)`.

use serde::{Deserialize, Serialize};

/// The discriminator a restored timeseries collection's payload opens with.
pub const TIMESERIES_RESTORE: &str = "timeseries_restore";

/// Some restored rows of one timeseries collection.
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
pub struct TimeseriesRestoreRedo {
    /// The collection, as the destination Data Plane stores it.
    pub collection: String,
    /// The rows: the native-`Value` msgpack encoding of `Value::Array` of
    /// row objects, the shape the `msgpack` ingest format decodes.
    pub rows: Vec<u8>,
}

impl TimeseriesRestoreRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(TIMESERIES_RESTORE, self)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal timeseries restore of '{}': {e}", self.collection),
            }
        })
    }

    /// Decode a sub-record payload. `None` when it is no restored
    /// timeseries collection.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == TIMESERIES_RESTORE).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restored_rows_round_trip() {
        let redo = TimeseriesRestoreRedo {
            collection: "cpu".into(),
            rows: vec![1, 2, 3],
        };
        let bytes = redo.encode().expect("encode");
        assert_eq!(TimeseriesRestoreRedo::decode(&bytes), Some(redo));
        assert!(crate::wal::ColumnarRestoreRedo::decode(&bytes).is_none());
    }
}
