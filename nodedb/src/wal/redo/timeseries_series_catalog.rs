// SPDX-License-Identifier: BUSL-1.1

//! The series catalog of one timeseries collection a RESTORE installs: a
//! `RecordType::TimeseriesBatch` sub-record.
//!
//! Every stored row carries the id its series key resolved to in the
//! collection's series catalog, and the last-value cache keys on it. A
//! backup keeps each collection's catalog, so the restored rows and ended
//! partitions keep the ids they carried. The sub-record carries the catalog
//! file's bytes. Its apply merges every registration into the destination's
//! catalog, and refuses the batch when an id or a key is registered
//! otherwise there. The catalog installs ahead of the collection's rows, at
//! the same log position on every replica.
//!
//! The payload is the pair
//! `("timeseries_series_catalog", TimeseriesSeriesCatalogRedo)`.

/// The discriminator a series-catalog payload opens with.
pub const TIMESERIES_SERIES_CATALOG: &str = "timeseries_series_catalog";

/// The series catalog of one timeseries collection.
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
pub struct TimeseriesSeriesCatalogRedo {
    /// The collection, as the destination Data Plane stores it.
    pub collection: String,
    /// The catalog, in the `series.catalog` file encoding.
    pub catalog: Vec<u8>,
}

impl TimeseriesSeriesCatalogRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(TIMESERIES_SERIES_CATALOG, self)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal series catalog of '{}': {e}", self.collection),
            }
        })
    }

    /// Decode a sub-record payload. `None` when it is no series catalog.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == TIMESERIES_SERIES_CATALOG).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_series_catalog_round_trips() {
        let redo = TimeseriesSeriesCatalogRedo {
            collection: "cpu".into(),
            catalog: vec![1, 2],
        };
        let bytes = redo.encode().expect("encode");
        assert_eq!(TimeseriesSeriesCatalogRedo::decode(&bytes), Some(redo));
        assert!(crate::wal::TimeseriesRestoreRedo::decode(&bytes).is_none());
    }
}
