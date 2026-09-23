// SPDX-License-Identifier: Apache-2.0

//! Columnar WAL record payload.
//!
//! The plain-columnar engine carries a per-row global [`Surrogate`] (the
//! cross-engine `u32` identity). Earlier WAL records encoded only
//! `(kind, collection, payload, provenance)` as a msgpack **array tuple** and
//! dropped the surrogates, so on restart replay re-derived them as `None` and
//! the identity changed across the crash boundary — a durability bug.
//!
//! [`ColumnarWalRecord`] persists the surrogates alongside the row bytes. The
//! `surrogates` vector is **parallel to the rows** encoded in `payload`:
//! `surrogates[i]` is the surrogate for the i-th row. It may be empty (the
//! sync/CRDT columnar path does not currently carry surrogates), in which case
//! replay falls back to allocating fresh identity exactly as before.
//!
//! ## Backward compatibility
//!
//! The new record is encoded as a msgpack **map** (`#[msgpack(map)]`), whereas
//! legacy on-disk records are msgpack **arrays** (the 4-tuple). The two wire
//! shapes are mutually distinguishable, so the replay decoder first attempts
//! [`ColumnarWalRecord`] and falls back to the legacy tuple on failure, treating
//! the surrogates as empty for old records. The `#[msgpack(default)]` on
//! `surrogates` additionally lets a future map-encoded record that omits the
//! field decode with an empty vector.

use serde::{Deserialize, Serialize};

use crate::Surrogate;
use crate::sync::wire::SyncProvenance;

/// Map-encoded columnar WAL record carrying per-row surrogates.
///
/// `surrogates` is index-aligned with the rows serialized in `payload`.
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
pub struct ColumnarWalRecord {
    /// Record kind tag — always `"columnar"`. Distinguishes this record from
    /// timeseries batches sharing the same `TimeseriesBatch` WAL record type.
    pub kind: String,
    /// Target collection name.
    pub collection: String,
    /// MessagePack-encoded rows (`Value::Array([Value::Object, ...])`).
    pub payload: Vec<u8>,
    /// Optional sync idempotency context.
    #[serde(default)]
    #[msgpack(default)]
    pub provenance: Option<SyncProvenance>,
    /// Per-row surrogates, parallel to the rows in `payload`. Empty when the
    /// originating path does not carry cross-engine identity (e.g. sync/CRDT).
    #[serde(default)]
    #[msgpack(default)]
    pub surrogates: Vec<Surrogate>,
    /// What a row whose primary key already exists does, encoded by the
    /// writer that knows the insert's conflict intent and `ON CONFLICT`
    /// assignments. Empty for a plain insert, which replaces the row. Replay
    /// decides each row the way the live insert did.
    #[serde(default)]
    #[msgpack(default)]
    pub conflict_policy: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_surrogates() {
        let prov = SyncProvenance {
            producer_id: 7,
            epoch: 2,
            stream_id: 99,
            seq: 13,
        };
        let rec = ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: "metrics".to_string(),
            payload: vec![1, 2, 3, 4],
            provenance: Some(prov.clone()),
            surrogates: vec![Surrogate::new(10), Surrogate::new(11), Surrogate::new(12)],
            conflict_policy: Vec::new(),
        };

        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode ColumnarWalRecord");
        let decoded: ColumnarWalRecord =
            zerompk::from_msgpack(&bytes).expect("decode ColumnarWalRecord");

        assert_eq!(decoded.kind, "columnar");
        assert_eq!(decoded.collection, "metrics");
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
        assert_eq!(decoded.provenance, Some(prov));
        assert_eq!(
            decoded.surrogates,
            vec![Surrogate::new(10), Surrogate::new(11), Surrogate::new(12)]
        );
    }

    #[test]
    fn map_record_with_empty_surrogates_round_trips() {
        let rec = ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: "c".to_string(),
            payload: vec![9],
            provenance: None,
            surrogates: Vec::new(),
            conflict_policy: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.surrogates, Vec::<Surrogate>::new());
        assert_eq!(decoded.provenance, None);
    }

    #[test]
    fn legacy_tuple_does_not_decode_as_map_record() {
        // A legacy 4-tuple is a msgpack ARRAY; decoding it as the map-shaped
        // ColumnarWalRecord must fail so the replay decoder can fall back to
        // the legacy tuple decode.
        let prov: Option<SyncProvenance> = None;
        let legacy = zerompk::to_msgpack_vec(&(
            "columnar".to_string(),
            "metrics".to_string(),
            vec![1u8, 2, 3],
            prov,
        ))
        .expect("encode legacy tuple");

        let as_map: Result<ColumnarWalRecord, _> = zerompk::from_msgpack(&legacy);
        assert!(
            as_map.is_err(),
            "legacy array tuple must not decode as map-shaped ColumnarWalRecord"
        );
    }
}
