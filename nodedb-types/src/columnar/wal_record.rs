// SPDX-License-Identifier: Apache-2.0

//! Columnar WAL record payload.
//!
//! The plain-columnar engine carries a per-row global [`Surrogate`] (the
//! cross-engine `u32` identity). [`ColumnarWalRecord`] persists the
//! surrogates alongside the row bytes. The `surrogates` vector is **parallel
//! to the rows** encoded in `payload`: `surrogates[i]` is the surrogate for the
//! i-th row. It is empty when the insert carried no surrogates. Replay then
//! leaves the rows unbound, as the live insert did.
//!
//! The record is a msgpack **map** (`#[msgpack(map)]`). It decodes only with
//! every field present.

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
    pub provenance: Option<SyncProvenance>,
    /// Per-row surrogates, parallel to the rows in `payload`. Empty when the
    /// originating insert carries no cross-engine identity.
    pub surrogates: Vec<Surrogate>,
    /// What a row whose primary key already exists does, encoded by the
    /// writer that knows the insert's conflict intent and `ON CONFLICT`
    /// assignments. Empty for a plain insert, which replaces the row. Replay
    /// decides each row the way the live insert did.
    pub conflict_policy: Vec<u8>,
    /// System-time ordinal of the first row of a bitemporal insert. Row `i`
    /// of `payload` stamps `system_from + i`. Replay stamps the ordinals the
    /// live insert stamped. `None` for a collection that is not bitemporal,
    /// and for a writer that leaves the stamp to the apply.
    pub system_from: Option<i64>,
    /// The stamps of each row of a RESTORE re-issue, index-aligned with the
    /// rows in `payload`. Replay installs each row at exactly these stamps.
    /// Empty for every other insert.
    pub restored_versions: Vec<super::RestoredVersion>,
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
            system_from: Some(1_700_000_000_000_000_007),
            restored_versions: vec![
                super::super::RestoredVersion {
                    sys_from: 5,
                    sys_to: Some(9),
                    clone_copy: false,
                },
                super::super::RestoredVersion {
                    sys_from: 9,
                    sys_to: None,
                    clone_copy: false,
                },
            ],
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
        assert_eq!(decoded.system_from, Some(1_700_000_000_000_000_007));
        assert_eq!(decoded.restored_versions, rec.restored_versions);
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
            system_from: None,
            restored_versions: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.surrogates, Vec::<Surrogate>::new());
        assert_eq!(decoded.provenance, None);
    }

    #[test]
    fn a_tuple_does_not_decode_as_a_record() {
        let prov: Option<SyncProvenance> = None;
        let tuple = zerompk::to_msgpack_vec(&(
            "columnar".to_string(),
            "metrics".to_string(),
            vec![1u8, 2, 3],
            prov,
        ))
        .expect("encode tuple");

        let as_map: Result<ColumnarWalRecord, _> = zerompk::from_msgpack(&tuple);
        assert!(
            as_map.is_err(),
            "an array tuple must not decode as a record"
        );
    }
}
