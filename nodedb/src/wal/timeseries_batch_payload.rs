// SPDX-License-Identifier: BUSL-1.1

//! Decoding of `TimeseriesBatch` WAL record payloads, shared by restart
//! replay and the Control Plane's WAL catch-up, and the columnar insert's
//! conflict policy those records carry.

use nodedb_physical::physical_plan::{ColumnarInsertIntent, UpdateValue};

/// What a columnar insert does with a row whose primary key already exists:
/// the insert's intent and its `ON CONFLICT DO UPDATE` assignments.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnarConflictPolicy {
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: Vec<(String, UpdateValue)>,
    pub rls_write_check: nodedb_types::RlsWriteCheck,
}

impl ColumnarConflictPolicy {
    /// A plain insert whose policy decision is already complete.
    #[cfg(test)]
    pub(crate) fn replace() -> Self {
        Self {
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        }
    }

    /// Encode the exact conflict and write-policy inputs.
    pub(crate) fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(
            self.intent,
            &self.on_conflict_updates,
            &self.rls_write_check,
        ))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar conflict policy: {e}"),
        })
    }

    /// Decode the mandatory conflict and write-policy tuple.
    pub(crate) fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let (intent, on_conflict_updates, rls_write_check) = zerompk::from_msgpack::<(
            ColumnarInsertIntent,
            Vec<(String, UpdateValue)>,
            nodedb_types::RlsWriteCheck,
        )>(bytes)
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar conflict policy: {e}"),
        })?;
        Ok(Self {
            intent,
            on_conflict_updates,
            rls_write_check,
        })
    }
}

/// Decoded fields of a `TimeseriesBatch` WAL record.
pub(crate) struct DecodedBatchRecord {
    /// `"columnar"` for a map record, `"timeseries"` for an ingest tuple.
    pub kind: String,
    pub collection: String,
    pub payload: Vec<u8>,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    pub rls_write_check: nodedb_types::RlsWriteCheck,
    /// Present in the seven-element timeseries tuple. A map record carries none.
    pub format: Option<String>,
    /// Non-empty only for map-shaped columnar records.
    pub surrogates: Vec<nodedb_types::Surrogate>,
    /// A columnar insert's encoded [`ColumnarConflictPolicy`]. Empty only for timeseries records.
    pub conflict_policy: Vec<u8>,
    /// The statement instant of a timeseries ingest. Present in the
    /// seven-element timeseries tuple every ingest record takes.
    pub default_timestamp_ms: Option<i64>,
    /// The system-time ordinal of a bitemporal columnar insert's first row.
    /// `None` for every other record shape.
    pub system_from: Option<i64>,
    /// The stamps of each row of a RESTORE re-issue, index-aligned with the
    /// payload rows. Empty for every other record.
    pub restored_versions: Vec<nodedb_types::columnar::RestoredVersion>,
}

impl DecodedBatchRecord {
    fn tuple(
        kind: String,
        collection: String,
        payload: Vec<u8>,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        format: Option<String>,
        default_timestamp_ms: Option<i64>,
        rls_write_check: nodedb_types::RlsWriteCheck,
    ) -> Self {
        Self {
            kind,
            collection,
            payload,
            provenance,
            rls_write_check,
            format,
            surrogates: Vec::new(),
            conflict_policy: Vec::new(),
            default_timestamp_ms,
            system_from: None,
            restored_versions: Vec::new(),
        }
    }
}

type ProvenanceField = Option<nodedb_types::sync::wire::SyncProvenance>;

/// Decode a `TimeseriesBatch` WAL payload into its logical fields.
///
/// Two shapes decode: the map-shaped `ColumnarWalRecord`, and the seven-element
/// timeseries ingest tuple. zerompk enforces tuple arity, so every other
/// shape fails.
pub(crate) fn decode_batch_record(payload: &[u8]) -> Result<DecodedBatchRecord, ()> {
    if let Ok(rec) = zerompk::from_msgpack::<nodedb_types::columnar::ColumnarWalRecord>(payload) {
        return Ok(DecodedBatchRecord {
            kind: rec.kind,
            collection: rec.collection,
            payload: rec.payload,
            provenance: rec.provenance,
            rls_write_check: ColumnarConflictPolicy::decode(&rec.conflict_policy)
                .map_err(|_| ())?
                .rls_write_check,
            format: None,
            surrogates: rec.surrogates,
            conflict_policy: rec.conflict_policy,
            default_timestamp_ms: None,
            system_from: rec.system_from,
            restored_versions: rec.restored_versions,
        });
    }
    zerompk::from_msgpack::<(
        String,
        String,
        Vec<u8>,
        ProvenanceField,
        String,
        i64,
        nodedb_types::RlsWriteCheck,
    )>(payload)
    .map(
        |(kind, collection, payload, provenance, format, default_ms, rls_write_check)| {
            DecodedBatchRecord::tuple(
                kind,
                collection,
                payload,
                provenance,
                Some(format),
                Some(default_ms),
                rls_write_check,
            )
        },
    )
    .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timeseries_record_requires_its_exact_write_policy() {
        let without_policy = zerompk::to_msgpack_vec(&(
            "timeseries",
            "cpu",
            b"cpu value=1i".to_vec(),
            None::<nodedb_types::sync::wire::SyncProvenance>,
            "ilp",
            1000_i64,
        ))
        .expect("encode incomplete record");
        assert!(decode_batch_record(&without_policy).is_err());
        let policy = nodedb_types::RlsWriteCheck::Predicate(vec![1, 2, 3]);
        let encoded = crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
            crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                collection: "cpu",
                payload: b"cpu value=1i",
                provenance: None,
                format: "ilp",
                default_timestamp_ms: 1000,
                rls_write_check: &policy,
            },
        )
        .expect("encode record");
        assert_eq!(
            decode_batch_record(&encoded)
                .expect("decode current record")
                .rls_write_check,
            policy
        );
    }
}
