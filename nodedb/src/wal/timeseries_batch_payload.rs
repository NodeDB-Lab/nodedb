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
}

impl ColumnarConflictPolicy {
    /// A plain insert: an existing row is replaced.
    pub(crate) fn replace() -> Self {
        Self {
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
        }
    }

    /// The record encoding. A plain insert encodes as empty bytes.
    pub(crate) fn encode(&self) -> crate::Result<Vec<u8>> {
        if *self == Self::replace() {
            return Ok(Vec::new());
        }
        zerompk::to_msgpack_vec(&(self.intent, &self.on_conflict_updates)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("columnar conflict policy: {e}"),
            }
        })
    }

    /// Decode the record encoding. Empty bytes are a plain insert.
    pub(crate) fn decode(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::replace());
        }
        let (intent, on_conflict_updates) =
            zerompk::from_msgpack::<(ColumnarInsertIntent, Vec<(String, UpdateValue)>)>(bytes)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("columnar conflict policy: {e}"),
                })?;
        Ok(Self {
            intent,
            on_conflict_updates,
        })
    }
}

/// Decoded fields of a `TimeseriesBatch` WAL record.
pub(crate) struct DecodedBatchRecord {
    /// `Some("columnar")` / `Some("timeseries")` for tagged records, `None`
    /// for the untagged 2-tuple shape.
    pub kind: Option<String>,
    pub collection: String,
    pub payload: Vec<u8>,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    /// Present in the format-preserving timeseries tuples. Absent records
    /// use the UTF-8 heuristic.
    pub format: Option<String>,
    /// Non-empty only for map-shaped columnar records.
    pub surrogates: Vec<nodedb_types::Surrogate>,
    /// A columnar insert's encoded [`ColumnarConflictPolicy`]. Empty for a
    /// plain insert and for every other record shape.
    pub conflict_policy: Vec<u8>,
    /// The timestamp every untimed row takes. Present only in the six-element
    /// autocommit ingest tuple.
    pub default_timestamp_ms: Option<i64>,
}

impl DecodedBatchRecord {
    fn tuple(
        kind: Option<String>,
        collection: String,
        payload: Vec<u8>,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        format: Option<String>,
        default_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            kind,
            collection,
            payload,
            provenance,
            format,
            surrogates: Vec::new(),
            conflict_policy: Vec::new(),
            default_timestamp_ms,
        }
    }
}

type ProvenanceField = Option<nodedb_types::sync::wire::SyncProvenance>;

/// Decode a `TimeseriesBatch` WAL payload into its logical fields.
///
/// Tries the map form and the longest tuple forms first. The map form is
/// unambiguous from the tuple forms, and zerompk enforces tuple arity.
pub(crate) fn decode_batch_record(payload: &[u8]) -> Result<DecodedBatchRecord, ()> {
    if let Ok(rec) = zerompk::from_msgpack::<nodedb_types::columnar::ColumnarWalRecord>(payload) {
        return Ok(DecodedBatchRecord {
            kind: Some(rec.kind),
            collection: rec.collection,
            payload: rec.payload,
            provenance: rec.provenance,
            format: None,
            surrogates: rec.surrogates,
            conflict_policy: rec.conflict_policy,
            default_timestamp_ms: None,
        });
    }
    zerompk::from_msgpack::<(String, String, Vec<u8>, ProvenanceField, String, i64)>(payload)
        .map(
            |(kind, collection, payload, provenance, format, default_ms)| {
                DecodedBatchRecord::tuple(
                    Some(kind),
                    collection,
                    payload,
                    provenance,
                    Some(format),
                    Some(default_ms),
                )
            },
        )
        .or_else(|_| {
            zerompk::from_msgpack::<(String, String, Vec<u8>, ProvenanceField, String)>(payload)
                .map(|(kind, collection, payload, provenance, format)| {
                    DecodedBatchRecord::tuple(
                        Some(kind),
                        collection,
                        payload,
                        provenance,
                        Some(format),
                        None,
                    )
                })
        })
        .or_else(|_| {
            zerompk::from_msgpack::<(String, String, Vec<u8>, ProvenanceField)>(payload).map(
                |(kind, collection, payload, provenance)| {
                    DecodedBatchRecord::tuple(
                        Some(kind),
                        collection,
                        payload,
                        provenance,
                        None,
                        None,
                    )
                },
            )
        })
        .or_else(|_| {
            zerompk::from_msgpack::<(String, String, Vec<u8>)>(payload).map(
                |(kind, collection, payload)| {
                    DecodedBatchRecord::tuple(Some(kind), collection, payload, None, None, None)
                },
            )
        })
        .or_else(|_| {
            zerompk::from_msgpack::<(String, Vec<u8>)>(payload).map(|(collection, payload)| {
                DecodedBatchRecord::tuple(None, collection, payload, None, None, None)
            })
        })
        .map_err(|_| ())
}
