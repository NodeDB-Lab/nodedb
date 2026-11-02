// SPDX-License-Identifier: BUSL-1.1

//! Raise the version stamps of a session commit's redo record to the
//! system-time ordinal its record carries.
//!
//! A session commit resolves its versions before its record has an ordinal.
//! The resolve stamps each version from the core clock, one above the row's
//! newest stored version. That stamp is not reserved. The record's ordinal
//! is: the data-group leader's write gate reserves it when the record enters
//! the log, and a reservation stays open until the record installed on the
//! leader. A read cut closes only once no reservation at or below it is open
//! (`control::read_cut::reservations`).
//!
//! An unreserved stamp breaks the cut. A snapshot `S` above the resolve
//! stamp can close before the record installs, and read the row's older
//! version. The install then lands a version at or below `S`, and a commit
//! validated at `S` misses it: a lost update.
//!
//! So the write funnel raises every stamp below the record's ordinal to that
//! ordinal before it appends the record. Every version then sits at or above
//! a reserved ordinal:
//!
//! - A cut that closed before the reservation is below the ordinal, so below
//!   the version.
//! - A cut at or above the reservation waits for the install.
//!
//! The raise reads only the record and its ordinal, and the replicated entry
//! carries both. Every replica installs the same version keys, and the WAL
//! record holds the raised stamps, so restart replay installs them too.
//!
//! The raise keeps each version above the row's newest version: the resolve
//! stamp already sits above it, and a raise only moves the stamp up.

use nodedb_types::Value;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::columnar::{
    COLUMNAR_IMAGE_KIND, ColumnarImageWalRecord, ColumnarTruncateWalRecord,
};
use nodedb_wal::record::{ENCRYPTED_FLAG, RecordType};

use super::document::{DocumentDeleteRedo, DocumentPutRedo, StampedDelete, StampedPut};
use super::node_label::NodeLabelRedo;
use super::record::{EdgeDeleteRedo, EdgePutRedo, RedoRecord, RedoSubRecord};

impl RedoRecord {
    /// Raise every version stamp below `floor` to `floor`. Returns whether a
    /// stamp moved.
    ///
    /// A sub-record that carries no stamp of its own (a KV row, a timeseries
    /// batch, an array cell, and the rest) stamps the record's ordinal at
    /// its apply, and stays as it is.
    ///
    /// `Err` when a stamped sub-record does not re-encode, or a columnar
    /// image row is not an object.
    pub fn raise_stamps_to(&mut self, floor: i64) -> crate::Result<bool> {
        let mut moved = false;
        for sub in &mut self.ops {
            if let Some(payload) = raised_payload(sub, floor)? {
                sub.payload = payload;
                moved = true;
            }
        }
        Ok(moved)
    }
}

/// The payload of `sub` with its stamps raised to `floor`, `None` when no
/// stamp of it is below `floor`.
fn raised_payload(sub: &RedoSubRecord, floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let payload = sub.payload.as_slice();
    match RecordType::from_raw(sub.record_type & !ENCRYPTED_FLAG) {
        // A document put is a positional tuple, an edge put a map: neither
        // decodes as the other, and a KV put (a tuple of another arity)
        // decodes as neither.
        Some(RecordType::Put) => match DocumentPutRedo::decode(payload) {
            Some(put) => raised_document_put(put, floor),
            None => raised_edge_put(payload, floor),
        },
        Some(RecordType::Delete) => match DocumentDeleteRedo::decode(payload) {
            Some(delete) => raised_document_delete(delete, floor),
            None => raised_edge_delete(payload, floor),
        },
        Some(RecordType::GraphNodeLabelSet | RecordType::GraphNodeLabelRemove) => {
            raised_node_label(payload, floor)
        }
        // A columnar image travels as a timeseries batch tagged by its kind.
        Some(RecordType::TimeseriesBatch) => raised_columnar_image(payload, floor),
        Some(RecordType::ColumnarTruncate) => raised_columnar_truncate(payload, floor),
        // Every other sub-record stamps the record's ordinal at its apply.
        _ => Ok(None),
    }
}

/// `floor` when `stamp` sits below it.
fn below(stamp: i64, floor: i64) -> Option<i64> {
    (stamp < floor).then_some(floor)
}

fn raised_document_put(put: DocumentPutRedo, floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Some(stamp) = put.stamp else {
        return Ok(None);
    };
    let Some(sys_from) = below(stamp.sys_from, floor) else {
        return Ok(None);
    };
    let tuple: StampedPut = (
        put.collection,
        put.document_id,
        put.value,
        put.provenance,
        put.surrogate,
        sys_from,
        stamp.valid_from_ms,
        stamp.valid_until_ms,
    );
    encode(&tuple, "document put").map(Some)
}

fn raised_document_delete(
    delete: DocumentDeleteRedo,
    floor: i64,
) -> crate::Result<Option<Vec<u8>>> {
    let Some(sys_from) = delete.sys_from.and_then(|stamp| below(stamp, floor)) else {
        return Ok(None);
    };
    let tuple: StampedDelete = (
        delete.collection,
        delete.document_id,
        delete.provenance,
        delete.surrogate,
        sys_from,
    );
    encode(&tuple, "document delete").map(Some)
}

/// The raised `(system_from, applied)` of an edge version, `None` when
/// neither is below `floor`.
///
/// `applied` names the ordinal the version is applied at when it differs
/// from `system_from`, and it is never above `system_from`. Both rise to
/// `floor`, and `applied` stays only while it still differs.
fn raised_edge_stamps(
    system_from: Option<i64>,
    applied: Option<i64>,
    floor: i64,
) -> Option<(Option<i64>, Option<i64>)> {
    let system_from = system_from?;
    let applied_at = applied.unwrap_or(system_from);
    let raised_applied = below(applied_at, floor)?;
    let raised_from = system_from.max(floor);
    Some((
        Some(raised_from),
        (raised_applied != raised_from).then_some(raised_applied),
    ))
}

fn raised_edge_put(payload: &[u8], floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Ok(mut edge) = zerompk::from_msgpack::<EdgePutRedo>(payload) else {
        return Ok(None);
    };
    let Some((system_from, applied)) = raised_edge_stamps(edge.system_from, edge.applied, floor)
    else {
        return Ok(None);
    };
    edge.system_from = system_from;
    edge.applied = applied;
    encode(&edge, "edge put").map(Some)
}

fn raised_edge_delete(payload: &[u8], floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Ok(mut edge) = zerompk::from_msgpack::<EdgeDeleteRedo>(payload) else {
        return Ok(None);
    };
    let Some((system_from, applied)) = raised_edge_stamps(edge.system_from, edge.applied, floor)
    else {
        return Ok(None);
    };
    edge.system_from = system_from;
    edge.applied = applied;
    encode(&edge, "edge delete").map(Some)
}

fn raised_node_label(payload: &[u8], floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Ok(mut label) = zerompk::from_msgpack::<NodeLabelRedo>(payload) else {
        return Ok(None);
    };
    let Some(system_from) = label.system_from.and_then(|stamp| below(stamp, floor)) else {
        return Ok(None);
    };
    label.system_from = Some(system_from);
    label.encode().map(Some)
}

fn raised_columnar_image(payload: &[u8], floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Ok(mut record) = zerompk::from_msgpack::<ColumnarImageWalRecord>(payload) else {
        return Ok(None);
    };
    if record.kind != COLUMNAR_IMAGE_KIND {
        return Ok(None);
    }
    let mut moved = false;
    for row in &mut record.rows {
        let Some(system_from) = row.system_from.and_then(|stamp| below(stamp, floor)) else {
            continue;
        };
        row.system_from = Some(system_from);
        // The post-image's `_ts_system` is the row's stamp. A delete carries
        // no image.
        if !row.image_msgpack.is_empty() {
            row.image_msgpack =
                image_with_system_time(&row.image_msgpack, system_from, &record.collection)?;
        }
        moved = true;
    }
    if !moved {
        return Ok(None);
    }
    encode(&record, "columnar image").map(Some)
}

/// `image` with its `_ts_system` column set to `system_from`. An image with
/// no such column keeps every value.
fn image_with_system_time(
    image: &[u8],
    system_from: i64,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    let value =
        nodedb_types::value_from_msgpack(image).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar image row of '{collection}': {e}"),
        })?;
    let Value::Object(mut columns) = value else {
        return Err(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar image row of '{collection}' is not a column object"),
        });
    };
    if let Some(cell) = columns.get_mut(TS_SYSTEM) {
        *cell = Value::Integer(system_from);
    }
    nodedb_types::value_to_msgpack(&Value::Object(columns)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar image row of '{collection}': {e}"),
        }
    })
}

fn raised_columnar_truncate(payload: &[u8], floor: i64) -> crate::Result<Option<Vec<u8>>> {
    let Ok(mut record) = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(payload) else {
        return Ok(None);
    };
    let Some(system_from) = record.system_from.and_then(|stamp| below(stamp, floor)) else {
        return Ok(None);
    };
    record.system_from = Some(system_from);
    encode(&record, "columnar truncate").map(Some)
}

fn encode<T: zerompk::ToMessagePack>(value: &T, what: &str) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("raised {what} redo sub-record: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::columnar::ColumnarImageWalRow;
    use nodedb_types::sync::wire::SyncProvenance;

    use super::*;

    const FLOOR: i64 = 1_000;

    fn record(ops: Vec<RedoSubRecord>) -> RedoRecord {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        }
    }

    fn sub(record_type: RecordType, payload: Vec<u8>) -> RedoSubRecord {
        RedoSubRecord {
            record_type: record_type as u32,
            payload,
        }
    }

    fn doc_put(sys_from: i64) -> RedoSubRecord {
        let none: Option<SyncProvenance> = None;
        sub(
            RecordType::Put,
            zerompk::to_msgpack_vec(&("c", "d1", vec![7u8], none, 3u32, sys_from, 5i64, 9i64))
                .expect("encode"),
        )
    }

    fn doc_delete(sys_from: i64) -> RedoSubRecord {
        let none: Option<SyncProvenance> = None;
        sub(
            RecordType::Delete,
            zerompk::to_msgpack_vec(&("c", "d1", none, 3u32, sys_from)).expect("encode"),
        )
    }

    fn edge_put(system_from: i64, applied: Option<i64>) -> RedoSubRecord {
        sub(
            RecordType::Put,
            zerompk::to_msgpack_vec(&EdgePutRedo {
                collection: "g".into(),
                src_id: "a".into(),
                label: "L".into(),
                dst_id: "b".into(),
                properties: Vec::new(),
                src_surrogate: 1,
                dst_surrogate: 2,
                system_from: Some(system_from),
                applied,
                valid_from_ms: None,
                valid_until_ms: None,
            })
            .expect("encode"),
        )
    }

    #[test]
    fn a_document_stamp_below_the_floor_rises_to_it() {
        let mut redo = record(vec![doc_put(400), doc_delete(500)]);
        assert!(redo.raise_stamps_to(FLOOR).expect("raise"));
        let put = DocumentPutRedo::decode(&redo.ops[0].payload).expect("put");
        let stamp = put.stamp.expect("stamped");
        assert_eq!(stamp.sys_from, FLOOR);
        assert_eq!((stamp.valid_from_ms, stamp.valid_until_ms), (5, 9));
        assert_eq!((put.collection.as_str(), put.surrogate), ("c", 3));
        assert_eq!(put.value, vec![7u8]);
        let delete = DocumentDeleteRedo::decode(&redo.ops[1].payload).expect("delete");
        assert_eq!(delete.sys_from, Some(FLOOR));
    }

    #[test]
    fn a_stamp_at_or_above_the_floor_stays() {
        let ops = vec![doc_put(FLOOR), doc_delete(FLOOR + 3)];
        let mut redo = record(ops.clone());
        assert!(!redo.raise_stamps_to(FLOOR).expect("raise"));
        assert_eq!(redo.ops, ops, "nothing re-encoded");
    }

    #[test]
    fn an_unstamped_record_stays() {
        let none: Option<SyncProvenance> = None;
        let kv_put = sub(
            RecordType::Put,
            zerompk::to_msgpack_vec(&(
                "kv_put",
                "c",
                vec![1u8],
                vec![2u8],
                0u64,
                None::<u64>,
                4u32,
            ))
            .expect("encode"),
        );
        let plain_put = sub(
            RecordType::Put,
            zerompk::to_msgpack_vec(&("c", "d1", vec![7u8], none, 3u32)).expect("encode"),
        );
        let ops = vec![kv_put, plain_put];
        let mut redo = record(ops.clone());
        assert!(!redo.raise_stamps_to(FLOOR).expect("raise"));
        assert_eq!(redo.ops, ops);
    }

    #[test]
    fn an_edge_version_rises_and_keeps_its_applied_ordinal_only_while_it_differs() {
        let mut redo = record(vec![edge_put(400, None), edge_put(1_500, Some(600))]);
        assert!(redo.raise_stamps_to(FLOOR).expect("raise"));
        let below: EdgePutRedo = zerompk::from_msgpack(&redo.ops[0].payload).expect("edge");
        assert_eq!((below.system_from, below.applied), (Some(FLOOR), None));
        let raised: EdgePutRedo = zerompk::from_msgpack(&redo.ops[1].payload).expect("edge");
        assert_eq!(
            (raised.system_from, raised.applied),
            (Some(1_500), Some(FLOOR)),
            "a version raised above an earlier write applies at the floor"
        );
    }

    #[test]
    fn a_node_label_and_a_columnar_truncate_rise() {
        let label = NodeLabelRedo {
            node_id: "n".into(),
            labels: vec!["L".into()],
            system_from: Some(10),
            exact: false,
        };
        let truncate = ColumnarTruncateWalRecord {
            collection: "m".into(),
            system_from: Some(20),
            erase_history: false,
        };
        let mut redo = record(vec![
            sub(
                RecordType::GraphNodeLabelSet,
                label.encode().expect("encode"),
            ),
            sub(
                RecordType::ColumnarTruncate,
                zerompk::to_msgpack_vec(&truncate).expect("encode"),
            ),
        ]);
        assert!(redo.raise_stamps_to(FLOOR).expect("raise"));
        let label: NodeLabelRedo = zerompk::from_msgpack(&redo.ops[0].payload).expect("label");
        assert_eq!(label.system_from, Some(FLOOR));
        let truncate: ColumnarTruncateWalRecord =
            zerompk::from_msgpack(&redo.ops[1].payload).expect("truncate");
        assert_eq!(truncate.system_from, Some(FLOOR));
    }

    #[test]
    fn a_columnar_image_row_rises_with_its_system_time_column() {
        let mut columns = HashMap::new();
        columns.insert(TS_SYSTEM.to_string(), Value::Integer(300));
        columns.insert("v".to_string(), Value::Integer(42));
        let image = nodedb_types::value_to_msgpack(&Value::Object(columns)).expect("image");
        let record_bytes = zerompk::to_msgpack_vec(&ColumnarImageWalRecord {
            kind: COLUMNAR_IMAGE_KIND.to_string(),
            collection: "m".into(),
            schema_bytes: Vec::new(),
            rows: vec![
                ColumnarImageWalRow {
                    surrogate: 1,
                    prior_pk_msgpack: Vec::new(),
                    image_msgpack: image,
                    system_from: Some(300),
                },
                ColumnarImageWalRow {
                    surrogate: 2,
                    prior_pk_msgpack: vec![1],
                    image_msgpack: Vec::new(),
                    system_from: Some(FLOOR + 1),
                },
            ],
        })
        .expect("encode");
        let mut redo = record(vec![sub(RecordType::TimeseriesBatch, record_bytes)]);
        assert!(redo.raise_stamps_to(FLOOR).expect("raise"));
        let raised: ColumnarImageWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("image record");
        assert_eq!(raised.rows[0].system_from, Some(FLOOR));
        assert_eq!(
            raised.rows[1].system_from,
            Some(FLOOR + 1),
            "above the floor"
        );
        let Value::Object(columns) =
            nodedb_types::value_from_msgpack(&raised.rows[0].image_msgpack).expect("decode")
        else {
            panic!("image is a column object");
        };
        assert_eq!(columns.get(TS_SYSTEM), Some(&Value::Integer(FLOOR)));
        assert_eq!(columns.get("v"), Some(&Value::Integer(42)));
    }
}
