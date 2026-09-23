// SPDX-License-Identifier: BUSL-1.1

//! Typed views of the redo sub-records the committed-redo apply inspects
//! before and after the replay arms run.
//!
//! The decoders accept exactly the payload shapes the transaction resolver
//! emits and the replay arms decode: a document `Put` is the 5-tuple or the
//! bitemporal 8-tuple, a document `Delete` the 4-tuple or the bitemporal
//! 5-tuple, a KV write carries its leading `"kv_*"` discriminator. KV and
//! graph `Put` / `Delete` payloads never decode as a document tuple, and
//! document payloads never decode as KV.

use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::wal::RedoSubRecord;

/// A document write inside a redo record.
pub(super) enum RedoDocOp {
    Put {
        collection: String,
        /// The post-image as MessagePack.
        value: Vec<u8>,
        surrogate: u32,
    },
    Delete {
        collection: String,
        surrogate: u32,
    },
}

impl RedoDocOp {
    pub(super) fn collection(&self) -> &str {
        match self {
            Self::Put { collection, .. } | Self::Delete { collection, .. } => collection,
        }
    }

    pub(super) fn surrogate(&self) -> u32 {
        match self {
            Self::Put { surrogate, .. } | Self::Delete { surrogate, .. } => *surrogate,
        }
    }
}

type BitemporalPut = (
    String,
    String,
    Vec<u8>,
    Option<SyncProvenance>,
    u32,
    i64,
    i64,
    i64,
);
type PlainPut = (String, String, Vec<u8>, Option<SyncProvenance>, u32);
type BitemporalDelete = (String, String, Option<SyncProvenance>, u32, i64);
type PlainDelete = (String, String, Option<SyncProvenance>, u32);

/// The document writes in `ops`, in record order.
pub(super) fn document_ops(ops: &[RedoSubRecord]) -> Vec<RedoDocOp> {
    ops.iter().filter_map(document_op).collect()
}

fn document_op(sub: &RedoSubRecord) -> Option<RedoDocOp> {
    match RecordType::from_raw(sub.record_type) {
        Some(RecordType::Put) => zerompk::from_msgpack::<BitemporalPut>(&sub.payload)
            .map(|(collection, _, value, _, surrogate, _, _, _)| (collection, value, surrogate))
            .or_else(|_| {
                zerompk::from_msgpack::<PlainPut>(&sub.payload)
                    .map(|(collection, _, value, _, surrogate)| (collection, value, surrogate))
            })
            .ok()
            .map(|(collection, value, surrogate)| RedoDocOp::Put {
                collection,
                value,
                surrogate,
            }),
        Some(RecordType::Delete) => zerompk::from_msgpack::<BitemporalDelete>(&sub.payload)
            .map(|(collection, _, _, surrogate, _)| (collection, surrogate))
            .or_else(|_| {
                zerompk::from_msgpack::<PlainDelete>(&sub.payload)
                    .map(|(collection, _, _, surrogate)| (collection, surrogate))
            })
            .ok()
            .map(|(collection, surrogate)| RedoDocOp::Delete {
                collection,
                surrogate,
            }),
        _ => None,
    }
}

/// A KV write inside a redo record.
pub(super) enum RedoKvOp {
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
}

type KvPut = (String, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32);
type KvDelete = (String, String, Vec<Vec<u8>>);

/// The KV point writes in `ops`, in record order. A `kv_truncate` carries no
/// per-key identity and is not listed.
pub(super) fn kv_ops(ops: &[RedoSubRecord]) -> Vec<RedoKvOp> {
    ops.iter().filter_map(kv_op).collect()
}

fn kv_op(sub: &RedoSubRecord) -> Option<RedoKvOp> {
    match RecordType::from_raw(sub.record_type) {
        Some(RecordType::Put) => zerompk::from_msgpack::<KvPut>(&sub.payload)
            .ok()
            .filter(|(disc, ..)| disc == "kv_put")
            .map(|(_, collection, key, value, _, _, _)| RedoKvOp::Put {
                collection,
                key,
                value,
            }),
        Some(RecordType::Delete) => zerompk::from_msgpack::<KvDelete>(&sub.payload)
            .ok()
            .filter(|(disc, ..)| disc == "kv_delete")
            .map(|(_, collection, keys)| RedoKvOp::Delete { collection, keys }),
        _ => None,
    }
}

/// A graph node-label delta inside a redo record.
pub(super) struct RedoLabelOp {
    pub node_id: String,
    pub labels: Vec<String>,
    /// `true` for a label set, `false` for a removal.
    pub is_set: bool,
}

/// The node-label deltas in `ops`, in record order.
pub(super) fn label_ops(ops: &[RedoSubRecord]) -> Vec<RedoLabelOp> {
    ops.iter()
        .filter_map(|sub| {
            let is_set = match RecordType::from_raw(sub.record_type) {
                Some(RecordType::GraphNodeLabelSet) => true,
                Some(RecordType::GraphNodeLabelRemove) => false,
                _ => return None,
            };
            let (node_id, labels) =
                zerompk::from_msgpack::<(String, Vec<String>)>(&sub.payload).ok()?;
            Some(RedoLabelOp {
                node_id,
                labels,
                is_set,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(record_type: RecordType, payload: Vec<u8>) -> RedoSubRecord {
        RedoSubRecord {
            record_type: record_type as u32,
            payload,
        }
    }

    #[test]
    fn document_and_kv_shapes_decode_to_their_own_kind_only() {
        let doc_put =
            zerompk::to_msgpack_vec(&("docs", "d1", vec![1u8, 2], None::<SyncProvenance>, 7u32))
                .expect("encode doc put");
        let doc_delete = zerompk::to_msgpack_vec(&("docs", "d2", None::<SyncProvenance>, 8u32))
            .expect("encode doc delete");
        let kv_put = zerompk::to_msgpack_vec(&(
            "kv_put",
            "kvs",
            b"k".to_vec(),
            b"v".to_vec(),
            0u64,
            None::<u64>,
            9u32,
        ))
        .expect("encode kv put");
        let kv_delete = zerompk::to_msgpack_vec(&("kv_delete", "kvs", vec![b"k".to_vec()]))
            .expect("encode kv delete");
        let ops = vec![
            sub(RecordType::Put, doc_put),
            sub(RecordType::Delete, doc_delete),
            sub(RecordType::Put, kv_put),
            sub(RecordType::Delete, kv_delete),
        ];

        let docs = document_ops(&ops);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].surrogate(), 7);
        assert!(matches!(docs[1], RedoDocOp::Delete { surrogate: 8, .. }));

        let kvs = kv_ops(&ops);
        assert_eq!(kvs.len(), 2);
        assert!(matches!(&kvs[0], RedoKvOp::Put { key, .. } if key == b"k"));
        assert!(matches!(&kvs[1], RedoKvOp::Delete { keys, .. } if keys.len() == 1));
    }

    #[test]
    fn bitemporal_document_put_decodes_with_its_value() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            "docs",
            "d1",
            vec![5u8],
            prov,
            3u32,
            10i64,
            i64::MIN,
            i64::MAX,
        ))
        .expect("encode bitemporal put");
        let docs = document_ops(&[sub(RecordType::Put, payload)]);
        assert!(matches!(
            &docs[0],
            RedoDocOp::Put { value, surrogate: 3, .. } if value == &vec![5u8]
        ));
    }

    #[test]
    fn bitemporal_document_delete_decodes_as_a_delete() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&("docs", "d1", prov, 4u32, 77i64))
            .expect("encode bitemporal delete");
        let docs = document_ops(&[sub(RecordType::Delete, payload)]);
        assert!(matches!(docs[0], RedoDocOp::Delete { surrogate: 4, .. }));
    }
}
