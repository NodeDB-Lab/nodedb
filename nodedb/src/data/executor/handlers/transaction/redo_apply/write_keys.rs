// SPDX-License-Identifier: BUSL-1.1

//! The keyed rows a committed redo record writes, for the
//! first-committer-wins check of a single-shard commit: document rows by
//! surrogate, KV rows by key, and edges by `(src, label, dst)`. A
//! sub-record with no per-key identity (columnar, timeseries, array, a
//! truncate) lists nothing: the transaction's predicate reads validate it.

use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::txn_validate::TxnWrite;
use crate::types::KeyRepr;
use crate::wal::RedoSubRecord;

use super::sub_ops::{RedoKvOp, document_op, kv_op};

/// The edge `sub` writes, `None` for any sub-record that is no edge put or
/// delete.
pub(super) fn edge_write(sub: &RedoSubRecord) -> Option<TxnWrite> {
    let (collection, src, label, dst) = match RecordType::from_raw(sub.record_type)? {
        RecordType::Put => {
            let put = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&sub.payload).ok()?;
            (put.collection, put.src_id, put.label, put.dst_id)
        }
        RecordType::Delete => {
            let delete = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&sub.payload).ok()?;
            (
                delete.collection,
                delete.src_id,
                delete.label,
                delete.dst_id,
            )
        }
        _ => return None,
    };
    Some(TxnWrite {
        collection,
        key: KeyRepr::Edge {
            src: src.into_boxed_str(),
            label: label.into_boxed_str(),
            dst: dst.into_boxed_str(),
        },
    })
}

/// The keyed rows one sub-record writes, split by the order
/// [`redo_writes`] lists a record's writes in: every document row first,
/// then every KV row, then every edge.
pub(super) struct SubRecordWrites {
    pub(super) document: Option<TxnWrite>,
    pub(super) kv: Vec<TxnWrite>,
    pub(super) edge: Option<TxnWrite>,
}

/// The keyed rows `sub` writes.
pub(super) fn sub_record_writes(sub: &RedoSubRecord) -> SubRecordWrites {
    let document = document_op(sub).map(|op| TxnWrite {
        collection: op.collection().to_string(),
        key: KeyRepr::Surrogate(op.surrogate()),
    });
    let kv = match kv_op(sub) {
        Some(RedoKvOp::Put {
            collection, key, ..
        }) => vec![TxnWrite {
            collection,
            key: KeyRepr::KvKey(key.into_boxed_slice()),
        }],
        Some(RedoKvOp::Delete { collection, keys }) => keys
            .into_iter()
            .map(|key| TxnWrite {
                collection: collection.clone(),
                key: KeyRepr::KvKey(key.into_boxed_slice()),
            })
            .collect(),
        None => Vec::new(),
    };
    SubRecordWrites {
        document,
        kv,
        edge: edge_write(sub),
    }
}

/// The keyed rows `ops` write: the document rows in record order, then the
/// KV rows, then the edges.
pub(super) fn redo_writes(ops: &[RedoSubRecord]) -> Vec<TxnWrite> {
    let mut documents = Vec::new();
    let mut kv = Vec::new();
    let mut edges = Vec::new();
    for sub in ops {
        let writes = sub_record_writes(sub);
        documents.extend(writes.document);
        kv.extend(writes.kv);
        edges.extend(writes.edge);
    }
    documents.extend(kv);
    documents.extend(edges);
    documents
}
