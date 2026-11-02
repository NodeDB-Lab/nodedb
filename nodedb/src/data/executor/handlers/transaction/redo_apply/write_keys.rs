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

use super::sub_ops::{RedoKvOp, document_ops, kv_ops};

fn edge_write(sub: &RedoSubRecord) -> Option<TxnWrite> {
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

/// The keyed rows `ops` write, in record order.
pub(super) fn redo_writes(ops: &[RedoSubRecord]) -> Vec<TxnWrite> {
    let mut out: Vec<TxnWrite> = document_ops(ops)
        .iter()
        .map(|op| TxnWrite {
            collection: op.collection().to_string(),
            key: KeyRepr::Surrogate(op.surrogate()),
        })
        .collect();
    for op in kv_ops(ops) {
        match op {
            RedoKvOp::Put {
                collection, key, ..
            } => out.push(TxnWrite {
                collection,
                key: KeyRepr::KvKey(key.into_boxed_slice()),
            }),
            RedoKvOp::Delete { collection, keys } => {
                out.extend(keys.into_iter().map(|key| TxnWrite {
                    collection: collection.clone(),
                    key: KeyRepr::KvKey(key.into_boxed_slice()),
                }))
            }
        }
    }
    out.extend(ops.iter().filter_map(edge_write));
    out
}
