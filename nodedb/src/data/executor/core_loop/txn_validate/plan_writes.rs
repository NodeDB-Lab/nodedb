// SPDX-License-Identifier: BUSL-1.1

//! The keyed rows a batch of write plans writes: document point writes,
//! KV key writes, and edge writes. A predicate-resolved write carries no
//! key here: its predicate read validates it.

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::KeyRepr;

use super::writes::TxnWrite;

fn row(collection: &str, key: KeyRepr) -> TxnWrite {
    TxnWrite {
        collection: collection.to_string(),
        key,
    }
}

fn kv(collection: &str, key: &[u8]) -> TxnWrite {
    row(collection, KeyRepr::KvKey(Box::from(key)))
}

fn edge(collection: &str, src: &str, label: &str, dst: &str) -> TxnWrite {
    row(
        collection,
        KeyRepr::Edge {
            src: Box::from(src),
            label: Box::from(label),
            dst: Box::from(dst),
        },
    )
}

fn document_writes(op: &DocumentOp, out: &mut Vec<TxnWrite>) {
    if let DocumentOp::PointPut {
        collection,
        surrogate,
        ..
    }
    | DocumentOp::PointInsert {
        collection,
        surrogate,
        ..
    } = op
    {
        out.push(row(
            collection.as_str(),
            KeyRepr::Surrogate(surrogate.as_u32()),
        ));
    }
    if let DocumentOp::PointDelete {
        collection,
        surrogate: Some(surrogate),
        ..
    }
    | DocumentOp::PointUpdate {
        collection,
        surrogate: Some(surrogate),
        ..
    } = op
    {
        out.push(row(
            collection.as_str(),
            KeyRepr::Surrogate(surrogate.as_u32()),
        ));
    }
}

fn kv_writes(op: &KvOp, out: &mut Vec<TxnWrite>) {
    if let KvOp::Put {
        collection, key, ..
    }
    | KvOp::Insert {
        collection, key, ..
    }
    | KvOp::InsertIfAbsent {
        collection, key, ..
    }
    | KvOp::InsertOnConflictUpdate {
        collection, key, ..
    }
    | KvOp::Expire {
        collection, key, ..
    }
    | KvOp::Persist {
        collection, key, ..
    }
    | KvOp::FieldSet {
        collection, key, ..
    }
    | KvOp::Incr {
        collection, key, ..
    }
    | KvOp::IncrFloat {
        collection, key, ..
    }
    | KvOp::Cas {
        collection, key, ..
    }
    | KvOp::GetSet {
        collection, key, ..
    } = op
    {
        out.push(kv(collection.as_str(), key));
    }
    if let KvOp::Delete {
        collection, keys, ..
    } = op
    {
        out.extend(keys.iter().map(|key| kv(collection.as_str(), key)));
    }
    if let KvOp::BatchPut {
        collection,
        entries,
        ..
    } = op
    {
        out.extend(entries.iter().map(|(key, _)| kv(collection.as_str(), key)));
    }
}

fn graph_writes(op: &GraphOp, out: &mut Vec<TxnWrite>) {
    if let GraphOp::EdgePut {
        collection,
        src_id,
        label,
        dst_id,
        ..
    }
    | GraphOp::EdgeDelete {
        collection,
        src_id,
        label,
        dst_id,
        ..
    } = op
    {
        out.push(edge(collection.as_str(), src_id, label, dst_id));
    }
    if let GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } = op {
        out.extend(
            edges
                .iter()
                .map(|e| edge(e.collection.as_str(), &e.src_id, &e.label, &e.dst_id)),
        );
    }
}

/// The keyed rows `plans` write.
pub(in crate::data::executor) fn plan_writes(plans: &[PhysicalPlan]) -> Vec<TxnWrite> {
    let mut out = Vec::new();
    for plan in plans {
        if let PhysicalPlan::Document(op) = plan {
            document_writes(op, &mut out);
        }
        if let PhysicalPlan::Kv(op) = plan {
            kv_writes(op, &mut out);
        }
        if let PhysicalPlan::Graph(op) = plan {
            graph_writes(op, &mut out);
        }
    }
    out
}
