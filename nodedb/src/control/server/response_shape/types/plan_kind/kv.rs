// SPDX-License-Identifier: BUSL-1.1

//! `KvOp` classification.

use nodedb_physical::physical_plan::KvOp;

use super::kind::PlanKind;

pub(super) fn describe_kv(op: &KvOp) -> PlanKind {
    match op {
        KvOp::Get { .. } | KvOp::FieldGet { .. } => PlanKind::SingleDocument,

        KvOp::Scan { .. } | KvOp::BatchGet { .. } => PlanKind::MultiRow,

        // A write with a projection returns real stored rows and must be
        // decoded and redacted, never passed through unshaped.
        KvOp::Insert {
            returning: Some(_), ..
        }
        | KvOp::InsertIfAbsent {
            returning: Some(_), ..
        }
        | KvOp::InsertOnConflictUpdate {
            returning: Some(_), ..
        }
        | KvOp::Put {
            returning: Some(_), ..
        }
        | KvOp::BatchPut {
            returning: Some(_), ..
        }
        | KvOp::FieldSet {
            returning: Some(_), ..
        }
        | KvOp::PredicateUpdate {
            returning: Some(_), ..
        }
        | KvOp::Delete {
            returning: Some(_), ..
        }
        | KvOp::PredicateDelete {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,

        // The SQL `UPSERT` statement, tagged like `DocumentOp::Upsert`.
        KvOp::Put { .. } => PlanKind::DmlResult("UPSERT"),

        // `InsertIfAbsent`: `ON CONFLICT DO NOTHING` makes it no-op-capable,
        // so the count must come from the write's response.
        KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } | KvOp::BatchPut { .. } => {
            PlanKind::DmlResult("INSERT")
        }

        // Insert-vs-update is decided by the handler; the payload says which.
        KvOp::InsertOnConflictUpdate { .. } => PlanKind::DmlResultByOp,

        // Reports `{"affected": n}`. `FieldSet` is the keyed UPDATE.
        KvOp::FieldSet { .. } | KvOp::PredicateUpdate { .. } => PlanKind::DmlResult("UPDATE"),

        // Counts the keys removed.
        KvOp::Delete { .. } | KvOp::PredicateDelete { .. } => PlanKind::DmlResult("DELETE"),

        KvOp::Truncate { .. } => PlanKind::DmlResult("TRUNCATE"),

        // One-object payloads: `{"ttl_ms": n}`, `{"rank": n}`, `{"count": n}`,
        // `{"score": ..}`.
        KvOp::GetTtl { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => PlanKind::SingleDocument,

        // One row per sorted-index entry.
        KvOp::SortedIndexTopK { .. } | KvOp::SortedIndexRange { .. } => PlanKind::MultiRow,

        // TTL metadata mutations: no row count.
        KvOp::Expire { .. }
        | KvOp::Persist { .. }
        // Index DDL.
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        // Function-call results (`KV_INCR(..)` and friends): the payload is a
        // computed value its dispatcher reads directly, not a row count.
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        // Clone materializer payload (`[cursor, entries]`), decoded by its caller.
        | KvOp::MaterializeScan { .. }
        // Read-only resolve: payload is the internal mutation list, never a client row.
        | KvOp::ResolveWrite(_)
        // Never reaches this classifier: write-resolve returns the response itself,
        // shaped from the intercepted plan whose `returning` slot decides.
        | KvOp::ResolvedWrite { .. } => PlanKind::Execution,
    }
}
