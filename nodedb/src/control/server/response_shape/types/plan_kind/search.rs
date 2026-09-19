// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp` and `TextOp` classification.

use nodedb_physical::physical_plan::{TextOp, VectorOp};

use super::kind::PlanKind;

pub(super) fn describe_vector(op: &VectorOp) -> PlanKind {
    match op {
        VectorOp::DirectUpsert {
            returning: Some(_), ..
        }
        | VectorOp::DirectInsert {
            returning: Some(_), ..
        }
        | VectorOp::DirectInsertIfAbsent {
            returning: Some(_), ..
        }
        | VectorOp::DirectDelete {
            returning: Some(_), ..
        }
        | VectorOp::DirectUpdate {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,
        // The vector-primary write family: each handler reports its affected
        // row count. An `ON CONFLICT DO UPDATE` upsert reports the verb it
        // applied, exactly as `KvOp::InsertOnConflictUpdate` does.
        VectorOp::DirectInsert { .. } | VectorOp::DirectInsertIfAbsent { .. } => {
            PlanKind::DmlResult("INSERT")
        }
        VectorOp::DirectUpsert {
            on_conflict_updates,
            ..
        } if !on_conflict_updates.is_empty() => PlanKind::DmlResultByOp,
        VectorOp::DirectUpsert { .. } => PlanKind::DmlResult("UPSERT"),
        VectorOp::DirectDelete { .. } => PlanKind::DmlResult("DELETE"),
        VectorOp::DirectTruncate { .. } => PlanKind::DmlResult("TRUNCATE"),
        VectorOp::DirectUpdate { .. } => PlanKind::DmlResult("UPDATE"),

        // Read-only resolve: payload is the internal mutation list, never a client row.
        VectorOp::ResolveDirectWrite(_)
        // Never reaches this classifier: write-resolve returns the response itself,
        // shaped from the intercepted plan whose `returning` slot decides.
        | VectorOp::ResolvedDirectWrite { .. } => PlanKind::Execution,

        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::SparseSearch { .. } => PlanKind::MultiRow,

        // Index-maintenance and config ops: none is a client DML statement,
        // and none carries a row payload. Enumerated explicitly so a future
        // read op can't silently strand its hits.
        VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. } => PlanKind::Execution,
    }
}

pub(super) fn describe_text(op: &TextOp) -> PlanKind {
    match op {
        TextOp::Search { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::FtsIndexDoc { .. }
        | TextOp::FtsDeleteDoc { .. } => PlanKind::MultiRow,

        // Config write: opaque status.
        TextOp::SetTextConfig { .. } => PlanKind::Execution,
    }
}
