// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp` and `TextOp` classification.

use nodedb_physical::physical_plan::{TextOp, VectorOp};

use super::kind::PlanKind;

pub(super) fn describe_vector(op: &VectorOp) -> PlanKind {
    match op {
        VectorOp::DirectUpsert {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,
        // The vector-primary `INSERT`: the handler reports one affected row.
        VectorOp::DirectUpsert { .. } => PlanKind::DmlResult("INSERT"),

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
