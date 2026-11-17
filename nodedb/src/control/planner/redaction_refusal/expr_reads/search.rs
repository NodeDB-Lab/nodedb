// SPDX-License-Identifier: BUSL-1.1

//! Column reads of the text and vector search operations.
//!
//! A text match over a redacted field is refused by the text check. The
//! predicates a search carries beside its match are read here, and so is the
//! vector column a similarity search ranks by.

use nodedb_physical::physical_plan::{TextOp, VectorOp, VectorWriteTargets};

use super::reads::{Clause, NodeReads};

pub(super) fn text_reads(op: &TextOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        TextOp::Search { filters, .. }
        | TextOp::BM25ScoreScan { filters, .. }
        | TextOp::PhraseSearch { filters, .. } => out.filter_bytes(filters)?,
        TextOp::HybridSearch {
            filters,
            vector_field,
            ..
        }
        | TextOp::HybridSearchTriple {
            filters,
            vector_field,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.field(vector_field, Clause::Sort);
        }
        TextOp::FtsIndexDoc { .. }
        | TextOp::FtsDeleteDoc { .. }
        | TextOp::SetTextConfig { .. }
        | TextOp::RebuildIndex { .. } => {}
    }
    Ok(())
}

pub(super) fn vector_reads(op: &VectorOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        // The statement's `WHERE` conjuncts ride in `rls_filters`. The check
        // runs before the read policy joins them there.
        VectorOp::Search {
            field_name,
            payload_filters,
            rls_filters,
            ..
        } => {
            out.filter_bytes(rls_filters)?;
            out.field(field_name, Clause::Sort);
            for atom in payload_filters {
                out.field(atom.field(), Clause::Filter);
            }
        }
        VectorOp::MultiSearch { rls_filters, .. } => out.filter_bytes(rls_filters)?,
        // The sparse search's `WHERE` conjuncts ride in `filters`.
        VectorOp::SparseSearch {
            field_name,
            filters,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.field(field_name, Clause::Sort);
        }
        VectorOp::MultiVectorScoreSearch { field_name, .. } => {
            out.field(field_name, Clause::Sort);
        }
        VectorOp::DirectDelete { targets, .. } => target_reads(targets, out)?,
        VectorOp::DirectUpdate {
            targets,
            payload_patch,
            ..
        } => {
            target_reads(targets, out)?;
            out.updates(payload_patch);
        }
        VectorOp::ResolveDirectWrite(inner) => vector_reads(inner, out)?,
        // Writes by surrogate, index DDL, and maintenance read no column in a
        // predicate or an expression.
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
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::ResolvedDirectWrite { .. } => {}
    }
    Ok(())
}

fn target_reads(targets: &VectorWriteTargets, out: &mut NodeReads) -> crate::Result<()> {
    match targets {
        VectorWriteTargets::Predicate(filters) => out.filter_bytes(filters),
        VectorWriteTargets::Surrogates(_) => Ok(()),
    }
}
