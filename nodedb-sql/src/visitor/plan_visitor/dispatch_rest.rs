// SPDX-License-Identifier: Apache-2.0
//! Second half of the exhaustive [`SqlPlan`] → [`PlanVisitor`] dispatcher.
//!
//! Handles set-ops, array DDL/DML/TVF, merge, lateral, and index-DDL
//! variants. Called only from [`super::dispatch::dispatch`]'s trailing
//! `other => dispatch_rest(visitor, other)` arm; the variants handled there
//! reach the trailing `unreachable!()` arm here, which exists solely to
//! satisfy the compiler that this match is exhaustive over the full
//! `SqlPlan` enum without repeating their field lists.
use super::args::{
    CreateArrayVisitArgs, LateralLoopVisitArgs, LateralTopKVisitArgs, MergeVisitArgs,
};
use super::trait_def::PlanVisitor;
use crate::types::SqlPlan;

pub(super) fn dispatch_rest<V: PlanVisitor>(
    visitor: &mut V,
    plan: &SqlPlan,
) -> Result<V::Output, V::Error> {
    match plan {
        SqlPlan::Union { inputs, distinct } => visitor.union(inputs, *distinct),
        SqlPlan::Intersect { left, right, all } => visitor.intersect(left, right, *all),
        SqlPlan::Except { left, right, all } => visitor.except(left, right, *all),
        SqlPlan::Cte { definitions, outer } => visitor.cte(definitions, outer),
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            sort_keys,
            offset,
            distinct,
            limit,
        } => visitor.subquery(super::args::SubqueryVisitArgs {
            input,
            filters,
            projection,
            sort_keys,
            offset: *offset,
            distinct: *distinct,
            limit: *limit,
        }),
        SqlPlan::CreateArray {
            name,
            dims,
            attrs,
            tile_extents,
            cell_order,
            tile_order,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        } => visitor.create_array(CreateArrayVisitArgs {
            name,
            dims,
            attrs,
            tile_extents,
            cell_order: *cell_order,
            tile_order: *tile_order,
            prefix_bits: *prefix_bits,
            audit_retain_ms: *audit_retain_ms,
            minimum_audit_retain_ms: *minimum_audit_retain_ms,
        }),
        SqlPlan::DropArray { name, if_exists } => visitor.drop_array(name, *if_exists),
        SqlPlan::AlterArray {
            name,
            audit_retain_ms,
            minimum_audit_retain_ms,
        } => visitor.alter_array(name, *audit_retain_ms, *minimum_audit_retain_ms),
        SqlPlan::InsertArray { name, rows } => visitor.insert_array(name, rows),
        SqlPlan::DeleteArray { name, coords } => visitor.delete_array(name, coords),
        SqlPlan::ArraySlice {
            name,
            slice,
            attr_projection,
            limit,
            temporal,
        } => visitor.array_slice(name, slice, attr_projection, *limit, temporal),
        SqlPlan::ArrayProject {
            name,
            attr_projection,
        } => visitor.array_project(name, attr_projection),
        SqlPlan::ArrayAgg {
            name,
            attr,
            reducer,
            group_by_dim,
            temporal,
        } => visitor.array_agg(name, attr, reducer, group_by_dim.as_deref(), temporal),
        SqlPlan::ArrayElementwise {
            left,
            right,
            op,
            attr,
        } => visitor.array_elementwise(left, right, *op, attr),
        SqlPlan::ArrayFlush { name } => visitor.array_flush(name),
        SqlPlan::ArrayCompact { name } => visitor.array_compact(name),
        SqlPlan::Merge {
            target,
            engine,
            source,
            target_join_col,
            source_join_col,
            source_alias,
            clauses,
            returning,
        } => visitor.merge(MergeVisitArgs {
            target,
            engine: *engine,
            source,
            target_join_col,
            source_join_col,
            source_alias,
            clauses,
            returning: *returning,
        }),
        SqlPlan::LateralTopK {
            outer,
            outer_alias,
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join,
        } => visitor.lateral_top_k(LateralTopKVisitArgs {
            outer,
            outer_alias: outer_alias.as_deref(),
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit: *inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join: *left_join,
        }),
        SqlPlan::LateralLoop {
            outer,
            outer_alias,
            inner,
            correlation_predicates,
            lateral_alias,
            projection,
            outer_row_cap,
            left_join,
        } => visitor.lateral_loop(LateralLoopVisitArgs {
            outer,
            outer_alias: outer_alias.as_deref(),
            inner,
            correlation_predicates,
            lateral_alias,
            projection,
            outer_row_cap: *outer_row_cap,
            left_join: *left_join,
        }),
        SqlPlan::VectorPrimaryInsert {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            rows,
            // Cache eligibility only; lowering does not read it.
            volatile_defaults: _,
        } => visitor.vector_primary_insert(
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            rows,
        ),
        SqlPlan::CreateIndex {
            index_name,
            collection,
            field,
            unique,
            if_not_exists,
            case_insensitive,
        } => visitor.create_index(
            index_name.as_deref(),
            collection,
            field,
            *unique,
            *if_not_exists,
            *case_insensitive,
        ),
        SqlPlan::DropIndex {
            index_name,
            collection,
            if_exists,
        } => visitor.drop_index(index_name, collection.as_deref(), *if_exists),
        _ => unreachable!(
            "dispatch() already handles every remaining SqlPlan variant before forwarding here"
        ),
    }
}
