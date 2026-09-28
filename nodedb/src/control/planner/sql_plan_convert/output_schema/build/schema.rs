// SPDX-License-Identifier: BUSL-1.1

//! Derives the planner-authoritative [`OutputSchema`] from a compiled
//! `SqlPlan` list, threaded into response shaping so the pgwire encoder can
//! advertise correct RowDescription type OIDs.
//!
//! A read plan announces its projection. A write plan announces the columns
//! its `RETURNING` clause projects, and nothing when it carries none — see
//! [`build_returning_schema`](super::super::returning::build_returning_schema).

use std::collections::HashMap;

use nodedb_query::agg_key::canonical_agg_key;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::{AggOutputSlot, Projection};
use nodedb_sql::types::{
    ArrayProjectPlan, ArraySlicePlan, CtePlan, DocumentIndexLookupPlan, HybridSearchPlan,
    HybridSearchTriplePlan, InsertPlan, KvInsertPlan, LateralLoopPlan, LateralTopKPlan, MergePlan,
    RangeScanPlan, RecursiveScanPlan, RecursiveValuePlan, SqlPlan, TimeseriesIngestPlan,
    TimeseriesScanPlan, UpsertPlan, VectorPrimaryDeletePlan, VectorPrimaryInsertPlan,
    VectorPrimaryUpdatePlan,
};

use crate::control::planner::sql_plan_convert::aggregate::agg_expr_to_pair;
use crate::control::planner::sql_plan_convert::lateral::collection_name_from_plan;
use crate::control::planner::sql_plan_convert::output_schema_types::infer_aggregate_type;
use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;

use super::super::columns::{
    column_types_for, group_by_key_column, ordered_columns_for, schema_from_projection,
};
use super::super::returning::build_returning_schema;

/// Derives the planner-authoritative output schema of a compiled plan list.
///
/// A read plan announces the columns its projection names. A write plan
/// announces the columns `returning` projects: the clause is stripped from the
/// statement text before planning, so the plan itself carries no column list
/// and the caller supplies the projection resolved against the target. `None`
/// means the statement carries no `RETURNING` clause, and a write then
/// announces nothing.
pub fn build_output_schema<C: SqlCatalog + ?Sized>(
    plans: &[SqlPlan],
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    returning: Option<&[Projection]>,
) -> OutputSchema {
    let Some(plan) = plans.first() else {
        return OutputSchema {
            columns: Vec::new(),
            is_star: false,
            cp_computed: Vec::new(),
        };
    };

    match plan {
        // A grouped timeseries scan announces the columns its aggregate
        // encoder emits, in that encoder's order: each GROUP BY key, then
        // each aggregate. A GROUP BY key carries its own catalog type, so one
        // stored instant renders the same grouped as it does through a plain
        // `SELECT`. An aggregate result stays `Text`: the timeseries plan
        // carries no SELECT-list alias for it, so its type cannot be resolved
        // with certainty.
        //
        // A `time_bucket` query is excluded: its encoder prepends a `bucket`
        // boundary column that the plan's GROUP BY list does not name, so the
        // announced shape would not be the emitted one.
        SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection,
            group_by,
            aggregates,
            bucket_interval_ms,
            ..
        }) if !group_by.is_empty() && *bucket_interval_ms == 0 => {
            let types = column_types_for(catalog, database_id, collection);
            let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
            for key in group_by {
                columns.push(OutputColumn {
                    display_name: key.clone(),
                    lookup_key: key.clone(),
                    ty: types.get(key).copied().unwrap_or(DdlColType::Text),
                });
            }
            for agg in aggregates {
                let (function, field) = agg_expr_to_pair(agg);
                let key = canonical_agg_key(&function, &field);
                columns.push(OutputColumn {
                    display_name: key.clone(),
                    lookup_key: key,
                    ty: DdlColType::Text,
                });
            }
            OutputSchema {
                columns,
                is_star: false,
                cp_computed: Vec::new(),
            }
        }
        SqlPlan::Scan {
            collection,
            projection,
            ..
        }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::SpatialScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::PointGet {
            collection,
            projection,
            ..
        }
        | SqlPlan::RangeScan(RangeScanPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::RecursiveScan(RecursiveScanPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::VectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::MultiVectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::SparseSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::TextSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::HybridSearch(HybridSearchPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
            collection,
            projection,
            ..
        }) => {
            let types = column_types_for(catalog, database_id, collection);
            let ordered_cols = ordered_columns_for(catalog, database_id, collection);
            schema_from_projection(projection, &types, &ordered_cols)
        }
        SqlPlan::Join {
            left,
            right,
            projection,
            ..
        } => {
            // Each projected column resolves against the catalog of the side
            // it came from, keyed on the qualified name the join executor
            // emits (`orders.ts`). A bare name two sides declare differently
            // cannot be attributed and stays `Text`. A star here has no
            // single catalog to expand against, so no ordered columns are
            // supplied.
            let types =
                super::super::join_types::join_column_types(left, right, catalog, database_id);
            schema_from_projection(projection, &types, &[])
        }
        SqlPlan::ConstantResult {
            columns, values, ..
        } => {
            // The row payload keys each cell by the unique per-column key
            // (`cell_keys`), not the raw display name: two constant columns may
            // share a name (`SELECT nextval('s'), nextval('s')`), and a single
            // object would collapse them. `display_name` keeps the
            // client-facing name; `lookup_key` is the cell key.
            //
            // The type mirrors the cell `convert_constant_result` encodes:
            // `Int`/`Float`/`Bool` keep their typed cell, every other variant
            // (`String`/`Null`/`Decimal`/`Bytes`/`Array`/`Timestamp`/
            // `Timestamptz`) is encoded as text.
            let lookup_keys = crate::control::server::response_shape::project::cell_keys(columns);
            OutputSchema {
                columns: columns
                    .iter()
                    .zip(lookup_keys)
                    .enumerate()
                    .map(|(index, (c, lookup_key))| OutputColumn {
                        display_name: c.clone(),
                        lookup_key,
                        ty: constant_cell_type(values.get(index)),
                    })
                    .collect(),
                is_star: false,
                cp_computed: Vec::new(),
            }
        }
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            ..
        } => {
            // Catalog types of the aggregate's underlying columns, resolved from
            // the input plan's single source collection when it has one (Scan /
            // point-get style). GROUP BY bare keys and MIN/MAX/SUM/AVG argument
            // columns are typed against this; anything unresolvable stays Text.
            let types = match collection_name_from_plan(input) {
                Some(collection) => column_types_for(catalog, database_id, &collection),
                None => HashMap::new(),
            };
            // Derives the `OutputColumn` for one GROUP BY key: `group_by_aliases`
            // is parallel to `group_by` when populated, but may be empty when the
            // plan was built without a projection in scope — treat a
            // missing/`None` entry as "no alias".
            let key_column = |index: usize| {
                group_by.get(index).map(|key| {
                    let alias = group_by_aliases.get(index).and_then(|a| a.as_deref());
                    group_by_key_column(key, index, alias, &types)
                })
            };
            // Derives the `OutputColumn` for one aggregate. `AggregateExpr::alias`
            // is always populated by the planner: either the user's explicit
            // alias, or (for unnamed projections) the lowercased unparsed
            // expression text — e.g. `count(*)` — matching this module's own
            // lowercasing of non-column expressions. So the alias is already the
            // canonical name; no separate derivation needed. The result type is
            // inferred conservatively (COUNT -> Int8, MIN/MAX preserve the input
            // column type, SUM/AVG of a float -> Float8, else Text).
            let agg_column = |index: usize| {
                aggregates.get(index).map(|agg| OutputColumn {
                    display_name: agg.alias.clone(),
                    lookup_key: agg.alias.clone(),
                    ty: infer_aggregate_type(agg, &types),
                })
            };
            let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
            if output_order.is_empty() {
                // Built without a projection in scope: fall back to
                // group-keys-first, then aggregates.
                for index in 0..group_by.len() {
                    columns.extend(key_column(index));
                }
                for index in 0..aggregates.len() {
                    columns.extend(agg_column(index));
                }
            } else {
                // Emit columns in the recorded SELECT-list order.
                for slot in output_order {
                    match slot {
                        AggOutputSlot::GroupKey(index) => columns.extend(key_column(*index)),
                        AggOutputSlot::Aggregate(index) => columns.extend(agg_column(*index)),
                    }
                }
            }
            OutputSchema {
                columns,
                is_star: false,
                cp_computed: Vec::new(),
            }
        }
        // Set operations take their column names/types from the first
        // (left) branch, matching standard SQL set-op semantics.
        SqlPlan::Union { inputs, .. } => match inputs.first() {
            Some(first) => {
                build_output_schema(std::slice::from_ref(first), catalog, database_id, returning)
            }
            None => OutputSchema::default(),
        },
        SqlPlan::Intersect { left, .. } | SqlPlan::Except { left, .. } => build_output_schema(
            std::slice::from_ref(left.as_ref()),
            catalog,
            database_id,
            returning,
        ),
        SqlPlan::RecursiveValue(RecursiveValuePlan { columns, .. }) => OutputSchema {
            columns: columns
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
            cp_computed: Vec::new(),
        },
        // The outer query determines the final projected shape; the CTE
        // definitions themselves are only inputs to it.
        SqlPlan::Cte(CtePlan { outer, .. }) => build_output_schema(
            std::slice::from_ref(outer.as_ref()),
            catalog,
            database_id,
            returning,
        ),
        // A post-processor's projected shape is its outer projection; an empty
        // projection (SELECT *) inherits the body's columns. (Synthesized during
        // conversion, so this is normally unreached — the schema is derived from
        // the pre-inline `Cte` above — but the arm keeps derivation correct if a
        // `Subquery` is ever schema-derived directly.)
        SqlPlan::Subquery {
            input, projection, ..
        } => {
            if projection.is_empty() {
                build_output_schema(
                    std::slice::from_ref(input.as_ref()),
                    catalog,
                    database_id,
                    returning,
                )
            } else {
                let types = HashMap::new();
                schema_from_projection(projection, &types, &[])
            }
        }
        SqlPlan::LateralTopK(LateralTopKPlan { projection, .. })
        | SqlPlan::LateralLoop(LateralLoopPlan { projection, .. }) => {
            // No single source collection spans both outer and inner rows;
            // default every projected field to `Text` rather than picking
            // one side's catalog arbitrarily. A star has no single catalog to
            // expand against, so no ordered columns are supplied.
            let types = HashMap::new();
            schema_from_projection(projection, &types, &[])
        }
        SqlPlan::ArraySlice(ArraySlicePlan {
            attr_projection, ..
        })
        | SqlPlan::ArrayProject(ArrayProjectPlan {
            attr_projection, ..
        }) => OutputSchema {
            columns: attr_projection
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
            cp_computed: Vec::new(),
        },
        // A write announces exactly what its `RETURNING` clause projects, from
        // the target collection's declared columns. `RETURNING` is a
        // projection, so it is typed like one: a `SELECT ts, host, v` and an
        // `INSERT ... RETURNING ts, host, v` announce the same three types and
        // render the same stored row identically.
        SqlPlan::Insert(InsertPlan { collection, .. })
        | SqlPlan::KvInsert(KvInsertPlan { collection, .. })
        | SqlPlan::Upsert(UpsertPlan { collection, .. })
        | SqlPlan::Update { collection, .. }
        | SqlPlan::UpdateFrom { collection, .. }
        | SqlPlan::Delete { collection, .. }
        | SqlPlan::TimeseriesIngest(TimeseriesIngestPlan { collection, .. })
        | SqlPlan::VectorPrimaryInsert(VectorPrimaryInsertPlan { collection, .. })
        | SqlPlan::VectorPrimaryDelete(VectorPrimaryDeletePlan { collection, .. })
        | SqlPlan::VectorPrimaryUpdate(VectorPrimaryUpdatePlan { collection, .. }) => {
            build_returning_schema(returning, collection, catalog, database_id)
        }
        // Same rule, for the two writes that name their target `target`.
        SqlPlan::Merge(MergePlan { target, .. }) | SqlPlan::InsertSelect { target, .. } => {
            build_returning_schema(returning, target, catalog, database_id)
        }
        // No rows to shape. `TRUNCATE`, index DDL, and the whole `CREATE ARRAY`
        // family answer with a command tag; the array DML ops answer with an
        // affected count, and `inject_returning_spec` attaches no spec to them,
        // so announcing columns for one would hold a count payload to a row
        // shape it does not have.
        SqlPlan::Truncate { .. }
        | SqlPlan::VectorPrimaryTruncate { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. } => OutputSchema::default(),
        // `ArrayAgg` / `ArrayElementwise` compile to `ArrayOp::Aggregate` /
        // `ArrayOp::Elementwise`, which `describe_plan` classifies as
        // `PlanKind::MultiRow`. `MultiRow` responses are shaped by
        // `shape_generic_rows` -> `shape_decoded_rows`, which derives column
        // names from the decoded JSON payload itself, not from
        // `OutputSchema`. An empty schema here is therefore correct.
        SqlPlan::ArrayAgg { .. } | SqlPlan::ArrayElementwise { .. } => OutputSchema::default(),
    }
}

/// Wire type of one constant cell. A column with no value (a plan built
/// without values) is `Text`.
fn constant_cell_type(value: Option<&nodedb_sql::types_expr::SqlValue>) -> DdlColType {
    use nodedb_sql::types_expr::SqlValue;
    match value {
        Some(SqlValue::Int(_)) => DdlColType::Int8,
        Some(SqlValue::Float(_)) => DdlColType::Float8,
        Some(SqlValue::Bool(_)) => DdlColType::Bool,
        Some(
            SqlValue::String(_)
            | SqlValue::Null
            | SqlValue::Decimal(_)
            | SqlValue::Bytes(_)
            | SqlValue::Array(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Timestamptz(_),
        )
        | None => DdlColType::Text,
    }
}
