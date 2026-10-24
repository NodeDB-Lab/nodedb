// SPDX-License-Identifier: Apache-2.0

//! Array table-valued function resolution.
//!
//! An `ARRAY_*(...)` factor in a FROM clause names an array, not a
//! collection. This module synthesizes the relation the call's rows expose
//! so column references and equi-join keys against it resolve.

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;
use crate::planner::array_fn::{ArrayTableFn, try_plan_array_table_fn};
use crate::resolver::columns::ResolvedTable;
use crate::temporal::TemporalScope;
use crate::types::{
    ArrayAggPlan, ArrayCatalogView, ArraySlicePlan, CollectionInfo, ColumnInfo, EngineType,
    SqlCatalog, SqlDataType,
};
use crate::types_array::{ArrayAttrAst, ArrayAttrType, ArrayDimType, ArrayReducerAst};
use nodedb_types::columnar::{FloatWidth, IntWidth};

/// The column an audit-log slice (`AS OF SYSTEM TIME NULL`) stamps on each
/// row: the version's system time in milliseconds.
pub const ARRAY_SYSTEM_TIME_COLUMN: &str = "_ts_system";

/// The flag of the summary row a temporal aggregate or elementwise read
/// appends: whether its system-time cutoff predates the retained history.
pub const ARRAY_HORIZON_COLUMN: &str = "truncated_before_horizon";

/// If `factor` is `ARRAY_*(name, ...)`, look up the array via the
/// catalog and build a `ResolvedTable` whose columns are the ones the
/// call's rows carry. Returns `Ok(None)` for any non-array-TVF factor.
pub(super) fn resolve_array_tvf(
    catalog: &dyn SqlCatalog,
    factor: &sqlparser::ast::TableFactor,
    temporal: TemporalScope,
) -> Result<Option<ResolvedTable>> {
    let (fn_name, args, alias) = match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => (
            normalize_object_name_checked(name)?,
            args,
            alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?,
        ),
        _ => return Ok(None),
    };
    if !matches!(
        fn_name.as_str(),
        "array_slice" | "array_project" | "array_agg" | "array_elementwise"
    ) {
        return Ok(None);
    }

    // First positional arg is the array name as a string literal.
    let first = args.args.first().ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array-name argument"),
    })?;
    let array_name = extract_string_literal_arg(first).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: array-name argument must be a string literal"),
    })?;
    let view = catalog
        .lookup_array(&array_name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: array_name.clone(),
        })?;

    let twj = sqlparser::ast::TableWithJoins {
        relation: factor.clone(),
        joins: Vec::new(),
    };
    let Some(call) = try_plan_array_table_fn(std::slice::from_ref(&twj), catalog, temporal)? else {
        return Ok(None);
    };

    let info = CollectionInfo {
        name: view.name.clone(),
        engine: EngineType::Array,
        columns: array_table_fn_columns(catalog, &call)?,
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: false,
    };
    Ok(Some(ResolvedTable {
        name: view.name,
        alias,
        info,
    }))
}

/// The columns of the relation an array cell read exposes: every dim in
/// schema order, then each attr `attrs` names in that order, or every attr
/// in schema order when `attrs` is empty.
///
/// A `physical` read types an attr with a scale or offset as `float8`, and
/// an attr with a nodata as nullable, since a nodata cell reads as NULL.
pub fn array_relation_columns(
    view: &ArrayCatalogView,
    attrs: &[String],
    physical: bool,
) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(view.dims.len() + view.attrs.len());
    for d in &view.dims {
        cols.push(column(&d.name, dim_type_to_sql(d.dtype), false));
    }
    let attr_col = |a: &ArrayAttrAst| {
        let nullable = a.nullable || (physical && a.meta.nodata.is_some());
        attr_column(&a.name, read_type(a, physical), nullable)
    };
    if attrs.is_empty() {
        for a in &view.attrs {
            cols.push(attr_col(a));
        }
    } else {
        for name in attrs {
            if let Some(a) = view.attrs.iter().find(|a| &a.name == name) {
                cols.push(attr_col(a));
            }
        }
    }
    cols
}

/// The type a read of `attr` carries: `FLOAT64` for a physical read of an
/// attr with a scale or offset, the attr's own type otherwise.
fn read_type(attr: &ArrayAttrAst, physical: bool) -> ArrayAttrType {
    if physical && attr.meta.has_transform() {
        ArrayAttrType::Float64
    } else {
        attr.dtype
    }
}

/// The columns a slice's rows carry: its dims and attrs, plus
/// [`ARRAY_SYSTEM_TIME_COLUMN`] when it reads every system-time version.
pub fn array_slice_columns(view: &ArrayCatalogView, slice: &ArraySlicePlan) -> Vec<ColumnInfo> {
    let mut cols = array_relation_columns(view, &slice.attr_projection, slice.physical);
    if matches!(
        slice.temporal.system_time,
        nodedb_types::SystemTimeScope::AllVersions
    ) {
        cols.push(column(ARRAY_SYSTEM_TIME_COLUMN, SqlDataType::Int64, false));
    }
    cols
}

/// The columns an aggregate's rows carry: `group`, typed as the group dim,
/// when it groups, then `result`, typed by the reducer. A histogram carries
/// `bin_lo`, `bin_hi` and `count` in place of `result`, one row per bin.
///
/// Refuses an attr the array does not hold, or a reducer the attr type cannot answer.
pub fn array_agg_columns(view: &ArrayCatalogView, agg: &ArrayAggPlan) -> Result<Vec<ColumnInfo>> {
    let mut cols = Vec::with_capacity(4);
    if let Some(dim) = &agg.group_by_dim
        && let Some(d) = view.dims.iter().find(|d| &d.name == dim)
    {
        cols.push(column("group", dim_type_to_sql(d.dtype), false));
    }
    let attr = view
        .attrs
        .iter()
        .find(|a| a.name == agg.attr)
        .ok_or_else(|| SqlError::UnknownColumn {
            table: view.name.clone(),
            column: agg.attr.clone(),
        })?;
    check_agg_reducer(agg.reducer, &attr.name, attr.dtype)?;
    if let ArrayReducerAst::Histogram { .. } = agg.reducer {
        cols.push(column("bin_lo", SqlDataType::Float64, false));
        cols.push(column("bin_hi", SqlDataType::Float64, false));
        cols.push(column("count", SqlDataType::Int64, false));
        return Ok(cols);
    }
    cols.push(agg_result_column(
        agg.reducer,
        &attr.name,
        read_type(attr, agg.physical),
    )?);
    Ok(cols)
}

/// The columns an aggregate's rows carry: [`array_agg_columns`], then
/// [`ARRAY_HORIZON_COLUMN`] when the aggregate is temporal and ends with a
/// summary row.
pub fn array_agg_row_columns(
    view: &ArrayCatalogView,
    agg: &ArrayAggPlan,
) -> Result<Vec<ColumnInfo>> {
    let mut cols = array_agg_columns(view, agg)?;
    if agg.temporal.is_temporal() {
        cols.push(horizon_column());
    }
    Ok(cols)
}

/// The flag column of the summary row a temporal aggregate or elementwise
/// read appends. Every other row holds no value for it.
fn horizon_column() -> ColumnInfo {
    column(ARRAY_HORIZON_COLUMN, SqlDataType::Bool, true)
}

/// Refuse a reducer that an attr of type `attr` cannot answer.
///
/// The engine folds only numeric cells and skips every other cell. Every
/// reducer but `COUNT` over a `STRING` or `BYTES` attr sees no cell, so its
/// rows can only carry NULL, and the planner refuses it. `COUNT` counts the
/// non-null cells of every attr type.
pub fn check_agg_reducer(
    reducer: ArrayReducerAst,
    attr_name: &str,
    attr: ArrayAttrType,
) -> Result<()> {
    let answers = match reducer {
        ArrayReducerAst::Count => true,
        ArrayReducerAst::Sum
        | ArrayReducerAst::Min
        | ArrayReducerAst::Max
        | ArrayReducerAst::Mean
        | ArrayReducerAst::Median
        | ArrayReducerAst::Percentile(_)
        | ArrayReducerAst::StdDevPop
        | ArrayReducerAst::StdDevSamp
        | ArrayReducerAst::VarPop
        | ArrayReducerAst::VarSamp
        | ArrayReducerAst::Mode
        | ArrayReducerAst::Histogram { .. } => attr.is_integer() || attr.is_float(),
    };
    if answers {
        Ok(())
    } else {
        Err(SqlError::TypeMismatch {
            detail: format!(
                "ARRAY_AGG: {} cannot reduce {} attr '{attr_name}'; \
                 only COUNT accepts a non-numeric attr",
                reducer.sql_name(),
                attr.sql_name()
            ),
        })
    }
}

/// The columns the rows of one array table-function call carry.
///
/// - A slice or project row holds the array's dims and the requested attrs.
/// - An aggregate row holds `result`, after `group` when it groups by a dim.
/// - An elementwise row holds the `coords` and `attrs` arrays of one cell.
/// - A temporal aggregate or elementwise read also holds
///   [`ARRAY_HORIZON_COLUMN`], set on its trailing summary row.
pub fn array_table_fn_columns(
    catalog: &dyn SqlCatalog,
    call: &ArrayTableFn,
) -> Result<Vec<ColumnInfo>> {
    match call {
        ArrayTableFn::Slice(slice) | ArrayTableFn::Project(slice) => {
            Ok(array_slice_columns(&view_of(catalog, &slice.name)?, slice))
        }
        ArrayTableFn::Agg(agg) => array_agg_row_columns(&view_of(catalog, &agg.name)?, agg),
        ArrayTableFn::Elementwise(elementwise) => {
            let mut cols = vec![
                column("coords", SqlDataType::Json, false),
                column("attrs", SqlDataType::Json, false),
            ];
            if elementwise.temporal.is_temporal() {
                cols.push(horizon_column());
            }
            Ok(cols)
        }
    }
}

fn view_of(catalog: &dyn SqlCatalog, name: &str) -> Result<ArrayCatalogView> {
    catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })
}

fn column(name: &str, data_type: SqlDataType, nullable: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable,
        is_primary_key: false,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

fn dim_type_to_sql(t: ArrayDimType) -> SqlDataType {
    match t {
        ArrayDimType::Int64 => SqlDataType::Int64,
        ArrayDimType::Float64 => SqlDataType::Float64,
        ArrayDimType::TimestampMs => SqlDataType::Timestamp,
        ArrayDimType::String => SqlDataType::String,
    }
}

/// The column of attr `name`, typed to its wire type: `INT8`, `UINT8` and
/// `INT16` are `int2`, `UINT16` and `INT32` are `int4`, `UINT32` and `INT64` are
/// `int8`, `FLOAT32` is `float4`, and `FLOAT64` is `float8`. An unsigned
/// type takes the next signed width, so every stored value fits. A width of
/// `None` keeps the base `int8` / `float8`.
fn attr_column(name: &str, t: ArrayAttrType, nullable: bool) -> ColumnInfo {
    let (data_type, int_width, float_width) = attr_wire_type(t);
    ColumnInfo {
        int_width,
        float_width,
        ..column(name, data_type, nullable)
    }
}

fn attr_wire_type(t: ArrayAttrType) -> (SqlDataType, Option<IntWidth>, Option<FloatWidth>) {
    match t {
        ArrayAttrType::Int64 | ArrayAttrType::UInt32 => (SqlDataType::Int64, None, None),
        ArrayAttrType::Int32 | ArrayAttrType::UInt16 => {
            (SqlDataType::Int64, Some(IntWidth::I32), None)
        }
        ArrayAttrType::Int16 | ArrayAttrType::UInt8 | ArrayAttrType::Int8 => {
            (SqlDataType::Int64, Some(IntWidth::I16), None)
        }
        ArrayAttrType::Float64 => (SqlDataType::Float64, None, None),
        ArrayAttrType::Float32 => (SqlDataType::Float64, None, Some(FloatWidth::F32)),
        ArrayAttrType::String => (SqlDataType::String, None, None),
        ArrayAttrType::Bytes => (SqlDataType::Bytes, None, None),
    }
}

/// The `result` column of an aggregate. `COUNT` and the `SUM` of an integer
/// attr are `int8`. `MIN`, `MAX` and `MODE` take the attr's own wire type.
/// `MEAN`, `MEDIAN`, `PERCENTILE`, the variances, the standard deviations
/// and the `SUM` of a float attr are `float8`.
/// A reducer the attr type cannot answer is refused, see [`check_agg_reducer`].
fn agg_result_column(
    reducer: ArrayReducerAst,
    attr_name: &str,
    attr: ArrayAttrType,
) -> Result<ColumnInfo> {
    check_agg_reducer(reducer, attr_name, attr)?;
    let float8 = || column("result", SqlDataType::Float64, true);
    let int8 = || column("result", SqlDataType::Int64, true);
    Ok(match reducer {
        ArrayReducerAst::Count => int8(),
        ArrayReducerAst::Mean
        | ArrayReducerAst::Median
        | ArrayReducerAst::Percentile(_)
        | ArrayReducerAst::StdDevPop
        | ArrayReducerAst::StdDevSamp
        | ArrayReducerAst::VarPop
        | ArrayReducerAst::VarSamp
        | ArrayReducerAst::Histogram { .. } => float8(),
        ArrayReducerAst::Sum if attr.is_integer() => int8(),
        ArrayReducerAst::Sum => float8(),
        ArrayReducerAst::Min | ArrayReducerAst::Max | ArrayReducerAst::Mode => {
            attr_column("result", attr, true)
        }
    })
}

fn extract_string_literal_arg(arg: &sqlparser::ast::FunctionArg) -> Option<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrow_attrs_announce_their_wire_widths() {
        let cases = [
            (ArrayAttrType::UInt8, Some(IntWidth::I16), None),
            (ArrayAttrType::Int16, Some(IntWidth::I16), None),
            (ArrayAttrType::UInt16, Some(IntWidth::I32), None),
            (ArrayAttrType::Int32, Some(IntWidth::I32), None),
            (ArrayAttrType::UInt32, None, None),
            (ArrayAttrType::Int64, None, None),
            (ArrayAttrType::Float32, None, Some(FloatWidth::F32)),
            (ArrayAttrType::Float64, None, None),
        ];
        for (t, int_width, float_width) in cases {
            let c = attr_column("v", t, true);
            assert_eq!(
                (c.int_width, c.float_width),
                (int_width, float_width),
                "{t:?}"
            );
        }
    }

    #[test]
    fn aggregate_results_take_the_reducer_type() {
        let result = |reducer, attr| agg_result_column(reducer, "v", attr).expect("answerable");
        let sum = result(ArrayReducerAst::Sum, ArrayAttrType::UInt16);
        assert_eq!(sum.data_type, SqlDataType::Int64);
        let count = result(ArrayReducerAst::Count, ArrayAttrType::String);
        assert_eq!(count.data_type, SqlDataType::Int64);
        let count_bytes = result(ArrayReducerAst::Count, ArrayAttrType::Bytes);
        assert_eq!(count_bytes.data_type, SqlDataType::Int64);
        let mean = result(ArrayReducerAst::Mean, ArrayAttrType::Int16);
        assert_eq!(mean.data_type, SqlDataType::Float64);
        let max = result(ArrayReducerAst::Max, ArrayAttrType::Float32);
        assert_eq!(max.float_width, Some(FloatWidth::F32));
        let min = result(ArrayReducerAst::Min, ArrayAttrType::Int16);
        assert_eq!(min.int_width, Some(IntWidth::I16));
    }

    #[test]
    fn temporal_aggregate_rows_carry_the_horizon_flag() {
        use crate::types_array::{ArrayAttrAst, ArrayDimAst, ArrayDomainBound, ArraySliceAst};
        let view = ArrayCatalogView {
            name: "img".into(),
            dims: vec![ArrayDimAst {
                name: "x".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(3),
            }],
            attrs: vec![ArrayAttrAst {
                name: "nir".into(),
                dtype: ArrayAttrType::Float64,
                nullable: true,
                meta: Default::default(),
            }],
            tile_extents: vec![2],
            georef: None,
        };
        let agg = |temporal: TemporalScope| ArrayAggPlan {
            name: "img".into(),
            attr: "nir".into(),
            reducer: ArrayReducerAst::Sum,
            group_by_dim: Some("x".into()),
            slice: ArraySliceAst::default(),
            zone: None,
            temporal,
            physical: false,
        };
        let names = |plan: &ArrayAggPlan| -> Vec<String> {
            array_agg_row_columns(&view, plan)
                .expect("answerable")
                .into_iter()
                .map(|c| c.name)
                .collect()
        };
        assert_eq!(names(&agg(TemporalScope::default())), ["group", "result"]);
        let as_of = TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AsOf(1),
            ..TemporalScope::default()
        };
        assert_eq!(
            names(&agg(as_of)),
            ["group", "result", ARRAY_HORIZON_COLUMN]
        );
    }

    #[test]
    fn histograms_and_physical_reads_announce_their_columns() {
        use crate::types_array::{ArrayAttrMetaAst, ArrayDimAst, ArrayDomainBound, ArraySliceAst};
        let view = ArrayCatalogView {
            name: "img".into(),
            dims: vec![ArrayDimAst {
                name: "x".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(3),
            }],
            attrs: vec![ArrayAttrAst {
                name: "red".into(),
                dtype: ArrayAttrType::UInt16,
                nullable: false,
                meta: ArrayAttrMetaAst {
                    nodata: Some(0.0),
                    scale: Some(0.5),
                    ..ArrayAttrMetaAst::default()
                },
            }],
            tile_extents: vec![2],
            georef: None,
        };
        let agg = |reducer, physical| ArrayAggPlan {
            name: "img".into(),
            attr: "red".into(),
            reducer,
            group_by_dim: Some("x".into()),
            slice: ArraySliceAst::default(),
            zone: None,
            temporal: TemporalScope::default(),
            physical,
        };
        let hist = array_agg_columns(
            &view,
            &agg(
                ArrayReducerAst::Histogram {
                    bins: 4,
                    lo: 0.0,
                    hi: 1.0,
                },
                false,
            ),
        )
        .expect("answerable");
        let names: Vec<&str> = hist.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["group", "bin_lo", "bin_hi", "count"]);
        let raw_max = array_agg_columns(&view, &agg(ArrayReducerAst::Max, false)).expect("max");
        assert_eq!(raw_max[1].int_width, Some(IntWidth::I32));
        let physical_max =
            array_agg_columns(&view, &agg(ArrayReducerAst::Max, true)).expect("physical max");
        assert_eq!(physical_max[1].data_type, SqlDataType::Float64);
        let mode = array_agg_columns(&view, &agg(ArrayReducerAst::Mode, false)).expect("mode");
        assert_eq!(mode[1].int_width, Some(IntWidth::I32));
        let median = array_agg_columns(&view, &agg(ArrayReducerAst::Median, false)).expect("med");
        assert_eq!(median[1].data_type, SqlDataType::Float64);

        let physical = array_relation_columns(&view, &[], true);
        assert_eq!(physical[1].data_type, SqlDataType::Float64);
        assert!(physical[1].nullable, "a nodata cell reads as NULL");
        let raw = array_relation_columns(&view, &[], false);
        assert_eq!(raw[1].int_width, Some(IntWidth::I32));
        assert!(!raw[1].nullable);
    }

    #[test]
    fn non_numeric_attrs_refuse_numeric_reducers() {
        for attr in [ArrayAttrType::String, ArrayAttrType::Bytes] {
            for reducer in [
                ArrayReducerAst::Sum,
                ArrayReducerAst::Mean,
                ArrayReducerAst::Min,
                ArrayReducerAst::Max,
                ArrayReducerAst::Median,
                ArrayReducerAst::Mode,
                ArrayReducerAst::VarPop,
            ] {
                match agg_result_column(reducer, "label", attr) {
                    Err(SqlError::TypeMismatch { detail }) => {
                        assert!(detail.contains(reducer.sql_name()), "{detail}");
                        assert!(detail.contains(attr.sql_name()), "{detail}");
                        assert!(detail.contains("'label'"), "{detail}");
                    }
                    other => panic!("{reducer:?} over {attr:?} must be refused: {other:?}"),
                }
            }
        }
    }
}
