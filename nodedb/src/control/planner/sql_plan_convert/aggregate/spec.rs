// SPDX-License-Identifier: BUSL-1.1

//! Aggregate-spec construction, collection/alias extraction, and join-side
//! embedding helpers shared by `convert_aggregate` and the scan converters.

use nodedb_sql::types::{AggregateExpr, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::convert::{ConvertContext, convert_one, db_qualified};
use super::super::expr::{sql_expr_to_bridge_expr, sql_expr_to_bridge_expr_qualified};

/// Convert a join-side sub-plan to an optional embedded `PhysicalPlan`.
///
/// Plain `Scan` and `PointGet` sides are handled via `left_collection` /
/// `right_collection` on the parent `HashJoin`, so they return `None` here.
///
/// For any other sub-plan (a nested join, an aggregate, etc.) we convert it
/// to a physical plan. If the result is a sharded source it is wrapped in
/// `Exchange{Broadcast}` so the coordinator gathers it to the coordinator
/// and embeds it as the inline build-side input.  Constant / catalog
/// `ProviderScan` inputs are embedded directly without Exchange.
pub(in crate::control::planner::sql_plan_convert) fn inline_join_side(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Catalog scan sides are coordinator-materialized: lower them to a
    // `ProviderScan` embedded as the join input. This keeps a catalog⋈catalog
    // join coordinator-local (see `is_sharded_source`) so it runs once instead
    // of being broadcast to every core (which would duplicate every row N
    // times). The converter (`convert_scan`) already emits the right
    // `ProviderScan{provider: Some(name)}` shape for catalog scans, and that
    // node is not a sharded source, so it is embedded directly without
    // Exchange.
    if matches!(plan, SqlPlan::PointGet { .. }) {
        return Ok(None);
    }
    // Plain current-state user-collection scan side: handled via the parent
    // HashJoin's `left_collection` / `right_collection` by name. Catalog
    // scans fall through to conversion so we embed their ProviderScan. A scan
    // under `FOR SYSTEM_TIME` / `FOR VALID_TIME`, or inside a transaction
    // block that reads its snapshot, is converted and embedded too, since
    // only the engine scan carries the system-time scope.
    if let SqlPlan::Scan { temporal, .. } = plan
        && !plan_is_provider(plan)
        && !temporal.is_temporal()
        && !ctx.reads_txn_snapshot
    {
        return Ok(None);
    }

    let mut tasks = convert_one(plan, tenant_id, ctx)?;
    if tasks.len() > 1 {
        return Err(crate::Error::PlanError {
            detail: format!(
                "inline join side must produce exactly 1 task, got {}",
                tasks.len()
            ),
        });
    }
    Ok(tasks.pop().map(|t| {
        let p = t.plan;
        // A sharded side, or a side homed on one collection's core (a
        // timeseries or KV scan), is gathered on the coordinator from where
        // its rows live. Embedded bare, it would run on the join's core.
        if p.is_sharded_source() || p.collection().is_some() {
            Box::new(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(p),
                mode: ExchangeMode::Broadcast,
            })))
        } else {
            Box::new(p)
        }
    }))
}

/// Whether a scan-shaped plan reads a provider relation: a system catalog
/// table (pg_class, information_schema.*, _system.*, etc.) or a continuous
/// aggregate. Provider relations are coordinator-materialized, not
/// per-shard. A join answers for its left side, the side that names its
/// routing collection.
fn plan_is_provider(plan: &SqlPlan) -> bool {
    match plan {
        SqlPlan::Scan {
            collection, engine, ..
        }
        | SqlPlan::PointGet {
            collection, engine, ..
        } => super::super::scan::is_provider_relation(collection, engine),
        SqlPlan::Join { left, .. } => plan_is_provider(left),
        _ => false,
    }
}

/// Resolve the `left_collection` / `right_collection` name for a `HashJoin`
/// side. Catalog sides return an EMPTY string: their rows come from the
/// embedded `ProviderScan` input (see `inline_join_side`), and an empty
/// collection name is what `is_sharded_source` and the executor use to
/// recognize a non-per-shard side. Plain user-collection sides return the
/// db-qualified name as before.
pub(in crate::control::planner::sql_plan_convert) fn join_side_collection(
    plan: &SqlPlan,
    ctx: &ConvertContext,
) -> String {
    let database_id = ctx.database_id;
    // An input-sourced join side carries no routing collection; its rows come
    // from `left_input` / `right_input`. A temporal scan side, and every scan
    // side of a transaction snapshot read, is embedded (see
    // `inline_join_side`), so it names no collection either.
    if let SqlPlan::Scan { temporal, .. } = plan
        && (temporal.is_temporal() || ctx.reads_txn_snapshot)
    {
        return String::new();
    }
    match extract_collection_name(plan) {
        Some(raw) if !plan_is_provider(plan) => db_qualified(database_id, &raw),
        Some(_) | None => String::new(),
    }
}

/// Routing collection for a scan-shaped input. `None` for an input-sourced
/// (materialized) body.
///
/// A `Join` yields its left side's collection as the routing hint for a join
/// over scans. An `Aggregate` input is a materialized relation, not a scan.
pub(in crate::control::planner::sql_plan_convert) fn extract_collection_name(
    plan: &SqlPlan,
) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. } => Some(collection.clone()),
        SqlPlan::PointGet { collection, .. } => Some(collection.clone()),
        SqlPlan::Join { left, .. } => extract_collection_name(left),
        _ => None,
    }
}

/// Scan alias for a scan-shaped input. `None` for an input-sourced body.
pub(in crate::control::planner::sql_plan_convert) fn extract_scan_alias(
    plan: &SqlPlan,
) -> Option<String> {
    match plan {
        SqlPlan::Scan { alias, .. } => alias.clone(),
        SqlPlan::PointGet { alias, .. } => alias.clone(),
        SqlPlan::Join { left, .. } => extract_scan_alias(left),
        _ => None,
    }
}

/// The constant second argument of a parameterized aggregate
/// (`approx_topk(v, 3)`, `approx_percentile(v, 0.9)`), as its typed
/// parameter. `None` for a function that takes none. A missing, non-constant
/// or out-of-range parameter is refused: the Data Plane has no default to
/// fall back to.
fn aggregate_parameter(function: &str, a: &AggregateExpr) -> crate::Result<Option<AggregateParam>> {
    use nodedb_sql::types::SqlValue;
    use rust_decimal::prelude::ToPrimitive;
    let refuse = |what: &str| crate::Error::PlanError {
        detail: format!("{}() needs {what} as its second argument", a.function),
    };
    let constant = || -> crate::Result<SqlValue> {
        let param = a.args.get(1).ok_or_else(|| refuse("a numeric constant"))?;
        let (negative, inner) = match param {
            SqlExpr::UnaryOp {
                op: nodedb_sql::types::UnaryOp::Neg,
                expr,
            } => (true, expr.as_ref()),
            other => (false, other),
        };
        match inner {
            SqlExpr::Literal(SqlValue::Int(n)) => Ok(SqlValue::Int(if negative {
                n.saturating_neg()
            } else {
                *n
            })),
            SqlExpr::Literal(SqlValue::Float(f)) => {
                Ok(SqlValue::Float(if negative { -f } else { *f }))
            }
            SqlExpr::Literal(SqlValue::Decimal(d)) => {
                Ok(SqlValue::Decimal(if negative { -*d } else { *d }))
            }
            _ => Err(refuse("a numeric constant")),
        }
    };
    match function {
        "approx_percentile" | "percentile_cont" => {
            let fraction = match constant()? {
                SqlValue::Int(n) => n as f64,
                SqlValue::Float(f) => f,
                SqlValue::Decimal(d) => d.to_f64().ok_or_else(|| refuse("a fraction"))?,
                _ => return Err(refuse("a numeric constant")),
            };
            if !(0.0..=1.0).contains(&fraction) {
                return Err(refuse("a fraction in [0, 1]"));
            }
            Ok(Some(AggregateParam::Percentile { fraction }))
        }
        "approx_topk" => match constant()? {
            SqlValue::Int(n) => match u32::try_from(n) {
                Ok(k) if k >= 1 => Ok(Some(AggregateParam::TopK { k })),
                _ => Err(refuse("a k from 1 to 4294967295")),
            },
            _ => Err(refuse("an integer k")),
        },
        _ => Ok(None),
    }
}

/// How the aggregate's input rows key their fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::control::planner::sql_plan_convert) enum InputFieldNames {
    /// Each field is keyed by its bare column name.
    Bare,
    /// Each field is keyed `qualifier.column`, as in a join's merged rows.
    /// The qualifier is the side's alias, else its collection name.
    Qualified,
}

impl InputFieldNames {
    /// The input-row key a column reference reads.
    fn field(self, table: Option<&str>, name: &str) -> String {
        match self {
            Self::Bare => name.to_string(),
            Self::Qualified => nodedb_sql::planner::qualified_name(table, name),
        }
    }

    /// The bridge expression evaluated against one input row.
    fn expr(self, expr: &SqlExpr) -> crate::bridge::expr_eval::SqlExpr {
        match self {
            Self::Bare => sql_expr_to_bridge_expr(expr),
            Self::Qualified => sql_expr_to_bridge_expr_qualified(expr),
        }
    }
}

/// Convert an `AggregateExpr` to the Data Plane aggregate spec over rows
/// keyed by bare column names.
pub(in crate::control::planner::sql_plan_convert) fn agg_expr_to_spec(
    a: &AggregateExpr,
) -> crate::Result<AggregateSpec> {
    agg_expr_to_spec_over(a, InputFieldNames::Bare)
}

/// Convert an `AggregateExpr` to the Data Plane aggregate spec.
///
/// Refuses a function the generic aggregate has no accumulator for, and a
/// `GROUPING()` call the planner did not resolve to a GROUP BY key. A
/// parameterized aggregate carries its constant in `param`. `names` says how
/// the input rows key the columns the argument reads.
pub(in crate::control::planner::sql_plan_convert) fn agg_expr_to_spec_over(
    a: &AggregateExpr,
    names: InputFieldNames,
) -> crate::Result<AggregateSpec> {
    // GROUPING(col) pseudo-aggregate: encode the canonical key index in the
    // `field` so the Data Plane executor can read the grouping-set bitmask.
    if a.function == "grouping" {
        let idx = a.grouping_col_index.ok_or(crate::Error::PlanError {
            detail: format!("GROUPING() in '{}' names no GROUP BY key", a.alias),
        })?;
        let field = idx.to_string();
        let canonical = format!("grouping({field})");
        let user_alias = if a.alias.eq_ignore_ascii_case(&canonical) {
            None
        } else {
            Some(a.alias.clone())
        };
        return Ok(AggregateSpec {
            function: "grouping".into(),
            alias: canonical,
            user_alias,
            field,
            expr: None,
            param: None,
        });
    }

    let function = nodedb_sql::planner::agg_naming::aggregate_function_name(a);
    if !GENERIC_AGGREGATE_FUNCTIONS.contains(&function.as_str()) {
        return Err(crate::Error::FeatureNotSupported {
            detail: format!(
                "aggregate function {}() is not supported in this query shape",
                a.function
            ),
        });
    }

    let (field, expr) = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { table, name } => (names.field(table.as_deref(), name), None),
            SqlExpr::Wildcard => ("*".into(), None),
            _ => ("*".into(), Some(names.expr(arg))),
        })
        .unwrap_or_else(|| ("*".into(), None));
    let param = aggregate_parameter(&function, a)?;

    // The same key HAVING binds to, so a computed or filtered aggregate
    // lands under a key no other aggregate shares.
    let canonical = nodedb_sql::planner::agg_naming::aggregate_output_key(a);
    let user_alias = if a.alias.eq_ignore_ascii_case(&canonical) {
        None
    } else {
        Some(a.alias.clone())
    };

    Ok(AggregateSpec {
        function,
        alias: canonical,
        user_alias,
        field,
        expr,
        param,
    })
}

/// Convert an `AggregateExpr` to the `(op, field)` pair the grouped
/// timeseries scan accumulates.
///
/// That scan reads one column per aggregate. A computed argument has no
/// column to read, so it is refused; the planner routes such aggregates to
/// the generic aggregate before they reach here.
pub(in crate::control::planner::sql_plan_convert) fn agg_expr_to_pair(
    a: &AggregateExpr,
) -> crate::Result<(String, String)> {
    let field = match a.args.first() {
        None | Some(SqlExpr::Wildcard) => "*".to_string(),
        Some(SqlExpr::Column { name, .. }) => name.clone(),
        Some(_) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "aggregate '{}' has a computed argument the grouped timeseries scan cannot \
                     read",
                    a.alias
                ),
            });
        }
    };
    Ok((
        nodedb_sql::planner::agg_naming::aggregate_function_name(a),
        field,
    ))
}

pub(super) fn group_by_to_strings(exprs: &[SqlExpr]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Build the coordinator-local `Aggregate{input: Some(child)}` task shared by
/// the catalog and derived-table-body lowerings: an aggregate that runs once
/// over an already-materialized `child` plan instead of scanning a per-shard
/// collection. `raw_collection` is the RAW catalog source name for a catalog
/// body, or `String::new()` for a body with no routing collection (derived
/// table, constant result, union) — either way the task's vshard is the
/// coordinator's empty-collection vshard, so it is never broadcast.
/// Inputs to [`build_input_sourced_aggregate_task`].
pub(in crate::control::planner::sql_plan_convert) struct InputSourcedTaskParams<'a> {
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
    pub raw_collection: String,
    pub child: PhysicalPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having_bytes: Vec<u8>,
    pub limit: usize,
    /// ROLLUP / CUBE / GROUPING SETS, as indices into `group_by`.
    pub grouping_sets: Vec<Vec<u32>>,
    pub sort_keys: Vec<SortKeySpec>,
    /// SELECT-list output alias of each GROUP BY key, parallel to `group_by`.
    pub group_by_aliases: &'a [Option<String>],
    /// How `child`'s rows key their fields.
    pub names: InputFieldNames,
}

pub(in crate::control::planner::sql_plan_convert) fn build_input_sourced_aggregate_task(
    p: InputSourcedTaskParams<'_>,
) -> crate::Result<PhysicalTask> {
    let InputSourcedTaskParams {
        tenant_id,
        ctx,
        raw_collection,
        child,
        group_by,
        aggregates,
        having_bytes,
        limit,
        grouping_sets,
        sort_keys,
        group_by_aliases,
        names,
    } = p;
    let group_specs = group_by_to_specs(group_by, names);
    let agg_specs = aggregates
        .iter()
        .map(|a| agg_expr_to_spec_over(a, names))
        .collect::<crate::Result<Vec<AggregateSpec>>>()?;
    Ok(PhysicalTask {
        tenant_id,
        // Coordinator-local: empty collection keeps the task on the
        // coordinator vshard (the child's rows are not per-shard).
        vshard_id: nodedb_types::CollectionKey::from_bare(ctx.database_id, "").vshard(),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::from_stored(raw_collection),
            input: Some(Box::new(child)),
            group_by: group_specs,
            aggregates: agg_specs,
            // The child carries its own WHERE / ProviderScan filters; the
            // aggregate node applies none of its own.
            filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            having: having_bytes,
            limit,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets,
            sort_keys,
            group_key_aliases: group_by_aliases.to_vec(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    })
}

/// Lower GROUP BY expressions to Data-Plane group-key specs.
///
/// A bare `Column` key extracts from, and is emitted under, its own column
/// name, so its output stays byte-identical to the string-keyed form. A
/// computed-expression key carries the bridge-evaluated expression (`field:
/// None, expr: Some(..)`) and is emitted under the shared index-based
/// [`group_key_output_name`] name (`group_{index}`) — a purely internal
/// executor↔shaper handshake, matching the response shaper's `lookup_key`. The
/// SELECT alias never enters here; it reaches only the shaper's `display_name`.
///
/// `names` says how the input rows key their fields. Over qualified rows a
/// column key reads `qualifier.column` and is still emitted under its bare
/// column name, the name the response shaper looks up.
pub(super) fn group_by_to_specs(exprs: &[SqlExpr], names: InputFieldNames) -> Vec<GroupKeySpec> {
    use super::super::group_key_name::group_key_output_name;
    exprs
        .iter()
        .enumerate()
        .map(|(index, e)| match e {
            SqlExpr::Column { table, name } => GroupKeySpec {
                output_name: group_key_output_name(e, index),
                field: Some(names.field(table.as_deref(), name)),
                expr: None,
            },
            _ => GroupKeySpec {
                output_name: group_key_output_name(e, index),
                field: None,
                expr: Some(names.expr(e)),
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::{BinaryOp, SqlValue};

    fn column(table: &str, name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    fn agg(function: &str, arg: SqlExpr) -> AggregateExpr {
        AggregateExpr {
            function: function.into(),
            args: vec![arg],
            alias: format!("{function}(x)"),
            distinct: false,
            grouping_col_index: None,
            filter_text: None,
            arg_text: None,
        }
    }

    #[test]
    fn qualified_group_key_reads_the_qualified_field_under_its_bare_name() {
        let specs = group_by_to_specs(&[column("c", "region")], InputFieldNames::Qualified);
        assert_eq!(specs[0].field.as_deref(), Some("c.region"));
        assert_eq!(specs[0].output_name, "region");
        assert!(specs[0].expr.is_none());
    }

    #[test]
    fn bare_group_key_reads_the_bare_field() {
        let specs = group_by_to_specs(&[column("c", "region")], InputFieldNames::Bare);
        assert_eq!(specs[0].field.as_deref(), Some("region"));
        assert_eq!(specs[0].output_name, "region");
    }

    #[test]
    fn qualified_aggregate_argument_reads_the_qualified_field() {
        let spec = agg_expr_to_spec_over(
            &agg("sum", column("o", "amount")),
            InputFieldNames::Qualified,
        )
        .expect("sum is a generic aggregate");
        assert_eq!(spec.field, "o.amount");
    }

    #[test]
    fn qualified_computed_argument_reads_qualified_columns() {
        let arg = SqlExpr::BinaryOp {
            left: Box::new(column("o", "amount")),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
        };
        let spec = agg_expr_to_spec_over(&agg("sum", arg), InputFieldNames::Qualified)
            .expect("sum is a generic aggregate");
        let Some(crate::bridge::expr_eval::SqlExpr::BinaryOp { left, .. }) = spec.expr else {
            panic!("expected a computed argument");
        };
        assert!(matches!(
            *left,
            crate::bridge::expr_eval::SqlExpr::Column(ref name) if name == "o.amount"
        ));
    }
}
