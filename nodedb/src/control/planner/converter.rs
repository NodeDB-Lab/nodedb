use datafusion::logical_expr::{FetchType, LogicalPlan};
use datafusion::prelude::*;
use nodedb_types::CollectionType;
use nodedb_types::columnar::ColumnarProfile;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{
    ColumnarOp, DocumentOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
};
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::credential::CredentialStore;
use crate::types::{TenantId, VShardId};

use super::extract::{expr_to_scan_filters, expr_to_usize, try_range_scan_from_predicate};
use super::search::try_extract_text_match_predicate;
use super::sql_expr_convert::try_convert_projection;

/// Converts DataFusion logical plans into NodeDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
pub struct PlanConverter {
    /// Optional catalog reference for collection-type-aware routing.
    credentials: Option<std::sync::Arc<CredentialStore>>,
    /// Optional retention policy registry for AUTO_TIER routing.
    retention_policy_registry: Option<
        std::sync::Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
    >,
}

#[allow(clippy::derivable_impls)]
impl Default for PlanConverter {
    fn default() -> Self {
        Self {
            credentials: None,
            retention_policy_registry: None,
        }
    }
}

impl PlanConverter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a converter with catalog access for timeseries detection.
    pub fn with_credentials(credentials: std::sync::Arc<CredentialStore>) -> Self {
        Self {
            credentials: Some(credentials),
            retention_policy_registry: None,
        }
    }

    /// Create a converter with catalog + retention policy registry for AUTO_TIER.
    pub fn with_full_context(
        credentials: std::sync::Arc<CredentialStore>,
        retention_policy_registry: std::sync::Arc<
            crate::engine::timeseries::retention_policy::RetentionPolicyRegistry,
        >,
    ) -> Self {
        Self {
            credentials: Some(credentials),
            retention_policy_registry: Some(retention_policy_registry),
        }
    }

    /// Get the retention policy for a collection (if AUTO_TIER enabled).
    pub(super) fn auto_tier_policy(
        &self,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<crate::engine::timeseries::retention_policy::RetentionPolicyDef> {
        let registry = self.retention_policy_registry.as_ref()?;
        let policy = registry.get_for_collection(tenant_id.as_u32(), collection)?;
        if policy.auto_tier && policy.enabled && policy.tiers.len() > 1 {
            Some(policy)
        } else {
            None
        }
    }

    /// Single catalog lookup returning the collection's storage type.
    ///
    /// Returns `None` when: no catalog available, collection not found,
    /// or catalog read error. Callers treat `None` as "default to document".
    pub(super) fn collection_type(
        &self,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<CollectionType> {
        let creds = self.credentials.as_ref()?;
        let catalog = creds.catalog().as_ref()?;
        let coll = catalog
            .get_collection(tenant_id.as_u32(), collection)
            .ok()??;
        Some(coll.collection_type.clone())
    }

    /// Convert a DataFusion logical plan into one or more physical tasks.
    pub fn convert(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        self.convert_inner(plan, tenant_id, false)
    }

    /// Convert with a RETURNING flag propagated to DML plans.
    pub fn convert_returning(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
        returning: bool,
    ) -> crate::Result<Vec<PhysicalTask>> {
        self.convert_inner(plan, tenant_id, returning)
    }

    fn convert_inner(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
        returning: bool,
    ) -> crate::Result<Vec<PhysicalTask>> {
        match plan {
            LogicalPlan::Projection(proj) => {
                let mut tasks = self.convert_inner(&proj.input, tenant_id, returning)?;

                // Try to convert computed expressions (e.g., price * qty AS total).
                if let Some(computed) = try_convert_projection(&proj.expr) {
                    let computed_bytes = zerompk::to_msgpack_vec(&computed).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            computed_columns, ..
                        }) = &mut task.plan
                        {
                            *computed_columns = computed_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Simple column projection (no computed expressions).
                let columns: Vec<String> = proj
                    .expr
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Column(col) = e {
                            Some(col.name.clone())
                        } else if let Expr::Alias(a) = e
                            && let Expr::Column(col) = &*a.expr
                        {
                            Some(col.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if !columns.is_empty() {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { projection, .. }) =
                            &mut task.plan
                        {
                            *projection = columns.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::Filter(filter) => {
                // Check for text_match() UDF in filter → TextSearch plan.
                if let Some((collection, query_text)) =
                    try_extract_text_match_predicate(&filter.predicate, &filter.input)
                {
                    let vshard = VShardId::from_collection(&collection);
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Text(TextOp::Search {
                            collection,
                            query: query_text,
                            top_k: 1000,
                            fuzzy: true,
                            rls_filters: Vec::new(),
                        }),
                    }]);
                }

                // Check for ST_* spatial predicates → SpatialScan plan.
                if let Some(task) = super::extract::spatial_expr::try_extract_spatial_scan(
                    &filter.predicate,
                    &filter.input,
                    tenant_id,
                ) {
                    return Ok(vec![task]);
                }

                // Filter(TableScan): dispatch by collection type.
                if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                    let collection = scan.table_name.to_string().to_lowercase();
                    let vshard = VShardId::from_collection(&collection);
                    let coll_type = self.collection_type(tenant_id, &collection);

                    match coll_type {
                        Some(CollectionType::KeyValue(_)) => {
                            // Try O(1) hash lookup: WHERE <col> = <literal>.
                            if let Some(key_bytes) = extract_equality_key(&filter.predicate) {
                                return Ok(vec![PhysicalTask {
                                    tenant_id,
                                    vshard_id: vshard,
                                    plan: PhysicalPlan::Kv(KvOp::Get {
                                        collection,
                                        key: key_bytes,
                                        rls_filters: Vec::new(),
                                    }),
                                }]);
                            }

                            // Fall back to KV scan with filters.
                            let filter_bytes = serialize_predicate_filters(&filter.predicate)?;
                            let limit = scan.fetch.unwrap_or(1000);
                            return Ok(vec![PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Kv(KvOp::Scan {
                                    collection,
                                    cursor: Vec::new(),
                                    count: limit,
                                    filters: filter_bytes,
                                    match_pattern: None,
                                }),
                            }]);
                        }
                        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
                            // Combine Filter predicate with any pushed-down
                            // TableScan filters, then extract time-range bounds.
                            let mut all_filters = vec![filter.predicate.clone()];
                            all_filters.extend(scan.filters.iter().cloned());
                            let (time_range, filter_bytes) =
                                super::converter_helpers::extract_timeseries_filters(&all_filters)?;
                            let limit = scan.fetch.unwrap_or(10_000);
                            let projection = resolve_scan_projection(scan);

                            return Ok(vec![PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                                    collection,
                                    time_range,
                                    projection,
                                    limit,
                                    filters: filter_bytes,
                                    bucket_interval_ms: 0,
                                    group_by: Vec::new(),
                                    aggregates: Vec::new(),
                                    gap_fill: String::new(),
                                    rls_filters: Vec::new(),
                                }),
                            }]);
                        }
                        Some(CollectionType::Columnar(ColumnarProfile::Plain)) => {
                            let filter_bytes = serialize_predicate_filters(&filter.predicate)?;
                            let limit = scan.fetch.unwrap_or(10_000);
                            let projection = resolve_scan_projection(scan);

                            return Ok(vec![PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Columnar(ColumnarOp::Scan {
                                    collection,
                                    projection,
                                    limit,
                                    filters: filter_bytes,
                                    rls_filters: Vec::new(),
                                }),
                            }]);
                        }
                        Some(CollectionType::Columnar(ColumnarProfile::Spatial { .. })) => {
                            // Spatial ST_* predicates are handled above
                            // by try_extract_spatial_scan. Non-spatial
                            // WHERE on spatial collections → columnar scan.
                            let filter_bytes = serialize_predicate_filters(&filter.predicate)?;
                            let limit = scan.fetch.unwrap_or(10_000);
                            let projection = resolve_scan_projection(scan);

                            return Ok(vec![PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Columnar(ColumnarOp::Scan {
                                    collection,
                                    projection,
                                    limit,
                                    filters: filter_bytes,
                                    rls_filters: Vec::new(),
                                }),
                            }]);
                        }
                        // Document (schemaless/strict) or unknown: try
                        // point-get, range-scan, then full document scan.
                        Some(CollectionType::Document(_)) | None => {
                            if let Some(task) = self.try_point_get(
                                &collection,
                                std::slice::from_ref(&filter.predicate),
                                tenant_id,
                                vshard,
                            )? {
                                return Ok(vec![task]);
                            }

                            if let Some(task) = try_range_scan_from_predicate(
                                &collection,
                                &filter.predicate,
                                tenant_id,
                                vshard,
                            ) {
                                return Ok(vec![task]);
                            }

                            let filter_bytes = serialize_predicate_filters(&filter.predicate)?;
                            let limit = scan.fetch.unwrap_or(1000);

                            return Ok(vec![PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Document(DocumentOp::Scan {
                                    collection,
                                    limit,
                                    offset: 0,
                                    sort_keys: Vec::new(),
                                    filters: filter_bytes,
                                    distinct: false,
                                    projection: Vec::new(),
                                    computed_columns: Vec::new(),
                                    window_functions: Vec::new(),
                                }),
                            }]);
                        }
                    }
                }
                // Filter wrapping Aggregate = HAVING clause.
                if matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)) {
                    let mut tasks = self.convert_inner(&filter.input, tenant_id, returning)?;
                    let having_filters = expr_to_scan_filters(&filter.predicate);
                    let having_bytes = zerompk::to_msgpack_vec(&having_filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("having serialization: {e}"),
                        }
                    })?;
                    for task in &mut tasks {
                        if let PhysicalPlan::Query(QueryOp::Aggregate { having, .. }) =
                            &mut task.plan
                        {
                            *having = having_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Filter wrapping something else — recurse and apply filters.
                let mut tasks = self.convert_inner(&filter.input, tenant_id, returning)?;
                let filters = expr_to_scan_filters(&filter.predicate);
                let filter_bytes =
                    zerompk::to_msgpack_vec(&filters).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                // Inject WHERE filters into any scan-type plan that carries a
                // `filters` field. Covers Document, Timeseries, Columnar, KV,
                // and Spatial scans. If the plan already has filters (e.g.,
                // time-range predicates from TableScan extraction), merge them.
                for task in &mut tasks {
                    let existing_filters: Option<&mut Vec<u8>> = match &mut task.plan {
                        PhysicalPlan::Document(DocumentOp::Scan { filters, .. }) => Some(filters),
                        PhysicalPlan::Timeseries(TimeseriesOp::Scan { filters, .. }) => {
                            Some(filters)
                        }
                        PhysicalPlan::Columnar(ColumnarOp::Scan { filters, .. }) => Some(filters),
                        PhysicalPlan::Kv(KvOp::Scan { filters, .. }) => Some(filters),
                        PhysicalPlan::Spatial(SpatialOp::Scan {
                            attribute_filters, ..
                        }) => Some(attribute_filters),
                        _ => None,
                    };
                    if let Some(filters) = existing_filters {
                        if filters.is_empty() {
                            *filters = filter_bytes.clone();
                        } else {
                            // Merge existing filters (e.g., time-range) with new
                            // WHERE predicates.
                            let mut existing: Vec<crate::bridge::scan_filter::ScanFilter> =
                                zerompk::from_msgpack(filters).unwrap_or_default();
                            let new: Vec<crate::bridge::scan_filter::ScanFilter> =
                                zerompk::from_msgpack(&filter_bytes).unwrap_or_default();
                            existing.extend(new);
                            *filters = zerompk::to_msgpack_vec(&existing).unwrap_or_default();
                        }
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string().to_lowercase();
                let vshard = VShardId::from_collection(&collection);
                let coll_type = self.collection_type(tenant_id, &collection);

                match coll_type {
                    Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
                        let limit = scan.fetch.unwrap_or(10_000);
                        let (time_range, filter_bytes) =
                            super::converter_helpers::extract_timeseries_filters(&scan.filters)?;
                        let projection = resolve_scan_projection(scan);

                        Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                                collection,
                                time_range,
                                projection,
                                limit,
                                filters: filter_bytes,
                                bucket_interval_ms: 0,
                                group_by: Vec::new(),
                                aggregates: Vec::new(),
                                gap_fill: String::new(),
                                rls_filters: Vec::new(),
                            }),
                        }])
                    }
                    Some(CollectionType::Columnar(ColumnarProfile::Plain)) => {
                        let limit = scan.fetch.unwrap_or(10_000);
                        let filter_bytes = serialize_scan_filters(&scan.filters)?;
                        let projection = resolve_scan_projection(scan);

                        Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Columnar(ColumnarOp::Scan {
                                collection,
                                projection,
                                limit,
                                filters: filter_bytes,
                                rls_filters: Vec::new(),
                            }),
                        }])
                    }
                    Some(CollectionType::Columnar(ColumnarProfile::Spatial { .. })) => {
                        let limit = scan.fetch.unwrap_or(10_000);
                        let projection = resolve_scan_projection(scan);

                        Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Columnar(ColumnarOp::Scan {
                                collection,
                                projection,
                                limit,
                                filters: Vec::new(),
                                rls_filters: Vec::new(),
                            }),
                        }])
                    }
                    Some(CollectionType::KeyValue(_)) => {
                        let limit = scan.fetch.unwrap_or(1000);
                        Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Kv(KvOp::Scan {
                                collection,
                                cursor: Vec::new(),
                                count: limit,
                                filters: Vec::new(),
                                match_pattern: None,
                            }),
                        }])
                    }
                    Some(CollectionType::Document(_)) | None => {
                        // Try filter pushdown: equality on id → point get.
                        if let Some(task) =
                            self.try_point_get(&collection, &scan.filters, tenant_id, vshard)?
                        {
                            return Ok(vec![task]);
                        }

                        let limit = scan.fetch.unwrap_or(1000);
                        let filter_bytes = serialize_scan_filters(&scan.filters)?;

                        Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Document(DocumentOp::Scan {
                                collection,
                                limit,
                                offset: 0,
                                sort_keys: Vec::new(),
                                filters: filter_bytes,
                                distinct: false,
                                projection: Vec::new(),
                                computed_columns: Vec::new(),
                                window_functions: Vec::new(),
                            }),
                        }])
                    }
                }
            }

            LogicalPlan::Limit(limit_plan) => {
                let mut tasks = self.convert_inner(&limit_plan.input, tenant_id, returning)?;

                // Extract LIMIT (fetch) value — propagate to all scan types.
                if let Ok(FetchType::Literal(Some(n))) = limit_plan.get_fetch_type() {
                    for task in &mut tasks {
                        match &mut task.plan {
                            PhysicalPlan::Document(DocumentOp::Scan { limit, .. })
                            | PhysicalPlan::Document(DocumentOp::RangeScan { limit, .. })
                            | PhysicalPlan::Columnar(ColumnarOp::Scan { limit, .. })
                            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { limit, .. })
                            | PhysicalPlan::Spatial(SpatialOp::Scan { limit, .. }) => *limit = n,
                            PhysicalPlan::Kv(KvOp::Scan { count, .. }) => *count = n,
                            _ => {}
                        }
                    }
                }

                // Extract OFFSET (skip) value.
                if let Some(skip) = &limit_plan.skip
                    && let Ok(skip_n) = expr_to_usize(skip)
                {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { offset, .. }) =
                            &mut task.plan
                        {
                            *offset = skip_n;
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::SubqueryAlias(alias) => {
                self.convert_inner(&alias.input, tenant_id, returning)
            }

            LogicalPlan::Sort(sort) => self.convert_sort(sort, tenant_id),

            LogicalPlan::Aggregate(agg) => self.convert_aggregate(agg, tenant_id),

            LogicalPlan::Dml(dml) => self.convert_dml(dml, tenant_id, returning),

            LogicalPlan::Join(join) => super::join::convert_join(join, tenant_id),

            // DISTINCT: recurse into input, then mark DocumentScan for dedup.
            LogicalPlan::Distinct(distinct) => {
                let mut tasks = self.convert(distinct.input(), tenant_id)?;
                for task in &mut tasks {
                    if let PhysicalPlan::Document(DocumentOp::Scan { distinct, .. }) =
                        &mut task.plan
                    {
                        *distinct = true;
                    }
                }
                Ok(tasks)
            }

            // Window functions: convert inner plan + extract window specs.
            LogicalPlan::Window(window) => {
                let mut tasks = self.convert_inner(&window.input, tenant_id, returning)?;

                // Convert window expressions to WindowFuncSpec.
                let specs = super::sql_expr_convert::convert_window_exprs(&window.window_expr);
                if !specs.is_empty() {
                    let spec_bytes = zerompk::to_msgpack_vec(&specs).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            window_functions, ..
                        }) = &mut task.plan
                        {
                            *window_functions = spec_bytes.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            // Subqueries: DataFusion's optimizer decorrelates many subqueries
            // into joins before they reach the converter. If a Subquery node
            // arrives here, it means the optimizer couldn't decorrelate it.
            // We handle it by converting the subquery's inner plan — this
            // works for uncorrelated subqueries (IN, EXISTS, scalar).
            // Correlated subqueries that reference outer columns will fail
            // at execution time if the Data Plane can't resolve them.
            LogicalPlan::Subquery(subquery) => {
                self.convert_inner(&subquery.subquery, tenant_id, returning)
            }

            // UNION / UNION ALL: convert each input plan and concatenate tasks.
            // The Data Plane executes each sub-plan independently; the Control
            // Plane concatenates the result sets (UNION ALL) or deduplicates
            // (UNION DISTINCT — handled by wrapping in Distinct if needed).
            LogicalPlan::Union(union) => {
                let mut all_tasks = Vec::new();
                for input in &union.inputs {
                    let tasks = self.convert(input, tenant_id)?;
                    all_tasks.extend(tasks);
                }
                Ok(all_tasks)
            }

            LogicalPlan::RecursiveQuery(recursive) => {
                // Extract base collection from the static (base) term.
                let collection = super::search::extract_table_name(&recursive.static_term)
                    .ok_or_else(|| crate::Error::PlanError {
                        detail: "WITH RECURSIVE base term must reference a table".into(),
                    })?;
                let vshard = VShardId::from_collection(&collection);

                // Extract filters from base and recursive terms.
                let base_filters = extract_filters_from_plan(&recursive.static_term);
                let recursive_filters = extract_filters_from_plan(&recursive.recursive_term);

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
                        collection,
                        base_filters,
                        recursive_filters,
                        max_iterations: 100,
                        distinct: recursive.is_distinct,
                        limit: 10_000,
                    }),
                }])
            }

            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported logical plan type: {}", plan.display()),
            }),
        }
    }
}

/// Resolve column projection from DataFusion's index-based representation
/// to named columns. Returns empty vec for "select all".
fn resolve_scan_projection(scan: &datafusion::logical_expr::TableScan) -> Vec<String> {
    scan.projection
        .as_ref()
        .map(|indices| {
            let schema = scan.source.schema();
            indices
                .iter()
                .filter_map(|&idx| schema.fields().get(idx).map(|f| f.name().clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Serialize a single Filter predicate expression into ScanFilter bytes.
fn serialize_predicate_filters(predicate: &Expr) -> crate::Result<Vec<u8>> {
    let filters = expr_to_scan_filters(predicate);
    zerompk::to_msgpack_vec(&filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

/// Serialize pushed-down TableScan filter expressions into ScanFilter bytes.
/// Returns empty vec when no filters are present.
fn serialize_scan_filters(scan_filters: &[Expr]) -> crate::Result<Vec<u8>> {
    if scan_filters.is_empty() {
        return Ok(Vec::new());
    }
    let mut all_filters = Vec::new();
    for f in scan_filters {
        all_filters.extend(expr_to_scan_filters(f));
    }
    zerompk::to_msgpack_vec(&all_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

/// Extract serialized filter predicates from a LogicalPlan (for recursive CTE terms).
fn extract_filters_from_plan(plan: &LogicalPlan) -> Vec<u8> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let filters = super::extract::expr_to_scan_filters(&filter.predicate);
            zerompk::to_msgpack_vec(&filters).unwrap_or_default()
        }
        LogicalPlan::Projection(proj) => extract_filters_from_plan(&proj.input),
        LogicalPlan::SubqueryAlias(alias) => extract_filters_from_plan(&alias.input),
        _ => Vec::new(),
    }
}

/// Extract key bytes from `col = literal` for KV point-get optimization.
/// Returns None for complex predicates — those fall through to scan.
fn extract_equality_key(expr: &Expr) -> Option<Vec<u8>> {
    use datafusion::logical_expr::Operator;

    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
    {
        let lit_value = match (&*binary.left, &*binary.right) {
            (Expr::Column(_), Expr::Literal(lit, _)) => Some(lit),
            (Expr::Literal(lit, _), Expr::Column(_)) => Some(lit),
            _ => None,
        }?;

        // Convert the scalar value to key bytes.
        let key = match lit_value {
            datafusion::common::ScalarValue::Utf8(Some(s))
            | datafusion::common::ScalarValue::LargeUtf8(Some(s)) => s.as_bytes().to_vec(),
            datafusion::common::ScalarValue::Int64(Some(i)) => i.to_be_bytes().to_vec(),
            datafusion::common::ScalarValue::Int32(Some(i)) => (*i as i64).to_be_bytes().to_vec(),
            datafusion::common::ScalarValue::Binary(Some(b))
            | datafusion::common::ScalarValue::LargeBinary(Some(b)) => b.clone(),
            _ => {
                // Other types: serialize as string representation.
                let s = lit_value.to_string().trim_matches('\'').to_string();
                s.into_bytes()
            }
        };

        return Some(key);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    async fn make_context_with_table() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE users (id VARCHAR, name VARCHAR, email VARCHAR) AS VALUES ('u1', 'alice', 'a@b.com'), ('u2', 'bob', 'b@c.com')")
            .await
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn converts_point_get() {
        let ctx = make_context_with_table().await;
        let df = ctx
            .sql("SELECT * FROM users WHERE id = 'u1'")
            .await
            .unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);

        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::PointGet { document_id, .. }) => {
                assert_eq!(document_id, "u1");
            }
            other => panic!("expected PointGet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn converts_table_scan() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT name FROM users").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
        ));
    }

    #[tokio::test]
    async fn limit_propagates() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT * FROM users LIMIT 5").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) => assert_eq!(*limit, 5),
            other => panic!("expected DocumentScan, got {other:?}"),
        }
    }
}
