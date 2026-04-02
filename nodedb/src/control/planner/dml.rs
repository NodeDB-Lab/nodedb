//! DML plan conversion: INSERT, UPDATE, DELETE.

use datafusion::prelude::*;
use nodedb_types::CollectionType;
use nodedb_types::columnar::ColumnarProfile;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{ColumnarOp, DocumentOp, KvOp, TimeseriesOp};
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use datafusion::logical_expr::LogicalPlan;

use super::converter::PlanConverter;
use super::extract::{
    expr_to_scan_filters, extract_insert_values, extract_point_targets, extract_update_assignments,
    extract_where_filters,
};

/// Extract source table, filters, and limit from a SELECT plan for INSERT...SELECT.
fn extract_select_source(plan: &LogicalPlan) -> crate::Result<(String, Vec<u8>, usize)> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let source = scan.table_name.to_string().to_lowercase();
            let limit = scan.fetch.unwrap_or(100_000);
            let filter_bytes = if !scan.filters.is_empty() {
                let mut all_filters = Vec::new();
                for f in &scan.filters {
                    all_filters.extend(expr_to_scan_filters(f));
                }
                rmp_serde::to_vec_named(&all_filters).unwrap_or_default()
            } else {
                Vec::new()
            };
            Ok((source, filter_bytes, limit))
        }
        LogicalPlan::Filter(filter) => {
            let (source, _, limit) = extract_select_source(&filter.input)?;
            let scan_filters = expr_to_scan_filters(&filter.predicate);
            let filter_bytes = rmp_serde::to_vec_named(&scan_filters).unwrap_or_default();
            Ok((source, filter_bytes, limit))
        }
        LogicalPlan::Projection(proj) => extract_select_source(&proj.input),
        LogicalPlan::Limit(limit_plan) => {
            let (source, filters, _) = extract_select_source(&limit_plan.input)?;
            let fetch = limit_plan
                .fetch
                .as_ref()
                .and_then(|f| {
                    if let Expr::Literal(lit, _) = f.as_ref() {
                        lit.to_string().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(100_000);
            Ok((source, filters, fetch))
        }
        LogicalPlan::SubqueryAlias(alias) => extract_select_source(&alias.input),
        _ => Err(crate::Error::PlanError {
            detail: format!(
                "INSERT ... SELECT: unsupported source plan: {}",
                plan.display()
            ),
        }),
    }
}

impl PlanConverter {
    /// Convert DML operations (INSERT, UPDATE, DELETE) to physical plans.
    pub(super) fn convert_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        let collection = dml.table_name.to_string().to_lowercase();
        let vshard = VShardId::from_collection(&collection);

        // Dispatch by collection type — exhaustive match ensures new
        // types get a compile error instead of silent misrouting.
        match self.collection_type(tenant_id, &collection) {
            Some(CollectionType::KeyValue(_)) => {
                return self.convert_kv_dml(dml, tenant_id, &collection, vshard);
            }
            Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
                return self.convert_timeseries_dml(dml, tenant_id, &collection, vshard);
            }
            Some(CollectionType::Columnar(ColumnarProfile::Plain)) => {
                return self.convert_columnar_dml(dml, &collection, tenant_id, vshard);
            }
            Some(CollectionType::Columnar(ColumnarProfile::Spatial { .. })) => {
                return self.convert_columnar_dml(dml, &collection, tenant_id, vshard);
            }
            // Document (schemaless/strict) or unknown catalog.
            Some(CollectionType::Document(_)) | None => {}
        }

        // Strict and schemaless document collections both use DocumentOp.
        // The encoding difference (MessagePack vs Binary Tuple) is handled
        // at the Data Plane based on the collection's StorageMode, which is
        // propagated via DocumentOp::Register at CREATE COLLECTION time.

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                // Try VALUES-based INSERT first.
                match extract_insert_values(&dml.input) {
                    Ok(values) if !values.is_empty() => {
                        let mut tasks = Vec::with_capacity(values.len());
                        for (doc_id, value_bytes) in values {
                            tasks.push(PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Document(DocumentOp::PointPut {
                                    collection: collection.clone(),
                                    document_id: doc_id,
                                    value: value_bytes,
                                }),
                            });
                        }
                        return Ok(tasks);
                    }
                    _ => {}
                }

                // INSERT ... SELECT: extract source table and filters from
                // the SELECT plan and create an InsertSelect physical plan.
                let (source_collection, source_filters, source_limit) =
                    extract_select_source(&dml.input)?;

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::InsertSelect {
                        target_collection: collection,
                        source_collection,
                        source_filters,
                        source_limit,
                    }),
                }])
            }
            WriteOp::Delete => {
                // Try point delete (WHERE id = 'value') first.
                let doc_ids = extract_point_targets(&dml.input, &collection).unwrap_or_default();

                if !doc_ids.is_empty() {
                    return Ok(doc_ids
                        .into_iter()
                        .map(|doc_id| PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                                collection: collection.clone(),
                                document_id: doc_id,
                            }),
                        })
                        .collect());
                }

                // Fall back to bulk delete with arbitrary WHERE predicates.
                let filter_bytes = extract_where_filters(&dml.input)?;
                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                        collection,
                        filters: filter_bytes,
                    }),
                }])
            }

            WriteOp::Update => {
                let updates = extract_update_assignments(&dml.input)?;
                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "UPDATE requires at least one SET assignment".into(),
                    });
                }

                // Try point update (WHERE id = 'value') first.
                let doc_ids = extract_point_targets(&dml.input, &collection).unwrap_or_default();

                if !doc_ids.is_empty() {
                    return Ok(doc_ids
                        .into_iter()
                        .map(|doc_id| PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                                collection: collection.clone(),
                                document_id: doc_id,
                                updates: updates.clone(),
                            }),
                        })
                        .collect());
                }

                // Fall back to bulk update with arbitrary WHERE predicates.
                let filter_bytes = extract_where_filters(&dml.input)?;
                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                        collection,
                        filters: filter_bytes,
                        updates,
                    }),
                }])
            }
            WriteOp::Truncate => Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::Truncate { collection }),
            }]),
        }
    }

    /// Convert DML for a timeseries collection.
    ///
    /// Routes INSERT → TimeseriesOp::Ingest (converts SQL values to ILP format).
    /// DELETE and UPDATE are not supported on timeseries (append-only semantics).
    fn convert_timeseries_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
        collection: &str,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values =
                    extract_insert_values(&dml.input).map_err(|_| crate::Error::PlanError {
                        detail: "timeseries INSERT requires VALUES clause".into(),
                    })?;

                if values.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "timeseries INSERT requires at least one row".into(),
                    });
                }

                // Convert SQL row values to ILP lines for the timeseries ingest handler.
                // Each row is a MessagePack map: {"col1": val1, "col2": val2, ...}
                // Convert to ILP: `collection field1=val1,field2=val2 timestamp_ns`
                let mut ilp_batch = String::new();
                for (_doc_id, value_bytes) in &values {
                    let row: serde_json::Value =
                        rmp_serde::from_slice(value_bytes).unwrap_or_default();
                    if let serde_json::Value::Object(map) = row {
                        // Extract timestamp (look for common timestamp field names).
                        let ts_ns = map
                            .get("ts")
                            .or_else(|| map.get("timestamp"))
                            .or_else(|| map.get("time"))
                            .or_else(|| map.get("created_at"))
                            .and_then(|v| v.as_i64())
                            .map(|ms| ms * 1_000_000) // ms → ns
                            .unwrap_or_else(|| {
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_nanos() as i64)
                                    .unwrap_or(0)
                            });

                        // Build ILP fields from remaining columns.
                        let mut fields = Vec::new();
                        for (k, v) in &map {
                            if k == "ts"
                                || k == "timestamp"
                                || k == "time"
                                || k == "created_at"
                                || k == "id"
                                || k == "document_id"
                            {
                                continue;
                            }
                            match v {
                                serde_json::Value::Number(n) => {
                                    fields.push(format!("{k}={n}"));
                                }
                                serde_json::Value::String(s) => {
                                    fields.push(format!("{k}=\"{s}\""));
                                }
                                serde_json::Value::Bool(b) => {
                                    fields.push(format!("{k}={b}"));
                                }
                                _ => {}
                            }
                        }

                        if !fields.is_empty() {
                            ilp_batch.push_str(collection);
                            ilp_batch.push(' ');
                            ilp_batch.push_str(&fields.join(","));
                            ilp_batch.push(' ');
                            ilp_batch.push_str(&ts_ns.to_string());
                            ilp_batch.push('\n');
                        }
                    }
                }

                if ilp_batch.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "timeseries INSERT: no valid field values extracted".into(),
                    });
                }

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                        collection: collection.to_string(),
                        payload: ilp_batch.into_bytes(),
                        format: "ilp".to_string(),
                        wal_lsn: None,
                    }),
                }])
            }
            WriteOp::Update => Err(crate::Error::PlanError {
                detail: "UPDATE not supported on timeseries collections (append-only)".into(),
            }),
            WriteOp::Delete => Err(crate::Error::PlanError {
                detail: "DELETE not supported on timeseries collections (use retention policies)"
                    .into(),
            }),
            WriteOp::Truncate => Err(crate::Error::PlanError {
                detail: "TRUNCATE not supported on timeseries collections (use retention policies)"
                    .into(),
            }),
        }
    }

    /// Convert DML for a plain columnar collection.
    ///
    /// Routes INSERT → ColumnarOp::Insert (JSON payload).
    fn convert_columnar_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        collection: &str,
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values =
                    extract_insert_values(&dml.input).map_err(|_| crate::Error::PlanError {
                        detail: "columnar INSERT requires VALUES clause".into(),
                    })?;

                if values.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "columnar INSERT requires at least one row".into(),
                    });
                }

                // Convert SQL row values to JSON array for columnar insert handler.
                let mut json_rows = Vec::with_capacity(values.len());
                for (_doc_id, value_bytes) in &values {
                    let row: serde_json::Value =
                        rmp_serde::from_slice(value_bytes).unwrap_or_default();
                    json_rows.push(row);
                }
                let payload = serde_json::to_vec(&json_rows).unwrap_or_default();

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                        collection: collection.to_string(),
                        payload,
                        format: "json".to_string(),
                    }),
                }])
            }
            _ => Err(crate::Error::PlanError {
                detail: format!(
                    "{:?} not supported on columnar collections (append-only)",
                    dml.op
                ),
            }),
        }
    }

    /// Convert DML for a KV collection.
    ///
    /// Routes INSERT → KvPut, DELETE → KvDelete. UPDATE and TRUNCATE fall
    /// through to the standard Document path (which the Data Plane will
    /// reject with a clear error if the collection type doesn't match).
    fn convert_kv_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
        collection: &str,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                // Extract inserted values: each (doc_id, value_bytes) maps to a KV PUT.
                // The doc_id serves as the primary key, value_bytes are the serialized row.
                match extract_insert_values(&dml.input) {
                    Ok(values) if !values.is_empty() => {
                        let mut tasks = Vec::with_capacity(values.len());
                        for (key_str, value_bytes) in values {
                            tasks.push(PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Kv(KvOp::Put {
                                    collection: collection.to_string(),
                                    key: key_str.into_bytes(),
                                    value: value_bytes,
                                    ttl_ms: 0, // Collection-default TTL applied by engine.
                                }),
                            });
                        }
                        return Ok(tasks);
                    }
                    _ => {}
                }

                Err(crate::Error::PlanError {
                    detail: "KV INSERT requires VALUES clause".into(),
                })
            }
            WriteOp::Delete => {
                let doc_ids = extract_point_targets(&dml.input, collection).unwrap_or_default();
                if !doc_ids.is_empty() {
                    let keys: Vec<Vec<u8>> =
                        doc_ids.into_iter().map(|id| id.into_bytes()).collect();
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Kv(KvOp::Delete {
                            collection: collection.to_string(),
                            keys,
                        }),
                    }]);
                }

                // Bulk KV delete not yet supported — fall back to error.
                Err(crate::Error::PlanError {
                    detail: "KV DELETE requires WHERE with primary key(s)".into(),
                })
            }
            WriteOp::Update => {
                let updates = extract_update_assignments(&dml.input)?;
                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "KV UPDATE requires at least one SET assignment".into(),
                    });
                }

                // Point UPDATE: WHERE key = 'x' (or WHERE id = 'x').
                let key_ids = extract_point_targets(&dml.input, collection).unwrap_or_default();
                if key_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "KV UPDATE requires WHERE with primary key \
                                 (e.g., WHERE key = 'mykey')"
                            .into(),
                    });
                }

                let mut tasks = Vec::with_capacity(key_ids.len());
                for key_str in key_ids {
                    tasks.push(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Kv(KvOp::FieldSet {
                            collection: collection.to_string(),
                            key: key_str.into_bytes(),
                            updates: updates.clone(),
                        }),
                    });
                }
                Ok(tasks)
            }
            WriteOp::Truncate => Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::Truncate {
                    collection: collection.to_string(),
                }),
            }]),
        }
    }

    /// Try to convert equality filters into a point get.
    ///
    /// Matches patterns like `WHERE id = 'value'` or `WHERE document_id = 'value'`.
    pub(super) fn try_point_get(
        &self,
        collection: &str,
        filters: &[Expr],
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Option<PhysicalTask>> {
        use datafusion::logical_expr::Operator;

        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter
                && binary.op == Operator::Eq
            {
                let (col_name, value) = match (&*binary.left, &*binary.right) {
                    (Expr::Column(col), Expr::Literal(lit, _)) => {
                        (col.name.as_str(), lit.to_string())
                    }
                    (Expr::Literal(lit, _), Expr::Column(col)) => {
                        (col.name.as_str(), lit.to_string())
                    }
                    _ => continue,
                };

                if col_name == "id" || col_name == "document_id" {
                    let doc_id = value.trim_matches('\'').trim_matches('"').to_string();

                    return Ok(Some(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Document(DocumentOp::PointGet {
                            collection: collection.to_string(),
                            document_id: doc_id,
                            rls_filters: Vec::new(),
                        }),
                    }));
                }
            }
        }
        Ok(None)
    }
}
