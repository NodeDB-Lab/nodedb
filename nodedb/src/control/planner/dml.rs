//! DML plan conversion: INSERT, UPDATE, DELETE for document collections.
//!
//! Engine-specific DML converters live in sibling modules:
//! - `dml_timeseries`: timeseries collections
//! - `dml_columnar`: columnar (plain + spatial) collections
//! - `dml_kv`: key-value collections

use datafusion::prelude::*;
use nodedb_types::CollectionType;
use nodedb_types::columnar::ColumnarProfile;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::DocumentOp;
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
                zerompk::to_msgpack_vec(&all_filters).unwrap_or_default()
            } else {
                Vec::new()
            };
            Ok((source, filter_bytes, limit))
        }
        LogicalPlan::Filter(filter) => {
            let (source, _, limit) = extract_select_source(&filter.input)?;
            let scan_filters = expr_to_scan_filters(&filter.predicate);
            let filter_bytes = zerompk::to_msgpack_vec(&scan_filters).unwrap_or_default();
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
    ///
    /// Dispatches to engine-specific converters for KV, timeseries, and columnar
    /// collections. Document (schemaless + strict) collections are handled here.
    pub(super) fn convert_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
        returning: bool,
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
                                returning,
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
                        returning,
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
