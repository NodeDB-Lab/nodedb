// SPDX-License-Identifier: BUSL-1.1

//! The surrogate keys a batch of write plans resolves while it converts, read
//! off the plans before conversion. Each batch derives its keys the way the
//! plan's converter does, so conversion finds every key already answered.
//!
//! A key this module does not derive is still correct: conversion records it
//! as a miss, and the bind step resolves it and converts the plan again.

use nodedb_sql::types::{
    CtePlan, EngineType, InsertArrayPlan, InsertPlan, KvInsertPlan, SqlPlan, SqlValue,
    TimeseriesIngestPlan, UpsertPlan, VectorPrimaryDeletePlan, VectorPrimaryInsertPlan,
    VectorPrimaryUpdatePlan,
};

use super::super::convert::ConvertContext;
use super::super::value::{sql_value_to_bytes, sql_value_to_string};
use super::crdt_gate::document_collection_is_crdt;
use super::insert::{
    declared_primary_key_name, doc_identity_keys, fresh_identity_count, is_auto_rowid_pk,
};

/// The keys of one collection that one plan resolves.
pub(in super::super) struct KeyBatch<'p> {
    /// The collection name as the plan names it. The converter builds the
    /// collection key from this same name.
    pub collection: &'p str,
    pub pks: Vec<Vec<u8>>,
    /// Whether conversion binds an absent key (a write that creates the row)
    /// or only reads the key's binding.
    pub binds: bool,
    /// How many rows conversion mints a fresh surrogate for.
    pub fresh: usize,
}

/// The key batches of every plan in `plans`, in plan order. A plan whose
/// conversion resolves no key yields none.
///
/// A plan whose keys cannot be read yet yields none either: an
/// `INSERT INTO ARRAY` whose array an earlier statement of the batch
/// creates, say. Its conversion reports the real error or records its keys.
pub(in super::super) fn plan_key_batches<'p>(
    plans: &'p [SqlPlan],
    ctx: &ConvertContext,
) -> Vec<KeyBatch<'p>> {
    let mut batches = Vec::new();
    for plan in plans {
        collect_key_batches(plan, ctx, &mut batches);
    }
    batches
}

/// The key batches of `plan` and of every plan it converts with it: a CTE's
/// definitions and outer statement (`WITH ... INSERT`), and the inputs of a
/// join, set operation, aggregate or subquery.
fn collect_key_batches<'p>(
    plan: &'p SqlPlan,
    ctx: &ConvertContext,
    batches: &mut Vec<KeyBatch<'p>>,
) {
    match plan {
        SqlPlan::Cte(CtePlan { definitions, outer }) => {
            for (_, definition) in definitions {
                collect_key_batches(definition, ctx, batches);
            }
            collect_key_batches(outer, ctx, batches);
        }
        SqlPlan::Join { left, right, .. }
        | SqlPlan::Intersect { left, right, .. }
        | SqlPlan::Except { left, right, .. } => {
            collect_key_batches(left, ctx, batches);
            collect_key_batches(right, ctx, batches);
        }
        SqlPlan::Union { inputs, .. } => {
            for input in inputs {
                collect_key_batches(input, ctx, batches);
            }
        }
        SqlPlan::Aggregate { input, .. } | SqlPlan::Subquery { input, .. } => {
            collect_key_batches(input, ctx, batches);
        }
        _ => {
            if let Ok(Some(batch)) = plan_key_batch(plan, ctx) {
                batches.push(batch);
            }
        }
    }
}

fn plan_key_batch<'p>(
    plan: &'p SqlPlan,
    ctx: &ConvertContext,
) -> crate::Result<Option<KeyBatch<'p>>> {
    let batch = match plan {
        SqlPlan::Insert(InsertPlan {
            collection,
            rows,
            primary_key: Some(primary_key),
            ..
        })
        | SqlPlan::Upsert(UpsertPlan {
            collection,
            rows,
            primary_key: Some(primary_key),
            ..
        }) => row_identity_batch(ctx, collection, primary_key, rows)?,
        SqlPlan::VectorPrimaryInsert(VectorPrimaryInsertPlan {
            collection,
            rows,
            primary_key: Some(primary_key),
            ..
        }) => {
            let rows: Vec<Vec<(String, SqlValue)>> = rows
                .iter()
                .map(|row| {
                    row.payload_fields
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                })
                .collect();
            row_identity_batch(ctx, collection, primary_key, &rows)?
        }
        SqlPlan::KvInsert(KvInsertPlan {
            collection,
            entries,
            ..
        }) => KeyBatch {
            collection,
            pks: entries
                .iter()
                .filter(|(key, _)| !matches!(key, SqlValue::Null))
                .filter_map(|(key, _)| sql_value_to_bytes(key).ok())
                .collect(),
            binds: true,
            fresh: 0,
        },
        SqlPlan::Update {
            collection,
            engine,
            target_keys,
            ..
        } => match engine {
            // A KV UPDATE resolves each key through the binding path.
            EngineType::KeyValue => KeyBatch {
                collection,
                pks: kv_keys(target_keys),
                binds: true,
                fresh: 0,
            },
            EngineType::Timeseries | EngineType::Columnar | EngineType::Spatial => {
                return Ok(None);
            }
            // A CRDT UPDATE creates an absent key. A document UPDATE only
            // reads the binding. A document UPDATE naming several keys
            // converts to one predicate write, which resolves no key.
            _ => {
                let crdt = !target_keys.is_empty() && document_collection_is_crdt(ctx, collection)?;
                if !crdt && target_keys.len() > 1 {
                    return Ok(None);
                }
                KeyBatch {
                    collection,
                    pks: document_keys(target_keys),
                    binds: crdt,
                    fresh: 0,
                }
            }
        },
        // A document point read resolves its key's binding read-only.
        SqlPlan::PointGet {
            collection,
            engine: EngineType::DocumentSchemaless | EngineType::DocumentStrict,
            key_value,
            ..
        } => KeyBatch {
            collection,
            pks: document_keys(std::slice::from_ref(key_value)),
            binds: false,
            fresh: 0,
        },
        SqlPlan::Delete {
            collection,
            engine,
            target_keys,
            ..
        } => match engine {
            EngineType::KeyValue
            | EngineType::Timeseries
            | EngineType::Columnar
            | EngineType::Spatial => return Ok(None),
            _ => KeyBatch {
                collection,
                pks: document_keys(target_keys),
                binds: false,
                fresh: 0,
            },
        },
        SqlPlan::VectorPrimaryDelete(VectorPrimaryDeletePlan {
            collection,
            target_keys,
            ..
        })
        | SqlPlan::VectorPrimaryUpdate(VectorPrimaryUpdatePlan {
            collection,
            target_keys,
            ..
        }) => KeyBatch {
            collection,
            pks: document_keys(target_keys),
            binds: false,
            fresh: 0,
        },
        // Every timeseries row takes a fresh identity.
        SqlPlan::TimeseriesIngest(TimeseriesIngestPlan {
            collection, rows, ..
        }) => KeyBatch {
            collection,
            pks: Vec::new(),
            binds: true,
            fresh: rows.len(),
        },
        SqlPlan::InsertArray(InsertArrayPlan { name, rows }) => KeyBatch {
            collection: name,
            pks: super::super::array_convert::insert_array_cell_pks(
                name,
                rows,
                ctx.tenant_id,
                ctx,
            )?,
            binds: true,
            fresh: 0,
        },
        _ => return Ok(None),
    };
    Ok((!batch.pks.is_empty() || batch.fresh > 0).then_some(batch))
}

/// The content keys of rows whose identity comes from the collection's
/// primary key, found as the insert converters find them.
fn row_identity_batch<'p>(
    ctx: &ConvertContext,
    collection: &'p str,
    primary_key: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> crate::Result<KeyBatch<'p>> {
    let declared = if is_auto_rowid_pk(primary_key) {
        None
    } else {
        declared_primary_key_name(ctx, collection)?
    };
    Ok(KeyBatch {
        collection,
        pks: doc_identity_keys(primary_key, declared.as_deref(), rows)
            .into_iter()
            .map(String::into_bytes)
            .collect(),
        binds: true,
        fresh: fresh_identity_count(primary_key, declared.as_deref(), rows),
    })
}

/// A document key's bytes: its rendered string.
fn document_keys(target_keys: &[SqlValue]) -> Vec<Vec<u8>> {
    target_keys
        .iter()
        .map(|key| sql_value_to_string(key).into_bytes())
        .collect()
}

/// A KV key's bytes. A key that has no byte form is left to conversion, which
/// reports it.
fn kv_keys(target_keys: &[SqlValue]) -> Vec<Vec<u8>> {
    target_keys
        .iter()
        .filter_map(|key| sql_value_to_bytes(key).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types::{
        CtePlan, EngineType, InsertPlan, SqlPlan, SqlValue, TimeseriesIngestPlan, WriteRoute,
    };

    use super::plan_key_batches;
    use crate::control::planner::sql_plan_convert::{ConvertContext, PlanningPurpose};

    fn ctx() -> ConvertContext {
        ConvertContext {
            purpose: PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner:
                crate::control::planner::sql_plan_convert::test_support::test_assigner(),
            cluster_enabled: true,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            prefetched: Default::default(),
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn row(id: Option<&str>) -> Vec<(String, SqlValue)> {
        let mut row = vec![("name".to_string(), SqlValue::String("n".to_string()))];
        if let Some(id) = id {
            row.push(("id".to_string(), SqlValue::String(id.to_string())));
        }
        row
    }

    fn insert(collection: &str, primary_key: &str, rows: Vec<Vec<(String, SqlValue)>>) -> SqlPlan {
        SqlPlan::Insert(InsertPlan {
            collection: collection.to_string(),
            engine: EngineType::DocumentSchemaless,
            route: WriteRoute::Document,
            rows,
            volatile_defaults: false,
            if_absent: false,
            column_schema: Vec::new(),
            primary_key: Some(primary_key.to_string()),
        })
    }

    fn keys(pks: &[Vec<u8>]) -> Vec<&[u8]> {
        pks.iter().map(Vec::as_slice).collect()
    }

    #[test]
    fn insert_binds_named_keys_and_counts_fresh_rows() {
        let ctx = ctx();
        let plans = vec![insert(
            "users",
            "id",
            vec![row(Some("a")), row(Some("b")), row(None)],
        )];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].collection, "users");
        assert_eq!(keys(&batches[0].pks), vec![&b"a"[..], &b"b"[..]]);
        assert!(batches[0].binds);
        assert_eq!(batches[0].fresh, 1);
    }

    #[test]
    fn auto_rowid_insert_draws_one_fresh_identity_per_row() {
        let ctx = ctx();
        let plans = vec![insert("events", "_rowid", vec![row(Some("a")), row(None)])];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].pks.is_empty());
        assert_eq!(batches[0].fresh, 2);
    }

    #[test]
    fn document_delete_reads_and_kv_update_binds() {
        let ctx = ctx();
        let plans = vec![
            SqlPlan::Delete {
                collection: "users".to_string(),
                engine: EngineType::DocumentSchemaless,
                filters: Vec::new(),
                target_keys: vec![SqlValue::String("x".to_string())],
            },
            SqlPlan::Update {
                collection: "cache".to_string(),
                engine: EngineType::KeyValue,
                assignments: Vec::new(),
                filters: Vec::new(),
                target_keys: vec![SqlValue::String("k".to_string())],
                returning: false,
            },
            SqlPlan::Delete {
                collection: "cache".to_string(),
                engine: EngineType::KeyValue,
                filters: Vec::new(),
                target_keys: vec![SqlValue::String("k".to_string())],
            },
        ];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].collection, "users");
        assert!(!batches[0].binds);
        assert_eq!(keys(&batches[0].pks), vec![&b"x"[..]]);
        assert_eq!(batches[1].collection, "cache");
        assert!(batches[1].binds);
        assert_eq!(batches[1].pks.len(), 1);
    }

    #[test]
    fn cte_outer_write_is_collected() {
        let ctx = ctx();
        let plans = vec![SqlPlan::Cte(CtePlan {
            definitions: Vec::new(),
            outer: Box::new(insert("users", "id", vec![row(Some("c"))])),
        })];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 1);
        assert_eq!(keys(&batches[0].pks), vec![&b"c"[..]]);
    }

    #[test]
    fn document_point_read_inside_a_union_is_collected() {
        let ctx = ctx();
        let point_get = |key: &str| SqlPlan::PointGet {
            collection: "users".to_string(),
            alias: None,
            engine: EngineType::DocumentStrict,
            key_column: "id".to_string(),
            key_value: SqlValue::String(key.to_string()),
            projection: Vec::new(),
        };
        let plans = vec![SqlPlan::Union {
            inputs: vec![point_get("p"), point_get("q")],
            distinct: false,
        }];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| !batch.binds));
        assert_eq!(keys(&batches[0].pks), vec![&b"p"[..]]);
        assert_eq!(keys(&batches[1].pks), vec![&b"q"[..]]);
    }

    #[test]
    fn a_plan_with_no_keys_yields_no_batch() {
        let ctx = ctx();
        let plans = vec![insert("users", "id", Vec::new())];
        assert!(plan_key_batches(&plans, &ctx).is_empty());
    }

    #[test]
    fn timeseries_ingest_draws_one_fresh_identity_per_row() {
        let ctx = ctx();
        let plans = vec![SqlPlan::TimeseriesIngest(TimeseriesIngestPlan {
            collection: "metrics".to_string(),
            rows: vec![Vec::new(), Vec::new(), Vec::new()],
            volatile_defaults: false,
        })];
        let batches = plan_key_batches(&plans, &ctx);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].collection, "metrics");
        assert!(batches[0].pks.is_empty());
        assert_eq!(batches[0].fresh, 3);
    }
}
