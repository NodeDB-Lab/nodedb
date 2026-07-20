// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CONTINUOUS AGGREGATE` handler.
//!
//! Ported from the pgwire `ddl::continuous_agg::create` handler. The catalog
//! path (`propose_and_apply` for the `PutContinuousAggregate` entry, then the
//! target-collection `propose_and_apply` + `dispatch_register_from_stored`, then
//! the `log_index == 0` single-node `RegisterContinuousAggregate` sync dispatch),
//! the source-existence / timeseries / duplicate checks, the def serialization,
//! and the target-collection descriptor are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use std::time::Duration;

use nodedb_types::DatabaseId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::{StoredCollection, StoredContinuousAggregate, StoredOwner};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::collection;
use super::parse::{extract_with_options, parse_create_sql};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Parsed `CREATE CONTINUOUS AGGREGATE` request.
///
/// Syntax:
/// ```text
/// CREATE CONTINUOUS AGGREGATE <name> ON <source> BUCKET '<interval>'
///   AGGREGATE <func>(col) [AS alias], ...
///   [GROUP BY col, ...]
///   [WITH (refresh_policy = 'on_flush', retention = '7d')]
/// ```
///
/// `aggregate_exprs_raw` is the raw text after the AGGREGATE keyword.
/// `with_clause_raw` is the raw inner text of the trailing WITH(...), or empty.
#[derive(Clone, Copy)]
pub struct CreateContinuousAggregateRequest<'a> {
    pub name: &'a str,
    pub source: &'a str,
    pub bucket_raw: &'a str,
    pub aggregate_exprs_raw: &'a str,
    pub group_by: &'a [String],
    pub with_clause_raw: &'a str,
    /// Session database the continuous aggregate is created in. Scopes
    /// catalog lookups and Data-Plane dispatch routing.
    pub database_id: DatabaseId,
}

/// Handle `CREATE CONTINUOUS AGGREGATE`.
pub async fn create_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateContinuousAggregateRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateContinuousAggregateRequest {
        name,
        source,
        bucket_raw,
        aggregate_exprs_raw,
        group_by,
        with_clause_raw,
        database_id,
    } = *req;
    // Reconstruct minimal SQL for parse_create_sql reuse.
    // This avoids duplicating the complex AggregateExpr parsing logic.
    let reconstructed = format!(
        "CREATE CONTINUOUS AGGREGATE {name} ON {source} BUCKET '{bucket_raw}' AGGREGATE {aggregate_exprs_raw}"
    );
    let def_from_parts = parse_create_sql(&reconstructed)?;

    // Apply group_by and with_clause_raw overrides.
    let (refresh_policy, retention_period_ms) = if with_clause_raw.is_empty() {
        (
            def_from_parts.refresh_policy,
            def_from_parts.retention_period_ms,
        )
    } else {
        let fake_with_sql = format!("dummy WITH ({with_clause_raw})");
        let (rp, ret) = extract_with_options(&fake_with_sql.to_uppercase(), &fake_with_sql);
        (rp, ret)
    };

    let def = ContinuousAggregateDef {
        database_id: database_id.as_u64(),
        name: def_from_parts.name,
        source: def_from_parts.source,
        bucket_interval: def_from_parts.bucket_interval,
        bucket_interval_ms: def_from_parts.bucket_interval_ms,
        group_by: group_by.to_vec(),
        aggregates: def_from_parts.aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
    };

    // Validate source collection exists and is timeseries.
    let tenant_id = identity.tenant_id;
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(database_id, tenant_id.as_u64(), &def.source) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(err(
                    "42809",
                    format!("'{}' is not a timeseries collection", def.source),
                ));
            }
            _ => {
                return Err(err(
                    "42P01",
                    format!("collection '{}' does not exist", def.source),
                ));
            }
        }

        if let Ok(Some(_)) =
            catalog.get_continuous_aggregate(database_id.as_u64(), tenant_id.as_u64(), &def.name)
        {
            return Err(err(
                "42P07",
                format!("continuous aggregate '{}' already exists", def.name),
            ));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Serialize the runtime def into the catalog row. Stored opaquely
    // so the on-disk format does not depend on Data Plane tuning
    // fields — the def is decoded on register dispatch in
    // `post_apply::async_dispatch::continuous_aggregate::put_async`.
    let def_bytes = zerompk::to_msgpack_vec(&def)
        .map_err(|e| err("XX000", format!("serialize continuous aggregate def: {e}")))?;

    let stored = StoredContinuousAggregate {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: def.name.clone(),
        source: def.source.clone(),
        def_bytes,
        owner: identity.username.clone(),
        created_at: now,
        // Stamped by the metadata applier at commit time.
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutContinuousAggregate(Box::new(
        stored.clone(),
    ));
    let log_index = propose_and_apply(state, &entry)?;

    // Create the target collection so `SELECT * FROM <ca_name>` resolves
    // like any other relation. Schemaless document by parity with
    // materialized-view targets; refresh-path writes (when wired) will
    // upsert rolled-up documents into it. Idempotent when a collection
    // of the same name already exists.
    let target_exists = {
        let catalog = state.credentials.catalog();
        matches!(
            catalog.get_collection(database_id, tenant_id.as_u64(), &def.name),
            Ok(Some(c)) if c.is_active
        )
    };
    if !target_exists {
        let target = StoredCollection {
            tenant_id: tenant_id.as_u64(),
            name: def.name.clone(),
            owner: identity.username.clone(),
            created_at: now,
            descriptor_version: 0,
            constraint_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
            fields: Vec::new(),
            field_defs: Vec::new(),
            event_defs: Vec::new(),
            collection_type: nodedb_types::CollectionType::document(),
            timeseries_config: None,
            conflict_policy: None,
            is_active: true,
            append_only: false,
            hash_chain: false,
            balanced: None,
            last_chain_hash: None,
            period_lock: None,
            retention_period: None,
            legal_holds: Vec::new(),
            state_constraints: Vec::new(),
            transition_checks: Vec::new(),
            type_guards: Vec::new(),
            check_constraints: Vec::new(),
            materialized_sums: Vec::new(),
            lvc_enabled: false,
            bitemporal: false,
            crdt: false,
            permission_tree_def: None,
            indexes: Vec::new(),
            size_bytes_estimate: 0,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            database_id,
            cloned_from: None,
            clone_status: nodedb_types::CloneStatus::default(),
            has_implicit_edges: false,
            declared_primary_key: None,
        };
        let coll_entry =
            crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(target.clone()));
        propose_and_apply(state, &coll_entry)?;
        collection::dispatch_register_from_stored(state, &target)
            .await
            .map_err(|e| err("XX000", e.to_string()))?;
    }

    // Single-node / no-applier path: the async post-apply dispatcher
    // only fires for `log_index > 0` (the raft-applier path). Mirror
    // the dispatch here so the local `continuous_agg_mgr` registers
    // immediately, matching the cluster behaviour.
    if log_index == 0 {
        state.permissions.install_replicated_owner(&StoredOwner {
            database_id: stored.database_id,
            object_type:
                crate::control::security::catalog::auth_types::object_type::CONTINUOUS_AGGREGATE
                    .to_string(),
            object_name: stored.name.clone(),
            tenant_id: stored.tenant_id,
            owner_username: stored.owner.clone(),
        });
        let plan = PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate { def: def.clone() });
        sync_dispatch::dispatch_async(
            state,
            tenant_id,
            database_id,
            &def.source,
            plan,
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| err("XX000", format!("dispatch failed: {e}")))?;
    }

    tracing::info!(
        name = def.name,
        source = def.source,
        interval = def.bucket_interval,
        tenant = tenant_id.as_u64(),
        "continuous aggregate created"
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE CONTINUOUS AGGREGATE".to_string(),
        rows_affected: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::super::parse::parse_create_sql;
    use crate::engine::timeseries::continuous_agg::{AggFunction, RefreshPolicy};

    #[test]
    fn parse_create_basic() {
        let sql = "CREATE CONTINUOUS AGGREGATE metrics_1m ON metrics \
                    BUCKET '1m' \
                    AGGREGATE sum(value) AS value_sum, count(*) AS row_count";
        let def = parse_create_sql(sql).expect("parse_create_sql failed");
        assert_eq!(def.name, "metrics_1m");
        assert_eq!(def.source, "metrics");
        assert_eq!(def.bucket_interval, "1m");
        assert_eq!(def.bucket_interval_ms, 60_000);
        assert_eq!(def.aggregates.len(), 2);
        assert_eq!(def.aggregates[0].output_column, "value_sum");
        assert_eq!(def.aggregates[1].output_column, "row_count");
        assert!(matches!(def.aggregates[0].function, AggFunction::Sum));
        assert!(matches!(def.aggregates[1].function, AggFunction::Count));
    }

    #[test]
    fn parse_create_with_group_by_and_options() {
        let sql = "CREATE CONTINUOUS AGGREGATE cpu_5m ON cpu_metrics \
                    BUCKET '5m' \
                    AGGREGATE avg(cpu) AS cpu_avg, max(cpu) AS cpu_max \
                    GROUP BY host, region \
                    WITH (refresh_policy = 'on_flush', retention = '7d')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.name, "cpu_5m");
        assert_eq!(def.source, "cpu_metrics");
        assert_eq!(def.bucket_interval_ms, 300_000);
        assert_eq!(def.group_by, vec!["host", "region"]);
        assert_eq!(def.refresh_policy, RefreshPolicy::OnFlush);
        assert_eq!(def.retention_period_ms, 604_800_000); // 7d
    }

    #[test]
    fn parse_create_auto_alias() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1h' \
                    AGGREGATE min(value), max(value)";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.aggregates[0].output_column, "min_value");
        assert_eq!(def.aggregates[1].output_column, "max_value");
    }

    #[test]
    fn parse_create_manual_refresh() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1d' \
                    AGGREGATE sum(val) \
                    WITH (refresh_policy = 'manual')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.refresh_policy, RefreshPolicy::Manual);
    }

    #[test]
    fn parse_create_periodic_refresh() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1h' \
                    AGGREGATE count(*) \
                    WITH (refresh = '5m')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.refresh_policy, RefreshPolicy::Periodic(300_000));
    }

    #[test]
    fn parse_missing_bucket_errors() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics AGGREGATE sum(val)";
        assert!(parse_create_sql(sql).is_err());
    }
}
