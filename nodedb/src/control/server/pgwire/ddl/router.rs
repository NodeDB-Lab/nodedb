use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::DocumentOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Try to handle a SQL statement as a Control Plane DDL command.
///
/// These execute directly on the Control Plane without going through
/// DataFusion or the Data Plane. Returns `None` if not recognized.
///
/// Async because DSL commands (SEARCH, CRDT) dispatch to the Data Plane
/// and must await the response without blocking the Tokio runtime.
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = sql.to_uppercase();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // User management.
    if upper.starts_with("CREATE USER ") {
        return Some(super::user::create_user(state, identity, &parts));
    }
    if upper.starts_with("ALTER USER ") {
        return Some(super::user::alter_user(state, identity, &parts));
    }
    if upper.starts_with("DROP USER ") {
        return Some(super::user::drop_user(state, identity, &parts));
    }

    // Service accounts.
    if upper.starts_with("CREATE SERVICE ACCOUNT ") {
        return Some(super::service_account::create_service_account(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP SERVICE ACCOUNT ") {
        return Some(super::service_account::drop_service_account(
            state, identity, &parts,
        ));
    }

    // Tenant management.
    if upper.starts_with("CREATE TENANT ") {
        return Some(super::tenant::create_tenant(state, identity, &parts));
    }
    if upper.starts_with("ALTER TENANT ") {
        return Some(super::tenant::alter_tenant(state, identity, &parts));
    }
    if upper.starts_with("DROP TENANT ") {
        return Some(super::tenant::drop_tenant(state, identity, &parts));
    }

    // GRANT / REVOKE.
    if upper.starts_with("GRANT ") {
        return Some(super::grant::handle_grant(state, identity, &parts));
    }
    if upper.starts_with("REVOKE ") {
        return Some(super::grant::handle_revoke(state, identity, &parts));
    }

    // Role management.
    if upper.starts_with("CREATE ROLE ") {
        return Some(super::role::create_role(state, identity, &parts));
    }
    if upper.starts_with("ALTER ROLE ") {
        return Some(super::role::alter_role(state, identity, &parts));
    }
    if upper.starts_with("DROP ROLE ") {
        return Some(super::role::drop_role(state, identity, &parts));
    }

    // Backup / Restore.
    if upper.starts_with("BACKUP TENANT ") {
        return Some(super::backup::backup_tenant(state, identity, &parts).await);
    }
    if upper.starts_with("RESTORE TENANT ") {
        if upper.ends_with(" DRY RUN") || upper.ends_with(" DRYRUN") {
            return Some(super::backup::restore_tenant_dry_run(
                state, identity, &parts,
            ));
        }
        return Some(super::backup::restore_tenant(state, identity, &parts).await);
    }

    // User-defined functions.
    if upper.starts_with("CREATE OR REPLACE FUNCTION ") || upper.starts_with("CREATE FUNCTION ") {
        return Some(super::function::create_function(state, identity, sql));
    }
    if upper.starts_with("DROP FUNCTION ") {
        return Some(super::function::drop_function(state, identity, &parts));
    }
    if upper == "SHOW FUNCTIONS" || upper.starts_with("SHOW FUNCTIONS") {
        return Some(super::function::show_functions(state, identity));
    }

    // Stored procedures.
    if upper.starts_with("CREATE OR REPLACE PROCEDURE ") || upper.starts_with("CREATE PROCEDURE ") {
        return Some(super::procedure::create_procedure(state, identity, sql));
    }
    if upper.starts_with("DROP PROCEDURE ") {
        return Some(super::procedure::drop_procedure(state, identity, &parts));
    }
    if upper == "SHOW PROCEDURES" || upper.starts_with("SHOW PROCEDURES") {
        return Some(super::procedure::show_procedures(state, identity));
    }
    if upper.starts_with("CALL ") {
        return Some(super::procedure::call_procedure(state, identity, sql).await);
    }

    // Change Streams: CREATE/DROP/SHOW CHANGE STREAM ...
    if upper.starts_with("CREATE CHANGE STREAM ") {
        return Some(super::change_stream::create_change_stream(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP CHANGE STREAM ") {
        return Some(super::change_stream::drop_change_stream(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW CHANGE STREAM") {
        return Some(super::change_stream::show_change_streams(state, identity));
    }

    // Consumer Groups: CREATE/DROP/SHOW CONSUMER GROUP + COMMIT OFFSET(S)
    if upper.starts_with("CREATE CONSUMER GROUP ") {
        return Some(super::consumer_group::create_consumer_group(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP CONSUMER GROUP ") {
        return Some(super::consumer_group::drop_consumer_group(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW CONSUMER GROUPS ") {
        return Some(super::consumer_group::show_consumer_groups(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        return Some(super::consumer_group::show_partitions(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ") {
        return Some(super::consumer_group::commit_offset(
            state, identity, &parts,
        ));
    }

    // Stream consumption: SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    if upper.starts_with("SELECT ")
        && upper.contains("FROM STREAM ")
        && upper.contains("CONSUMER GROUP")
    {
        return Some(super::stream_select::select_from_stream(state, identity, &parts).await);
    }

    // Streaming materialized views: CREATE MATERIALIZED VIEW ... STREAMING AS ...
    if upper.starts_with("CREATE MATERIALIZED VIEW ") && upper.contains(" STREAMING ") {
        return Some(super::streaming_mv::create_streaming_mv(
            state, identity, sql,
        ));
    }

    // Topics: CREATE/DROP/SHOW TOPIC + PUBLISH TO
    if upper.starts_with("CREATE TOPIC ") {
        return Some(super::topic::create_topic(state, identity, &parts, sql));
    }
    if upper.starts_with("DROP TOPIC ") {
        return Some(super::topic::drop_topic(state, identity, &parts));
    }
    if upper.starts_with("SHOW TOPIC") {
        return Some(super::topic::show_topics(state, identity));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(super::topic::handle_publish(state, identity, sql).await);
    }

    // Stream/Topic consumption: SELECT * FROM STREAM/TOPIC ... CONSUMER GROUP ...
    if upper.starts_with("SELECT ")
        && upper.contains("FROM TOPIC ")
        && upper.contains("CONSUMER GROUP")
    {
        // Rewrite: topics use "topic:<name>" buffer keys.
        // The stream_select handler works for both — we just need to prefix the name.
        return Some(select_from_topic(state, identity, &parts).await);
    }

    // Schedules: CREATE/DROP/SHOW SCHEDULE
    if upper.starts_with("CREATE SCHEDULE ") {
        return Some(super::schedule::create_schedule(state, identity, sql));
    }
    if upper.starts_with("DROP SCHEDULE ") {
        return Some(super::schedule::drop_schedule(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(super::schedule::show_schedules(state, identity));
    }

    // Triggers: CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER ...
    if upper.starts_with("CREATE TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE TRIGGER ")
        || upper.starts_with("CREATE SYNC TRIGGER ")
        || upper.starts_with("CREATE DEFERRED TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE SYNC TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE DEFERRED TRIGGER ")
    {
        return Some(super::trigger::create_trigger(state, identity, sql));
    }
    if upper.starts_with("DROP TRIGGER ") {
        return Some(super::trigger::drop_trigger(state, identity, &parts));
    }
    if upper.starts_with("ALTER TRIGGER ") {
        return Some(super::trigger::alter_trigger(state, identity, &parts));
    }
    if upper == "SHOW TRIGGERS" || upper.starts_with("SHOW TRIGGERS") {
        return Some(super::trigger::show_triggers(state, identity, &parts));
    }

    // Schema introspection.
    if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        return Some(super::collection::describe_collection(
            state, identity, &parts,
        ));
    }

    // Collection management.
    if upper.starts_with("CREATE COLLECTION ") {
        let result = super::collection::create_collection(state, identity, &parts, sql);
        // If creation succeeded, register the collection's storage mode with the
        // Data Plane so it knows how to encode/decode documents (MessagePack vs
        // Binary Tuple). This must happen after the catalog persist so the collection
        // metadata is durable.
        if result.is_ok() {
            super::collection::dispatch_register_if_needed(state, identity, &parts, sql).await;
        }
        return Some(result);
    }
    if upper.starts_with("DROP COLLECTION ") {
        return Some(super::collection::drop_collection(state, identity, &parts));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(super::collection::show_collections(state, identity));
    }
    // Timeseries: CREATE TIMESERIES, SHOW PARTITIONS, ALTER TIMESERIES.
    if upper.starts_with("CREATE TIMESERIES ") {
        return Some(super::timeseries::create_timeseries(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        return Some(super::timeseries::show_partitions(state, identity, &parts));
    }
    if upper.starts_with("ALTER TIMESERIES ") {
        return Some(super::timeseries::alter_timeseries(state, identity, &parts));
    }
    if upper.starts_with("REWRITE PARTITIONS ") {
        return Some(super::timeseries::rewrite_partitions(
            state, identity, &parts,
        ));
    }

    // Continuous aggregates.
    if upper.starts_with("CREATE CONTINUOUS AGGREGATE ") {
        return Some(
            super::continuous_agg::create_continuous_aggregate(state, identity, sql).await,
        );
    }
    if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
        return Some(
            super::continuous_agg::drop_continuous_aggregate(state, identity, &parts).await,
        );
    }
    if upper.starts_with("SHOW CONTINUOUS AGGREGATE") {
        return Some(
            super::continuous_agg::show_continuous_aggregates(state, identity, &parts).await,
        );
    }

    // CONVERT COLLECTION.
    if upper.starts_with("CONVERT COLLECTION ")
        || upper.starts_with("CONVERT ") && upper.contains(" TO ")
    {
        return Some(super::convert::convert_collection(state, identity, sql).await);
    }

    // Materialized views (HTAP).
    if upper.starts_with("CREATE MATERIALIZED VIEW ") {
        return Some(super::materialized_view::create_materialized_view(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP MATERIALIZED VIEW ") {
        return Some(super::materialized_view::drop_materialized_view(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
        return Some(
            super::materialized_view::refresh_materialized_view(state, identity, &parts).await,
        );
    }
    if upper.starts_with("SHOW MATERIALIZED VIEW") {
        return Some(super::materialized_view::show_materialized_views(
            state, identity, &parts,
        ));
    }

    // Pub/Sub: SUBSCRIBE TO (legacy). CREATE/DROP/SHOW TOPIC and PUBLISH TO
    // are handled by Event Plane topic handlers above (lines 167-180).
    if upper.starts_with("SUBSCRIBE TO ") {
        return Some(super::pubsub::subscribe_to(state, identity, sql, &parts));
    }

    if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
        return Some(super::collection::create_index(
            state, identity, &parts, sql,
        ));
    }
    if upper.starts_with("DROP INDEX ") {
        return Some(super::collection::drop_index(state, identity, &parts));
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        return Some(super::collection::show_indexes(state, identity, &parts));
    }

    // Ownership transfer.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("OWNER TO") {
        return Some(super::ownership::alter_collection_owner(
            state, identity, &parts,
        ));
    }

    // ALTER TABLE ADD COLUMN — schema modification for strict/columnar collections.
    if upper.starts_with("ALTER TABLE ") && (upper.contains("ADD COLUMN") || upper.contains("ADD "))
    {
        return Some(super::collection::alter_table_add_column(
            state, identity, &parts, sql,
        ));
    }

    // RLS policies.
    if upper.starts_with("CREATE RLS POLICY ") {
        return Some(super::rls::create_rls_policy(state, identity, &parts));
    }
    if upper.starts_with("DROP RLS POLICY ") {
        return Some(super::rls::drop_rls_policy(state, identity, &parts));
    }
    if upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY") {
        return Some(super::rls::show_rls_policies(state, identity, &parts));
    }

    // Blacklist management.
    if upper.starts_with("BLACKLIST ") {
        return Some(super::blacklist_ddl::handle_blacklist(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW BLACKLIST") {
        return Some(super::blacklist_ddl::show_blacklist(
            state, identity, &parts,
        ));
    }
    // Auth user management (JIT-provisioned users).
    if upper.starts_with("DEACTIVATE AUTH USER ") || upper.starts_with("ALTER AUTH USER ") {
        return Some(super::auth_user_ddl::handle_auth_user(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("PURGE AUTH USERS ") {
        return Some(super::auth_user_ddl::purge_auth_users(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW AUTH USERS") {
        return Some(super::auth_user_ddl::show_auth_users(
            state, identity, &parts,
        ));
    }

    // Organization management.
    if upper.starts_with("CREATE ORG ")
        || upper.starts_with("ALTER ORG ")
        || upper.starts_with("DROP ORG ")
    {
        return Some(super::org_ddl::handle_org(state, identity, &parts));
    }
    if upper.starts_with("SHOW ORGS") {
        return Some(super::org_ddl::show_orgs(state, identity, &parts));
    }
    if upper.starts_with("SHOW MEMBERS OF ORG") {
        return Some(super::org_ddl::show_members(state, identity, &parts));
    }

    // Scope management.
    if upper.starts_with("DEFINE SCOPE ") {
        return Some(super::scope_ddl::define_scope(state, identity, &parts));
    }
    if upper.starts_with("DROP SCOPE ") {
        return Some(super::scope_ddl::drop_scope(state, identity, &parts));
    }
    if upper.starts_with("GRANT SCOPE ") {
        return Some(super::scope_ddl::grant_scope(state, identity, &parts));
    }
    if upper.starts_with("REVOKE SCOPE ") {
        return Some(super::scope_ddl::revoke_scope(state, identity, &parts));
    }
    if upper.starts_with("ALTER SCOPE ") {
        return Some(super::scope_query_ddl::alter_scope(state, identity, &parts));
    }
    if upper.starts_with("SHOW MY SCOPES") {
        return Some(super::scope_query_ddl::show_my_scopes(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW SCOPES FOR ") {
        return Some(super::scope_query_ddl::show_scopes_for(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("RENEW SCOPE ") {
        return Some(super::scope_ddl::renew_scope(state, identity, &parts));
    }
    // EXPLAIN PERMISSION / EXPLAIN SCOPE.
    if upper.starts_with("EXPLAIN PERMISSION ") {
        return Some(super::explain_ddl::explain_permission(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("EXPLAIN SCOPE ") {
        return Some(super::explain_ddl::explain_scope(state, identity, &parts));
    }

    // Emergency response.
    if upper.starts_with("EMERGENCY LOCKDOWN") {
        return Some(super::emergency_ddl::emergency_lockdown(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("EMERGENCY UNLOCK") {
        return Some(super::emergency_ddl::emergency_unlock(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("BLACKLIST AUTH USERS WHERE") {
        return Some(super::emergency_ddl::bulk_blacklist(
            state, identity, &parts,
        ));
    }

    // Auth-scoped API keys.
    if upper.starts_with("CREATE AUTH KEY ") {
        return Some(super::auth_key_ddl::create_auth_key(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("ROTATE AUTH KEY ") {
        return Some(super::auth_key_ddl::rotate_auth_key(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("LIST AUTH KEYS") {
        return Some(super::auth_key_ddl::list_auth_keys(state, identity, &parts));
    }

    // Impersonation & delegation.
    if upper.starts_with("IMPERSONATE AUTH USER ") {
        return Some(super::impersonation_ddl::impersonate(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("STOP IMPERSONATION") {
        return Some(super::impersonation_ddl::stop_impersonation(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DELEGATE AUTH USER ") {
        return Some(super::impersonation_ddl::delegate(state, identity, &parts));
    }
    if upper.starts_with("REVOKE DELEGATION ") {
        return Some(super::impersonation_ddl::revoke_delegation(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW DELEGATIONS") {
        return Some(super::impersonation_ddl::show_delegations(
            state, identity, &parts,
        ));
    }

    // Session management.
    if upper.starts_with("SHOW SESSIONS") {
        return Some(super::session_ddl::show_sessions(state, identity, &parts));
    }
    if upper.starts_with("KILL SESSION ") {
        return Some(super::session_ddl::kill_session(state, identity, &parts));
    }
    if upper.starts_with("KILL USER SESSIONS ") {
        return Some(super::session_ddl::kill_user_sessions(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("VERIFY AUDIT CHAIN") {
        return Some(super::session_ddl::verify_audit_chain(
            state, identity, &parts,
        ));
    }

    // Usage metering.
    if upper.starts_with("DEFINE METERING DIMENSION ") {
        return Some(super::metering_ddl::define_dimension(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW USAGE ") {
        return Some(super::metering_ddl::show_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW QUOTA ") {
        return Some(super::metering_ddl::show_quota(state, identity, &parts));
    }

    if upper.starts_with("SHOW SCOPE GRANTS") {
        return Some(super::scope_ddl::show_scope_grants(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPE") {
        return Some(super::scope_ddl::show_scopes(state, identity, &parts));
    }

    // API keys.
    if upper.starts_with("CREATE API KEY ") {
        return Some(super::apikey::create_api_key(state, identity, &parts));
    }
    if upper.starts_with("REVOKE API KEY ") {
        return Some(super::apikey::revoke_api_key(state, identity, &parts));
    }
    if upper.starts_with("LIST API KEYS") {
        return Some(super::apikey::list_api_keys(state, identity, &parts));
    }

    // Cluster management & observability.
    if upper.starts_with("SHOW CLUSTER") {
        return Some(super::cluster::show_cluster(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUPS") {
        return Some(super::cluster::show_raft_groups(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        return Some(super::cluster::show_raft_group(state, identity, &parts));
    }
    if upper.starts_with("ALTER RAFT GROUP ") {
        return Some(super::cluster::alter_raft_group(state, identity, &parts));
    }
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(super::cluster::show_migrations(state, identity));
    }
    if upper.starts_with("REBALANCE") {
        return Some(super::cluster::rebalance(state, identity));
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(super::cluster::show_peer_health(state, identity));
    }
    if upper.starts_with("SHOW NODES") {
        return Some(super::cluster::show_nodes(state, identity));
    }
    if upper.starts_with("SHOW NODE ") {
        return Some(super::cluster::show_node(state, identity, &parts));
    }
    if upper.starts_with("REMOVE NODE ") {
        return Some(super::cluster::remove_node(state, identity, &parts));
    }

    // Introspection.
    if upper.starts_with("SHOW USERS") {
        return Some(super::inspect::show_users(state, identity));
    }
    if upper.starts_with("SHOW TENANTS") {
        return Some(super::inspect::show_tenants(state, identity));
    }
    if upper.starts_with("SHOW SESSION") {
        return Some(super::inspect::show_session(identity));
    }
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(super::super::types::sqlstate_error(
            "42501",
            "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).",
        )));
    }
    if upper.starts_with("EXPORT AUDIT") {
        return Some(super::inspect::export_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        return Some(super::inspect::show_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW PERMISSIONS") {
        return Some(super::inspect::show_permissions(state, identity, &parts));
    }
    if upper.starts_with("SHOW GRANTS") {
        return Some(super::inspect::show_grants(state, identity, &parts));
    }

    // DSL: SEARCH commands (async — dispatches to Data Plane).
    if upper.starts_with("SEARCH ") && upper.contains("USING VECTOR") {
        return Some(super::dsl::search_vector(state, identity, sql).await);
    }
    if upper.starts_with("SEARCH ") && upper.contains("USING FUSION") {
        return Some(super::dsl::search_fusion(state, identity, sql).await);
    }

    // DSL: CREATE VECTOR INDEX / CREATE FULLTEXT INDEX.
    if upper.starts_with("CREATE VECTOR INDEX ") {
        return Some(super::dsl::create_vector_index(state, identity, &parts));
    }
    if upper.starts_with("CREATE FULLTEXT INDEX ") {
        return Some(super::dsl::create_fulltext_index(state, identity, &parts));
    }
    if upper.starts_with("CREATE SPATIAL INDEX ") {
        return Some(super::spatial::create_spatial_index(
            state, identity, &parts,
        ));
    }

    // DSL: CRDT MERGE INTO (async — dispatches to Data Plane).
    if upper.starts_with("CRDT MERGE ") {
        return Some(super::dsl::crdt_merge(state, identity, &parts).await);
    }

    // CRDT operations via SQL-like syntax (async).
    if upper.starts_with("SELECT CRDT_STATE(") || upper.starts_with("SELECT CRDT_STATE (") {
        return Some(super::crdt_ops::crdt_state(state, identity, sql).await);
    }
    if upper.starts_with("SELECT CRDT_APPLY(") || upper.starts_with("SELECT CRDT_APPLY (") {
        return Some(super::crdt_ops::crdt_apply(state, identity, sql).await);
    }

    // DSL: Graph operations (async — dispatches to Data Plane).
    if upper.starts_with("GRAPH INSERT EDGE ") {
        return Some(super::graph_ops::insert_edge(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH DELETE EDGE ") {
        return Some(super::graph_ops::delete_edge(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH TRAVERSE ") {
        return Some(super::graph_ops::traverse(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH NEIGHBORS ") {
        return Some(super::graph_ops::neighbors(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH PATH ") {
        return Some(super::graph_ops::shortest_path(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH ALGO ") {
        return Some(super::graph_ops::algo(state, identity, &parts, sql).await);
    }

    // Cypher-style MATCH pattern queries.
    if upper.starts_with("MATCH ") || upper.starts_with("OPTIONAL MATCH ") {
        return Some(super::match_ops::match_query(state, identity, sql).await);
    }

    // COPY FROM file.
    if upper.starts_with("COPY ") && upper.contains(" FROM ") {
        return Some(super::bulk::copy_from(state, identity, &parts).await);
    }

    // INSERT INTO — intercept for schemaless collections (DataFusion rejects
    // columns not in the Arrow schema, but NodeDB collections are document stores).
    if upper.starts_with("INSERT INTO ")
        && upper.contains("VALUES")
        && let Some(result) = super::collection_insert::insert_document(state, identity, sql).await
    {
        return Some(result);
    }

    // UPSERT INTO — same as INSERT but merges into existing document if it exists.
    if upper.starts_with("UPSERT INTO ")
        && upper.contains("VALUES")
        && let Some(result) = super::collection_insert::upsert_document(state, identity, sql).await
    {
        return Some(result);
    }

    // SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]
    if upper.starts_with("SHOW CHANGES ") {
        if let Some(coll_name) = super::sql_parse::extract_collection_after(sql, " FOR ") {
            let since_ms: u64 = if let Some(since_pos) = upper.find(" SINCE ") {
                let since_str = sql[since_pos + 7..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("0");
                match super::sql_parse::parse_since_timestamp(since_str) {
                    Ok(ms) => ms,
                    Err(msg) => {
                        return Some(Err(super::super::types::sqlstate_error(
                            "22007",
                            &msg.to_string(),
                        )));
                    }
                }
            } else {
                // Default: last 24 hours of changes.
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                now_ms.saturating_sub(86_400 * 1000)
            };

            let limit = upper
                .find(" LIMIT ")
                .and_then(|pos| sql[pos + 7..].split_whitespace().next())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1000);

            let changes = state
                .change_stream
                .query_changes(Some(&coll_name), since_ms, limit);

            use futures::stream;
            use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};

            let schema = std::sync::Arc::new(vec![
                super::super::types::text_field("collection"),
                super::super::types::text_field("operation"),
                super::super::types::text_field("document_id"),
                super::super::types::text_field("timestamp_ms"),
                super::super::types::text_field("lsn"),
            ]);

            let mut rows = Vec::with_capacity(changes.len());
            for change in &changes {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let _ = encoder.encode_field(&change.collection);
                let _ = encoder.encode_field(&change.operation.as_str().to_string());
                let _ = encoder.encode_field(&change.document_id);
                let _ = encoder.encode_field(&change.timestamp_ms.to_string());
                let _ = encoder.encode_field(&change.lsn.as_u64().to_string());
                rows.push(Ok(encoder.take_row()));
            }

            return Some(Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows),
            ))]));
        }
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "syntax: SHOW CHANGES FOR <collection> [SINCE <timestamp>]",
        )));
    }

    // LIVE SELECT is handled in sql_exec.rs (needs session state for subscription storage).

    // DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]
    if upper.starts_with("DEFINE FIELD ") {
        return Some(super::field_def::define_field(state, identity, sql));
    }

    // DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>
    if upper.starts_with("DEFINE EVENT ") {
        return Some(super::field_def::define_event(state, identity, sql));
    }

    // ESTIMATE_COUNT('collection', 'field') — fast approximate count from HLL stats.
    if upper.starts_with("SELECT ESTIMATE_COUNT(") || upper.starts_with("SELECT ESTIMATE_COUNT (") {
        // Parse: SELECT ESTIMATE_COUNT('collection', 'field')
        let inner = sql
            .find('(')
            .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
        if let Some(args_str) = inner {
            let args: Vec<&str> = args_str
                .split(',')
                .map(|s| s.trim().trim_matches('\''))
                .collect();
            if args.len() >= 2 {
                let coll = args[0].to_lowercase();
                let field = args[1].to_string();
                let tenant_id = identity.tenant_id;
                let vshard = crate::types::VShardId::from_collection(&coll);
                let plan =
                    crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::EstimateCount {
                        collection: coll,
                        field,
                    });
                match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    state, tenant_id, vshard, plan, 0,
                )
                .await
                {
                    Ok(resp) => {
                        let payload_text =
                            crate::data::executor::response_codec::decode_payload_to_json(
                                &resp.payload,
                            );
                        use futures::stream;
                        use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
                        let schema = std::sync::Arc::new(vec![super::super::types::text_field(
                            "estimate_count",
                        )]);
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        let _ = encoder.encode_field(&payload_text);
                        let row = encoder.take_row();
                        return Some(Ok(vec![Response::Query(QueryResponse::new(
                            schema,
                            stream::iter(vec![Ok(row)]),
                        ))]));
                    }
                    Err(e) => {
                        return Some(Err(super::super::types::sqlstate_error(
                            "XX000",
                            &e.to_string(),
                        )));
                    }
                }
            }
        }
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "usage: SELECT ESTIMATE_COUNT('collection', 'field')",
        )));
    }

    // TRUNCATE <collection> — fast delete-all without filter scan.
    if upper.starts_with("TRUNCATE ") {
        let coll_name = parts.get(1).map(|s| s.to_lowercase()).unwrap_or_default();
        if coll_name.is_empty() {
            return Some(Err(super::super::types::sqlstate_error(
                "42601",
                "TRUNCATE requires a collection name",
            )));
        }
        let tenant_id = identity.tenant_id;
        let vshard = crate::types::VShardId::from_collection(&coll_name);
        let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::Truncate {
            collection: coll_name,
        });
        if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard, plan, 0,
        )
        .await
        {
            return Some(Err(super::super::types::sqlstate_error(
                "XX000",
                &e.to_string(),
            )));
        }
        return Some(Ok(vec![pgwire::api::results::Response::Execution(
            pgwire::api::results::Tag::new("TRUNCATE"),
        )]));
    }

    None
}

/// Handle `SELECT * FROM TOPIC <topic> CONSUMER GROUP <group> [LIMIT <n>]`.
///
/// Topics use the same buffer pool as change streams, prefixed with "topic:".
/// We rewrite the parts to use the prefixed name and delegate to stream_select.
/// Handle `SELECT * FROM TOPIC <topic> CONSUMER GROUP <group> [LIMIT <n>]`.
///
/// Topics use "topic:<name>" as buffer keys. We parse the parts directly
/// and call stream_select's underlying consume logic with the prefixed name.
async fn select_from_topic(
    state: &crate::control::state::SharedState,
    identity: &crate::control::security::identity::AuthenticatedIdentity,
    parts: &[&str],
) -> pgwire::error::PgWireResult<Vec<pgwire::api::results::Response>> {
    // parts: [SELECT, *, FROM, TOPIC, <topic>, CONSUMER, GROUP, <group>, ...]
    if parts.len() < 8
        || !parts[3].eq_ignore_ascii_case("TOPIC")
        || !parts[5].eq_ignore_ascii_case("CONSUMER")
        || !parts[6].eq_ignore_ascii_case("GROUP")
    {
        return Err(super::super::types::sqlstate_error(
            "42601",
            "expected SELECT * FROM TOPIC <topic> CONSUMER GROUP <group>",
        ));
    }

    // Build a rewritten parts slice with STREAM and prefixed name.
    // Only two owned strings needed — the rest are borrowed from the original.
    let prefixed_name = format!("topic:{}", parts[4].to_lowercase());
    let stream_keyword = "STREAM";

    let mut rewritten = Vec::with_capacity(parts.len());
    for (i, &p) in parts.iter().enumerate() {
        match i {
            3 => rewritten.push(stream_keyword),
            4 => rewritten.push(&prefixed_name),
            _ => rewritten.push(p),
        }
    }

    super::stream_select::select_from_stream(state, identity, &rewritten).await
}
