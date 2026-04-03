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
    if upper.starts_with("PURGE TENANT ") {
        return Some(super::tenant::purge_tenant(state, identity, &parts).await);
    }
    if upper.starts_with("SHOW TENANT USAGE") {
        return Some(super::tenant::show_tenant_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW TENANT QUOTA") {
        return Some(super::tenant::show_tenant_quota(state, identity, &parts));
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
    if upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
        || upper.starts_with("CREATE AGGREGATE FUNCTION ")
    {
        return Some(super::function::create_wasm_aggregate(state, identity, sql));
    }
    if upper.starts_with("CREATE OR REPLACE FUNCTION ") || upper.starts_with("CREATE FUNCTION ") {
        if upper.contains("LANGUAGE WASM") {
            return Some(super::function::create_wasm_function(state, identity, sql));
        }
        return Some(super::function::create_function(state, identity, sql));
    }
    if upper.starts_with("DROP FUNCTION ") {
        return Some(super::function::drop_function(state, identity, &parts));
    }
    if upper.starts_with("ALTER FUNCTION ") {
        return Some(super::function::alter_function(state, identity, &parts));
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

    // Schedules: CREATE/DROP/ALTER/SHOW SCHEDULE
    if upper.starts_with("CREATE SCHEDULE ") {
        return Some(super::schedule::create_schedule(state, identity, sql));
    }
    if upper.starts_with("DROP SCHEDULE ") {
        return Some(super::schedule::drop_schedule(state, identity, &parts));
    }
    if upper.starts_with("ALTER SCHEDULE ") {
        return Some(super::schedule::alter_schedule(state, identity, sql));
    }
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let name = parts.get(3).unwrap_or(&"");
        return Some(super::schedule::show_schedule_history(
            state, identity, name,
        ));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(super::schedule::show_schedules(state, identity));
    }

    // Sequences: CREATE/DROP/ALTER/SHOW SEQUENCE
    if upper.starts_with("CREATE SEQUENCE ") {
        return Some(super::sequence::create_sequence(state, identity, sql));
    }
    if upper.starts_with("DROP SEQUENCE ") {
        return Some(super::sequence::drop_sequence(state, identity, &parts));
    }
    if upper.starts_with("ALTER SEQUENCE ") {
        return Some(super::sequence::alter_sequence(state, identity, sql));
    }
    if upper == "SHOW SEQUENCES" || upper.starts_with("SHOW SEQUENCES ") {
        return Some(super::sequence::show_sequences(state, identity));
    }

    // Maintenance: ANALYZE, COMPACT, REINDEX, SHOW STORAGE, SHOW COMPACTION
    if upper.starts_with("ANALYZE ") {
        return Some(super::maintenance::handle_analyze(state, identity, sql).await);
    }
    if upper.starts_with("COMPACT ") {
        return Some(super::maintenance::handle_compact(state, identity, &parts));
    }
    if upper.starts_with("REINDEX ") {
        return Some(super::maintenance::handle_reindex(state, identity, &parts));
    }
    if upper.starts_with("SHOW STORAGE ") {
        return Some(super::maintenance::handle_show_storage(
            state, identity, &parts,
        ));
    }
    if upper == "SHOW COMPACTION STATUS" || upper.starts_with("SHOW COMPACTION STATUS ") {
        return Some(super::maintenance::handle_show_compaction_status(
            state, identity,
        ));
    }

    // Vector index lifecycle: SHOW VECTOR INDEX, ALTER VECTOR INDEX.
    if upper.starts_with("SHOW VECTOR INDEX ") {
        return Some(super::maintenance::handle_show_vector_index(state, identity, sql).await);
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" SEAL") {
        return Some(
            super::maintenance::handle_alter_vector_index_seal(state, identity, sql).await,
        );
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" COMPACT") {
        return Some(
            super::maintenance::handle_alter_vector_index_compact(state, identity, sql).await,
        );
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" SET ") {
        return Some(super::maintenance::handle_alter_vector_index_set(state, identity, sql).await);
    }

    // Vector model metadata: ALTER COLLECTION ... SET VECTOR METADATA ON ...
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET VECTOR METADATA ON") {
        return Some(super::collection::handle_set_vector_metadata(
            state, identity, sql,
        ));
    }

    // SHOW VECTOR MODELS — catalog view.
    if upper.starts_with("SHOW VECTOR MODELS") || upper == "SHOW VECTOR MODELS" {
        return Some(super::collection::handle_show_vector_models(
            state, identity,
        ));
    }

    // SELECT VECTOR_METADATA('collection', 'column') — inline query.
    if upper.starts_with("SELECT VECTOR_METADATA(") || upper.starts_with("SELECT VECTOR_METADATA (")
    {
        let inner = sql
            .find('(')
            .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
        if let Some(args_str) = inner {
            let args: Vec<&str> = args_str
                .split(',')
                .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
                .collect();
            if args.len() >= 2 && !args[0].is_empty() && !args[1].is_empty() {
                return Some(super::collection::handle_vector_metadata_query(
                    state,
                    identity,
                    &args[0].to_lowercase(),
                    &args[1].to_lowercase(),
                ));
            }
        }
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "usage: SELECT VECTOR_METADATA('collection', 'column')",
        )));
    }

    // CHUNK_TEXT table-valued function: SELECT * FROM CHUNK_TEXT(...).
    if (upper.starts_with("SELECT ") && upper.contains("CHUNK_TEXT("))
        || upper.starts_with("SELECT CHUNK_TEXT(")
    {
        return Some(execute_chunk_text(sql));
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
    if upper.starts_with("DESCRIBE SEQUENCE ") {
        let name = parts.get(2).unwrap_or(&"");
        return Some(super::sequence::describe_sequence(state, identity, name));
    }
    if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        return Some(super::collection::describe_collection(
            state, identity, &parts,
        ));
    }

    // Query functions.
    if upper.contains("VERIFY_AUDIT_CHAIN") {
        return Some(super::query_functions::verify_audit_chain(state, identity, sql).await);
    }
    if upper.contains("VERIFY_HASH_CHAIN") {
        return Some(super::query_functions::verify_hash_chain(state, identity, sql).await);
    }
    if upper.contains("BALANCE_AS_OF") {
        return Some(super::query_functions::balance_as_of(state, identity, sql).await);
    }
    if upper.contains("TEMPORAL_LOOKUP") {
        return Some(super::query_functions::temporal_lookup(state, identity, sql).await);
    }
    if upper.contains("VERIFY_BALANCE") {
        return Some(super::query_functions::verify_balance(state, identity, sql).await);
    }
    if upper.contains("CONVERT_CURRENCY_LOOKUP") {
        return Some(super::query_functions::convert_currency_lookup(state, identity, sql).await);
    }

    // Graph index and tree operations.
    if upper.starts_with("CREATE GRAPH INDEX ") {
        return Some(super::tree_ops::create_graph_index(state, identity, sql).await);
    }
    if upper.starts_with("SELECT TREE_SUM") || upper.starts_with("TREE_SUM") {
        return Some(super::tree_ops::tree_sum(state, identity, sql).await);
    }
    if upper.starts_with("SELECT TREE_CHILDREN") || upper.starts_with("TREE_CHILDREN") {
        return Some(super::tree_ops::tree_children(state, identity, sql).await);
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

    // State transition constraints and transition checks.
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("TRANSITIONS")
    {
        return Some(super::constraint::add_state_constraint(
            state, identity, sql,
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD TRANSITION CHECK") {
        return Some(super::constraint::add_transition_check(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP CONSTRAINT ") {
        return Some(super::constraint::drop_constraint(state, identity, &parts));
    }

    // Materialized sum: ALTER COLLECTION x ADD COLUMN ... AS MATERIALIZED_SUM ...
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("MATERIALIZED_SUM") {
        return Some(super::collection::alter::add_materialized_sum(
            state, identity, sql,
        ));
    }

    // Period lock management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD PERIOD LOCK") {
        return Some(super::period_lock::add_period_lock(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERIOD LOCK") {
        return Some(super::period_lock::drop_period_lock(
            state, identity, &parts,
        ));
    }

    // Retention and legal hold management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET RETENTION") {
        return Some(super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "retention",
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("LEGAL_HOLD") {
        return Some(super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "legal_hold",
        ));
    }

    // Set append-only (one-way).
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET APPEND_ONLY") {
        return Some(super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "append_only",
        ));
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
    if upper.starts_with("SHOW USAGE FOR TENANT ") {
        return Some(super::metering_ddl::show_usage_for_tenant(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("EXPORT USAGE ") {
        return Some(super::metering_ddl::export_usage(state, identity, &parts));
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
    if upper.starts_with("CREATE SPARSE INDEX ") {
        return Some(super::dsl::create_sparse_index(state, identity, &parts));
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

/// Execute `CHUNK_TEXT(text, chunk_size, overlap, strategy)` and return rows.
///
/// Parses named (`text => '...'`) or positional arguments from the SQL string,
/// delegates to `nodedb_query::chunk_text`, and returns a pgwire result set.
fn execute_chunk_text(
    sql: &str,
) -> pgwire::error::PgWireResult<Vec<pgwire::api::results::Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};

    // Extract the parenthesized args from CHUNK_TEXT(...).
    let paren_start = sql
        .find("CHUNK_TEXT(")
        .or_else(|| sql.find("chunk_text("))
        .or_else(|| sql.find("Chunk_Text("))
        .and_then(|p| sql[p..].find('(').map(|off| p + off))
        .ok_or_else(|| super::super::types::sqlstate_error("42601", "expected CHUNK_TEXT(...)"))?;
    let paren_end = sql.rfind(')').ok_or_else(|| {
        super::super::types::sqlstate_error("42601", "expected closing ) for CHUNK_TEXT")
    })?;

    let inner = &sql[paren_start + 1..paren_end];

    // Parse named or positional arguments.
    let mut text_val = String::new();
    let mut chunk_size = 0usize;
    let mut overlap = 0usize;
    let mut strategy_str = String::from("character");

    // Split on commas, but respect quoted strings.
    let args = split_args_respecting_quotes(inner);

    if args.is_empty() {
        return Err(super::super::types::sqlstate_error(
            "42601",
            "CHUNK_TEXT requires at least text and chunk_size arguments",
        ));
    }

    // Detect named vs positional by checking if first arg contains `=>`.
    let is_named = args[0].contains("=>");

    if is_named {
        for arg in &args {
            if let Some((key, val)) = arg.split_once("=>") {
                let key = key.trim().to_lowercase();
                let val = val.trim().trim_matches('\'').trim_matches('"');
                match key.as_str() {
                    "text" => text_val = val.to_string(),
                    "chunk_size" => {
                        chunk_size = val.parse().map_err(|_| {
                            super::super::types::sqlstate_error(
                                "22023",
                                &format!("invalid chunk_size: {val}"),
                            )
                        })?;
                    }
                    "overlap" => {
                        overlap = val.parse().map_err(|_| {
                            super::super::types::sqlstate_error(
                                "22023",
                                &format!("invalid overlap: {val}"),
                            )
                        })?;
                    }
                    "strategy" => strategy_str = val.to_string(),
                    other => {
                        return Err(super::super::types::sqlstate_error(
                            "42601",
                            &format!("unknown CHUNK_TEXT parameter: {other}"),
                        ));
                    }
                }
            }
        }
    } else {
        // Positional: CHUNK_TEXT('text', chunk_size, overlap, 'strategy')
        if args.len() < 2 {
            return Err(super::super::types::sqlstate_error(
                "42601",
                "CHUNK_TEXT requires at least (text, chunk_size)",
            ));
        }
        text_val = args[0]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        chunk_size = args[1]
            .trim()
            .parse()
            .map_err(|_| super::super::types::sqlstate_error("22023", "invalid chunk_size"))?;
        if args.len() > 2 {
            overlap = args[2]
                .trim()
                .parse()
                .map_err(|_| super::super::types::sqlstate_error("22023", "invalid overlap"))?;
        }
        if args.len() > 3 {
            strategy_str = args[3]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
        }
    }

    if text_val.is_empty() {
        return Err(super::super::types::sqlstate_error(
            "42601",
            "CHUNK_TEXT: text argument is required",
        ));
    }
    if chunk_size == 0 {
        return Err(super::super::types::sqlstate_error(
            "42601",
            "CHUNK_TEXT: chunk_size must be > 0",
        ));
    }

    let strategy = nodedb_query::ChunkStrategy::parse(&strategy_str).ok_or_else(|| {
        super::super::types::sqlstate_error(
            "22023",
            &format!(
                "unknown strategy '{strategy_str}'; supported: character, sentence, paragraph"
            ),
        )
    })?;

    let chunks = nodedb_query::chunk_text(&text_val, chunk_size, overlap, strategy)
        .map_err(|e| super::super::types::sqlstate_error("22023", &e.to_string()))?;

    let schema = std::sync::Arc::new(vec![
        super::super::types::text_field("index"),
        super::super::types::text_field("start"),
        super::super::types::text_field("end"),
        super::super::types::text_field("text"),
    ]);

    let rows: Vec<_> = chunks
        .iter()
        .map(|c| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&c.index.to_string());
            let _ = encoder.encode_field(&c.start.to_string());
            let _ = encoder.encode_field(&c.end.to_string());
            let _ = encoder.encode_field(&c.text);
            Ok(encoder.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Split a comma-separated argument string, respecting single-quoted strings.
fn split_args_respecting_quotes(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                in_quote = false;
                current.push(ch);
            }
            ',' if !in_quote => {
                args.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
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
