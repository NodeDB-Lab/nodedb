use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

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
        return Some(super::backup::backup_tenant(state, identity, &parts));
    }
    if upper.starts_with("RESTORE TENANT ") {
        if upper.ends_with(" DRY RUN") || upper.ends_with(" DRYRUN") {
            return Some(super::backup::restore_tenant_dry_run(
                state, identity, &parts,
            ));
        }
        return Some(super::backup::restore_tenant(state, identity, &parts));
    }

    // Collection management.
    if upper.starts_with("CREATE COLLECTION ") {
        return Some(super::collection::create_collection(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP COLLECTION ") {
        return Some(super::collection::drop_collection(state, identity, &parts));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(super::collection::show_collections(state, identity));
    }
    if upper.starts_with("CREATE INDEX ") {
        return Some(super::collection::create_index(state, identity, &parts));
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

    // COPY FROM file.
    if upper.starts_with("COPY ") && upper.contains(" FROM ") {
        return Some(super::bulk::copy_from(state, identity, &parts).await);
    }

    // INSERT INTO — intercept for schemaless collections (DataFusion rejects
    // columns not in the Arrow schema, but NodeDB collections are document stores).
    if upper.starts_with("INSERT INTO ")
        && upper.contains("VALUES")
        && let Some(result) = super::collection::insert_document(state, identity, sql).await
    {
        return Some(result);
    }

    None
}
