use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Try to handle a SQL statement as a Control Plane DDL command.
///
/// These execute directly on the Control Plane without going through
/// DataFusion or the Data Plane. Returns `None` if not recognized.
pub fn dispatch(
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

    // Tenant management.
    if upper.starts_with("CREATE TENANT ") {
        return Some(super::tenant::create_tenant(state, identity, &parts));
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
    if upper.starts_with("DROP ROLE ") {
        return Some(super::role::drop_role(state, identity, &parts));
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
    if upper.starts_with("SHOW GRANTS") {
        return Some(super::inspect::show_grants(state, identity, &parts));
    }

    None
}
