// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures for `pgwire_auth_*` integration tests.
//!
//! Each split test file needs: a `SharedState` that serves DDL, two canonical
//! identities (superuser + readonly), and two DDL runners (expect ok /
//! expect err). Keeping them here avoids copy-paste drift across files.
//!
//! A DDL proposes through the metadata group, so every state here is served
//! by a booted one-node cluster ([`BootedState`]).

#![allow(dead_code)]

use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
use nodedb::control::server::pgwire::ddl_encode;
use nodedb::control::server::pgwire::types::wire_type::TEXT_RESULTS;
use nodedb::control::server::shared::ddl;
use nodedb::control::server::shared::session::DetachedTxnScope;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;

use crate::booted_state::BootOptions;
pub use crate::booted_state::BootedState;

/// A booted state whose catalog holds no database yet.
pub fn make_state() -> BootedState {
    BootedState::boot(BootOptions {
        default_database: false,
        ..BootOptions::default()
    })
}

/// A booted state whose catalog holds the built-in `default` database. Use
/// this for DDL tests that resolve database names (e.g.
/// `FOR DATABASE default`).
pub fn make_state_with_catalog() -> BootedState {
    BootedState::boot(BootOptions::default())
}

/// [`make_state_with_catalog`], with `configure` applied to the state before
/// the gateway install. The installed gateway holds a back-reference, so no
/// `Arc::get_mut` succeeds after this returns: set every field here.
pub fn make_state_with_catalog_configured(
    configure: impl FnOnce(&mut SharedState) + Send + 'static,
) -> BootedState {
    BootedState::boot(BootOptions {
        default_database: true,
        configure: Box::new(configure),
        ..BootOptions::default()
    })
}

/// Superuser identity for DDL tests.
pub fn superuser() -> AuthenticatedIdentity {
    let store = nodedb::control::security::credential::store::CredentialStore::new()
        .expect("create test credential store");
    store
        .bootstrap_superuser("nodedb", "test-only-password")
        .expect("bootstrap test superuser");
    store
        .to_identity("nodedb", AuthMethod::Trust)
        .expect("build catalog-authenticated test superuser")
}

/// Readonly identity for permission tests.
pub fn readonly_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        99,
        "viewer",
        TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::ReadOnly],
        None,
        DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT,]),
    )
}

/// Run DDL, expect success.
pub async fn ddl_ok(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) {
    ddl_ok_in(state, identity, sql, nodedb_types::id::DatabaseId::DEFAULT).await;
}

/// Run DDL with `database_id` as the session database, expect success.
pub async fn ddl_ok_in(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: nodedb_types::id::DatabaseId,
) {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(state, identity, sql, database_id, &scope.ctx())
        .await
        .map(|result| ddl_encode::ddl_results_to_pgwire(result, &TEXT_RESULTS));
    let Some(result) = result else {
        panic!("DDL not recognized: {sql}");
    };
    result.unwrap_or_else(|e| panic!("DDL failed: {sql}: {e}"));
}

/// Run DDL, expect error; return the error string for assertions.
pub async fn ddl_err(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) -> String {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        state,
        identity,
        sql,
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .map(|result| ddl_encode::ddl_results_to_pgwire(result, &TEXT_RESULTS));
    let Some(result) = result else {
        panic!("DDL not recognized: {sql}");
    };
    let err = result.expect_err("DDL must fail");
    err.to_string()
}

/// Run DDL and return the result without panicking on either branch. Useful
/// when the gate test only cares whether the privilege check fired (look for
/// "42501" in the error string) and the underlying handler may legitimately
/// succeed or fail with a non-privilege error depending on cluster state.
pub async fn try_ddl(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<(), String> {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        state,
        identity,
        sql,
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .map(|result| ddl_encode::ddl_results_to_pgwire(result, &TEXT_RESULTS));
    let result = result.expect("DDL not recognized");
    result.map(|_| ()).map_err(|e| e.to_string())
}

/// Run DDL as a readonly identity and assert it is denied with `permission denied`.
pub async fn assert_readonly_denied(state: &SharedState, sql: &str) {
    let viewer = readonly_user();
    let err = ddl_err(state, &viewer, sql).await;
    assert!(err.contains("permission denied"), "{err}");
}

/// Cluster-admin identity (no implicit RLS bypass, no cross-DB data access).
pub fn cluster_admin_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        100,
        "cluster_admin",
        nodedb::types::TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::ClusterAdmin],
        None,
        DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT,]),
    )
}

/// Database-owner identity for `db_id`.
pub fn database_owner_user(db_id: nodedb_types::id::DatabaseId) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        101,
        "db_owner",
        nodedb::types::TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::DatabaseOwner(db_id)],
        None,
        DatabaseSet::Some(smallvec::smallvec![db_id]),
    )
}

/// Assert that the audit log contains at least one entry with `event` and `db_id`.
pub fn assert_audit_has(
    state: &SharedState,
    event: nodedb::control::security::audit::AuditEvent,
    db_id: Option<nodedb_types::id::DatabaseId>,
) {
    let log = state.audit.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        log.all()
            .iter()
            .any(|e| e.event == event && e.database_id == db_id),
        "expected audit event {event:?} for db {db_id:?}, got {:?}",
        log.all()
            .iter()
            .map(|e| (e.event.clone(), e.database_id))
            .collect::<Vec<_>>()
    );
}
