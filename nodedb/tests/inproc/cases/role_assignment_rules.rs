// SPDX-License-Identifier: BUSL-1.1

//! A user may hold only a built-in role or a custom role defined in its
//! tenant, on every statement that assigns one. A custom role that a user
//! holds is not dropped. Both refusals name the role and carry PostgreSQL's
//! SQLSTATE: 42704 for an undefined role, 2BP01 for a role still in use.

use nodedb::control::security::identity::Role;
use nodedb_test_support::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};

fn assert_undefined(error: &str, role: &str) {
    assert!(
        error.contains("42704") && error.contains(role),
        "expected 42704 naming role '{role}', got: {error}"
    );
}

#[tokio::test]
async fn every_role_assignment_refuses_an_undefined_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER rita WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;

    for sql in [
        "CREATE USER ghost WITH PASSWORD 'pass' ROLE read_write",
        "ALTER USER rita SET ROLE read_write",
        "GRANT ROLE read_write TO rita",
        "GRANT read_write TO rita",
        "CREATE SERVICE ACCOUNT ghost_svc ROLE read_write",
    ] {
        assert_undefined(&ddl_err(&state, &su, sql).await, "read_write");
    }

    assert!(state.credentials.get_user("ghost").is_none());
    assert!(state.credentials.get_user("ghost_svc").is_none());
    let rita = state.credentials.get_user("rita").expect("rita");
    assert_eq!(
        rita.roles,
        vec![Role::ReadOnly],
        "a refused statement changed rita"
    );
}

#[tokio::test]
async fn every_role_assignment_accepts_a_defined_custom_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE analyst").await;
    let analyst = Role::Custom("analyst".into());

    ddl_ok(
        &state,
        &su,
        "CREATE USER ana WITH PASSWORD 'pass' ROLE analyst",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE USER ben WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "ALTER USER ben SET ROLE analyst").await;
    ddl_ok(
        &state,
        &su,
        "CREATE USER cat WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE analyst TO cat").await;
    ddl_ok(&state, &su, "CREATE SERVICE ACCOUNT ana_svc ROLE analyst").await;

    for user in ["ana", "ben", "cat", "ana_svc"] {
        let record = state
            .credentials
            .get_user(user)
            .unwrap_or_else(|| panic!("{user} exists"));
        assert!(
            record.roles.contains(&analyst),
            "{user}: {:?}",
            record.roles
        );
    }
}

#[tokio::test]
async fn a_custom_role_of_another_tenant_is_undefined_here() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE home_only").await;

    let error = ddl_err(
        &state,
        &su,
        "CREATE USER stray WITH PASSWORD 'pass' ROLE home_only TENANT 4242",
    )
    .await;
    assert_undefined(&error, "home_only");
    assert!(state.credentials.get_user("stray").is_none());
}

#[tokio::test]
async fn a_held_role_is_not_dropped_until_no_user_holds_it() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE reviewer").await;
    ddl_ok(
        &state,
        &su,
        "CREATE USER rex WITH PASSWORD 'pass' ROLE reviewer",
    )
    .await;

    let error = ddl_err(&state, &su, "DROP ROLE reviewer").await;
    assert!(
        error.contains("2BP01") && error.contains("rex"),
        "expected 2BP01 naming the holder, got: {error}"
    );
    assert!(state.roles.get_role("reviewer").is_some());

    ddl_ok(&state, &su, "REVOKE reviewer FROM rex").await;
    ddl_ok(&state, &su, "DROP ROLE reviewer").await;
    assert!(state.roles.get_role("reviewer").is_none());
}

#[tokio::test]
async fn a_role_another_role_inherits_from_is_not_dropped() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE senior").await;
    ddl_ok(&state, &su, "CREATE ROLE junior INHERIT senior").await;

    let error = ddl_err(&state, &su, "DROP ROLE senior").await;
    assert!(
        error.contains("2BP01") && error.contains("junior"),
        "expected 2BP01 naming the child role, got: {error}"
    );
    assert!(state.roles.get_role("senior").is_some());
}

/// An OIDC claim mapping may add only roles its provider's tenant defines:
/// a login mapped to an undefined role would hold nothing.
#[tokio::test]
async fn an_oidc_claim_mapping_refuses_an_undefined_role() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT mapped_roles ID 43").await;

    let error = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER ghost_mapping \
         ISSUER 'https://ghost-idp.example/' \
         JWKS_URI 'https://ghost-idp.example/jwks' \
         AUDIENCE 'nodedb-api' \
         TENANT 43 \
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['ghost_role']",
    )
    .await;
    assert_undefined(&error, "ghost_role");
    assert!(
        state
            .credentials
            .catalog()
            .get_oidc_provider("ghost_mapping")
            .expect("catalog read")
            .is_none(),
        "a refused provider must not be stored"
    );
}
