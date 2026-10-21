// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for OIDC provider DDL.

use nodedb::control::server::shared::ddl;
use nodedb::control::server::shared::ddl::result::DdlResult;
use nodedb::control::server::shared::session::DetachedTxnScope;
use nodedb_test_support::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state_with_catalog, readonly_user, superuser,
};

#[tokio::test]
async fn create_oidc_provider_persists_in_catalog() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER okta \
         ISSUER 'https://acme.okta.com' \
         JWKS_URI 'https://acme.okta.com/.well-known/jwks.json' \
         AUDIENCE 'nodedb' \
         TENANT 42",
    )
    .await;

    let stored = state
        .credentials
        .catalog()
        .get_oidc_provider("okta")
        .expect("catalog read must succeed")
        .expect("provider must exist after CREATE");
    assert_eq!(stored.issuer, "https://acme.okta.com");
    assert_eq!(stored.audience.as_deref(), Some("nodedb"));
    let encoded = sonic_rs::to_string(&stored).expect("provider must serialize");
    assert!(encoded.contains("\"tenant_id\":42"));
}

#[tokio::test]
async fn create_oidc_provider_requires_superuser() {
    let state = make_state_with_catalog();
    let su = superuser();
    let viewer = readonly_user();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    let error = ddl_err(
        &state,
        &viewer,
        "CREATE OIDC PROVIDER bad \
         ISSUER 'https://x.example/' \
         JWKS_URI 'https://x.example/jwks' \
         TENANT 42",
    )
    .await;
    assert!(error.contains("42501") || error.contains("permission denied"));
}

/// Every key fetch goes through the `[egress]` policy, so a JWKS URI the
/// policy refuses is refused at definition time.
#[tokio::test]
async fn create_oidc_provider_refuses_a_jwks_uri_outside_the_egress_policy() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    for uri in [
        "https://127.0.0.1/jwks",
        "http://169.254.169.254/latest/meta-data/",
        "http://keycloak.internal/jwks",
    ] {
        let error = ddl_err(
            &state,
            &su,
            &format!(
                "CREATE OIDC PROVIDER refused \
                 ISSUER 'https://refused.example/' \
                 JWKS_URI '{uri}' \
                 TENANT 42"
            ),
        )
        .await;
        assert!(error.contains("[egress]"), "{uri}: {error}");
    }
    assert!(
        state
            .credentials
            .catalog()
            .get_oidc_provider("refused")
            .expect("catalog read must succeed")
            .is_none()
    );
}

/// A claim mapping that names a database that does not exist is refused with
/// 3D000 by `CREATE OIDC PROVIDER` and by `ALTER ... SET CLAIM MAPPING`. A
/// mapping to an existing database is accepted.
#[tokio::test]
async fn a_claim_mapping_to_a_missing_database_is_refused() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    for mapping in [
        "SET DEFAULT_DATABASE = 4242",
        "SET DEFAULT_DATABASE = 0 ADD DATABASES [4243]",
    ] {
        let error = ddl_err(
            &state,
            &su,
            &format!(
                "CREATE OIDC PROVIDER missing_db \
                 ISSUER 'https://missing-db.example/' \
                 JWKS_URI 'https://missing-db.example/jwks' \
                 TENANT 42 \
                 CLAIM MAPPING WHEN sub = '*' {mapping}"
            ),
        )
        .await;
        assert!(error.contains("3D000"), "{mapping}: {error}");
        assert!(error.contains("424"), "{mapping}: {error}");
    }
    assert!(
        state
            .credentials
            .catalog()
            .get_oidc_provider("missing_db")
            .expect("catalog read must succeed")
            .is_none()
    );

    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER missing_db \
         ISSUER 'https://missing-db.example/' \
         JWKS_URI 'https://missing-db.example/jwks' \
         TENANT 42 \
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0",
    )
    .await;
    let error = ddl_err(
        &state,
        &su,
        "ALTER OIDC PROVIDER missing_db SET CLAIM MAPPING \
         WHEN sub = '*' SET DEFAULT_DATABASE = 4244",
    )
    .await;
    assert!(error.contains("3D000") && error.contains("4244"), "{error}");
    let stored = state
        .credentials
        .catalog()
        .get_oidc_provider("missing_db")
        .expect("catalog read must succeed")
        .expect("the provider exists");
    assert_eq!(stored.claim_mapping[0].default_database, Some(0));
}

#[tokio::test]
async fn create_oidc_provider_rejects_unknown_tenant() {
    let state = make_state_with_catalog();
    let su = superuser();
    let error = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER unknown_tenant \
         ISSUER 'https://unknown-tenant.example/' \
         JWKS_URI 'https://unknown-tenant.example/jwks' \
         TENANT 999",
    )
    .await;
    assert!(error.contains("does not exist") || error.contains("unknown tenant"));
}

#[tokio::test]
async fn same_issuer_allows_distinct_nonempty_audiences_per_tenant() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT alpha ID 42").await;
    ddl_ok(&state, &su, "CREATE TENANT beta ID 43").await;
    ddl_ok(&state, &su, "CREATE TENANT gamma ID 44").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER alpha_idp \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'alpha-api' \
         TENANT 42",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER beta_idp \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'beta-api' \
         TENANT 43",
    )
    .await;

    let missing_audience = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER no_aud_route \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         TENANT 44",
    )
    .await;
    assert!(missing_audience.to_lowercase().contains("audience"));

    let duplicate_route = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER duplicate_route \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'alpha-api' \
         TENANT 43",
    )
    .await;
    assert!(duplicate_route.contains("42710") || duplicate_route.contains("already exists"));
}

#[tokio::test]
async fn drop_oidc_provider_removes_record() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT auth0_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER auth0 \
         ISSUER 'https://x.auth0.com' \
         JWKS_URI 'https://x.auth0.com/.well-known/jwks.json' \
         TENANT 42",
    )
    .await;
    ddl_ok(&state, &su, "DROP OIDC PROVIDER auth0").await;
    let stored = state
        .credentials
        .catalog()
        .get_oidc_provider("auth0")
        .expect("catalog read must succeed");
    assert!(stored.is_none());
}

#[tokio::test]
async fn drop_oidc_provider_unknown_returns_not_found() {
    let state = make_state_with_catalog();
    let error = ddl_err(&state, &superuser(), "DROP OIDC PROVIDER does_not_exist").await;
    assert!(error.contains("42704") || error.contains("does not exist"));
}

#[tokio::test]
async fn drop_oidc_provider_if_exists_unknown_succeeds() {
    let state = make_state_with_catalog();
    ddl_ok(
        &state,
        &superuser(),
        "DROP OIDC PROVIDER IF EXISTS does_not_exist",
    )
    .await;
}

#[tokio::test]
async fn show_oidc_providers_lists_registered() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT p1_tenant ID 42").await;
    ddl_ok(&state, &su, "CREATE TENANT p2_tenant ID 43").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p1 \
         ISSUER 'https://p1.example/' \
         JWKS_URI 'https://p1.example/jwks' \
         TENANT 42",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p2 \
         ISSUER 'https://p2.example/' \
         JWKS_URI 'https://p2.example/jwks' \
         TENANT 43",
    )
    .await;
    ddl_ok(&state, &su, "SHOW OIDC PROVIDERS").await;
}

#[tokio::test]
async fn show_oidc_providers_exposes_tenant_binding() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER acme_idp \
         ISSUER 'https://acme.example/' \
         JWKS_URI 'https://acme.example/jwks' \
         TENANT 42",
    )
    .await;

    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        &state,
        &su,
        "SHOW OIDC PROVIDERS",
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .expect("SHOW OIDC PROVIDERS must be recognized")
    .expect("SHOW OIDC PROVIDERS must succeed");

    match &result[0] {
        DdlResult::Rows(rows) => {
            assert!(rows.columns.iter().any(|column| column == "tenant_id"));
            assert_eq!(
                rows.rows[0]
                    .get("tenant_id")
                    .and_then(nodedb_types::Value::as_str),
                Some("42")
            );
        }
        other => panic!("expected Rows response, got: {other:?}"),
    }
}
