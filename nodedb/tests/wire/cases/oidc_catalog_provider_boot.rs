// SPDX-License-Identifier: BUSL-1.1

//! A catalog OIDC provider authenticates on a server with no `[auth.jwt]`
//! section. The server builds its JWKS registry with no static provider, a
//! provider created at runtime registers through its catalog entry, and boot
//! registers it again after a restart.

use nodedb_test_support::jwks_fixture::{JwksFixture, now_secs};
use nodedb_test_support::native_harness::{do_handshake, send_request};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, NativeResponse, OpCode};

use crate::harness::TestServer;

const ISSUER: &str = "https://catalog-only-idp.example/";
const AUDIENCE: &str = "nodedb-catalog-only";

/// Authenticate a fresh native connection with an OIDC bearer `token`.
async fn oidc_auth(server: &TestServer, token: &str) -> NativeResponse {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.native_port);
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::OidcBearer {
                token: token.to_string(),
                provider: None,
            }),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catalog_only_oidc_provider_authenticates() {
    let fixture = JwksFixture::spawn().await;
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT oidc_catalog_only ID 42")
        .await
        .expect("CREATE TENANT");
    // The mapping names the built-in default database, id 0, which every
    // server has. Native auth refuses a session whose mapped database does
    // not exist (`3D000`).
    server
        .exec(&format!(
            "CREATE OIDC PROVIDER catalog_only_idp \
             ISSUER '{ISSUER}' \
             JWKS_URI '{}' \
             AUDIENCE '{AUDIENCE}' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0",
            fixture.jwks_url()
        ))
        .await
        .expect("CREATE OIDC PROVIDER");

    let now = now_secs();
    let token = fixture.mint(&serde_json::json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "catalog-only-user",
        "iat": now,
        "exp": now + 600,
    }));
    let auth = oidc_auth(&server, &token).await;
    assert_eq!(auth.status, ResponseStatus::Ok, "{auth:?}");

    // Boot registers the provider the catalog holds.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    let auth = oidc_auth(&server, &token).await;
    assert_eq!(auth.status, ResponseStatus::Ok, "after a restart: {auth:?}");

    // A dropped provider authenticates nothing.
    server
        .exec("DROP OIDC PROVIDER catalog_only_idp")
        .await
        .expect("DROP OIDC PROVIDER");
    let auth = oidc_auth(&server, &token).await;
    assert_ne!(
        auth.status,
        ResponseStatus::Ok,
        "a dropped provider: {auth:?}"
    );
}
