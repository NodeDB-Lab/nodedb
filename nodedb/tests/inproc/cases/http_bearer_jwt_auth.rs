// SPDX-License-Identifier: BUSL-1.1

//! Bearer-JWT authentication on the HTTP API, exercised over a real socket.
//!
//! The HTTP bearer route runs on Tokio worker threads, so JWKS verification
//! has to be awaited rather than blocked on. Driving it with a nested
//! `block_on` aborts the request task, and the client observes a truncated
//! connection instead of any HTTP status — a failure mode no in-process call
//! to the resolver can reproduce.
//!
//! Both tests therefore issue a real request carrying
//! `Authorization: Bearer <jwt>` and assert on the response:
//!
//! 1. A token minted by the node's configured provider authenticates and the
//!    route does its work (200 + a session handle).
//! 2. A well-formed token the registry cannot verify is refused with 401 —
//!    still a definite HTTP response, never a dropped connection.

use std::sync::Arc;

use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb_test_support::jwks_fixture::{JwksFixture, loopback_egress, now_secs};
use nodedb_test_support::pgwire_harness::TestServer;

/// Tenant the test provider binds its identities to.
const TENANT: u64 = 1;
const ISSUER: &str = "https://http-bearer-jwt-auth.example/";
const AUDIENCE: &str = "nodedb-http";

/// A JWKS fixture plus the node whose `[auth.jwt]` section points at it.
struct HttpJwtFixture {
    server: TestServer,
    jwks: JwksFixture,
}

impl HttpJwtFixture {
    async fn start() -> Self {
        let jwks = JwksFixture::spawn().await;
        let registry = JwksRegistry::init(
            jwks.auth_config(ISSUER, AUDIENCE, TENANT),
            loopback_egress(),
        )
        .await
        .expect("test JWKS registry must initialize");
        let server = TestServer::start_with_jwks(Arc::new(registry)).await;
        Self { server, jwks }
    }

    /// `POST` here resolves the bearer token and, on success, mints a session
    /// handle — a route whose whole body is auth plus a cheap side effect, so
    /// the assertion is about authentication and nothing else.
    fn session_url(&self) -> String {
        format!("http://127.0.0.1:{}/v1/auth/session", self.server.http_port)
    }
}

/// The standard route and time claims, signed by `signer`.
///
/// Taking the signer as a parameter lets a test mint a structurally
/// identical token against a key the node's provider does not publish.
fn mint_token(signer: &JwksFixture, subject: &str) -> String {
    let now = now_secs();
    signer.mint(&serde_json::json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": subject,
        "roles": ["readwrite"],
        "iat": now,
        "exp": now + 600,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_bearer_jwt_authenticates_and_the_route_responds() {
    let fixture = HttpJwtFixture::start().await;
    let token = mint_token(&fixture.jwks, "http-bearer-user");

    let response = reqwest::Client::new()
        .post(fixture.session_url())
        .bearer_auth(token)
        .send()
        .await
        .expect("a JWT bearer request must produce an HTTP response, not a dropped connection");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "a token minted by the node's configured provider must authenticate"
    );

    let body: serde_json::Value = response
        .json()
        .await
        .expect("the session route must answer with a JSON body");
    let session_id = body
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .expect("an authenticated caller must receive a session handle");
    assert!(
        !session_id.is_empty(),
        "the session handle must be a non-empty opaque id, got {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_bearer_jwt_from_an_unknown_key_is_refused_with_a_status() {
    let fixture = HttpJwtFixture::start().await;
    // A second fixture publishes a different key pair, so this token is
    // well-formed (and reaches JWKS validation on its two dots) but carries a
    // signature the node's provider cannot verify.
    let foreign = JwksFixture::spawn().await;
    let token = mint_token(&foreign, "http-bearer-impostor");

    let response = reqwest::Client::new()
        .post(fixture.session_url())
        .bearer_auth(token)
        .send()
        .await
        .expect("a refused JWT must still produce an HTTP response, not a dropped connection");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "a token the registry cannot verify must be refused, and must not fall \
         through to the node's trust identity"
    );
}
