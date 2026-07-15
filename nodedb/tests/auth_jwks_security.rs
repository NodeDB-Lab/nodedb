// SPDX-License-Identifier: BUSL-1.1

//! Security tests for JWKS / JWT provider configuration and JWKS fetch path.
//!
//! Tests go through existing public entry points (`ServerConfig::from_file`,
//! `JwksRegistry::init` + `validate`, `fetch_and_cache`) and assert observable
//! behaviour. They do not name new helper functions so the fix is free to
//! express the invariants however it prefers (TOML deserialize hook, custom
//! reqwest redirect policy, dedicated validator, etc.) without churning the
//! tests.
//!
//! Shared invariant under test: security-sensitive JWT/JWKS config must be
//! validated fail-closed at load time, issuer routing must never fall back
//! to a single provider without a matching `iss`, and URL-typed config
//! fields must not flow unvalidated into an HTTP client (SSRF surface).

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use base64::Engine;
use nodedb::config::ServerConfig;
use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use nodedb::control::security::jwks::cache::JwksCache;
use nodedb::control::security::jwks::fetch::fetch_and_cache;
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::jwks::url::JwksPolicy;
use nodedb::control::security::jwt::JwtError;

// ── helpers ────────────────────────────────────────────────────────────

/// Bind a TCP listener that counts inbound connections but never replies.
/// Returned url is http://… so it additionally trips the https-only rule.
fn spawn_counting_listener() -> (String, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let c = counter.clone();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if let Ok((_s, _)) = listener.accept() {
                c.fetch_add(1, Ordering::SeqCst);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    (format!("http://{addr}/jwks.json"), counter)
}

/// Bind a listener that endlessly 302-redirects to itself over https-scheme
/// target (so https validation per-hop also applies).
async fn spawn_redirect_loop() -> (String, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let c = counter.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((mut s, _)) = listener.accept().await {
                c.fetch_add(1, Ordering::SeqCst);
                use tokio::io::AsyncWriteExt;
                let body = format!(
                    "HTTP/1.1 302 Found\r\nLocation: https://{addr}/next\r\nContent-Length: 0\r\n\r\n"
                );
                let _ = s.write_all(body.as_bytes()).await;
            }
        }
    });
    (format!("https://{addr}/jwks.json"), counter)
}

/// Spin a minimal HTTP server that returns a fixed body for every request.
async fn spawn_static_body(body: impl Into<String>) -> String {
    let body = body.into();
    let listener = tokio::net::TcpListener::bind("[::]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut s, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes()).await;
            }
        }
    });
    format!("http://localhost:{}/jwks.json", addr.port())
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Forge an unsigned JWT — header + payload + garbage sig. We are testing
/// the routing / issuer-validation path that runs BEFORE signature verify;
/// the sig bytes never matter for these assertions.
fn forged_token(iss: &str) -> String {
    let header = br#"{"alg":"RS256","kid":"k1"}"#;
    let payload = format!(
        r#"{{"iss":"{iss}","sub":"attacker","tenant_id":0,"is_superuser":true,"exp":9999999999}}"#
    );
    format!(
        "{}.{}.{}",
        b64(header),
        b64(payload.as_bytes()),
        b64(b"sig")
    )
}

fn single_provider_cfg(jwks_url: &str, issuer: &str) -> JwtAuthConfig {
    JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        providers: vec![JwtProviderConfig {
            name: "prod".into(),
            jwks_url: jwks_url.into(),
            issuer: issuer.into(),
            audience: String::new(),
            tenant_id: 1,
        }],
        ..JwtAuthConfig::default()
    }
}

fn signed_jwt_fixture(issuer: &str, audience: &str, claimed_tenant_id: u64) -> (String, String) {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;

    let mut rng = rsa::rand_core::OsRng;
    let private_key = rsa::RsaPrivateKey::new(&mut rng, 1024).unwrap();
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let jwks = format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"tenant-binding","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
        b64(&public_key.n().to_bytes_be()),
        b64(&public_key.e().to_bytes_be()),
    );
    let header = b64(br#"{"alg":"RS256","kid":"tenant-binding"}"#);
    let payload = b64(
        format!(
            r#"{{"iss":"{issuer}","aud":"{audience}","sub":"alice","tenant_id":{claimed_tenant_id},"roles":["readwrite"],"exp":9999999999,"user_id":42}}"#
        )
        .as_bytes(),
    );
    let signing_input = format!("{header}.{payload}");
    let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
    let signature: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
    let token = format!("{signing_input}.{}", b64(&signature.to_bytes()));

    (jwks, token)
}

fn local_jwks_config(provider_tables: &str) -> JwtAuthConfig {
    toml::from_str(&format!(
        r#"
allow_http_jwks = true
allow_jwks_hosts = ["localhost"]
allow_jwks_cidrs = ["127.0.0.0/8", "::1/128"]
{provider_tables}
"#
    ))
    .expect("JWT provider fixture must deserialize")
}

// ── 1. Issuer bypass ───────────────────────────────────────────────────

#[tokio::test]
async fn single_provider_does_not_accept_mismatched_issuer() {
    // JWKS server returns an empty key set — init succeeds but any token
    // will fail to find a matching kid. Crucially, the registry must
    // reject the token at the ISSUER stage (InvalidIssuer) rather than
    // fall through to sig/kid lookup on the sole provider.
    let jwks_url = spawn_static_body(r#"{"keys":[]}"#).await;
    let cfg = single_provider_cfg(&jwks_url, "https://auth.example.com/");
    let registry = JwksRegistry::init(cfg)
        .await
        .expect("valid static provider configuration must initialise");

    let token = forged_token("https://attacker-tenant.auth0.com/");
    let err = registry.validate(&token).await.expect_err("must reject");
    assert!(
        matches!(err, JwtError::InvalidIssuer),
        "mismatched iss must be InvalidIssuer (not fall through to sig check), got {err:?}"
    );
}

#[tokio::test]
async fn single_provider_with_empty_configured_issuer_is_rejected_at_init() {
    // Direct registry construction must fail closed too; callers are not
    // required to have loaded the configuration through ServerConfig first.
    let jwks_url = spawn_static_body(r#"{"keys":[]}"#).await;
    let cfg = single_provider_cfg(&jwks_url, "");
    assert!(
        JwksRegistry::init(cfg).await.is_err(),
        "empty-issuer provider must be rejected during registry initialisation"
    );
}

// ── 2. Config rejection at the real loader entry point ────────────────

/// Build a valid ServerConfig TOML with a single JWT provider whose jwks_url
/// and issuer are parameterised. Produces a structurally-valid config so the
/// only reason from_file can fail is the JWT validation we're testing.
fn config_toml_with_provider(
    jwks_url: &str,
    issuer: &str,
    audience: &str,
    tenant_id: u64,
) -> String {
    format!(
        r#"
[server]
data_dir         = "/tmp/nodedb-security-test"
data_plane_cores = 1
memory_limit     = 1073741824

[engines]
vector_budget_fraction     = 0.30
sparse_budget_fraction     = 0.15
crdt_budget_fraction       = 0.10
timeseries_budget_fraction = 0.10
query_budget_fraction      = 0.20

[auth]
mode                     = "password"
superuser_name           = "nodedb"
superuser_password       = "test-password"
min_password_length      = 8
max_failed_logins        = 5
lockout_duration_secs    = 300
idle_timeout_secs        = 3600
max_connections_per_user = 0
password_expiry_days     = 0
audit_retention_days     = 0

[auth.jwt]

[[auth.jwt.providers]]
name     = "prod"
jwks_url = "{jwks_url}"
issuer   = "{issuer}"
audience = "{audience}"
tenant_id = {tenant_id}
"#
    )
}

/// Sanity check: the template parses today with a known-good provider. Any
/// test rejection in sibling tests therefore points at the JWT-validation
/// invariant, not at a structural TOML mistake.
#[test]
fn config_template_is_structurally_valid() {
    let toml = config_toml_with_provider(
        "https://auth.example.com/.well-known/jwks.json",
        "https://auth.example.com/",
        "",
        1,
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    ServerConfig::from_file(f.path()).expect("template must parse");
}

#[test]
fn server_config_rejects_jwt_provider_with_empty_issuer() {
    // After the fix, loading a config with a JWT provider that omits
    // `issuer` must return Err — fail-closed at startup, not
    // silently-skip at validate time.
    let toml = config_toml_with_provider(
        "https://auth.example.com/.well-known/jwks.json",
        "", // empty issuer
        "",
        1,
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "config with empty-issuer JWT provider must be rejected at load"
    );
}

#[test]
fn server_config_rejects_http_jwks_url() {
    let toml = config_toml_with_provider(
        "http://auth.example.com/.well-known/jwks.json",
        "https://auth.example.com/",
        "",
        1,
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "http:// JWKS URL must be rejected at load"
    );
}

#[test]
fn server_config_rejects_ip_literal_jwks_host() {
    for host in [
        "169.254.169.254",
        "127.0.0.1",
        "10.0.0.5",
        "192.168.1.10",
        "172.16.0.1",
        "[::1]",
    ] {
        let toml = config_toml_with_provider(
            &format!("https://{host}/.well-known/jwks.json"),
            "https://auth.example.com/",
            "",
            1,
        );
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(toml.as_bytes()).unwrap();
        assert!(
            ServerConfig::from_file(f.path()).is_err(),
            "IP-literal host in JWKS URL must be rejected: {host}"
        );
    }
}

#[test]
fn server_config_rejects_jwt_provider_without_tenant_binding() {
    let mut toml = config_toml_with_provider(
        "https://auth.example.com/.well-known/jwks.json",
        "https://auth.example.com/",
        "",
        1,
    );
    let tenant_binding = "tenant_id = 1\n";
    let binding_offset = toml
        .find(tenant_binding)
        .expect("test configuration must include its tenant binding");
    toml.replace_range(binding_offset..binding_offset + tenant_binding.len(), "");
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "a static JWT provider without an explicit tenant binding must be rejected at load"
    );
}

#[test]
fn server_config_rejects_duplicate_static_provider_issuer_audience_routes() {
    let mut toml = config_toml_with_provider(
        "https://auth.example.com/.well-known/jwks.json",
        "https://auth.example.com/",
        "nodedb-api",
        1,
    );
    toml.push_str(
        r#"
[[auth.jwt.providers]]
name      = "duplicate-route"
jwks_url  = "https://auth.example.com/other-jwks.json"
issuer    = "https://auth.example.com/"
audience  = "nodedb-api"
tenant_id = 2
"#,
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "two static providers must not claim the same issuer and audience route"
    );
}

#[test]
fn server_config_rejects_duplicate_static_provider_names_with_distinct_routes() {
    let mut toml = config_toml_with_provider(
        "https://auth.example.com/.well-known/jwks.json",
        "https://auth.example.com/",
        "tenant-a-api",
        1,
    );
    toml.push_str(
        r#"
[[auth.jwt.providers]]
name      = "prod"
jwks_url  = "https://other-auth.example.com/.well-known/jwks.json"
issuer    = "https://other-auth.example.com/"
audience  = "tenant-b-api"
tenant_id = 2
"#,
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "static provider names must be unique even when issuer/audience routes differ"
    );
}

#[tokio::test]
async fn registry_init_rejects_duplicate_static_provider_issuer_audience_routes() {
    let config = JwtAuthConfig {
        providers: vec![
            JwtProviderConfig {
                name: "tenant-a".into(),
                jwks_url: "https://auth.example.com/a-jwks.json".into(),
                issuer: "https://auth.example.com/".into(),
                audience: "nodedb-api".into(),
                tenant_id: 1,
            },
            JwtProviderConfig {
                name: "tenant-b".into(),
                jwks_url: "https://auth.example.com/b-jwks.json".into(),
                issuer: "https://auth.example.com/".into(),
                audience: "nodedb-api".into(),
                tenant_id: 2,
            },
        ],
        ..JwtAuthConfig::default()
    };

    assert!(
        JwksRegistry::init(config).await.is_err(),
        "direct registry initialisation must reject duplicate issuer/audience routes"
    );
}

#[tokio::test]
async fn same_issuer_distinct_audiences_and_tenant_bindings_are_routed_independently() {
    let issuer = "https://issuer.example.com/";
    let (jwks, token) = signed_jwt_fixture(issuer, "tenant-b-api", 999);
    let jwks_url = spawn_static_body(jwks).await;
    let registry = JwksRegistry::init(local_jwks_config(&format!(
        r#"
[[providers]]
name = "tenant-a"
jwks_url = "{jwks_url}"
issuer = "{issuer}"
audience = "tenant-a-api"
tenant_id = 101

[[providers]]
name = "tenant-b"
jwks_url = "{jwks_url}"
issuer = "{issuer}"
audience = "tenant-b-api"
tenant_id = 202
"#
    )))
    .await
    .expect("valid static provider configuration must initialise");

    let identity = registry
        .validate(&token)
        .await
        .expect("the issuer/audience route for tenant-b must be accepted");
    assert_eq!(identity.tenant_id.as_u64(), 202);
}

#[tokio::test]
async fn static_provider_tenant_binding_overrides_signed_tenant_claim() {
    let issuer = "https://issuer.example.com/";
    let (jwks, token) = signed_jwt_fixture(issuer, "nodedb-api", 999);
    let jwks_url = spawn_static_body(jwks).await;
    let registry = JwksRegistry::init(local_jwks_config(&format!(
        r#"
[[providers]]
name = "bound-provider"
jwks_url = "{jwks_url}"
issuer = "{issuer}"
audience = "nodedb-api"
tenant_id = 42
"#
    )))
    .await
    .expect("valid static provider configuration must initialise");

    let identity = registry.validate(&token).await.expect("JWT must validate");
    assert_eq!(
        identity.tenant_id.as_u64(),
        42,
        "the provider binding, not the signed tenant_id claim, determines the identity tenant"
    );
}

// ── 2b. Allow-list relaxations ───────────────────────────────────────

#[test]
fn config_accepts_http_jwks_when_allow_http_and_host_listed() {
    // Self-hosted dev / in-cluster IdP: an explicit allow-list for the
    // hostname unlocks http:// for THAT host only.
    let toml = r#"
[server]
data_dir         = "/tmp/nodedb-security-test"
data_plane_cores = 1
memory_limit     = 1073741824

[engines]
vector_budget_fraction     = 0.30
sparse_budget_fraction     = 0.15
crdt_budget_fraction       = 0.10
timeseries_budget_fraction = 0.10
query_budget_fraction      = 0.20

[auth]
mode                     = "password"
superuser_name           = "nodedb"
superuser_password       = "test-password"
min_password_length      = 8
max_failed_logins        = 5
lockout_duration_secs    = 300
idle_timeout_secs        = 3600
max_connections_per_user = 0
password_expiry_days     = 0
audit_retention_days     = 0

[auth.jwt]
allow_http_jwks  = true
allow_jwks_hosts = ["keycloak.internal"]
allow_jwks_cidrs = ["10.42.0.0/16"]

[[auth.jwt.providers]]
name     = "prod"
jwks_url = "http://keycloak.internal/jwks.json"
issuer   = "https://auth.example.com/"
tenant_id = 1
"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    ServerConfig::from_file(f.path()).expect("allow-listed http host must pass config validation");
}

#[test]
fn config_rejects_http_jwks_for_non_allowlisted_host_even_when_allow_http() {
    // allow_http_jwks is scoped — a URL whose host isn't in the list
    // still fails. This is the property that keeps the relaxation narrow.
    let toml = r#"
[server]
data_dir         = "/tmp/nodedb-security-test"
data_plane_cores = 1
memory_limit     = 1073741824

[engines]
vector_budget_fraction     = 0.30
sparse_budget_fraction     = 0.15
crdt_budget_fraction       = 0.10
timeseries_budget_fraction = 0.10
query_budget_fraction      = 0.20

[auth]
mode                     = "password"
superuser_name           = "nodedb"
superuser_password       = "test-password"
min_password_length      = 8
max_failed_logins        = 5
lockout_duration_secs    = 300
idle_timeout_secs        = 3600
max_connections_per_user = 0
password_expiry_days     = 0
audit_retention_days     = 0

[auth.jwt]
allow_http_jwks  = true
allow_jwks_hosts = ["keycloak.internal"]

[[auth.jwt.providers]]
name     = "prod"
jwks_url = "http://evil.example.com/jwks.json"
issuer   = "https://auth.example.com/"
tenant_id = 1
"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "http:// must stay rejected for hosts outside allow_jwks_hosts"
    );
}

#[test]
fn config_rejects_ip_literal_jwks_even_with_allow_list() {
    // Literals are never allowed, regardless of CIDR allow-list.
    let toml = r#"
[server]
data_dir         = "/tmp/nodedb-security-test"
data_plane_cores = 1
memory_limit     = 1073741824

[engines]
vector_budget_fraction     = 0.30
sparse_budget_fraction     = 0.15
crdt_budget_fraction       = 0.10
timeseries_budget_fraction = 0.10
query_budget_fraction      = 0.20

[auth]
mode                     = "password"
superuser_name           = "nodedb"
superuser_password       = "test-password"
min_password_length      = 8
max_failed_logins        = 5
lockout_duration_secs    = 300
idle_timeout_secs        = 3600
max_connections_per_user = 0
password_expiry_days     = 0
audit_retention_days     = 0

[auth.jwt]
allow_jwks_cidrs = ["0.0.0.0/0"]

[[auth.jwt.providers]]
name     = "prod"
jwks_url = "https://10.0.0.5/jwks.json"
issuer   = "https://auth.example.com/"
tenant_id = 1
"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    assert!(
        ServerConfig::from_file(f.path()).is_err(),
        "IP-literal JWKS URL must be rejected even with a permissive CIDR list"
    );
}

// ── 3. SSRF: fetch path must not connect to bad URLs ──────────────────

#[tokio::test]
async fn fetch_does_not_connect_to_http_url() {
    // Even if we hand fetch_and_cache an http:// URL directly (bypassing
    // config), the fetch path must refuse — defense-in-depth. We spawn a
    // counting listener and assert zero inbound connections were made.
    let (url, counter) = spawn_counting_listener();
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    // Give any leaked connect a moment to land.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0, "must not parse keys from a refused URL");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "fetch must not open a socket to an http:// URL"
    );
}

#[tokio::test]
async fn fetch_does_not_connect_to_ip_literal_imds() {
    // Spin a listener on 127.0.0.1 but reference it via 127.0.0.1 in the
    // URL — IP-literal host must be refused pre-connect. Asserts zero
    // inbound connections.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if listener.accept().is_ok() {
                c.fetch_add(1, Ordering::SeqCst);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    // Use https + IP literal — scheme is fine, host is the problem.
    let url = format!(
        "https://{}/jwks.json",
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    );
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "fetch must not open a socket to an IP-literal host"
    );

    // Silence unused warning for IpAddr import in other test configs.
    let _ = IpAddr::V4(Ipv4Addr::LOCALHOST);
}

// ── 4. Redirect cap ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_caps_redirect_hops() {
    // A self-redirect loop would connect unbounded times without a cap.
    // Spec: fetch terminates with ≤ N connections (N small, e.g. ≤ 4 for
    // a 3-hop cap + initial). Today the fetch has no cap and runs until
    // reqwest's default (10) — which is still too high and per-hop is
    // not revalidated. We pin "no more than 4 total connections".
    let (url, counter) = spawn_redirect_loop().await;
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0);
    let n = counter.load(Ordering::SeqCst);
    assert!(
        n <= 4,
        "redirect hops must be capped at ≤3 (plus initial); got {n} connections"
    );
}

// ── 5. Response-body scrubbing on parse error ─────────────────────────

#[tokio::test]
async fn parse_error_log_does_not_leak_response_body() {
    // Serve a body that looks like AWS IMDS credentials and is not valid
    // JWKS. The current code logs `error = %e` where e embeds the
    // sonic_rs parse error, which echoes surrounding body bytes — so
    // credentials land in the log pipeline.
    //
    // Spec: the tracing event emitted by fetch_and_cache on parse failure
    // must not contain the body content. We capture tracing output into a
    // buffer and grep.
    use tracing::subscriber::set_default;
    use tracing_subscriber::fmt;

    let body = r#"{"AccessKeyId":"AKIA_BODY_MARKER","SecretAccessKey":"SECRET_BODY_MARKER"}"#;
    let url = spawn_static_body(body).await;

    let buf: Arc<std::sync::Mutex<Vec<u8>>> = Arc::default();
    let subscriber = {
        let buf = buf.clone();
        fmt()
            .with_writer(move || BufWriter(buf.clone()))
            .with_ansi(false)
            .finish()
    };

    {
        let _guard = set_default(subscriber);
        let cache = JwksCache::new(None);
        let _ = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    }

    let captured = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !captured.contains("AKIA_BODY_MARKER") && !captured.contains("SECRET_BODY_MARKER"),
        "parse-failure log must not embed response body bytes; got:\n{captured}"
    );
}

struct BufWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for BufWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
