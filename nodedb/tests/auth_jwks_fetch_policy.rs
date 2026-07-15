// SPDX-License-Identifier: BUSL-1.1

//! JWKS URL-policy, redirect, and response-scrubbing integration tests.

use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use nodedb::config::ServerConfig;
use nodedb::control::security::jwks::cache::JwksCache;
use nodedb::control::security::jwks::fetch::fetch_and_cache;
use nodedb::control::security::jwks::url::JwksPolicy;

fn provider_config(jwt_policy: &str, jwks_url: &str) -> String {
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
{jwt_policy}

[[auth.jwt.providers]]
name      = "prod"
jwks_url  = "{jwks_url}"
issuer    = "https://auth.example.com/"
tenant_id = 1
"#
    )
}

fn load_config(toml: &str) -> nodedb::Result<ServerConfig> {
    let mut file = tempfile::NamedTempFile::new().expect("config fixture must open");
    file.write_all(toml.as_bytes())
        .expect("config fixture must be written");
    ServerConfig::from_file(file.path())
}

fn spawn_counting_listener() -> (String, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = counter.clone();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if let Ok((_stream, _)) = listener.accept() {
                connections.fetch_add(1, Ordering::SeqCst);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    (format!("http://{addr}/jwks.json"), counter)
}

async fn spawn_redirect_loop() -> (String, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = counter.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                connections.fetch_add(1, Ordering::SeqCst);
                use tokio::io::AsyncWriteExt;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: https://{addr}/next\r\nContent-Length: 0\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    });
    (format!("https://{addr}/jwks.json"), counter)
}

async fn spawn_static_body(body: impl Into<String>) -> String {
    let body = body.into();
    let listener = tokio::net::TcpListener::bind("[::]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    });
    format!("http://localhost:{}/jwks.json", addr.port())
}

#[test]
fn config_accepts_allowlisted_http_jwks_host() {
    let toml = provider_config(
        "allow_http_jwks = true\nallow_jwks_hosts = [\"keycloak.internal\"]\nallow_jwks_cidrs = [\"10.42.0.0/16\"]",
        "http://keycloak.internal/jwks.json",
    );
    load_config(&toml).expect("allow-listed HTTP host must pass config validation");
}

#[test]
fn config_rejects_non_allowlisted_http_jwks_host() {
    let toml = provider_config(
        "allow_http_jwks = true\nallow_jwks_hosts = [\"keycloak.internal\"]",
        "http://evil.example.com/jwks.json",
    );
    assert!(load_config(&toml).is_err());
}

#[test]
fn config_rejects_ip_literal_jwks_even_with_allow_list() {
    let toml = provider_config(
        "allow_jwks_cidrs = [\"0.0.0.0/0\"]",
        "https://10.0.0.5/jwks.json",
    );
    assert!(load_config(&toml).is_err());
}

#[tokio::test]
async fn fetch_does_not_connect_to_http_url() {
    let (url, counter) = spawn_counting_listener();
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fetch_does_not_connect_to_ip_literal() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let counter = Arc::new(AtomicUsize::new(0));
    let connections = counter.clone();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if listener.accept().is_ok() {
                connections.fetch_add(1, Ordering::SeqCst);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let url = format!(
        "https://{}/jwks.json",
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    );
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_caps_redirect_hops() {
    let (url, counter) = spawn_redirect_loop().await;
    let cache = JwksCache::new(None);
    let keys = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(keys, 0);
    let connections = counter.load(Ordering::SeqCst);
    assert!(connections <= 4, "redirect cap exceeded: {connections}");
}

#[tokio::test]
async fn parse_error_log_does_not_leak_response_body() {
    use tracing::subscriber::set_default;
    use tracing_subscriber::fmt;

    let body = r#"{"AccessKeyId":"AKIA_BODY_MARKER","SecretAccessKey":"SECRET_BODY_MARKER"}"#;
    let url = spawn_static_body(body).await;
    let buffer: Arc<std::sync::Mutex<Vec<u8>>> = Arc::default();
    let subscriber = {
        let buffer = buffer.clone();
        fmt()
            .with_writer(move || BufWriter(buffer.clone()))
            .with_ansi(false)
            .finish()
    };
    {
        let _guard = set_default(subscriber);
        let cache = JwksCache::new(None);
        let _ = fetch_and_cache("prod", "prod", &url, &cache, &JwksPolicy::strict()).await;
    }

    let captured = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    assert!(!captured.contains("AKIA_BODY_MARKER"));
    assert!(!captured.contains("SECRET_BODY_MARKER"));
}

struct BufWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for BufWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
