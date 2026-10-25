// SPDX-License-Identifier: BUSL-1.1

//! JWKS HTTP fetcher: downloads and parses JWKS from provider endpoints.
//!
//! Handles HTTP fetch, JSON deserialization, key extraction, and error
//! reporting. Used by the registry for initial load and periodic refresh.
//!
//! SSRF defense: the request goes through the egress client. The URL and
//! every redirect hop pass the [`EgressPolicy`], and every address a host
//! resolves to is checked at connect time, so a rebinding DNS answer never
//! reaches a private address. A maximum of 3 redirect hops is allowed.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use super::cache::JwksCache;
use super::key::{JwksResponse, VerificationKey, parse_jwk};
use crate::control::security::egress::{
    EgressClientOptions, EgressError, EgressFault, EgressPolicy, SystemLookup, classify,
    egress_client,
};

const MAX_REDIRECTS: usize = 3;

/// Cap on a JWKS response body. 1 MiB is far beyond any real JWKS document.
const MAX_BODY: usize = 1 << 20;

/// Cap on one JWKS request, connect and read included.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetch JWKS from a provider's endpoint and update the cache.
///
/// `cache_identity` is an internal generated cache key; `provider_name` is
/// retained for operator-facing log fields.
///
/// Returns the number of keys successfully parsed.
/// On HTTP or parse failure, logs a warning and returns 0 (cache unchanged).
pub async fn fetch_and_cache(
    cache_identity: &str,
    provider_name: &str,
    jwks_url: &str,
    cache: &JwksCache,
    policy: &EgressPolicy,
) -> usize {
    match fetch_jwks(jwks_url, policy).await {
        Ok(keys) => {
            let count = keys.len();
            if count > 0 {
                info!(
                    provider = %provider_name,
                    url = %jwks_url,
                    keys = count,
                    "JWKS fetched successfully"
                );
                cache.update_provider(cache_identity, keys);
            } else {
                warn!(
                    provider = %provider_name,
                    url = %jwks_url,
                    "JWKS response contained no usable signature keys"
                );
            }
            count
        }
        Err(e) => {
            // Log only the variant (no embedded body bytes) — error bodies
            // could carry cloud-metadata contents if SSRF guards ever fail
            // open. We never want that material in the log pipeline.
            warn!(
                provider = %provider_name,
                url = %jwks_url,
                error = e.category(),
                "JWKS fetch failed — using cached keys if available"
            );
            0
        }
    }
}

/// Fetch and parse JWKS from a URL, with full SSRF defense.
async fn fetch_jwks(
    url: &str,
    policy: &EgressPolicy,
) -> Result<Vec<VerificationKey>, JwksFetchError> {
    debug!(url = %url, "fetching JWKS");

    // Scheme and host before any connection, respecting the policy.
    policy
        .check_url(url)
        .map_err(JwksFetchError::UrlValidation)?;

    let client = egress_client(
        Arc::new(policy.clone()),
        Arc::new(SystemLookup),
        EgressClientOptions {
            timeout: Some(FETCH_TIMEOUT),
            connect_timeout: FETCH_TIMEOUT,
            read_timeout: FETCH_TIMEOUT,
            max_redirects: MAX_REDIRECTS,
        },
    )
    .map_err(|_| JwksFetchError::HttpClient)?;

    let response = client
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| match classify(&e) {
            EgressFault::TooManyRedirects(_) => JwksFetchError::RedirectLimit,
            EgressFault::Refused(refused) => JwksFetchError::UrlValidation(refused),
            EgressFault::Other => JwksFetchError::HttpRequest,
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(JwksFetchError::HttpStatus(status.as_u16()));
    }
    let body = read_bounded(response).await?;

    let jwks: JwksResponse =
        crate::util::bounded_json::from_slice(&body).map_err(|_| JwksFetchError::JsonParse)?;

    let keys: Vec<VerificationKey> = jwks.keys.iter().filter_map(parse_jwk).collect();

    Ok(keys)
}

/// The response body, read chunk by chunk. A hostile endpoint that sends
/// more than [`MAX_BODY`] bytes fails before the buffer grows past the cap.
async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, JwksFetchError> {
    if response
        .content_length()
        .is_some_and(|len| len > MAX_BODY as u64)
    {
        return Err(JwksFetchError::BodyTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| JwksFetchError::HttpBody)?
    {
        if body.len() + chunk.len() > MAX_BODY {
            return Err(JwksFetchError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// JWKS fetch errors. Variants carry no external data so they cannot
/// smuggle response bodies into log sinks.
#[derive(Debug, thiserror::Error)]
pub enum JwksFetchError {
    #[error("JWKS URL failed validation: {0}")]
    UrlValidation(#[from] EgressError),
    #[error("HTTP client construction failed")]
    HttpClient,
    #[error("HTTP request failed")]
    HttpRequest,
    #[error("JWKS endpoint returned HTTP {0}")]
    HttpStatus(u16),
    #[error("failed to read response body")]
    HttpBody,
    #[error("JWKS response body exceeded size cap")]
    BodyTooLarge,
    #[error("JWKS JSON parse failed")]
    JsonParse,
    #[error("JWKS fetch exceeded redirect limit")]
    RedirectLimit,
}

impl JwksFetchError {
    /// Stable, body-free category string for log events.
    pub fn category(&self) -> &'static str {
        match self {
            Self::UrlValidation(_) => "url_validation",
            Self::HttpClient => "http_client",
            Self::HttpRequest => "http_request",
            Self::HttpStatus(_) => "http_status",
            Self::HttpBody => "http_body",
            Self::BodyTooLarge => "body_too_large",
            Self::JsonParse => "json_parse",
            Self::RedirectLimit => "redirect_limit",
        }
    }
}

/// Start the periodic JWKS refresh task.
///
/// Spawns a Tokio task that refreshes JWKS keys for every target at the
/// configured interval. Each tick reads `targets` afresh, so a catalog
/// provider registered after the start is refreshed too. Runs on the
/// Control Plane.
pub fn spawn_refresh_task(
    targets: Arc<parking_lot::RwLock<super::registry::targets::RefreshTargets>>,
    cache: std::sync::Arc<JwksCache>,
    refresh_interval_secs: u64,
    policy: std::sync::Arc<EgressPolicy>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
        // First tick fires immediately — skip it since we fetch on startup.
        interval.tick().await;

        loop {
            interval.tick().await;
            let snapshot = targets.read().snapshot();
            for target in &snapshot {
                fetch_and_cache(
                    &target.cache_identity,
                    &target.provider_name,
                    &target.jwks_url,
                    &cache,
                    &policy,
                )
                .await;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Serve `head` and then `body_chunks` as a chunked body on every
    /// connection. Returns the port.
    async fn serve(head: String, body_chunks: usize, chunk: usize) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let head = head.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 4096];
                    let _ = socket.read(&mut request).await;
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    let data = vec![b' '; chunk];
                    for _ in 0..body_chunks {
                        let frame = format!("{chunk:x}\r\n");
                        if socket.write_all(frame.as_bytes()).await.is_err()
                            || socket.write_all(&data).await.is_err()
                            || socket.write_all(b"\r\n").await.is_err()
                        {
                            return;
                        }
                    }
                    let _ = socket.write_all(b"0\r\n\r\n").await;
                });
            }
        });
        port
    }

    fn local_policy() -> EgressPolicy {
        EgressPolicy::from_parts(
            true,
            &["localhost".to_string()],
            &["127.0.0.0/8".to_string(), "::1/128".to_string()],
        )
        .expect("valid policy")
    }

    #[tokio::test]
    async fn a_chunked_body_over_the_cap_is_refused() {
        let head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                    Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            .to_string();
        // 2 MiB in 64 KiB chunks, with no Content-Length to check up front.
        let port = serve(head, 32, 64 * 1024).await;
        let url = format!("http://localhost:{port}/jwks.json");
        let err = fetch_jwks(&url, &local_policy())
            .await
            .expect_err("over the cap");
        assert!(matches!(err, JwksFetchError::BodyTooLarge), "{err}");
    }

    #[tokio::test]
    async fn a_redirect_loop_hits_the_redirect_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let head = format!(
            "HTTP/1.1 302 Found\r\nLocation: http://localhost:{port}/again\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n"
        );
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let head = head.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 4096];
                    let _ = socket.read(&mut request).await;
                    let _ = socket.write_all(head.as_bytes()).await;
                });
            }
        });
        let url = format!("http://localhost:{port}/jwks.json");
        let err = fetch_jwks(&url, &local_policy())
            .await
            .expect_err("redirect loop");
        assert!(matches!(err, JwksFetchError::RedirectLimit), "{err}");
    }

    #[tokio::test]
    async fn a_host_outside_the_allow_list_is_refused_at_resolve() {
        // https passes the URL check, and the strict policy refuses the
        // loopback address localhost resolves to before any connect.
        let url = "https://localhost:9/jwks.json";
        let err = fetch_jwks(url, &EgressPolicy::strict())
            .await
            .expect_err("loopback");
        assert!(
            matches!(
                err,
                JwksFetchError::UrlValidation(EgressError::PrivateAddress)
            ),
            "{err}"
        );
    }
}
