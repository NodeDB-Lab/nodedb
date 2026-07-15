// SPDX-License-Identifier: BUSL-1.1

//! JWKS HTTP fetcher: downloads and parses JWKS from provider endpoints.
//!
//! Handles HTTP fetch, JSON deserialization, key extraction, and error
//! reporting. Used by the registry for initial load and periodic refresh.
//!
//! SSRF defense: every URL (initial + each redirect hop) is re-validated
//! against [`super::url::validate_jwks_url`], and the hostname is resolved
//! up-front with every returned address checked against
//! [`super::url::validate_resolved_addrs`]. A maximum of 3 redirect hops
//! is allowed.

use std::net::{SocketAddr, ToSocketAddrs};

use tracing::{debug, info, warn};

use super::cache::JwksCache;
use super::key::{JwksResponse, VerificationKey, parse_jwk};
use super::url::{JwksPolicy, UrlValidationError};

const MAX_REDIRECTS: usize = 3;

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
    policy: &JwksPolicy,
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
    policy: &JwksPolicy,
) -> Result<Vec<VerificationKey>, JwksFetchError> {
    debug!(url = %url, "fetching JWKS");

    // 1. Syntactic validation (scheme + not-IP-literal, respecting policy).
    policy
        .check_url(url)
        .map_err(JwksFetchError::UrlValidation)?;

    // 2. Resolve host and validate every address against the policy.
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| JwksFetchError::UrlValidation(UrlValidationError::Malformed))?;
    let host = parsed
        .host_str()
        .ok_or(JwksFetchError::UrlValidation(UrlValidationError::NoHost))?
        .to_owned();
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addrs = resolve_host(&host, port).await?;
    let ips: Vec<_> = addrs.iter().map(|a| a.ip()).collect();
    policy
        .check_resolved(&host, &ips)
        .map_err(JwksFetchError::UrlValidation)?;

    // Pin the first resolved SocketAddr so reqwest uses the IP we
    // validated instead of re-resolving. This closes the DNS-rebinding
    // window between our validation and reqwest's own resolution.
    let pinned = addrs[0];

    // 3. Build a reqwest client that enforces per-hop URL check, caps
    //    redirect depth, and uses the pinned IP for the target host.
    let policy_for_redirect = policy.clone();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .resolve(&host, pinned)
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.error(redirect_limit_err());
            }
            if policy_for_redirect.check_parsed(attempt.url()).is_err() {
                return attempt.stop();
            }
            attempt.follow()
        }))
        .build()
        .map_err(|_| JwksFetchError::HttpClient)?;

    let response = client
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| {
            // Distinguish our redirect-limit sentinel from generic errors.
            if format!("{e}").contains(REDIRECT_LIMIT_SENTINEL) {
                JwksFetchError::RedirectLimit
            } else {
                JwksFetchError::HttpRequest
            }
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(JwksFetchError::HttpStatus(status.as_u16()));
    }

    // Cap body size to prevent a hostile endpoint from forcing unbounded
    // allocation. 1 MiB is well beyond any real JWKS document.
    const MAX_BODY: usize = 1 << 20;
    let body = response
        .bytes()
        .await
        .map_err(|_| JwksFetchError::HttpBody)?;
    if body.len() > MAX_BODY {
        return Err(JwksFetchError::BodyTooLarge);
    }

    let jwks: JwksResponse = sonic_rs::from_slice(&body).map_err(|_| JwksFetchError::JsonParse)?;

    let keys: Vec<VerificationKey> = jwks.keys.iter().filter_map(parse_jwk).collect();

    Ok(keys)
}

/// Resolve a hostname to socket addresses using the blocking resolver on a
/// worker thread (tokio's async resolver isn't available without
/// additional deps and std's resolver is well-exercised for this purpose).
async fn resolve_host(host: &str, port: u16) -> Result<Vec<SocketAddr>, JwksFetchError> {
    let h = host.to_owned();
    tokio::task::spawn_blocking(move || (h.as_str(), port).to_socket_addrs().map(|i| i.collect()))
        .await
        .map_err(|_| JwksFetchError::DnsResolution)?
        .map_err(|_| JwksFetchError::DnsResolution)
}

const REDIRECT_LIMIT_SENTINEL: &str = "nodedb-jwks-redirect-limit";

fn redirect_limit_err() -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(REDIRECT_LIMIT_SENTINEL)
}

/// JWKS fetch errors. Variants carry no external data so they cannot
/// smuggle response bodies into log sinks.
#[derive(Debug, thiserror::Error)]
pub enum JwksFetchError {
    #[error("JWKS URL failed validation: {0}")]
    UrlValidation(#[from] UrlValidationError),
    #[error("DNS resolution failed")]
    DnsResolution,
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
            Self::DnsResolution => "dns_resolution",
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
/// Spawns a Tokio task that refreshes JWKS keys for all providers
/// at the configured interval. Runs on the Control Plane.
pub fn spawn_refresh_task(
    providers: Vec<(String, String, String)>, // (cache identity, name, jwks_url) triples
    cache: std::sync::Arc<JwksCache>,
    refresh_interval_secs: u64,
    policy: std::sync::Arc<JwksPolicy>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
        // First tick fires immediately — skip it since we fetch on startup.
        interval.tick().await;

        loop {
            interval.tick().await;
            for (cache_identity, name, url) in &providers {
                fetch_and_cache(cache_identity, name, url, &cache, &policy).await;
            }
        }
    })
}
