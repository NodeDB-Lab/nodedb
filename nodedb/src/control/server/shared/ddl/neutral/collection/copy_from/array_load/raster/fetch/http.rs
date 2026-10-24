// SPDX-License-Identifier: BUSL-1.1

//! `HttpRange`: byte-range reads of an `http(s)://` raster.
//!
//! Each range is one `GET` with a `Range` header. The answer must be `206`
//! with a `Content-Range` that starts where the range starts and names the
//! same total length every time. The first strong `ETag` is pinned and sent
//! as `If-Match` on every later request, so a file that changes mid-load
//! fails with `55000` instead of mixing two versions. A server that ignores
//! the range (`200`) is refused: the load reads ranges only. The client is
//! the egress client: every URL and redirect hop passes the egress policy,
//! and every resolved address is checked at connect time.
//!
//! Messages name the URL without its query, so a signature in the query
//! never reaches an error or a log line.

use nodedb_raster::ByteRange;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, ETAG, IF_MATCH, RANGE};
use zeroize::Zeroizing;

use super::error::FetchError;
use super::pin::{ObjectVersion, PinnedObject};
use super::traits::{RangeFetch, fetch_bounded};
use crate::control::security::egress::{EgressFault, classify};

/// How a request authenticates.
pub enum HttpAuth {
    None,
    /// `Authorization: Bearer <token>`.
    Bearer(Zeroizing<String>),
    /// A shared access signature appended to the URL query.
    Sas(Zeroizing<String>),
}

/// One remote object read with HTTP range requests.
pub struct HttpRange {
    client: reqwest::Client,
    url: reqwest::Url,
    bearer: Option<Zeroizing<String>>,
    name: String,
    pinned: PinnedObject,
}

impl HttpRange {
    /// Read `url` with `client`, authenticating with `auth`. The caller
    /// checked `url` against the egress policy.
    pub fn new(client: reqwest::Client, url: reqwest::Url, auth: HttpAuth) -> Self {
        let name = display_name(&url);
        let (url, bearer) = match auth {
            HttpAuth::None => (url, None),
            HttpAuth::Bearer(token) => (url, Some(token)),
            HttpAuth::Sas(token) => (with_sas(url, &token), None),
        };
        Self {
            client,
            url,
            bearer,
            name,
            pinned: PinnedObject::new(),
        }
    }

    /// The bytes of `range`, fewer where the object ends.
    pub async fn fetch_one(&self, range: ByteRange) -> Result<Option<Vec<u8>>, FetchError> {
        if range.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut request = self
            .client
            .get(self.url.clone())
            .header(RANGE, format!("bytes={}-{}", range.start, range.end - 1));
        if let Some(etag) = self.pinned.etag()
            && !etag.starts_with("W/")
        {
            request = request.header(IF_MATCH, etag);
        }
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token.as_str());
        }
        let response = request.send().await.map_err(|e| self.send_error(e))?;
        let status = response.status();
        match status {
            StatusCode::PARTIAL_CONTENT => self.read_partial(range, response).await.map(Some),
            StatusCode::RANGE_NOT_SATISFIABLE => {
                if let Some(total) = header_text(&response, CONTENT_RANGE)
                    .and_then(|v| v.strip_prefix("bytes */").and_then(|t| t.parse().ok()))
                {
                    self.pinned.pin(
                        &self.name,
                        ObjectVersion {
                            size: total,
                            etag: self.pinned.etag().map(str::to_string),
                        },
                    )?;
                }
                Ok(Some(Vec::new()))
            }
            StatusCode::PRECONDITION_FAILED => Err(self.changed("its ETag no longer matches")),
            StatusCode::NOT_FOUND | StatusCode::GONE => Err(FetchError::NotFound {
                source_name: self.name.clone(),
            }),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(FetchError::Unauthorized {
                source_name: self.name.clone(),
                status: status.as_u16(),
            }),
            StatusCode::OK => Err(FetchError::RangeIgnored {
                source_name: self.name.clone(),
                status: status.as_u16(),
            }),
            other => Err(FetchError::Status {
                source_name: self.name.clone(),
                status: other.as_u16(),
            }),
        }
    }

    /// The body of a `206` for `range`, checked against its
    /// `Content-Range` and the pinned object.
    async fn read_partial(
        &self,
        range: ByteRange,
        mut response: reqwest::Response,
    ) -> Result<Vec<u8>, FetchError> {
        let content_range = header_text(&response, CONTENT_RANGE)
            .ok_or_else(|| self.protocol("a 206 answer has no Content-Range"))?;
        let (first, last, total) = parse_content_range(&content_range)
            .ok_or_else(|| self.protocol("a 206 answer has a malformed Content-Range"))?;
        if first != range.start || last >= range.end || last < first {
            return Err(self.protocol(format!(
                "asked for bytes {}-{}, the server sent {first}-{last}",
                range.start,
                range.end - 1
            )));
        }
        let etag = header_text(&response, ETAG);
        self.pinned
            .pin(&self.name, ObjectVersion { size: total, etag })?;
        let expected = usize::try_from(last - first + 1)
            .map_err(|_| self.protocol("the answer exceeds the address space"))?;
        let mut body = Vec::with_capacity(expected);
        while let Some(chunk) = response.chunk().await.map_err(|e| self.send_error(e))? {
            if body.len() + chunk.len() > expected {
                return Err(self.protocol(format!(
                    "the server sent more than the {expected} bytes its Content-Range names"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        if body.len() != expected {
            return Err(self.protocol(format!(
                "the server sent {} of the {expected} bytes its Content-Range names",
                body.len()
            )));
        }
        Ok(body)
    }

    fn changed(&self, what: &str) -> FetchError {
        FetchError::Changed {
            source_name: self.name.clone(),
            what: what.to_string(),
        }
    }

    fn protocol(&self, what: impl Into<String>) -> FetchError {
        FetchError::Protocol {
            source_name: self.name.clone(),
            what: what.into(),
        }
    }

    /// The error of a failed send or body read, with the URL removed.
    fn send_error(&self, error: reqwest::Error) -> FetchError {
        let source_name = self.name.clone();
        match classify(&error) {
            EgressFault::Refused(error) => FetchError::Egress { source_name, error },
            EgressFault::TooManyRedirects(max) => FetchError::Protocol {
                source_name,
                what: format!("more than {max} redirects"),
            },
            EgressFault::Other => FetchError::Network {
                source_name,
                what: error.without_url().to_string(),
            },
        }
    }
}

impl RangeFetch for HttpRange {
    async fn fetch(&self, ranges: &[ByteRange]) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
        fetch_bounded(ranges, |range| self.fetch_one(range)).await
    }
}

/// `bytes <first>-<last>/<total>`. A `*` total is refused: the load pins
/// the total length.
fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (span, total) = rest.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    Some((
        first.trim().parse().ok()?,
        last.trim().parse().ok()?,
        total.trim().parse().ok()?,
    ))
}

fn header_text(response: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// The URL without its query, fragment or user info.
pub(crate) fn display_name(url: &reqwest::Url) -> String {
    let mut shown = url.clone();
    shown.set_query(None);
    shown.set_fragment(None);
    // Clearing user info fails only for a URL that cannot hold any.
    let _cleared = shown.set_username("").and(shown.set_password(None));
    shown.to_string()
}

/// `url` with the signature `token` appended to its query.
fn with_sas(mut url: reqwest::Url, token: &str) -> reqwest::Url {
    let token = token.trim_start_matches('?');
    let query = match url.query() {
        Some(existing) if !existing.is_empty() => format!("{existing}&{token}"),
        Some(_) | None => token.to_string(),
    };
    url.set_query(Some(&query));
    url
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use nodedb_raster::ObjectId;

    use super::super::test_server::{Mode, serve};
    use super::*;
    use crate::control::security::egress::resolver::tests::table;
    use crate::control::security::egress::{EgressClientOptions, EgressPolicy, egress_client};

    const OBJECT: &[u8] = b"0123456789abcdefghij";

    fn policy(hosts: &[&str], cidrs: &[&str]) -> Arc<EgressPolicy> {
        let hosts: Vec<String> = hosts.iter().map(|h| (*h).to_string()).collect();
        let cidrs: Vec<String> = cidrs.iter().map(|c| (*c).to_string()).collect();
        Arc::new(EgressPolicy::from_parts(true, &hosts, &cidrs).expect("valid policy"))
    }

    fn client(policy: Arc<EgressPolicy>, entries: &[(&str, &str)]) -> reqwest::Client {
        egress_client(
            policy,
            table(entries),
            EgressClientOptions {
                timeout: Some(std::time::Duration::from_secs(10)),
                connect_timeout: std::time::Duration::from_secs(5),
                read_timeout: std::time::Duration::from_secs(5),
                max_redirects: 3,
            },
        )
        .expect("client")
    }

    fn range(start: u64, end: u64) -> ByteRange {
        ByteRange::new(ObjectId(0), start, end)
    }

    fn local(host: &str, port: u16) -> HttpRange {
        let url: reqwest::Url = format!("http://{host}:{port}/scene.tif?sig=secret")
            .parse()
            .expect("url");
        let client = client(policy(&[host], &["127.0.0.0/8"]), &[(host, "127.0.0.1")]);
        HttpRange::new(client, url, HttpAuth::None)
    }

    #[tokio::test]
    async fn ranges_read_with_the_first_etag_pinned() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Ranges).await;
        let http = local("raster.test", addr.port());
        let got = http
            .fetch(&[range(0, 4), range(18, 40)])
            .await
            .expect("reads");
        assert_eq!(got, vec![Some(b"0123".to_vec()), Some(b"ij".to_vec())]);
        let got = http.fetch(&[range(25, 30)]).await.expect("past the end");
        assert_eq!(got, vec![Some(Vec::new())]);
        let again = http.fetch(&[range(4, 6)]).await.expect("pinned read");
        assert_eq!(again, vec![Some(b"45".to_vec())]);
        assert!(served.if_match_seen.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn an_object_that_changes_mid_load_fails_55000() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Ranges).await;
        let http = local("raster.test", addr.port());
        http.fetch(&[range(0, 4)])
            .await
            .expect("first read pins v1");
        served.replace(b"another version of it".to_vec(), "\"v2\"");
        let err = http.fetch(&[range(4, 8)]).await.expect_err("v2 is refused");
        assert_eq!(err.sqlstate(), "55000", "{err}");
        assert!(!err.to_string().contains("secret"), "{err}");
    }

    #[tokio::test]
    async fn a_server_that_ignores_ranges_is_refused() {
        let (addr, _served) = serve(OBJECT.to_vec(), Mode::IgnoreRange).await;
        let http = local("raster.test", addr.port());
        let err = http
            .fetch(&[range(0, 4)])
            .await
            .expect_err("200 is refused");
        assert_eq!(err.sqlstate(), "0A000", "{err}");
    }

    #[tokio::test]
    async fn a_rebound_name_never_reaches_the_private_address() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Ranges).await;
        // http is allowed for the host, but no CIDR admits loopback.
        let client = client(
            policy(&["rebind.test"], &[]),
            &[("rebind.test", "127.0.0.1")],
        );
        let url = format!("http://rebind.test:{}/scene.tif", addr.port())
            .parse()
            .expect("url");
        let http = HttpRange::new(client, url, HttpAuth::None);
        let err = http.fetch(&[range(0, 4)]).await.expect_err("refused");
        assert_eq!(err.sqlstate(), "42501", "{err}");
        assert_eq!(served.requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_redirect_to_an_ip_literal_is_refused() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Redirect).await;
        *served.location.lock().unwrap_or_else(|p| p.into_inner()) =
            format!("http://127.0.0.1:{}/scene.tif", addr.port());
        let http = local("raster.test", addr.port());
        let err = http.fetch(&[range(0, 4)]).await.expect_err("hop refused");
        assert_eq!(err.sqlstate(), "42501", "{err}");
        assert_eq!(served.requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_sas_signature_joins_the_query_and_never_the_name() {
        let url: reqwest::Url = "https://acct.blob.example/c/a.tif?x=1"
            .parse()
            .expect("url");
        assert_eq!(display_name(&url), "https://acct.blob.example/c/a.tif");
        let signed = with_sas(url, "?sv=1&sig=abc");
        assert_eq!(signed.query(), Some("x=1&sv=1&sig=abc"));
        assert_eq!(parse_content_range("bytes 0-3/20"), Some((0, 3, 20)));
        assert_eq!(parse_content_range("bytes 0-3/*"), None);
    }
}
