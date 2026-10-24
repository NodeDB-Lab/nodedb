// SPDX-License-Identifier: BUSL-1.1

//! `ObjectStoreRange`: byte-range reads of an `s3://bucket/key` raster.
//!
//! Each range is one ranged `GET`. The first answer pins the object's size
//! and ETag, and every later request sends the ETag as `If-Match`, so an
//! object that changes mid-load fails with `55000`. Requests go through the
//! egress client, so every host passes the egress policy at connect time.
//! The endpoint of an S3-compatible store passes the egress policy again at
//! COPY time, so a policy narrowed after `CREATE CREDENTIAL` holds. A
//! requester-pays bucket bills the credential's account when the COPY sets
//! `REQUESTER_PAYS true`.

use std::sync::Arc;

use nodedb_raster::ByteRange;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::client::{HttpClient, HttpConnector};
use object_store::path::Path;
use object_store::{ClientOptions, GetOptions, GetRange, ObjectStore as _};
use zeroize::Zeroizing;

use super::error::FetchError;
use super::pin::{ObjectVersion, PinnedObject};
use super::traits::{RangeFetch, fetch_bounded};
use crate::control::security::egress::{EgressError, EgressPolicy};

/// AWS keys of a signed S3 read.
pub struct S3Keys {
    pub access_key_id: Zeroizing<String>,
    pub secret_access_key: Zeroizing<String>,
    pub session_token: Option<Zeroizing<String>>,
}

/// Where and how an S3 source is read.
pub struct S3Target {
    pub bucket: String,
    pub key: String,
    pub region: String,
    /// An S3-compatible endpoint in place of AWS.
    pub endpoint: Option<String>,
    /// `None` reads a public bucket unsigned.
    pub keys: Option<S3Keys>,
    pub requester_pays: bool,
}

/// The connector that hands object_store the egress client.
#[derive(Debug)]
struct EgressConnector(reqwest::Client);

impl HttpConnector for EgressConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.0.clone()))
    }
}

/// One S3 object read with ranged `GET`s.
pub struct ObjectStoreRange {
    store: Arc<AmazonS3>,
    path: Path,
    name: String,
    pinned: PinnedObject,
}

impl ObjectStoreRange {
    /// Read `target` through `client`, the egress client built on `policy`.
    pub fn new(
        client: reqwest::Client,
        target: S3Target,
        policy: &EgressPolicy,
    ) -> Result<Self, FetchError> {
        let name = format!("s3://{}/{}", target.bucket, target.key);
        let store_error = |what: String| FetchError::Store {
            source_name: name.clone(),
            what,
        };
        let path = Path::parse(&target.key)
            .map_err(|e| store_error(format!("the key is not a valid object path: {e}")))?;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&target.bucket)
            .with_region(&target.region)
            .with_http_connector(EgressConnector(client))
            .with_request_payer(target.requester_pays);
        if let Some(endpoint) = &target.endpoint {
            let egress = |error: EgressError| FetchError::Egress {
                source_name: name.clone(),
                error,
            };
            let url = reqwest::Url::parse(endpoint).map_err(|_| egress(EgressError::Malformed))?;
            policy.check_parsed(&url).map_err(egress)?;
            // The policy admits http only for an allow-listed host.
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(url.scheme() == "http");
        }
        builder = match &target.keys {
            Some(keys) => {
                let signed = builder
                    .with_access_key_id(keys.access_key_id.as_str())
                    .with_secret_access_key(keys.secret_access_key.as_str());
                match &keys.session_token {
                    Some(token) => signed.with_token(token.as_str()),
                    None => signed,
                }
            }
            None => builder.with_skip_signature(true),
        };
        let store = builder
            .build()
            .map_err(|e| store_error(format!("cannot configure the S3 client: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
            path,
            name,
            pinned: PinnedObject::new(),
        })
    }

    /// The bytes of `range`, fewer where the object ends.
    pub async fn fetch_one(&self, range: ByteRange) -> Result<Option<Vec<u8>>, FetchError> {
        if range.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if let Some(size) = self.pinned.size()
            && range.start >= size
        {
            return Ok(Some(Vec::new()));
        }
        let options = GetOptions {
            if_match: self.pinned.etag().map(str::to_string),
            range: Some(GetRange::Bounded(range.start..range.end)),
            ..GetOptions::default()
        };
        let result = self
            .store
            .get_opts(&self.path, options)
            .await
            .map_err(|e| self.get_error(e))?;
        self.pinned.pin(
            &self.name,
            ObjectVersion {
                size: result.meta.size,
                etag: result.meta.e_tag.clone(),
            },
        )?;
        let bytes = result.bytes().await.map_err(|e| self.get_error(e))?;
        if bytes.len() as u64 > range.len() {
            return Err(FetchError::Protocol {
                source_name: self.name.clone(),
                what: format!(
                    "asked for {} bytes, the store sent {}",
                    range.len(),
                    bytes.len()
                ),
            });
        }
        Ok(Some(bytes.to_vec()))
    }

    fn get_error(&self, error: object_store::Error) -> FetchError {
        let source_name = self.name.clone();
        match error {
            object_store::Error::NotFound { .. } if self.pinned.get().is_some() => {
                FetchError::Changed {
                    source_name,
                    what: "it was removed".into(),
                }
            }
            object_store::Error::NotFound { .. } => FetchError::NotFound { source_name },
            object_store::Error::Precondition { .. } => FetchError::Changed {
                source_name,
                what: "its ETag no longer matches".into(),
            },
            object_store::Error::PermissionDenied { .. } => FetchError::Unauthorized {
                source_name,
                status: 403,
            },
            object_store::Error::Unauthenticated { .. } => FetchError::Unauthorized {
                source_name,
                status: 401,
            },
            // The enum is non-exhaustive: every other failure is a store error.
            other => FetchError::Store {
                source_name,
                what: other.to_string(),
            },
        }
    }
}

impl RangeFetch for ObjectStoreRange {
    async fn fetch(&self, ranges: &[ByteRange]) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
        fetch_bounded(ranges, |range| self.fetch_one(range)).await
    }
}

/// The region of public `bucket`: AWS names it in the
/// `x-amz-bucket-region` header of a `HEAD` on the bucket.
pub async fn bucket_region(client: &reqwest::Client, bucket: &str) -> Result<String, FetchError> {
    let name = format!("s3://{bucket}");
    let url = format!("https://{bucket}.s3.amazonaws.com/");
    let response = client
        .head(&url)
        .send()
        .await
        .map_err(|e| FetchError::Network {
            source_name: name.clone(),
            what: e.without_url().to_string(),
        })?;
    response
        .headers()
        .get("x-amz-bucket-region")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| FetchError::Store {
            source_name: name,
            what: format!(
                "the bucket region is unknown (HTTP {}); name it with a CREDENTIAL of TYPE 's3' \
                 and its REGION",
                response.status().as_u16()
            ),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use nodedb_raster::ObjectId;

    use super::super::test_server::{Mode, serve};
    use super::*;
    use crate::control::security::egress::resolver::tests::table;
    use crate::control::security::egress::{EgressClientOptions, egress_client};

    const OBJECT: &[u8] = b"0123456789abcdefghij";

    fn http_policy() -> Arc<EgressPolicy> {
        Arc::new(
            EgressPolicy::from_parts(true, &["s3.test".into()], &["127.0.0.0/8".into()])
                .expect("valid policy"),
        )
    }

    fn client(policy: Arc<EgressPolicy>) -> reqwest::Client {
        egress_client(
            policy,
            table(&[("s3.test", "127.0.0.1")]),
            EgressClientOptions {
                timeout: Some(std::time::Duration::from_secs(10)),
                connect_timeout: std::time::Duration::from_secs(5),
                read_timeout: std::time::Duration::from_secs(5),
                max_redirects: 0,
            },
        )
        .expect("client")
    }

    fn target(endpoint: String) -> S3Target {
        S3Target {
            bucket: "bucket".into(),
            key: "scene.tif".into(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint),
            keys: None,
            requester_pays: false,
        }
    }

    fn range(start: u64, end: u64) -> ByteRange {
        ByteRange::new(ObjectId(0), start, end)
    }

    #[tokio::test]
    async fn an_allow_listed_http_endpoint_reads_and_pins_the_object() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Ranges).await;
        let policy = http_policy();
        let store = ObjectStoreRange::new(
            client(Arc::clone(&policy)),
            target(format!("http://s3.test:{}", addr.port())),
            &policy,
        )
        .expect("an allow-listed http endpoint");
        let got = store.fetch(&[range(0, 4)]).await.expect("reads");
        assert_eq!(got, vec![Some(b"0123".to_vec())]);
        let past = store.fetch(&[range(25, 30)]).await.expect("past the end");
        assert_eq!(past, vec![Some(Vec::new())]);
        served.replace(b"another version of it".to_vec(), "\"v2\"");
        let err = store
            .fetch(&[range(4, 8)])
            .await
            .expect_err("v2 is refused");
        assert_eq!(err.sqlstate(), "55000", "{err}");
        assert!(served.if_match_seen.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn an_endpoint_the_policy_refuses_at_copy_time_sends_nothing() {
        let (addr, served) = serve(OBJECT.to_vec(), Mode::Ranges).await;
        // The credential's endpoint passed a policy that allowed http; the
        // policy at COPY time does not.
        let strict = Arc::new(EgressPolicy::strict());
        for endpoint in [
            format!("http://s3.test:{}", addr.port()),
            format!("https://127.0.0.1:{}", addr.port()),
            "not a url".to_string(),
        ] {
            let err = ObjectStoreRange::new(client(Arc::clone(&strict)), target(endpoint), &strict)
                .err()
                .expect("refused");
            assert_eq!(err.sqlstate(), "42501", "{err}");
        }
        assert_eq!(served.requests.load(Ordering::SeqCst), 0);
    }
}
