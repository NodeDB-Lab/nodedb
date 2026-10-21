// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM <sources> WITH (FORMAT geotiff, ...)`: one transport
//! per source, then the one async driver.
//!
//! - A server path opens once through the jailed handle every check ran
//!   on. The entry point checked the superuser privilege and the path.
//! - An `http(s)://` URL passes the egress policy before the first request.
//!   A `CREDENTIAL` of `TYPE 'bearer'` sends its token as
//!   `Authorization: Bearer`; one of `TYPE 'sas'` appends its signature to
//!   the query.
//! - An `s3://bucket/key` object reads through object_store over the egress
//!   client. A `CREDENTIAL` of `TYPE 's3'` signs the requests and names the
//!   region; without one the bucket is read unsigned, in the region AWS
//!   reports for it.
//!
//! `OVERVIEW n` opens image `n` of every source: 0 is full resolution, 1
//! the first overview.

use std::sync::Arc;
use std::time::Duration;

use nodedb_raster::TiffOpener;
use nodedb_sql::ddl_ast::statement::{CopyRasterOptions, CopySource};
use zeroize::Zeroizing;

use crate::control::security::egress::{EgressClientOptions, SystemLookup, egress_client};
use crate::control::security::source_credential::{CredentialSecret, open_secret};
use crate::control::server::shared::ddl::result::DdlError;

use super::super::super::source_file::CopySourceFile;
use super::super::dispatch::ArrayLoadCtx;
use super::driver::drive_raster;
use super::fetch::http::display_name;
use super::fetch::{
    FetchError, HttpAuth, HttpRange, LocalFile, ObjectStoreRange, S3Keys, S3Target, SourceFetch,
    SourceSet, bucket_region,
};

/// Redirects one remote request follows.
const MAX_REDIRECTS: usize = 5;
/// Cap on connecting to a remote source.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on the time between two reads of a remote answer.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Load `sources` into `array`. Returns the number of rows loaded: every
/// pixel but the masked and all-nodata ones.
pub(crate) async fn copy_raster(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    sources: &[CopySource],
    options: &CopyRasterOptions,
) -> Result<u64, DdlError> {
    let secret = match &options.credential {
        Some(name) => Some(resolve_credential(ctx, name)?),
        None => None,
    };
    let remote = sources.iter().any(|s| !matches!(s, CopySource::File(_)));
    if secret.is_some() && !remote {
        return Err(DdlError::new(
            "22023",
            "COPY: CREDENTIAL applies to http(s):// and s3:// sources; every source is a server \
             file",
        ));
    }
    let client = if remote {
        Some(
            egress_client(
                Arc::clone(ctx.state.remote_sources.egress()),
                Arc::new(SystemLookup),
                EgressClientOptions {
                    timeout: None,
                    connect_timeout: CONNECT_TIMEOUT,
                    read_timeout: READ_TIMEOUT,
                    max_redirects: MAX_REDIRECTS,
                },
            )
            .map_err(|e| DdlError::internal(format!("COPY: HTTP client: {e}")))?,
        )
    } else {
        None
    };
    let mut names = Vec::with_capacity(sources.len());
    let mut fetches = Vec::with_capacity(sources.len());
    for source in sources {
        let (name, fetch) =
            transport(ctx, source, client.as_ref(), secret.as_ref(), options).await?;
        names.push(name);
        fetches.push(fetch);
    }
    let opener = TiffOpener::new().with_image(options.overview.unwrap_or(0));
    let fetch = Arc::new(SourceSet::new(fetches));
    drive_raster(ctx, array, &names, opener, fetch, options).await
}

/// The display name and transport of `source`.
async fn transport(
    ctx: &ArrayLoadCtx<'_>,
    source: &CopySource,
    client: Option<&reqwest::Client>,
    secret: Option<&CredentialSecret>,
    options: &CopyRasterOptions,
) -> Result<(String, SourceFetch), DdlError> {
    let client = || {
        client
            .cloned()
            .ok_or_else(|| DdlError::internal("COPY: a remote source has no HTTP client"))
    };
    match source {
        CopySource::File(path) => {
            let file = CopySourceFile::open(path).await?.into_std().await;
            Ok((
                path.clone(),
                SourceFetch::Local(LocalFile::new(file, path.clone())),
            ))
        }
        CopySource::Http(text) => {
            let url = reqwest::Url::parse(text)
                .map_err(|_| DdlError::new("22023", "COPY: a source URL is malformed"))?;
            let name = display_name(&url);
            ctx.state
                .remote_sources
                .egress()
                .check_parsed(&url)
                .map_err(|error| {
                    fetch_error(FetchError::Egress {
                        source_name: name.clone(),
                        error,
                    })
                })?;
            let auth = match secret {
                None => HttpAuth::None,
                Some(CredentialSecret::Bearer { token }) => {
                    HttpAuth::Bearer(Zeroizing::new(token.clone()))
                }
                Some(CredentialSecret::Sas { token }) => {
                    HttpAuth::Sas(Zeroizing::new(token.clone()))
                }
                Some(CredentialSecret::S3 { .. }) => {
                    return Err(wrong_type(&name, "s3", "s3:// sources"));
                }
            };
            Ok((
                name,
                SourceFetch::Http(HttpRange::new(client()?, url, auth)),
            ))
        }
        CopySource::S3 { bucket, key } => {
            let name = source.display();
            let client = client()?;
            let target = match secret {
                Some(CredentialSecret::S3 {
                    access_key_id,
                    secret_access_key,
                    region,
                    session_token,
                    endpoint,
                }) => S3Target {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    region: region.clone(),
                    endpoint: endpoint.clone(),
                    keys: Some(S3Keys {
                        access_key_id: Zeroizing::new(access_key_id.clone()),
                        secret_access_key: Zeroizing::new(secret_access_key.clone()),
                        session_token: session_token.clone().map(Zeroizing::new),
                    }),
                    requester_pays: options.requester_pays,
                },
                Some(other) => {
                    return Err(wrong_type(&name, other.type_name(), "http(s):// sources"));
                }
                None => S3Target {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    region: bucket_region(&client, bucket).await.map_err(fetch_error)?,
                    endpoint: None,
                    keys: None,
                    requester_pays: options.requester_pays,
                },
            };
            let store = ObjectStoreRange::new(client, target, ctx.state.remote_sources.egress())
                .map_err(fetch_error)?;
            Ok((name, SourceFetch::Store(store)))
        }
    }
}

/// Open credential `name` of the statement's tenant.
fn resolve_credential(ctx: &ArrayLoadCtx<'_>, name: &str) -> Result<CredentialSecret, DdlError> {
    let kek = ctx
        .state
        .remote_sources
        .credential_kek
        .as_ref()
        .ok_or_else(|| {
            DdlError::new(
                "55000",
                "COPY: CREDENTIAL needs a credential key: set [credential_encryption] key_path \
                 in the server config",
            )
        })?;
    let tenant_id = ctx.identity.tenant_id.as_u64();
    let row = ctx
        .state
        .credentials
        .catalog()
        .get_credential(tenant_id, name)?
        .ok_or_else(|| {
            DdlError::new("42704", format!("COPY: credential '{name}' does not exist"))
        })?;
    open_secret(kek, tenant_id, name, row.kind.type_name(), &row.sealed)
        .map_err(|e| DdlError::new("55000", format!("COPY: credential '{name}': {e}")))
}

fn wrong_type(source: &str, kind: &str, applies_to: &str) -> DdlError {
    DdlError::new(
        "22023",
        format!("COPY: '{source}': a credential of TYPE '{kind}' applies to {applies_to}"),
    )
}

fn fetch_error(e: FetchError) -> DdlError {
    DdlError::new(e.sqlstate(), format!("COPY: {e}"))
}
