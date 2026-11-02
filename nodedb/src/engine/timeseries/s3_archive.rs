// SPDX-License-Identifier: BUSL-1.1

//! S3-compatible cold storage archival for timeseries partitions.
//!
//! Uploads sealed partitions older than `archive_after` to any
//! S3-compatible object store (AWS S3, MinIO, R2, DigitalOcean Spaces,
//! Garage, etc.) as Parquet files.
//!
//! Uses the `object_store` crate which supports any S3-compatible
//! endpoint via the `endpoint` URL parameter.

use sonic_rs;
use std::path::Path;

use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, ObjectStoreExt};

use super::columnar_segment::{ColumnarSegmentReader, SegmentError};
use super::partition_registry::PartitionRegistry;

use nodedb_types::timeseries::PartitionState;
use nodedb_wal::crypto::WalEncryptionKey;

/// Configuration for S3-compatible archival.
#[derive(Debug, Clone)]
pub struct S3ArchiveConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix (e.g., "nodedb/timeseries/").
    pub prefix: String,
    /// S3-compatible endpoint URL (e.g., "http://localhost:9000" for MinIO).
    /// None = default AWS S3 endpoint.
    pub endpoint: Option<String>,
    /// AWS region (or equivalent for S3-compatible services).
    pub region: String,
    /// Access key ID.
    pub access_key_id: Option<String>,
    /// Secret access key.
    pub secret_access_key: Option<String>,
    /// Compression codec for Parquet files.
    pub compression: ParquetCompression,
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    Zstd,
    Lz4,
    Snappy,
    None,
}

impl Default for S3ArchiveConfig {
    fn default() -> Self {
        Self {
            bucket: "nodedb-archive".into(),
            prefix: "timeseries/".into(),
            endpoint: None,
            region: "us-east-1".into(),
            access_key_id: None,
            secret_access_key: None,
            compression: ParquetCompression::Zstd,
        }
    }
}

/// Build an S3-compatible object store client.
///
/// Works with AWS S3, MinIO, Cloudflare R2, DigitalOcean Spaces,
/// Backblaze B2, Garage, and any other S3-compatible service.
pub fn build_object_store(
    config: &S3ArchiveConfig,
) -> Result<object_store::aws::AmazonS3, SegmentError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region);

    if let Some(ref endpoint) = config.endpoint {
        builder = builder.with_endpoint(endpoint);
        // S3-compatible services need virtual-hosted-style disabled.
        builder = builder.with_virtual_hosted_style_request(false);
    }

    if let Some(ref key) = config.access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(ref secret) = config.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }

    // Allow HTTP for local dev (MinIO on localhost).
    builder = builder.with_allow_http(true);

    builder
        .build()
        .map_err(|e| SegmentError::Io(format!("build S3 client: {e}")))
}

/// Archive a single partition to S3 as columnar files.
///
/// Uploads each column file + metadata to the object store under
/// `{prefix}{collection}/{partition_dir}/`.
///
/// Returns the S3 key of the uploaded metadata file.
pub async fn archive_partition(
    store: &impl ObjectStore,
    config: &S3ArchiveConfig,
    collection: &str,
    partition_dir: &Path,
    partition_dir_name: &str,
    kek: Option<&WalEncryptionKey>,
) -> Result<String, SegmentError> {
    // Read partition metadata to verify it's sealed.
    let meta = ColumnarSegmentReader::read_meta(partition_dir, kek)?;
    if meta.state != PartitionState::Sealed && meta.state != PartitionState::Merged {
        return Err(SegmentError::Io(format!(
            "cannot archive partition in state {:?}",
            meta.state
        )));
    }

    let key_prefix = format!("{}{}/{}/", config.prefix, collection, partition_dir_name);

    // Upload all files in the partition directory.
    let entries =
        std::fs::read_dir(partition_dir).map_err(|e| SegmentError::Io(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::Io(format!("dir entry: {e}")))?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        let file_path = entry.path();

        if !file_path.is_file() {
            continue;
        }

        let data = std::fs::read(&file_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", file_path.display())))?;

        let object_key = format!("{key_prefix}{file_name_str}");
        let location = object_store::path::Path::from(object_key.clone());

        store
            .put(&location, object_store::PutPayload::from(data))
            .await
            .map_err(|e| SegmentError::Io(format!("S3 put {object_key}: {e}")))?;
    }

    // Upload a marker file indicating successful archival.
    let marker_key = format!("{key_prefix}_archived");
    let marker_location = object_store::path::Path::from(marker_key.clone());
    let marker_data = sonic_rs::to_vec(&serde_json::json!({
        "collection": collection,
        "partition": partition_dir_name,
        "min_ts": meta.min_ts,
        "max_ts": meta.max_ts,
        "row_count": meta.row_count,
        "archived_at_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    }))
    .unwrap_or_default();

    store
        .put(
            &marker_location,
            object_store::PutPayload::from(marker_data),
        )
        .await
        .map_err(|e| SegmentError::Io(format!("S3 put marker: {e}")))?;

    Ok(marker_key)
}

/// The collection one archival cycle reads its partitions from.
pub struct ArchiveSource<'a> {
    pub registry: &'a mut PartitionRegistry,
    pub collection: &'a str,
    pub base_dir: &'a Path,
    /// The timeseries segment key the partitions were written with, or
    /// `None` for plaintext partitions.
    pub kek: Option<&'a WalEncryptionKey>,
}

/// Run an archival cycle: find partitions older than `archive_after`,
/// upload to S3, mark as `Archived` in the registry.
///
/// Returns the number of partitions archived.
pub async fn run_archive_cycle(
    store: &impl ObjectStore,
    config: &S3ArchiveConfig,
    source: ArchiveSource<'_>,
    archive_after_ms: u64,
    now_ms: i64,
) -> Result<usize, SegmentError> {
    let ArchiveSource {
        registry,
        collection,
        base_dir,
        kek,
    } = source;
    if archive_after_ms == 0 {
        return Ok(0); // Archival disabled.
    }

    let cutoff = now_ms - archive_after_ms as i64;
    let mut archived = 0;

    // Collect candidates (sealed or merged, older than cutoff).
    let candidates: Vec<(super::partition_registry::PartitionKey, String)> = registry
        .iter()
        .filter(|(_, e)| {
            (e.meta.state == PartitionState::Sealed || e.meta.state == PartitionState::Merged)
                && e.meta.max_ts < cutoff
        })
        .map(|(partition_key, e)| (partition_key.clone(), e.dir_name.clone()))
        .collect();

    for (partition_key, dir_name) in candidates {
        let partition_dir = base_dir.join(&dir_name);
        if !partition_dir.exists() {
            continue;
        }

        match archive_partition(store, config, collection, &partition_dir, &dir_name, kek).await {
            Ok(key) => {
                tracing::info!(
                    collection,
                    partition = dir_name,
                    s3_key = key,
                    "partition archived to S3"
                );
                // Mark as archived in registry.
                if let Some(entry) = registry.get_mut(&partition_key) {
                    entry.meta.state = PartitionState::Archived;
                }
                archived += 1;
            }
            Err(e) => {
                tracing::warn!(
                    collection,
                    partition = dir_name,
                    error = %e,
                    "failed to archive partition"
                );
                // Don't delete local copy on failure — crash-safe.
            }
        }
    }

    // Persist manifest after archival state changes.
    if archived > 0
        && let Err(e) = registry.persist(base_dir, "partition_manifest.json")
    {
        tracing::warn!(error = %e, "failed to persist manifest after archival");
    }

    Ok(archived)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = S3ArchiveConfig::default();
        assert_eq!(cfg.bucket, "nodedb-archive");
        assert!(cfg.endpoint.is_none());
    }

    #[test]
    fn build_store_with_custom_endpoint() {
        // This tests that the builder accepts MinIO-style endpoints.
        // Actual connection would fail without a running MinIO instance.
        let cfg = S3ArchiveConfig {
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: Some("minioadmin".into()),
            secret_access_key: Some("minioadmin".into()),
            ..Default::default()
        };
        let store = build_object_store(&cfg);
        assert!(
            store.is_ok(),
            "should build with custom endpoint: {store:?}"
        );
    }
}
