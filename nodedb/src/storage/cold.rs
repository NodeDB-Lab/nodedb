//! L2 cold storage: Parquet encoding + S3-compatible object store.
//!
//! Cold L1 data is encoded as Parquet files and uploaded to any
//! S3-compatible object store (AWS S3, MinIO, Bunny, R2, B2, GCS).
//!
//! **Not vendor-locked**: uses `object_store` crate's generic `ObjectStore`
//! trait. The `AmazonS3Builder::with_endpoint()` method connects to any
//! S3-compatible API — not just AWS.
//!
//! ## Data Flow
//!
//! ```text
//! L1 NVMe segments → Parquet encode → Upload to S3-compatible store
//!                                    → Register in catalog
//!                                    → Query via DataFusion predicate pushdown
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::info;

/// Configuration for the cold storage layer.
#[derive(Debug, Clone)]
pub struct ColdStorageConfig {
    /// S3-compatible endpoint URL (e.g., "https://s3.us-east-1.amazonaws.com",
    /// "https://storage.bunnycdn.com", "http://localhost:9000" for MinIO).
    /// Empty = use local filesystem as cold storage (dev/testing).
    pub endpoint: String,
    /// Bucket name (or container name for Azure).
    pub bucket: String,
    /// Prefix path within the bucket (e.g., "nodedb/cold/").
    pub prefix: String,
    /// Access key (empty = use instance credentials / IAM role).
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Region (required for AWS S3, ignored by most S3-compatible stores).
    pub region: String,
    /// Local directory for cold storage (used when endpoint is empty).
    pub local_dir: Option<PathBuf>,
    /// Parquet compression algorithm.
    pub compression: ParquetCompression,
    /// Target Parquet row group size.
    pub row_group_size: usize,
}

/// Supported Parquet compression algorithms.
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    None,
    Snappy,
    Zstd,
    Lz4,
}

impl Default for ColdStorageConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: "nodedb-cold".into(),
            prefix: "data/".into(),
            access_key: String::new(),
            secret_key: String::new(),
            region: "us-east-1".into(),
            local_dir: None,
            compression: ParquetCompression::Zstd,
            row_group_size: 65_536,
        }
    }
}

/// Cold storage client: Parquet encoding + S3-compatible upload.
pub struct ColdStorage {
    config: ColdStorageConfig,
    store: Arc<dyn ObjectStore>,
    /// Total bytes uploaded.
    bytes_uploaded: std::sync::atomic::AtomicU64,
    /// Total files uploaded.
    files_uploaded: std::sync::atomic::AtomicU64,
}

impl ColdStorage {
    /// Create a cold storage client.
    ///
    /// Connects to the configured S3-compatible endpoint, or uses
    /// local filesystem if no endpoint is configured.
    pub fn new(config: ColdStorageConfig) -> crate::Result<Self> {
        let store: Arc<dyn ObjectStore> = if config.endpoint.is_empty() {
            // Local filesystem (dev/testing).
            let dir = config
                .local_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("/tmp/nodedb/cold"));
            std::fs::create_dir_all(&dir)?;
            Arc::new(LocalFileSystem::new_with_prefix(&dir).map_err(|e| {
                crate::Error::ColdStorage {
                    detail: format!("local cold storage: {e}"),
                }
            })?)
        } else {
            // S3-compatible object store.
            let mut builder = AmazonS3Builder::new()
                .with_endpoint(&config.endpoint)
                .with_bucket_name(&config.bucket)
                .with_region(&config.region)
                .with_allow_http(config.endpoint.starts_with("http://"));

            if !config.access_key.is_empty() {
                builder = builder
                    .with_access_key_id(&config.access_key)
                    .with_secret_access_key(&config.secret_key);
            }

            let s3 = builder.build().map_err(|e| crate::Error::ColdStorage {
                detail: format!("S3 client init: {e}"),
            })?;
            Arc::new(s3)
        };

        Ok(Self {
            config,
            store,
            bytes_uploaded: std::sync::atomic::AtomicU64::new(0),
            files_uploaded: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Encode document rows as a Parquet file and upload to cold storage.
    ///
    /// Returns the object path where the Parquet file was stored.
    pub async fn encode_and_upload(
        &self,
        collection: &str,
        tenant_id: u32,
        rows: &[(String, serde_json::Value)],
        min_lsn: u64,
        max_lsn: u64,
    ) -> crate::Result<String> {
        if rows.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: "no rows to encode".into(),
            });
        }

        // Build Arrow schema from first row.
        let first_obj = rows[0]
            .1
            .as_object()
            .ok_or_else(|| crate::Error::ColdStorage {
                detail: "first row is not an object".into(),
            })?;

        let mut fields = vec![Field::new("_id", DataType::Utf8, false)];
        for (key, value) in first_obj {
            let dt = match value {
                serde_json::Value::Number(n) if n.is_i64() => DataType::Int64,
                serde_json::Value::Number(_) => DataType::Float64,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(key, dt, true));
        }
        let schema = Arc::new(Schema::new(fields));

        // Build column arrays.
        let field_names: Vec<String> = first_obj.keys().cloned().collect();
        let mut ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut columns: Vec<Vec<serde_json::Value>> =
            vec![Vec::with_capacity(rows.len()); field_names.len()];

        for (doc_id, data) in rows {
            ids.push(doc_id.clone());
            let obj = data.as_object();
            for (i, name) in field_names.iter().enumerate() {
                let val = obj
                    .and_then(|o| o.get(name))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                columns[i].push(val);
            }
        }

        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(ids))];
        for (i, field) in schema.fields().iter().skip(1).enumerate() {
            let arr: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    let vals: Vec<Option<i64>> = columns[i].iter().map(|v| v.as_i64()).collect();
                    Arc::new(Int64Array::from(vals))
                }
                DataType::Float64 => {
                    let vals: Vec<Option<f64>> = columns[i].iter().map(|v| v.as_f64()).collect();
                    Arc::new(Float64Array::from(vals))
                }
                _ => {
                    let vals: Vec<Option<String>> = columns[i]
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Null => None,
                            other => Some(other.to_string()),
                        })
                        .collect();
                    Arc::new(StringArray::from(vals))
                }
            };
            arrays.push(arr);
        }

        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            crate::Error::ColdStorage {
                detail: format!("build RecordBatch: {e}"),
            }
        })?;

        // Write Parquet — CPU-intensive compression runs off the async executor.
        let compression = match self.config.compression {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
        };
        let row_group_size = self.config.row_group_size;

        let buf = tokio::task::spawn_blocking(move || {
            let props = WriterProperties::builder()
                .set_compression(compression)
                .set_max_row_group_size(row_group_size)
                .build();
            let mut buf: Vec<u8> = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).map_err(|e| {
                crate::Error::ColdStorage {
                    detail: format!("parquet writer init: {e}"),
                }
            })?;
            writer
                .write(&batch)
                .map_err(|e| crate::Error::ColdStorage {
                    detail: format!("parquet write: {e}"),
                })?;
            writer.close().map_err(|e| crate::Error::ColdStorage {
                detail: format!("parquet close: {e}"),
            })?;
            Ok::<_, crate::Error>(buf)
        })
        .await
        .map_err(|e| crate::Error::ColdStorage {
            detail: format!("parquet encoding task: {e}"),
        })??;

        let file_size = buf.len();

        // Upload to object store.
        let object_path = format!(
            "{}{}/{}/lsn-{}-{}.parquet",
            self.config.prefix, tenant_id, collection, min_lsn, max_lsn
        );
        let path = object_store::path::Path::from(object_path.clone());

        self.store
            .put_opts(
                &path,
                PutPayload::from(buf),
                object_store::PutOptions::default(),
            )
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("upload to {object_path}: {e}"),
            })?;

        self.bytes_uploaded
            .fetch_add(file_size as u64, std::sync::atomic::Ordering::Relaxed);
        self.files_uploaded
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        info!(
            collection,
            tenant_id,
            min_lsn,
            max_lsn,
            file_size,
            path = %object_path,
            "parquet uploaded to cold storage"
        );

        Ok(object_path)
    }

    /// Upload a raw WAL segment file to cold storage.
    ///
    /// Used for continuous WAL archiving (RPO guarantee).
    pub async fn upload_wal_segment(
        &self,
        segment_path: &Path,
        segment_name: &str,
    ) -> crate::Result<String> {
        let path_buf = segment_path.to_path_buf();
        let segment_display = segment_path.display().to_string();
        let data = tokio::task::spawn_blocking(move || std::fs::read(&path_buf))
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("spawn_blocking join: {e}"),
            })?
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read WAL segment {segment_display}: {e}"),
            })?;

        let object_path = format!("{}wal/{}", self.config.prefix, segment_name);
        let path = object_store::path::Path::from(object_path.clone());

        self.store
            .put_opts(
                &path,
                PutPayload::from(data),
                object_store::PutOptions::default(),
            )
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("upload WAL segment: {e}"),
            })?;

        info!(segment_name, path = %object_path, "WAL segment archived to cold storage");
        Ok(object_path)
    }

    /// Total bytes uploaded to cold storage.
    pub fn bytes_uploaded(&self) -> u64 {
        self.bytes_uploaded
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total files uploaded.
    pub fn files_uploaded(&self) -> u64 {
        self.files_uploaded
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the underlying object store (for DataFusion registration).
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Access the object store (used by cold_query module).
    pub(super) fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Access the configured prefix (used by cold_query module).
    pub(super) fn prefix(&self) -> &str {
        &self.config.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cold_query::read_parquet_with_predicate;

    #[tokio::test]
    async fn local_encode_and_download() {
        let dir = tempfile::tempdir().unwrap();
        let config = ColdStorageConfig {
            local_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let cold = ColdStorage::new(config).unwrap();

        let rows = vec![
            ("d1".into(), serde_json::json!({"name": "alice", "age": 30})),
            ("d2".into(), serde_json::json!({"name": "bob", "age": 25})),
        ];
        let path = cold
            .encode_and_upload("users", 1, &rows, 100, 200)
            .await
            .unwrap();

        assert!(path.contains("users"));
        assert!(path.ends_with(".parquet"));
        assert_eq!(cold.files_uploaded(), 1);

        // Download and verify.
        let bytes = cold.download_parquet(&path).await.unwrap();
        let batches = read_parquet_with_predicate(&bytes, &[]).unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn projected_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = ColdStorageConfig {
            local_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let cold = ColdStorage::new(config).unwrap();

        let rows = vec![(
            "d1".into(),
            serde_json::json!({"name": "alice", "score": 95.5, "rank": 1}),
        )];
        let path = cold
            .encode_and_upload("results", 1, &rows, 1, 1)
            .await
            .unwrap();

        let bytes = cold.download_parquet(&path).await.unwrap();
        let batches = read_parquet_with_predicate(&bytes, &["name".into()]).unwrap();
        assert_eq!(batches[0].num_columns(), 1); // Only "name" projected.
    }
}
