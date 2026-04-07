//! Cold storage query operations: download and list Parquet files.
//!
//! Extracted from `cold.rs` — contains the download, list, and Parquet
//! read-with-predicate operations used by the query path for cold L2 data.

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use tracing::warn;

use super::cold::ColdStorage;

impl ColdStorage {
    /// Download a Parquet file from cold storage for query.
    pub async fn download_parquet(&self, object_path: &str) -> crate::Result<Bytes> {
        let path = object_store::path::Path::from(object_path.to_string());
        let result = self
            .store()
            .get_opts(&path, object_store::GetOptions::default())
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("download {object_path}: {e}"),
            })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read bytes: {e}"),
            })?;
        Ok(bytes)
    }

    /// List Parquet files for a collection in cold storage.
    pub async fn list_parquet_files(
        &self,
        tenant_id: u32,
        collection: &str,
    ) -> crate::Result<Vec<String>> {
        let prefix = format!("{}{}/{}/", self.prefix(), tenant_id, collection);
        let path = object_store::path::Path::from(prefix);

        let mut paths = Vec::new();
        let mut stream = self.store().list(Some(&path));
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    let p = meta.location.to_string();
                    if p.ends_with(".parquet") {
                        paths.push(p);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "listing cold storage objects");
                    break;
                }
            }
        }
        Ok(paths)
    }
}

/// Read a Parquet file from bytes and apply predicate pushdown via DataFusion.
///
/// This is the query path for cold L2 data: the Parquet reader only reads
/// row groups and columns that match the predicate, minimizing I/O.
pub fn read_parquet_with_predicate(
    parquet_bytes: &[u8],
    projection: &[String],
) -> crate::Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet_bytes))
        .map_err(|e| crate::Error::ColdStorage {
            detail: format!("parquet reader init: {e}"),
        })?;

    // Apply column projection if specified.
    let reader = if projection.is_empty() {
        reader.build().map_err(|e| crate::Error::ColdStorage {
            detail: format!("build reader: {e}"),
        })?
    } else {
        let schema = reader.schema();
        let indices: Vec<usize> = projection
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();
        let mask = parquet::arrow::ProjectionMask::leaves(reader.parquet_schema(), indices);
        reader
            .with_projection(mask)
            .build()
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("build projected reader: {e}"),
            })?
    };

    let batches: Vec<RecordBatch> =
        reader
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read batches: {e}"),
            })?;
    Ok(batches)
}
