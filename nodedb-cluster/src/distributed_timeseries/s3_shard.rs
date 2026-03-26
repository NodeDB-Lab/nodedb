//! Cross-shard S3 archival with shard-aware key naming.
//!
//! Each shard archives its own partitions with shard-specific prefixes.
//! The query engine can read archived data from any shard's S3 prefix.
//!
//! S3 key format:
//! ```text
//! {bucket}/{prefix}/shard-{shard_id}/{collection}/ts-{start}_{end}/
//! ```

use serde::{Deserialize, Serialize};

/// Configuration for shard-aware S3 archival.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardedS3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix (e.g., "nodedb/v1/cluster-abc").
    pub prefix: String,
    /// Total number of shards in the cluster.
    pub total_shards: u16,
}

impl ShardedS3Config {
    /// Generate the S3 key prefix for a specific shard.
    pub fn shard_prefix(&self, shard_id: u16) -> String {
        format!("{}/shard-{:04}", self.prefix, shard_id)
    }

    /// Generate the full S3 key for a partition on a specific shard.
    pub fn partition_key(&self, shard_id: u16, collection: &str, partition_name: &str) -> String {
        format!(
            "{}/shard-{:04}/{collection}/{partition_name}",
            self.prefix, shard_id
        )
    }

    /// Generate S3 key for the packed partition file.
    pub fn packed_partition_key(
        &self,
        shard_id: u16,
        collection: &str,
        min_ts: i64,
        max_ts: i64,
    ) -> String {
        format!(
            "{}/shard-{:04}/{collection}/ts-{min_ts}_{max_ts}.ndpk",
            self.prefix, shard_id
        )
    }

    /// List all shard prefixes (for cross-shard scan).
    pub fn all_shard_prefixes(&self) -> Vec<String> {
        (0..self.total_shards)
            .map(|id| self.shard_prefix(id))
            .collect()
    }

    /// List all S3 key prefixes for a collection across all shards.
    ///
    /// Used by the query engine to scan archived data from all shards.
    pub fn collection_prefixes(&self, collection: &str) -> Vec<String> {
        (0..self.total_shards)
            .map(|id| format!("{}/shard-{:04}/{collection}/", self.prefix, id))
            .collect()
    }
}

/// Metadata for an archived partition on S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedPartition {
    /// S3 key.
    pub s3_key: String,
    /// Which shard archived this.
    pub shard_id: u16,
    /// Collection name.
    pub collection: String,
    /// Time range.
    pub min_ts: i64,
    pub max_ts: i64,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Archive timestamp (when the partition was uploaded).
    pub archived_at_ms: i64,
}

/// Plan for archiving a partition to S3.
#[derive(Debug, Clone)]
pub struct ArchivePlan {
    /// Source partition directory (local NVMe).
    pub source_dir: String,
    /// Target S3 key.
    pub target_key: String,
    /// Shard that owns this partition.
    pub shard_id: u16,
    /// Collection name.
    pub collection: String,
    /// Partition time range.
    pub min_ts: i64,
    pub max_ts: i64,
}

impl ShardedS3Config {
    /// Generate archive plans for partitions on a specific shard.
    pub fn plan_archive(
        &self,
        shard_id: u16,
        collection: &str,
        partitions: &[(String, i64, i64)], // (source_dir, min_ts, max_ts)
    ) -> Vec<ArchivePlan> {
        partitions
            .iter()
            .map(|(source, min_ts, max_ts)| ArchivePlan {
                source_dir: source.clone(),
                target_key: self.packed_partition_key(shard_id, collection, *min_ts, *max_ts),
                shard_id,
                collection: collection.to_string(),
                min_ts: *min_ts,
                max_ts: *max_ts,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ShardedS3Config {
        ShardedS3Config {
            bucket: "my-bucket".into(),
            prefix: "nodedb/v1/cluster-abc".into(),
            total_shards: 10,
        }
    }

    #[test]
    fn shard_prefix() {
        let cfg = test_config();
        assert_eq!(cfg.shard_prefix(3), "nodedb/v1/cluster-abc/shard-0003");
    }

    #[test]
    fn partition_key() {
        let cfg = test_config();
        assert_eq!(
            cfg.partition_key(5, "metrics", "ts-1000_2000"),
            "nodedb/v1/cluster-abc/shard-0005/metrics/ts-1000_2000"
        );
    }

    #[test]
    fn packed_key() {
        let cfg = test_config();
        assert_eq!(
            cfg.packed_partition_key(7, "metrics", 1000, 2000),
            "nodedb/v1/cluster-abc/shard-0007/metrics/ts-1000_2000.ndpk"
        );
    }

    #[test]
    fn all_shard_prefixes() {
        let cfg = test_config();
        let prefixes = cfg.all_shard_prefixes();
        assert_eq!(prefixes.len(), 10);
        assert!(prefixes[0].ends_with("shard-0000"));
        assert!(prefixes[9].ends_with("shard-0009"));
    }

    #[test]
    fn collection_prefixes() {
        let cfg = test_config();
        let prefixes = cfg.collection_prefixes("metrics");
        assert_eq!(prefixes.len(), 10);
        assert!(prefixes[0].contains("shard-0000/metrics/"));
    }

    #[test]
    fn archive_plan() {
        let cfg = test_config();
        let partitions = vec![
            ("/data/ts-1000_2000".into(), 1000i64, 2000i64),
            ("/data/ts-3000_4000".into(), 3000, 4000),
        ];
        let plans = cfg.plan_archive(2, "metrics", &partitions);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].shard_id, 2);
        assert!(plans[0].target_key.contains("shard-0002"));
    }
}
