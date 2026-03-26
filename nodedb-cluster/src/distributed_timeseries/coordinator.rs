//! Timeseries scatter-gather coordinator.
//!
//! Runs on the Control Plane. Dispatches aggregation queries to all shards
//! via `VShardEnvelope`, collects partial aggregates, and merges them.
//!
//! Follows the same pattern as `distributed_graph::BspCoordinator`:
//! coordinator → VShardEnvelope per shard → collect responses → merge.

use std::collections::HashMap;

use super::merge::{PartialAgg, PartialAggMerger};
use super::retention::RetentionCommand;
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Scatter-gather coordinator for cross-shard timeseries aggregation.
pub struct TsCoordinator {
    /// Source node ID (this coordinator's node).
    pub source_node: u64,
    /// Target shard IDs to fan out to.
    pub shard_ids: Vec<u16>,
    /// Collected responses, keyed by shard ID.
    responses: HashMap<u16, Vec<PartialAgg>>,
    /// Whether the scatter phase is complete.
    pub complete: bool,
}

impl TsCoordinator {
    pub fn new(source_node: u64, shard_ids: Vec<u16>) -> Self {
        Self {
            source_node,
            shard_ids,
            responses: HashMap::new(),
            complete: false,
        }
    }

    /// Build scatter envelopes for a timeseries aggregation query.
    ///
    /// Returns one `VShardEnvelope` per shard, each containing the query
    /// parameters as JSON payload. The caller sends these via the QUIC
    /// transport (same as graph algorithm barriers).
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        start_ms: i64,
        end_ms: i64,
        value_column: &str,
        bucket_interval_ms: i64,
    ) -> Vec<(u16, VShardEnvelope)> {
        let payload = serde_json::json!({
            "collection": collection,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "value_column": value_column,
            "bucket_interval_ms": bucket_interval_ms,
        });
        let payload_bytes = // Safety: json!() macro output is always serializable.
            serde_json::to_vec(&payload).expect("json! is always serializable");

        self.shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsScatterRequest,
                    self.source_node,
                    0, // target_node resolved by routing table
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect()
    }

    /// Record a shard's response (partial aggregates).
    pub fn record_response(&mut self, shard_id: u16, partials: Vec<PartialAgg>) {
        self.responses.insert(shard_id, partials);
        if self.responses.len() == self.shard_ids.len() {
            self.complete = true;
        }
    }

    /// Check if all shards have responded.
    pub fn all_responded(&self) -> bool {
        self.complete
    }

    /// Merge all shard responses into the final result.
    pub fn merge_results(&self) -> Vec<PartialAgg> {
        let mut merger = PartialAggMerger::new();
        for partials in self.responses.values() {
            merger.add_shard_results(partials);
        }
        merger.finalize()
    }

    /// Number of shards that have responded.
    pub fn response_count(&self) -> usize {
        self.responses.len()
    }

    /// Build retention command envelopes for coordinated retention.
    pub fn build_retention_envelopes(
        &self,
        command: &RetentionCommand,
    ) -> Vec<(u16, VShardEnvelope)> {
        let payload_bytes =
            serde_json::to_vec(command).expect("RetentionCommand is always serializable");

        self.shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsRetentionCommand,
                    self.source_node,
                    0,
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect()
    }

    /// Build S3 archive command envelopes.
    pub fn build_archive_envelopes(
        &self,
        collection: &str,
        archive_before_ts: i64,
        s3_prefix: &str,
    ) -> Vec<(u16, VShardEnvelope)> {
        let payload = serde_json::json!({
            "collection": collection,
            "archive_before_ts": archive_before_ts,
            "s3_prefix": s3_prefix,
        });
        let payload_bytes = // Safety: json!() macro output is always serializable.
            serde_json::to_vec(&payload).expect("json! is always serializable");

        self.shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsArchiveCommand,
                    self.source_node,
                    0,
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1, 2]);
        let envs = coord.build_scatter_envelopes("metrics", 1000, 2000, "cpu", 60_000);
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
            assert!(!env.payload.is_empty());
        }
    }

    #[test]
    fn collect_and_merge() {
        let mut coord = TsCoordinator::new(1, vec![0, 1]);
        assert!(!coord.all_responded());

        coord.record_response(
            0,
            vec![PartialAgg {
                count: 100,
                sum: 5000.0,
                ..PartialAgg::from_single(0, 1, 50.0)
            }],
        );
        assert!(!coord.all_responded());

        coord.record_response(
            1,
            vec![PartialAgg {
                count: 80,
                sum: 4000.0,
                ..PartialAgg::from_single(0, 2, 50.0)
            }],
        );
        assert!(coord.all_responded());

        let merged = coord.merge_results();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].count, 180);
        assert_eq!(merged[0].sum, 9000.0);
    }

    #[test]
    fn retention_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1, 2, 3]);
        let cmd = RetentionCommand {
            collection: "metrics".into(),
            drop_before_ts: 1000,
            command_id: 42,
        };
        let envs = coord.build_retention_envelopes(&cmd);
        assert_eq!(envs.len(), 4);
        for (_, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsRetentionCommand);
        }
    }

    #[test]
    fn archive_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1]);
        let envs = coord.build_archive_envelopes("metrics", 5000, "nodedb/v1/cluster-abc");
        assert_eq!(envs.len(), 2);
        for (_, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsArchiveCommand);
        }
    }
}
