//! Coordinated retention across shards.
//!
//! When retention drops partitions, all shards must drop the same time range
//! simultaneously. The coordinator broadcasts a retention command with
//! `(collection, drop_before_ts)`. Each shard applies locally.
//!
//! This prevents inconsistency: shard A has data from Jan 1, shard B already
//! dropped it → queries for that time range return partial results.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A coordinated retention command broadcast to all shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCommand {
    /// Collection to apply retention to.
    pub collection: String,
    /// Drop all partitions with max_ts < this timestamp.
    pub drop_before_ts: i64,
    /// Unique command ID for idempotency.
    pub command_id: u64,
}

/// Result from a single shard's retention execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRetentionResult {
    pub shard_id: u16,
    pub partitions_dropped: usize,
    pub bytes_reclaimed: u64,
    pub success: bool,
    pub error: Option<String>,
}

/// Coordinator for cross-shard retention.
pub struct CoordinatedRetention {
    /// Retention period per collection (ms). 0 = infinite.
    retention_periods: HashMap<String, u64>,
    /// Last executed command ID per collection for idempotency.
    last_command_ids: HashMap<String, u64>,
    /// Next command ID.
    next_command_id: u64,
}

impl CoordinatedRetention {
    pub fn new() -> Self {
        Self {
            retention_periods: HashMap::new(),
            last_command_ids: HashMap::new(),
            next_command_id: 1,
        }
    }

    /// Set retention period for a collection.
    pub fn set_retention(&mut self, collection: &str, period_ms: u64) {
        self.retention_periods
            .insert(collection.to_string(), period_ms);
    }

    /// Generate retention commands for all collections that need trimming.
    ///
    /// `now_ms` is the current timestamp. Returns commands to broadcast
    /// to all shards.
    pub fn generate_commands(&mut self, now_ms: i64) -> Vec<RetentionCommand> {
        let mut commands = Vec::new();

        for (collection, &period_ms) in &self.retention_periods {
            if period_ms == 0 {
                continue;
            }
            let drop_before = now_ms - period_ms as i64;
            let command_id = self.next_command_id;
            self.next_command_id += 1;

            commands.push(RetentionCommand {
                collection: collection.clone(),
                drop_before_ts: drop_before,
                command_id,
            });
            self.last_command_ids.insert(collection.clone(), command_id);
        }

        commands
    }

    /// Verify that all shards successfully applied a retention command.
    pub fn verify_results(
        command: &RetentionCommand,
        results: &[ShardRetentionResult],
    ) -> RetentionVerification {
        let total_shards = results.len();
        let successful = results.iter().filter(|r| r.success).count();
        let total_dropped: usize = results.iter().map(|r| r.partitions_dropped).sum();
        let total_reclaimed: u64 = results.iter().map(|r| r.bytes_reclaimed).sum();
        let failures: Vec<String> = results
            .iter()
            .filter(|r| !r.success)
            .filter_map(|r| {
                r.error
                    .as_ref()
                    .map(|e| format!("shard {}: {e}", r.shard_id))
            })
            .collect();

        RetentionVerification {
            collection: command.collection.clone(),
            command_id: command.command_id,
            total_shards,
            successful_shards: successful,
            total_partitions_dropped: total_dropped,
            total_bytes_reclaimed: total_reclaimed,
            failures,
        }
    }
}

impl Default for CoordinatedRetention {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of verifying a coordinated retention across all shards.
#[derive(Debug)]
pub struct RetentionVerification {
    pub collection: String,
    pub command_id: u64,
    pub total_shards: usize,
    pub successful_shards: usize,
    pub total_partitions_dropped: usize,
    pub total_bytes_reclaimed: u64,
    pub failures: Vec<String>,
}

impl RetentionVerification {
    pub fn all_succeeded(&self) -> bool {
        self.failures.is_empty() && self.successful_shards == self.total_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_commands() {
        let mut cr = CoordinatedRetention::new();
        cr.set_retention("metrics", 7 * 86_400_000); // 7 days
        cr.set_retention("logs", 3 * 86_400_000); // 3 days

        let now = 10 * 86_400_000i64; // day 10
        let commands = cr.generate_commands(now);
        assert_eq!(commands.len(), 2);

        let metrics_cmd = commands.iter().find(|c| c.collection == "metrics").unwrap();
        assert_eq!(metrics_cmd.drop_before_ts, 3 * 86_400_000); // day 3

        let logs_cmd = commands.iter().find(|c| c.collection == "logs").unwrap();
        assert_eq!(logs_cmd.drop_before_ts, 7 * 86_400_000); // day 7
    }

    #[test]
    fn verify_all_success() {
        let cmd = RetentionCommand {
            collection: "m".into(),
            drop_before_ts: 1000,
            command_id: 1,
        };
        let results = vec![
            ShardRetentionResult {
                shard_id: 0,
                partitions_dropped: 5,
                bytes_reclaimed: 1000,
                success: true,
                error: None,
            },
            ShardRetentionResult {
                shard_id: 1,
                partitions_dropped: 3,
                bytes_reclaimed: 800,
                success: true,
                error: None,
            },
        ];
        let v = CoordinatedRetention::verify_results(&cmd, &results);
        assert!(v.all_succeeded());
        assert_eq!(v.total_partitions_dropped, 8);
        assert_eq!(v.total_bytes_reclaimed, 1800);
    }

    #[test]
    fn verify_partial_failure() {
        let cmd = RetentionCommand {
            collection: "m".into(),
            drop_before_ts: 1000,
            command_id: 1,
        };
        let results = vec![
            ShardRetentionResult {
                shard_id: 0,
                partitions_dropped: 5,
                bytes_reclaimed: 1000,
                success: true,
                error: None,
            },
            ShardRetentionResult {
                shard_id: 1,
                partitions_dropped: 0,
                bytes_reclaimed: 0,
                success: false,
                error: Some("disk full".into()),
            },
        ];
        let v = CoordinatedRetention::verify_results(&cmd, &results);
        assert!(!v.all_succeeded());
        assert_eq!(v.failures.len(), 1);
    }

    #[test]
    fn infinite_retention_skipped() {
        let mut cr = CoordinatedRetention::new();
        cr.set_retention("metrics", 0); // infinite
        let commands = cr.generate_commands(1_000_000);
        assert!(commands.is_empty());
    }
}
