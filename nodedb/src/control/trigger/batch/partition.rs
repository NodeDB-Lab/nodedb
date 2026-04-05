//! Branch partitioning for RowAtATime triggers.
//!
//! When a trigger has row-dependent control flow (different DML targets per
//! IF branch), the batch is partitioned by the branch predicate. Each partition
//! is dispatched separately with its own bindings.
//!
//! Circuit breaker: if partitioning produces more than `max_partitions` groups,
//! fall back to row-at-a-time to avoid memory spikes.

use super::collector::TriggerBatchRow;

/// Default maximum partitions before circuit breaker triggers.
pub const DEFAULT_MAX_PARTITIONS: usize = 32;

/// A partition of rows that share the same branch path.
#[derive(Debug)]
pub struct BatchPartition {
    /// Partition key (e.g., the value that determined the branch).
    pub key: String,
    /// Indices into the original batch for rows in this partition.
    pub row_indices: Vec<usize>,
}

/// Result of partitioning a batch.
pub enum PartitionResult {
    /// Partitioning succeeded within the circuit breaker limit.
    Partitions(Vec<BatchPartition>),
    /// Too many partitions — fall back to row-at-a-time execution.
    CircuitBroken { partition_count: usize },
}

/// Partition a batch of rows by a key extracted from each row.
///
/// `key_fn` maps each row to a partition key (e.g., the branch a row takes
/// in an IF/ELSE trigger body). Rows with the same key go to the same partition.
///
/// If the number of unique keys exceeds `max_partitions`, returns
/// `CircuitBroken` and the caller should fall back to row-at-a-time.
pub fn partition_batch<F>(
    rows: &[TriggerBatchRow],
    max_partitions: usize,
    key_fn: F,
) -> PartitionResult
where
    F: Fn(&TriggerBatchRow) -> String,
{
    let mut partitions: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();

    for (idx, row) in rows.iter().enumerate() {
        let key = key_fn(row);
        partitions.entry(key).or_default().push(idx);

        // Early exit if we exceed the limit.
        if partitions.len() > max_partitions {
            return PartitionResult::CircuitBroken {
                partition_count: partitions.len(),
            };
        }
    }

    let result = partitions
        .into_iter()
        .map(|(key, row_indices)| BatchPartition { key, row_indices })
        .collect();

    PartitionResult::Partitions(result)
}

/// Partition rows by a field value from NEW fields.
///
/// Convenience wrapper: extracts `field_name` from each row's NEW fields
/// and uses the string representation as the partition key.
pub fn partition_by_field(
    rows: &[TriggerBatchRow],
    field_name: &str,
    max_partitions: usize,
) -> PartitionResult {
    partition_batch(rows, max_partitions, |row| {
        row.new_fields()
            .and_then(|m| m.get(field_name))
            .map(|v| match v {
                nodedb_types::Value::String(s) => s.clone(),
                other => other.to_sql_literal(),
            })
            .unwrap_or_else(|| "__null__".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_with_field(id: &str, field: &str, val: &str) -> TriggerBatchRow {
        let mut map = std::collections::HashMap::new();
        map.insert(
            field.to_string(),
            nodedb_types::Value::String(val.to_string()),
        );
        TriggerBatchRow::from_decoded(Some(map), None, id.to_string())
    }

    #[test]
    fn partition_by_key() {
        let rows = vec![
            row_with_field("r1", "status", "active"),
            row_with_field("r2", "status", "inactive"),
            row_with_field("r3", "status", "active"),
            row_with_field("r4", "status", "inactive"),
        ];

        match partition_by_field(&rows, "status", 32) {
            PartitionResult::Partitions(parts) => {
                assert_eq!(parts.len(), 2);
                let active = parts.iter().find(|p| p.key == "active").unwrap();
                assert_eq!(active.row_indices, vec![0, 2]);
                let inactive = parts.iter().find(|p| p.key == "inactive").unwrap();
                assert_eq!(inactive.row_indices, vec![1, 3]);
            }
            PartitionResult::CircuitBroken { .. } => panic!("unexpected circuit break"),
        }
    }

    #[test]
    fn circuit_breaker_triggers() {
        let rows: Vec<_> = (0..50)
            .map(|i| row_with_field(&format!("r{i}"), "id", &format!("val{i}")))
            .collect();

        match partition_by_field(&rows, "id", 10) {
            PartitionResult::CircuitBroken { partition_count } => {
                assert!(partition_count > 10);
            }
            PartitionResult::Partitions(_) => panic!("expected circuit break"),
        }
    }

    #[test]
    fn single_partition() {
        let rows = vec![
            row_with_field("r1", "type", "order"),
            row_with_field("r2", "type", "order"),
        ];

        match partition_by_field(&rows, "type", 32) {
            PartitionResult::Partitions(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0].row_indices.len(), 2);
            }
            PartitionResult::CircuitBroken { .. } => panic!("unexpected circuit break"),
        }
    }

    #[test]
    fn null_field_partitions_together() {
        let rows = vec![
            TriggerBatchRow::from_decoded(
                Some(std::collections::HashMap::new()),
                None,
                "r1".into(),
            ),
            TriggerBatchRow::from_decoded(
                Some(std::collections::HashMap::new()),
                None,
                "r2".into(),
            ),
        ];

        match partition_by_field(&rows, "missing", 32) {
            PartitionResult::Partitions(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0].key, "__null__");
            }
            PartitionResult::CircuitBroken { .. } => panic!("unexpected"),
        }
    }

    #[test]
    fn custom_key_fn() {
        let rows = vec![
            row_with_field("r1", "score", "10"),
            row_with_field("r2", "score", "90"),
            row_with_field("r3", "score", "50"),
        ];

        match partition_batch(&rows, 32, |row| {
            let score: i64 = row
                .new_fields()
                .and_then(|m| m.get("score"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if score >= 50 {
                "high".into()
            } else {
                "low".into()
            }
        }) {
            PartitionResult::Partitions(parts) => {
                assert_eq!(parts.len(), 2);
            }
            PartitionResult::CircuitBroken { .. } => panic!("unexpected"),
        }
    }
}
