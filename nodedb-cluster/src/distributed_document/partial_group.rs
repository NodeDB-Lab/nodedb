//! Distributed GROUP BY for document queries.
//!
//! Each shard computes local partial aggregates per group key. The
//! coordinator merges partials across shards to produce the global result.
//!
//! Example: `SELECT status, COUNT(*), AVG(age) FROM users GROUP BY status`
//! - Each shard returns: `[("active", count=50, sum_age=1500, count_age=50), ...]`
//! - Coordinator merges: `("active", count=150, avg_age=sum_ages/count_ages)`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A partial aggregate for one group key from one shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialGroup {
    /// The group key as a JSON string (e.g., `"active"` or `["us-east","web"]`).
    pub group_key: String,
    /// COUNT(*) partial.
    pub count: u64,
    /// Per-column partial aggregates: column_name → PartialColumnAgg.
    pub columns: HashMap<String, PartialColumnAgg>,
}

/// Partial aggregate state for a single column within a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialColumnAgg {
    pub sum: f64,
    pub count: u64,
    pub min: f64,
    pub max: f64,
}

impl PartialColumnAgg {
    pub fn merge(&mut self, other: &PartialColumnAgg) {
        self.sum += other.sum;
        self.count += other.count;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Merger for distributed GROUP BY results.
pub struct PartialGroupByMerger {
    /// group_key → merged partial.
    groups: HashMap<String, PartialGroup>,
}

impl PartialGroupByMerger {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Add a shard's partial GROUP BY results.
    pub fn add_shard_results(&mut self, partials: &[PartialGroup]) {
        for partial in partials {
            let entry = self
                .groups
                .entry(partial.group_key.clone())
                .or_insert_with(|| PartialGroup {
                    group_key: partial.group_key.clone(),
                    count: 0,
                    columns: HashMap::new(),
                });

            entry.count += partial.count;

            for (col_name, col_agg) in &partial.columns {
                entry
                    .columns
                    .entry(col_name.clone())
                    .and_modify(|existing| existing.merge(col_agg))
                    .or_insert_with(|| col_agg.clone());
            }
        }
    }

    /// Get the merged results.
    pub fn finalize(&self) -> Vec<&PartialGroup> {
        self.groups.values().collect()
    }

    /// Number of distinct groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

impl Default for PartialGroupByMerger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_two_shards() {
        let mut merger = PartialGroupByMerger::new();

        // Shard 0: status="active" has 50 users, avg age ~30.
        merger.add_shard_results(&[PartialGroup {
            group_key: "active".into(),
            count: 50,
            columns: HashMap::from([(
                "age".into(),
                PartialColumnAgg {
                    sum: 1500.0,
                    count: 50,
                    min: 18.0,
                    max: 65.0,
                },
            )]),
        }]);

        // Shard 1: status="active" has 80 users, avg age ~35.
        merger.add_shard_results(&[PartialGroup {
            group_key: "active".into(),
            count: 80,
            columns: HashMap::from([(
                "age".into(),
                PartialColumnAgg {
                    sum: 2800.0,
                    count: 80,
                    min: 20.0,
                    max: 70.0,
                },
            )]),
        }]);

        let results = merger.finalize();
        assert_eq!(results.len(), 1);
        let active = &results[0];
        assert_eq!(active.count, 130);
        let age = &active.columns["age"];
        assert_eq!(age.count, 130);
        assert_eq!(age.sum, 4300.0);
        assert!((age.avg() - 33.08).abs() < 0.1); // 4300/130 ≈ 33.08
        assert_eq!(age.min, 18.0);
        assert_eq!(age.max, 70.0);
    }

    #[test]
    fn merge_multiple_groups() {
        let mut merger = PartialGroupByMerger::new();

        merger.add_shard_results(&[
            PartialGroup {
                group_key: "active".into(),
                count: 50,
                columns: HashMap::new(),
            },
            PartialGroup {
                group_key: "inactive".into(),
                count: 10,
                columns: HashMap::new(),
            },
        ]);
        merger.add_shard_results(&[PartialGroup {
            group_key: "active".into(),
            count: 30,
            columns: HashMap::new(),
        }]);

        assert_eq!(merger.group_count(), 2);
        let active = merger.groups.get("active").unwrap();
        assert_eq!(active.count, 80);
        let inactive = merger.groups.get("inactive").unwrap();
        assert_eq!(inactive.count, 10);
    }
}
