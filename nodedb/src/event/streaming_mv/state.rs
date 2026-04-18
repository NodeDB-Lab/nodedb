//! Per-MV, per-group-key partial aggregate state.
//!
//! Supports incremental updates: each incoming event updates only the
//! affected group key's state. O(1) per event, not O(N) rescan.
//!
//! State is stored in-memory (HashMap) and persisted to redb periodically.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::{AggDef, AggFunction};

/// A row of aggregate results: (aggregate_name, value).
pub type AggRow = Vec<(String, f64)>;

/// MV result row: (group_key, aggregate_values, finalized).
pub type MvResultRow = (String, AggRow, bool);

/// Partial aggregate state for one group key.
#[derive(Debug, Clone, Default, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct GroupState {
    pub count: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// Whether this bucket is finalized (all partitions have advanced past it).
    /// Once finalized, no more events will arrive for this group key.
    #[msgpack(default)]
    pub finalized: bool,
    /// Latest event_time (wall-clock ms) seen for this group key.
    /// Used to map LSN watermarks to wall-clock time for time-bucket finalization.
    #[msgpack(default)]
    pub latest_event_time: u64,
}

impl GroupState {
    /// Update this state with a new value and event timestamp.
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    /// Update the latest event time for this group key.
    pub fn update_event_time(&mut self, event_time_ms: u64) {
        if event_time_ms > self.latest_event_time {
            self.latest_event_time = event_time_ms;
        }
    }

    /// Compute a specific aggregate from this state.
    pub fn compute(&self, func: AggFunction) -> f64 {
        match func {
            AggFunction::Count => self.count as f64,
            AggFunction::Sum => self.sum,
            AggFunction::Min => self.min.unwrap_or(0.0),
            AggFunction::Max => self.max.unwrap_or(0.0),
            AggFunction::Avg => {
                if self.count > 0 {
                    self.sum / self.count as f64
                } else {
                    0.0
                }
            }
        }
    }
}

/// In-memory aggregate state for one streaming MV.
///
/// Maps group_key (concatenated GROUP BY values) → per-aggregate-column state.
pub struct MvState {
    /// MV name.
    pub name: String,
    /// GROUP BY column names.
    pub group_by_columns: Vec<String>,
    /// Aggregate definitions.
    pub aggregates: Vec<AggDef>,
    /// group_key → { agg_index → GroupState }.
    groups: RwLock<HashMap<String, Vec<GroupState>>>,
}

impl MvState {
    pub fn new(name: String, group_by_columns: Vec<String>, aggregates: Vec<AggDef>) -> Self {
        Self {
            name,
            group_by_columns,
            aggregates,
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Update the MV state with a new event.
    ///
    /// `group_key` is the concatenated GROUP BY values (e.g., "INSERT" or "orders:INSERT").
    /// `agg_values` is one value per aggregate definition (NaN if not applicable).
    /// `event_time_ms` is the wall-clock timestamp of the event.
    pub fn update_with_time(&self, group_key: &str, agg_values: &[f64], event_time_ms: u64) {
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let states = groups
            .entry(group_key.to_string())
            .or_insert_with(|| vec![GroupState::default(); self.aggregates.len()]);

        for (i, &value) in agg_values.iter().enumerate() {
            if i < states.len() && !value.is_nan() {
                states[i].update(value);
                states[i].update_event_time(event_time_ms);
            }
        }
    }

    /// Update the MV state (without event time — backward compat).
    pub fn update(&self, group_key: &str, agg_values: &[f64]) {
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let states = groups
            .entry(group_key.to_string())
            .or_insert_with(|| vec![GroupState::default(); self.aggregates.len()]);

        for (i, &value) in agg_values.iter().enumerate() {
            if i < states.len() && !value.is_nan() {
                states[i].update(value);
            }
        }
    }

    /// Read the current aggregate results.
    ///
    /// Returns: Vec<(group_key, Vec<(agg_name, value)>)>
    pub fn read_results(&self) -> Vec<(String, AggRow)> {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        let mut results: Vec<(String, AggRow)> = groups
            .iter()
            .map(|(key, states)| {
                let values: Vec<(String, f64)> = self
                    .aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| {
                        let val = if i < states.len() {
                            states[i].compute(agg.function)
                        } else {
                            0.0
                        };
                        (agg.output_name.clone(), val)
                    })
                    .collect();
                (key.clone(), values)
            })
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    /// Finalize time buckets whose latest event_time is below the watermark time.
    ///
    /// `watermark_time_ms` is the wall-clock time corresponding to the global
    /// watermark LSN. All partitions have advanced past this point, so no more
    /// events will arrive for time buckets ending before this time.
    ///
    /// Returns the number of newly finalized groups.
    pub fn finalize_buckets(&self, watermark_time_ms: u64) -> u32 {
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let mut finalized_count = 0u32;

        for states in groups.values_mut() {
            for state in states.iter_mut() {
                if !state.finalized
                    && state.latest_event_time > 0
                    && state.latest_event_time < watermark_time_ms
                {
                    state.finalized = true;
                    finalized_count += 1;
                }
            }
        }

        finalized_count
    }

    /// Read results with finalization status.
    ///
    /// Returns: Vec<(group_key, Vec<(agg_name, value)>, finalized)>
    pub fn read_results_with_status(&self) -> Vec<MvResultRow> {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        let mut results: Vec<MvResultRow> = groups
            .iter()
            .map(|(key, states)| {
                let finalized = states.iter().all(|s| s.finalized);
                let values: Vec<(String, f64)> = self
                    .aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| {
                        let val = if i < states.len() {
                            states[i].compute(agg.function)
                        } else {
                            0.0
                        };
                        (agg.output_name.clone(), val)
                    })
                    .collect();
                (key.clone(), values, finalized)
            })
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    /// Number of distinct group keys.
    pub fn group_count(&self) -> usize {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups.len()
    }

    /// Serialize all group states for persistence.
    pub fn snapshot(&self) -> Vec<(String, Vec<GroupState>)> {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Restore group states from a persisted snapshot.
    pub fn restore(&self, snapshot: Vec<(String, Vec<GroupState>)>) {
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        groups.clear();
        for (key, states) in snapshot {
            groups.insert(key, states);
        }
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory(&self) -> usize {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups
            .iter()
            .map(|(k, v)| k.len() + v.len() * std::mem::size_of::<GroupState>())
            .sum::<usize>()
            + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_state_incremental() {
        let mut gs = GroupState::default();
        gs.update(10.0);
        gs.update(20.0);
        gs.update(5.0);

        assert_eq!(gs.count, 3);
        assert_eq!(gs.sum, 35.0);
        assert_eq!(gs.min, Some(5.0));
        assert_eq!(gs.max, Some(20.0));
        assert!((gs.compute(AggFunction::Avg) - 11.666666).abs() < 0.01);
    }

    #[test]
    fn mv_state_update_and_read() {
        let state = MvState::new(
            "test_mv".into(),
            vec!["event_type".into()],
            vec![AggDef {
                output_name: "cnt".into(),
                function: AggFunction::Count,
                input_expr: String::new(),
            }],
        );

        state.update("INSERT", &[1.0]);
        state.update("INSERT", &[1.0]);
        state.update("UPDATE", &[1.0]);

        let results = state.read_results();
        assert_eq!(results.len(), 2);

        let insert_row = results.iter().find(|(k, _)| k == "INSERT").unwrap();
        assert_eq!(insert_row.1[0].1, 2.0); // COUNT = 2

        let update_row = results.iter().find(|(k, _)| k == "UPDATE").unwrap();
        assert_eq!(update_row.1[0].1, 1.0); // COUNT = 1
    }
}
