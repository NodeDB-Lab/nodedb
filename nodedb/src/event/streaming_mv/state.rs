// SPDX-License-Identifier: BUSL-1.1

//! Per-MV, per-group-key partial aggregate state.
//!
//! Supports incremental updates: each incoming event updates only the
//! affected group key's state. O(1) per event, not O(N) rescan.
//!
//! The state follows the ad-hoc aggregate rule, so a view row equals the
//! same `SELECT ... GROUP BY` over the source rows:
//!
//! - COUNT counts events.
//! - SUM / AVG take numeric inputs into an [`ExactSum`]: an integer adds
//!   exactly, a float adds Kahan-compensated. SUM is an `Integer`, a
//!   `Decimal` past `i64`, or a `Float` when any input was a float. AVG is a
//!   `Float` from the exact total.
//! - MIN / MAX keep the original non-null input, compared exactly by
//!   [`value_replaces`].
//! - An aggregate with no input is NULL, except COUNT, which is `0`.
//!
//! State is stored in-memory (HashMap) and persisted to redb periodically.

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_query::window::extremum::value_replaces;
use nodedb_query::{EvalError, ExactSum};
use nodedb_types::Value;

use super::types::{AggDef, AggFunction};

/// A row of aggregate results: (aggregate_name, value).
pub type AggRow = Vec<(String, Value)>;

/// MV result row: (group_key, aggregate_values, finalized).
pub type MvResultRow = (String, AggRow, bool);

/// One event's input to one aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum AggInput {
    /// The event itself, for COUNT.
    Event,
    /// The aggregate's non-null source value.
    Value(Value),
    /// The event carries no value for the aggregate.
    Absent,
}

/// Partial aggregate state for one group key.
#[derive(Debug, Clone, Default, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct GroupState {
    /// Inputs taken: events for COUNT, values for every other function.
    pub count: u64,
    /// Exact SUM / AVG state.
    pub sum: ExactSum,
    /// Smallest input, as received.
    pub min: Option<Value>,
    /// Largest input, as received.
    pub max: Option<Value>,
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
    /// Take one input for `func`. Returns whether the input counted: SUM /
    /// AVG take only a numeric value, MIN / MAX any value, COUNT the event.
    pub fn update(&mut self, func: AggFunction, input: &AggInput) -> bool {
        let taken = match (func, input) {
            (AggFunction::Count, AggInput::Event) => true,
            (AggFunction::Sum | AggFunction::Avg, AggInput::Value(v)) => self.sum.add_value(v),
            (AggFunction::Min, AggInput::Value(v)) => {
                if value_replaces(v, self.min.as_ref(), false) {
                    self.min = Some(v.clone());
                }
                true
            }
            (AggFunction::Max, AggInput::Value(v)) => {
                if value_replaces(v, self.max.as_ref(), true) {
                    self.max = Some(v.clone());
                }
                true
            }
            _ => false,
        };
        if taken {
            self.count += 1;
        }
        taken
    }

    /// Update the latest event time for this group key.
    pub fn update_event_time(&mut self, event_time_ms: u64) {
        if event_time_ms > self.latest_event_time {
            self.latest_event_time = event_time_ms;
        }
    }

    /// Compute a specific aggregate from this state.
    pub fn compute(&self, func: AggFunction) -> Result<Value, EvalError> {
        match func {
            AggFunction::Count => i64::try_from(self.count)
                .map(Value::Integer)
                .map_err(|_| EvalError::NumericOverflow { function: "count" }),
            AggFunction::Sum => self.sum.sum(),
            AggFunction::Avg => self.sum.avg(),
            AggFunction::Min => Ok(self.min.clone().unwrap_or(Value::Null)),
            AggFunction::Max => Ok(self.max.clone().unwrap_or(Value::Null)),
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
    /// `inputs` is one input per aggregate definition.
    /// `event_time_ms` is the wall-clock timestamp of the event.
    pub fn update_with_time(&self, group_key: &str, inputs: &[AggInput], event_time_ms: u64) {
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let states = groups
            .entry(group_key.to_string())
            .or_insert_with(|| vec![GroupState::default(); self.aggregates.len()]);

        for ((state, agg), input) in states.iter_mut().zip(&self.aggregates).zip(inputs) {
            if state.update(agg.function, input) {
                state.update_event_time(event_time_ms);
            }
        }
    }

    /// The aggregate values of one group's states, in definition order.
    fn row(&self, states: &[GroupState]) -> Result<AggRow, EvalError> {
        self.aggregates
            .iter()
            .enumerate()
            .map(|(i, agg)| {
                let val = match states.get(i) {
                    Some(state) => state.compute(agg.function)?,
                    None => GroupState::default().compute(agg.function)?,
                };
                Ok((agg.output_name.clone(), val))
            })
            .collect()
    }

    /// Read the current aggregate results, sorted by group key.
    ///
    /// Fails with [`EvalError::NumericOverflow`] when a SUM or AVG total left
    /// the exact range.
    pub fn read_results(&self) -> Result<Vec<(String, AggRow)>, EvalError> {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        let mut results = groups
            .iter()
            .map(|(key, states)| Ok((key.clone(), self.row(states)?)))
            .collect::<Result<Vec<_>, EvalError>>()?;
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
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

    /// Read results with finalization status, sorted by group key.
    ///
    /// Fails like [`Self::read_results`].
    pub fn read_results_with_status(&self) -> Result<Vec<MvResultRow>, EvalError> {
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        let mut results = groups
            .iter()
            .map(|(key, states)| {
                let finalized = states.iter().all(|s| s.finalized);
                Ok((key.clone(), self.row(states)?, finalized))
            })
            .collect::<Result<Vec<_>, EvalError>>()?;
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
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

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    fn agg(output_name: &str, function: AggFunction) -> AggDef {
        AggDef {
            output_name: output_name.into(),
            function,
            input_expr: "v".into(),
        }
    }

    fn value(i: i64) -> AggInput {
        AggInput::Value(Value::Integer(i))
    }

    #[test]
    fn group_state_incremental() {
        let mut gs = GroupState::default();
        for v in [10.0, 20.0, 5.0] {
            assert!(gs.update(AggFunction::Avg, &AggInput::Value(Value::Float(v))));
        }

        assert_eq!(gs.count, 3);
        assert_eq!(gs.compute(AggFunction::Sum).unwrap(), Value::Float(35.0));
        let Value::Float(avg) = gs.compute(AggFunction::Avg).unwrap() else {
            panic!("AVG is a float");
        };
        assert!((avg - 11.666666).abs() < 0.01);
    }

    #[test]
    fn integers_above_two_pow_53_stay_exact() {
        let state = MvState::new(
            "m".into(),
            Vec::new(),
            vec![
                agg("cnt", AggFunction::Count),
                agg("s", AggFunction::Sum),
                agg("lo", AggFunction::Min),
                agg("hi", AggFunction::Max),
                agg("mean", AggFunction::Avg),
            ],
        );
        for v in [ABOVE, AT, i64::MAX] {
            state.update_with_time(
                "",
                &[AggInput::Event, value(v), value(v), value(v), value(v)],
                0,
            );
        }
        let rows = state.read_results().unwrap();
        let row: Vec<Value> = rows[0].1.iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(row[0], Value::Integer(3));
        assert_eq!(
            row[1],
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                i128::from(ABOVE) + i128::from(AT) + i128::from(i64::MAX),
                0
            ))
        );
        assert_eq!(row[2], Value::Integer(AT));
        assert_eq!(row[3], Value::Integer(i64::MAX));
        let exact_mean = (i128::from(ABOVE) + i128::from(AT) + i128::from(i64::MAX)) / 3;
        assert_eq!(row[4], Value::Float(exact_mean as f64));
    }

    #[test]
    fn absent_and_non_numeric_inputs_do_not_count() {
        let mut gs = GroupState::default();
        assert!(!gs.update(AggFunction::Sum, &AggInput::Absent));
        assert!(!gs.update(
            AggFunction::Sum,
            &AggInput::Value(Value::String("x".into()))
        ));
        assert_eq!(gs.count, 0);
        assert_eq!(gs.compute(AggFunction::Sum).unwrap(), Value::Null);
        assert_eq!(gs.compute(AggFunction::Avg).unwrap(), Value::Null);
        assert_eq!(gs.compute(AggFunction::Min).unwrap(), Value::Null);
        assert_eq!(gs.compute(AggFunction::Count).unwrap(), Value::Integer(0));
    }

    #[test]
    fn snapshot_round_trips_through_msgpack() {
        let mut gs = GroupState::default();
        for v in [ABOVE, i64::MAX, i64::MAX] {
            gs.update(AggFunction::Sum, &value(v));
        }
        gs.min = Some(Value::Integer(AT));
        gs.max = Some(Value::from_u64(u64::MAX));
        gs.latest_event_time = 7;
        let bytes = zerompk::to_msgpack_vec(&gs).unwrap();
        let back: GroupState = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, gs);
        assert_eq!(
            back.compute(AggFunction::Sum).unwrap(),
            gs.compute(AggFunction::Sum).unwrap()
        );
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

        state.update_with_time("INSERT", &[AggInput::Event], 0);
        state.update_with_time("INSERT", &[AggInput::Event], 0);
        state.update_with_time("UPDATE", &[AggInput::Event], 0);

        let results = state.read_results().unwrap();
        assert_eq!(results.len(), 2);

        let insert_row = results.iter().find(|(k, _)| k == "INSERT").unwrap();
        assert_eq!(insert_row.1[0].1, Value::Integer(2));

        let update_row = results.iter().find(|(k, _)| k == "UPDATE").unwrap();
        assert_eq!(update_row.1[0].1, Value::Integer(1));
    }
}
