// SPDX-License-Identifier: BUSL-1.1

//! Value ranges a node reserves durably before it hands a value out.
//!
//! The catalog row holds the last value any node reserved. A restart, a second
//! node or a promoted mirror then reserves above it, so no value repeats. A
//! node that stops loses the rest of its range: a sequence tolerates gaps.

use std::sync::Arc;

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};

use super::format::{self, ResetScope};
use super::registry::{SequenceRegistry, registry_key};
use super::types::SequenceError;

/// The fewest values one reservation asks for. A smaller range costs a metadata
/// commit per few `nextval` calls.
const MIN_RANGE_VALUES: u64 = 64;

/// How many ranges one `nextval` takes before it reports the contention.
const MAX_RESERVE_ROUNDS: usize = 8;

/// A run of values one reservation owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedRange {
    /// The next value to hand out.
    pub next: i64,
    /// The last value of the range.
    pub last: i64,
    /// The period the range belongs to. Empty with no reset scope.
    pub period_key: String,
}

/// Reserves ranges for this node and publishes operator-set counters.
pub trait SequenceRangeReserver: Send + Sync {
    /// Reserve up to `count` values of `def` for this node.
    fn reserve(
        &self,
        def: &StoredSequence,
        period_key: &str,
        count: u64,
    ) -> Result<ReservedRange, SequenceError>;

    /// Make `state` the durable counter on every node.
    fn publish(&self, state: &SequenceState) -> Result<(), SequenceError>;
}

/// The range after the row `held`, with up to `count` values.
///
/// A row whose period ended restarts at the sequence's start. A proposer's clock
/// that lags the row keeps the row's period.
pub(super) fn next_range(
    def: &StoredSequence,
    held: Option<&SequenceState>,
    period_key: &str,
    count: u64,
) -> Result<ReservedRange, SequenceError> {
    let increment = i128::from(def.increment);
    let turned = def.reset_scope != ResetScope::Never
        && held.is_none_or(|state| state.period_key.as_str() < period_key);
    let first = match held {
        Some(state) if !turned && state.is_called => i128::from(state.current_value) + increment,
        Some(state) if !turned => i128::from(state.current_value),
        Some(_) | None => i128::from(def.start_value),
    };
    let (min, max) = (i128::from(def.min_value), i128::from(def.max_value));
    let first = if (min..=max).contains(&first) {
        first
    } else if !def.cycle {
        return Err(SequenceError::Exhausted {
            name: def.name.clone(),
        });
    } else if increment > 0 {
        min
    } else {
        max
    };
    let room = if increment > 0 {
        (max - first) / increment
    } else {
        (first - min) / -increment
    };
    let take = room.min(i128::from(count.max(1)) - 1);
    let last = first + increment * take;
    let period_key = if turned {
        period_key.to_string()
    } else {
        held.map_or_else(String::new, |state| state.period_key.clone())
    };
    Ok(ReservedRange {
        // `min..=max` holds both bounds, and each bound is an i64.
        next: first as i64,
        last: last as i64,
        period_key,
    })
}

/// The period `scope` names today. Empty with no reset scope.
fn period_now(scope: &ResetScope) -> String {
    if *scope == ResetScope::Never {
        return String::new();
    }
    let parts = nodedb_types::NdbDateTime::now().components();
    format::compute_period_key(scope, parts.year as u16, parts.month, parts.day)
}

/// How many values one reservation of `def` asks for.
fn range_size(def: &StoredSequence) -> u64 {
    u64::try_from(def.cache_size)
        .unwrap_or(1)
        .max(MIN_RANGE_VALUES)
}

impl SequenceRegistry {
    /// Install the reserver. Every later `nextval` on a committed sequence draws
    /// from a reserved range.
    pub fn install_reserver(&self, reserver: Arc<dyn SequenceRangeReserver>) {
        if self.reserver.set(reserver).is_err() {
            tracing::debug!("sequence range reserver already installed");
        }
    }

    /// The next reserved value of a committed sequence. `None` means the caller
    /// keeps the node counter: no reserver, or a sequence this connection buffered.
    pub(super) fn next_reserved(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Result<Option<i64>, SequenceError> {
        let Some(reserver) = self.reserver.get() else {
            return Ok(None);
        };
        let key = registry_key(database_id, tenant_id, name);
        for _ in 0..MAX_RESERVE_ROUNDS {
            // The map's lock never spans the propose: the metadata apply this
            // reservation waits for can take it for write.
            let (def, period_key, generation) = {
                let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
                let Some(handle) = map.get(&key) else {
                    return Ok(None);
                };
                let period_key = period_now(&handle.def.reset_scope);
                handle.turn_period(&period_key);
                if let Some(value) = handle.take_reserved() {
                    return Ok(Some(value));
                }
                (handle.def.clone(), period_key, handle.range_generation())
            };
            let range = reserver.reserve(&def, &period_key, range_size(&def))?;
            let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
            let Some(handle) = map.get(&key) else {
                return Err(SequenceError::NotFound {
                    name: name.to_string(),
                });
            };
            handle.install_range(range, generation);
        }
        Err(SequenceError::Unavailable {
            name: name.to_string(),
            detail: "other sessions took every range this call reserved; retry".to_string(),
        })
    }

    /// Set the durable counter of a committed sequence through the reserver.
    /// `false` means the caller keeps the node counter.
    pub(super) fn publish_state(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        value: i64,
        is_called: bool,
    ) -> Result<bool, SequenceError> {
        let Some(reserver) = self.reserver.get() else {
            return Ok(false);
        };
        let key = registry_key(database_id, tenant_id, name);
        let state = {
            let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
            let Some(handle) = map.get(&key) else {
                return Ok(false);
            };
            handle.check_in_range(value)?;
            SequenceState {
                database_id,
                tenant_id,
                name: name.to_string(),
                current_value: value,
                is_called,
                epoch: handle.def.epoch,
                period_key: handle.period_key(),
                reservation: None,
            }
        };
        reserver.publish(&state)?;
        self.install_state(&state);
        Ok(true)
    }

    /// Make a replicated counter this node's: its reserved ranges end here.
    pub fn install_state(&self, state: &SequenceState) {
        let key = registry_key(state.database_id, state.tenant_id, &state.name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            handle.install_state(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def() -> StoredSequence {
        StoredSequence::new(0, 1, "s".to_string(), "owner".to_string())
    }

    fn called(value: i64, period: &str) -> SequenceState {
        SequenceState {
            current_value: value,
            is_called: true,
            period_key: period.to_string(),
            ..SequenceState::new(0, 1, "s".to_string(), 1, 1)
        }
    }

    #[test]
    fn a_range_starts_after_the_rows_last_value() {
        let fresh = next_range(&def(), None, "", 10).expect("range");
        assert_eq!((fresh.next, fresh.last), (1, 10));
        let later = next_range(&def(), Some(&called(10, "")), "", 10).expect("range");
        assert_eq!((later.next, later.last), (11, 20));
    }

    #[test]
    fn an_uncalled_row_starts_at_its_value() {
        let restarted = SequenceState::new(0, 1, "s".to_string(), 500, 1);
        let range = next_range(&def(), Some(&restarted), "", 2).expect("range");
        assert_eq!((range.next, range.last), (500, 501));
    }

    #[test]
    fn a_range_stops_at_the_bound_and_a_later_one_refuses() {
        let mut bounded = def();
        bounded.max_value = 12;
        let range = next_range(&bounded, Some(&called(10, "")), "", 10).expect("range");
        assert_eq!((range.next, range.last), (11, 12));
        assert!(matches!(
            next_range(&bounded, Some(&called(12, "")), "", 10),
            Err(SequenceError::Exhausted { .. })
        ));
    }

    #[test]
    fn a_cycling_sequence_wraps_at_the_bound() {
        let mut cycling = def();
        cycling.max_value = 12;
        cycling.cycle = true;
        let range = next_range(&cycling, Some(&called(12, "")), "", 3).expect("range");
        assert_eq!((range.next, range.last), (1, 3));
    }

    #[test]
    fn a_descending_range_walks_down() {
        let mut down = def();
        down.increment = -2;
        down.min_value = -7;
        down.max_value = -1;
        down.start_value = -1;
        let range = next_range(&down, None, "", 10).expect("range");
        assert_eq!((range.next, range.last), (-1, -7));
    }

    #[test]
    fn a_new_period_restarts_and_a_lagging_clock_keeps_the_rows_period() {
        let mut monthly = def();
        monthly.reset_scope = ResetScope::Monthly;
        let turned = next_range(&monthly, Some(&called(40, "2026-10")), "2026-11", 2).expect("r");
        assert_eq!((turned.next, turned.period_key.as_str()), (1, "2026-11"));
        let lagging = next_range(&monthly, Some(&called(40, "2026-11")), "2026-10", 2).expect("r");
        assert_eq!((lagging.next, lagging.period_key.as_str()), (41, "2026-11"));
    }
}
