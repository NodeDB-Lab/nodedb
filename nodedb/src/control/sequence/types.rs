// SPDX-License-Identifier: BUSL-1.1

//! Sequence runtime types.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use super::reserve::ReservedRange;

/// Lock-free handle for a single sequence's counter on this node.
///
/// `nextval` uses `AtomicI64::fetch_add` — zero contention, no mutex.
/// For GAP_FREE mode, the caller must use `GapFreeManager` externally.
pub struct SequenceHandle {
    /// Current counter value. Starts at `start_value - increment` so the
    /// first `fetch_add(increment)` returns `start_value`.
    counter: AtomicI64,
    /// Whether `nextval` has been called at least once.
    called: AtomicBool,
    /// Sequence definition (immutable after creation, replaced on ALTER).
    pub def: crate::control::security::catalog::sequence_types::StoredSequence,
    /// Current period key for reset scope tracking. Protected by mutex
    /// because period transitions are rare but must be atomic with counter reset.
    period_key: Mutex<String>,
    /// The ranges this node reserved and has not handed out, oldest first.
    reserved: Mutex<VecDeque<ReservedRange>>,
    /// Rises each time the reserved ranges end. A reservation that started
    /// before the rise installs nothing.
    range_generation: AtomicU64,
}

impl SequenceHandle {
    /// Create a new handle from a sequence definition and optional persisted state.
    ///
    /// A called state holds the last value `nextval` returned. An uncalled
    /// state holds the value the next `nextval` returns, as `RESTART` sets it.
    pub fn new(
        def: crate::control::security::catalog::sequence_types::StoredSequence,
        state: Option<crate::control::security::catalog::sequence_types::SequenceState>,
    ) -> Self {
        let (initial_counter, called, period_key) = if let Some(s) = state {
            if s.is_called {
                (s.current_value, true, s.period_key)
            } else {
                (
                    s.current_value.saturating_sub(def.increment),
                    false,
                    s.period_key,
                )
            }
        } else {
            (def.start_value - def.increment, false, String::new())
        };

        Self {
            counter: AtomicI64::new(initial_counter),
            called: AtomicBool::new(called),
            def,
            period_key: Mutex::new(period_key),
            reserved: Mutex::new(VecDeque::new()),
            range_generation: AtomicU64::new(0),
        }
    }

    /// Hand out the next reserved value. `None` means no range holds one.
    pub(super) fn take_reserved(&self) -> Option<i64> {
        let mut ranges = self.reserved.lock().unwrap_or_else(|p| p.into_inner());
        let front = ranges.front_mut()?;
        let value = front.next;
        if value == front.last {
            ranges.pop_front();
        } else {
            front.next = value + self.def.increment;
        }
        self.counter.store(value, Ordering::Relaxed);
        self.called.store(true, Ordering::Relaxed);
        Some(value)
    }

    /// The generation a reservation names when it installs its range.
    pub(super) fn range_generation(&self) -> u64 {
        self.range_generation.load(Ordering::Acquire)
    }

    /// Add `range`, unless the ranges ended since `generation`.
    pub(super) fn install_range(&self, range: ReservedRange, generation: u64) {
        let mut ranges = self.reserved.lock().unwrap_or_else(|p| p.into_inner());
        if self.range_generation.load(Ordering::Acquire) != generation {
            return;
        }
        *self.period_key.lock().unwrap_or_else(|p| p.into_inner()) = range.period_key.clone();
        ranges.push_back(range);
    }

    /// End the reserved ranges.
    fn end_ranges(&self) {
        let mut ranges = self.reserved.lock().unwrap_or_else(|p| p.into_inner());
        ranges.clear();
        self.range_generation.fetch_add(1, Ordering::AcqRel);
    }

    /// End the reserved ranges of a period older than `period_key`.
    pub(super) fn turn_period(&self, period_key: &str) {
        if period_key.is_empty() {
            return;
        }
        let behind = self
            .period_key
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_str()
            < period_key;
        if behind {
            self.end_ranges();
        }
    }

    /// Refuse a value outside the sequence's bounds.
    pub(super) fn check_in_range(&self, value: i64) -> Result<(), SequenceError> {
        if value < self.def.min_value || value > self.def.max_value {
            return Err(SequenceError::OutOfRange {
                name: self.def.name.clone(),
                value,
                min: self.def.min_value,
                max: self.def.max_value,
            });
        }
        Ok(())
    }

    /// Take a replicated counter, as `new` reads a row.
    pub(super) fn install_state(
        &self,
        state: &crate::control::security::catalog::sequence_types::SequenceState,
    ) {
        self.end_ranges();
        let counter = if state.is_called {
            state.current_value
        } else {
            state.current_value.saturating_sub(self.def.increment)
        };
        self.counter.store(counter, Ordering::Relaxed);
        self.called.store(state.is_called, Ordering::Relaxed);
        state
            .period_key
            .clone_into(&mut self.period_key.lock().unwrap_or_else(|p| p.into_inner()));
    }

    /// Advance to the next value (lock-free).
    ///
    /// Returns the new value, or an error if the sequence is exhausted
    /// and CYCLE is not enabled.
    pub fn nextval(&self) -> Result<i64, SequenceError> {
        let increment = self.def.increment;
        let prev = self.counter.fetch_add(increment, Ordering::Relaxed);
        let new_val = prev + increment;

        self.called.store(true, Ordering::Relaxed);

        // Check bounds.
        if increment > 0 && new_val > self.def.max_value {
            if self.def.cycle {
                // Wrap around to min_value.
                self.counter.store(self.def.min_value, Ordering::Relaxed);
                return Ok(self.def.min_value);
            }
            // Undo the increment.
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }
        if increment < 0 && new_val < self.def.min_value {
            if self.def.cycle {
                self.counter.store(self.def.max_value, Ordering::Relaxed);
                return Ok(self.def.max_value);
            }
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }

        Ok(new_val)
    }

    /// Advance the sequence by N values in one atomic operation.
    ///
    /// Returns a Vec of N consecutive values. Uses a single `fetch_add`
    /// for the entire batch — zero contention for bulk inserts.
    pub fn nextval_batch(&self, n: usize) -> Result<Vec<i64>, SequenceError> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let increment = self.def.increment;
        let total_advance = increment * n as i64;
        let prev = self.counter.fetch_add(total_advance, Ordering::Relaxed);

        self.called.store(true, Ordering::Relaxed);

        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            values.push(prev + increment * (i as i64 + 1));
        }

        // Check bounds on the last value (n > 0 guaranteed above).
        let last = values[n - 1];
        if increment > 0 && last > self.def.max_value {
            if self.def.cycle {
                // CAS loop to atomically wrap around — prevents race with concurrent nextval.
                let new_base = self.def.min_value;
                let new_counter = new_base + increment * (n as i64 - 1);
                let overflowed = prev + total_advance;
                let _ = self.counter.compare_exchange(
                    overflowed,
                    new_counter,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                values.clear();
                for i in 0..n {
                    values.push(new_base + increment * i as i64);
                }
                return Ok(values);
            }
            // Rollback via CAS (only if no concurrent modification).
            let _ = self.counter.compare_exchange(
                prev + total_advance,
                prev,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }
        if increment < 0 && last < self.def.min_value {
            if self.def.cycle {
                let new_base = self.def.max_value;
                let new_counter = new_base + increment * (n as i64 - 1);
                let overflowed = prev + total_advance;
                let _ = self.counter.compare_exchange(
                    overflowed,
                    new_counter,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                values.clear();
                for i in 0..n {
                    values.push(new_base + increment * i as i64);
                }
                return Ok(values);
            }
            // Rollback via CAS (only if no concurrent modification).
            let _ = self.counter.compare_exchange(
                prev + total_advance,
                prev,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }

        Ok(values)
    }

    /// Get the last value returned by nextval (for currval).
    pub fn currval(&self) -> Result<i64, SequenceError> {
        if !self.called.load(Ordering::Relaxed) {
            return Err(SequenceError::NotYetCalled {
                name: self.def.name.clone(),
            });
        }
        Ok(self.counter.load(Ordering::Relaxed))
    }

    /// Set the counter to a specific value (for setval).
    pub fn setval(&self, value: i64) -> Result<i64, SequenceError> {
        if value < self.def.min_value || value > self.def.max_value {
            return Err(SequenceError::OutOfRange {
                name: self.def.name.clone(),
                value,
                min: self.def.min_value,
                max: self.def.max_value,
            });
        }
        self.counter.store(value, Ordering::Relaxed);
        self.called.store(true, Ordering::Relaxed);
        Ok(value)
    }

    /// Restart the sequence so the next `nextval` returns `value` itself
    /// (`ALTER SEQUENCE ... RESTART WITH`, `TRUNCATE ... RESTART IDENTITY`).
    /// Unlike `setval`, the restart value counts as not yet called.
    pub fn restart_at(&self, value: i64) -> Result<(), SequenceError> {
        if value < self.def.min_value || value > self.def.max_value {
            return Err(SequenceError::OutOfRange {
                name: self.def.name.clone(),
                value,
                min: self.def.min_value,
                max: self.def.max_value,
            });
        }
        self.counter
            .store(value - self.def.increment, Ordering::Relaxed);
        self.called.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Check if the period has changed and reset the counter if needed.
    ///
    /// Must be called before `nextval()` when the sequence has a reset scope.
    /// Returns `true` if the counter was reset.
    pub fn check_period_reset(&self, new_period_key: &str) -> bool {
        if new_period_key.is_empty() {
            return false; // ResetScope::Never
        }

        let mut pk = self.period_key.lock().unwrap_or_else(|p| p.into_inner());
        if pk.as_str() != new_period_key {
            // Period changed — reset counter to start_value position.
            self.counter
                .store(self.def.start_value - self.def.increment, Ordering::Relaxed);
            self.called.store(false, Ordering::Relaxed);
            *pk = new_period_key.to_string();
            true
        } else {
            false
        }
    }

    /// Decrement the counter by one increment (for GAP_FREE rollback).
    pub fn rollback_one(&self) {
        self.counter
            .fetch_sub(self.def.increment, Ordering::Relaxed);
    }

    /// Get current counter value for persistence.
    pub fn current_value(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// The value a persisted state stores: the last value returned once
    /// called, else the value the next `nextval` returns.
    pub fn persisted_value(&self) -> i64 {
        let counter = self.current_value();
        if self.is_called() {
            counter
        } else {
            counter.saturating_add(self.def.increment)
        }
    }

    /// Whether nextval has been called.
    pub fn is_called(&self) -> bool {
        self.called.load(Ordering::Relaxed)
    }

    /// Get the current period key for persistence.
    pub fn period_key(&self) -> String {
        self.period_key
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// Sequence operation errors.
#[derive(Debug, Clone)]
pub enum SequenceError {
    /// Sequence reached max/min and CYCLE is off.
    Exhausted { name: String },
    /// currval called before nextval in this session.
    NotYetCalled { name: String },
    /// setval value outside [min, max] range.
    OutOfRange {
        name: String,
        value: i64,
        min: i64,
        max: i64,
    },
    /// Sequence not found.
    NotFound { name: String },
    /// Sequence already exists.
    AlreadyExists { name: String },
    /// Invalid definition.
    InvalidDefinition { detail: String },
    /// Format template parse error.
    FormatParse { detail: String },
    /// Invalid reset scope.
    InvalidResetScope { detail: String },
    /// The sequence belongs to a mirror that is not promoted.
    ReadOnlyMirror { name: String },
    /// No range landed, and a later call can succeed.
    Unavailable { name: String, detail: String },
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceError::Exhausted { name } => {
                write!(f, "nextval: reached maximum value for sequence \"{name}\"")
            }
            SequenceError::NotYetCalled { name } => {
                write!(
                    f,
                    "currval of sequence \"{name}\" is not yet defined in this session"
                )
            }
            SequenceError::OutOfRange {
                name,
                value,
                min,
                max,
            } => {
                write!(
                    f,
                    "setval: value {value} is outside allowed range [{min}, {max}] for sequence \"{name}\""
                )
            }
            SequenceError::NotFound { name } => {
                write!(f, "sequence \"{name}\" does not exist")
            }
            SequenceError::AlreadyExists { name } => {
                write!(f, "sequence \"{name}\" already exists")
            }
            SequenceError::InvalidDefinition { detail } => {
                write!(f, "invalid sequence definition: {detail}")
            }
            SequenceError::FormatParse { detail } => {
                write!(f, "format template error: {detail}")
            }
            SequenceError::InvalidResetScope { detail } => {
                write!(f, "invalid reset scope: {detail}")
            }
            SequenceError::ReadOnlyMirror { name } => {
                write!(
                    f,
                    "sequence \"{name}\" belongs to a read-only mirror; promote it before nextval"
                )
            }
            SequenceError::Unavailable { name, detail } => {
                write!(f, "sequence \"{name}\" reserved no values: {detail}")
            }
        }
    }
}

impl std::error::Error for SequenceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;

    fn make_handle(start: i64, inc: i64, min: i64, max: i64, cycle: bool) -> SequenceHandle {
        let mut def = StoredSequence::new(4, 1, "test".into(), "admin".into());
        def.start_value = start;
        def.increment = inc;
        def.min_value = min;
        def.max_value = max;
        def.cycle = cycle;
        SequenceHandle::new(def, None)
    }

    #[test]
    fn basic_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
    }

    #[test]
    fn restarted_state_survives_a_reload() {
        use crate::control::security::catalog::sequence_types::SequenceState;
        let mut def = StoredSequence::new(4, 1, "test".into(), "admin".into());
        def.increment = 5;
        let restarted = SequenceState::new(4, 1, "test".into(), 100, def.epoch);
        let h = SequenceHandle::new(def.clone(), Some(restarted));
        assert_eq!(h.persisted_value(), 100);
        assert_eq!(h.nextval().unwrap(), 100);

        let persisted = SequenceState {
            current_value: h.persisted_value(),
            is_called: h.is_called(),
            ..SequenceState::new(4, 1, "test".into(), 0, def.epoch)
        };
        let reloaded = SequenceHandle::new(def, Some(persisted));
        assert_eq!(reloaded.nextval().unwrap(), 105);
    }

    #[test]
    fn currval_before_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        assert!(h.currval().is_err());
    }

    #[test]
    fn currval_after_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        h.nextval().unwrap();
        assert_eq!(h.currval().unwrap(), 1);
        h.nextval().unwrap();
        assert_eq!(h.currval().unwrap(), 2);
    }

    #[test]
    fn exhausted_no_cycle() {
        let h = make_handle(1, 1, 1, 3, false);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
        assert!(h.nextval().is_err());
    }

    #[test]
    fn cycle_wraps_around() {
        let h = make_handle(1, 1, 1, 3, true);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
        assert_eq!(h.nextval().unwrap(), 1); // wraps
    }

    #[test]
    fn descending_sequence() {
        let h = make_handle(10, -1, 8, 10, false);
        assert_eq!(h.nextval().unwrap(), 10);
        assert_eq!(h.nextval().unwrap(), 9);
        assert_eq!(h.nextval().unwrap(), 8);
        assert!(h.nextval().is_err());
    }

    #[test]
    fn setval_in_range() {
        let h = make_handle(1, 1, 1, 100, false);
        assert_eq!(h.setval(50).unwrap(), 50);
        assert_eq!(h.currval().unwrap(), 50);
        assert_eq!(h.nextval().unwrap(), 51);
    }

    #[test]
    fn restart_at_makes_the_restart_value_the_next_value() {
        let h = make_handle(1, 1, 1, 100, false);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        h.restart_at(1).unwrap();
        assert!(!h.is_called());
        assert_eq!(h.nextval().unwrap(), 1);
        h.restart_at(40).unwrap();
        assert_eq!(h.nextval().unwrap(), 40);
        assert!(h.restart_at(101).is_err());
    }

    #[test]
    fn setval_out_of_range() {
        let h = make_handle(1, 1, 1, 100, false);
        assert!(h.setval(101).is_err());
        assert!(h.setval(0).is_err());
    }

    #[test]
    fn increment_by_10() {
        let h = make_handle(10, 10, 1, 100, false);
        assert_eq!(h.nextval().unwrap(), 10);
        assert_eq!(h.nextval().unwrap(), 20);
        assert_eq!(h.nextval().unwrap(), 30);
    }
}
