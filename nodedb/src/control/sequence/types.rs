//! Sequence runtime types.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

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
}

impl SequenceHandle {
    /// Create a new handle from a sequence definition and optional persisted state.
    pub fn new(
        def: crate::control::security::catalog::sequence_types::StoredSequence,
        state: Option<crate::control::security::catalog::sequence_types::SequenceState>,
    ) -> Self {
        let (initial_counter, called, period_key) = if let Some(s) = state {
            if s.is_called {
                (s.current_value, true, s.period_key)
            } else {
                (def.start_value - def.increment, false, s.period_key)
            }
        } else {
            (def.start_value - def.increment, false, String::new())
        };

        Self {
            counter: AtomicI64::new(initial_counter),
            called: AtomicBool::new(called),
            def,
            period_key: Mutex::new(period_key),
        }
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
        }
    }
}

impl std::error::Error for SequenceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;

    fn make_handle(start: i64, inc: i64, min: i64, max: i64, cycle: bool) -> SequenceHandle {
        let mut def = StoredSequence::new(1, "test".into(), "admin".into());
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
