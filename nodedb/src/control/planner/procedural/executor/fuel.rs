//! Fuel metering and timeout tracking for the statement executor.
//!
//! Prevents runaway loops in stored procedures:
//! - **Fuel counter**: each LOOP/WHILE/FOR iteration decrements fuel.
//!   On exhaustion → `EXECUTION_LIMIT_EXCEEDED`.
//! - **Wall-clock timeout**: checked at each statement boundary.
//!   On expiry → `EXECUTION_LIMIT_EXCEEDED`.

use std::time::Instant;

/// Execution budget for a stored procedure or trigger.
#[derive(Debug, Clone)]
pub struct ExecutionBudget {
    /// Remaining loop iterations. Decremented on each iteration.
    fuel_remaining: u64,
    /// Wall-clock deadline.
    deadline: Instant,
    /// Original fuel budget (for error messages).
    max_iterations: u64,
    /// Original timeout (for error messages).
    timeout_secs: u64,
}

impl ExecutionBudget {
    /// Create a new budget with the given limits.
    pub fn new(max_iterations: u64, timeout_secs: u64) -> Self {
        Self {
            fuel_remaining: max_iterations,
            deadline: Instant::now() + std::time::Duration::from_secs(timeout_secs),
            max_iterations,
            timeout_secs,
        }
    }

    /// Default budget for trigger bodies. Caps iterations and wall-clock
    /// time to prevent runaway loops from pinning Control Plane workers.
    pub fn trigger_default() -> Self {
        Self::new(100_000, 10)
    }

    /// Consume one iteration of fuel. Returns error if exhausted.
    pub fn consume_iteration(&mut self) -> crate::Result<()> {
        if self.fuel_remaining == 0 {
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!("loop exceeded {} iterations", self.max_iterations),
            });
        }
        self.fuel_remaining -= 1;
        Ok(())
    }

    /// Check if the wall-clock deadline has passed. Returns error if expired.
    pub fn check_timeout(&self) -> crate::Result<()> {
        if Instant::now() >= self.deadline {
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!("procedure exceeded {}s timeout", self.timeout_secs),
            });
        }
        Ok(())
    }

    /// Check both fuel and timeout in one call.
    pub fn check(&mut self) -> crate::Result<()> {
        self.check_timeout()?;
        // Fuel is checked per-iteration, not per-statement.
        // This is just the timeout check at statement boundaries.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuel_exhaustion() {
        let mut budget = ExecutionBudget::new(3, 60);
        assert!(budget.consume_iteration().is_ok());
        assert!(budget.consume_iteration().is_ok());
        assert!(budget.consume_iteration().is_ok());
        assert!(budget.consume_iteration().is_err());
    }

    #[test]
    fn timeout_not_expired() {
        let budget = ExecutionBudget::new(1000, 60);
        assert!(budget.check_timeout().is_ok());
    }

    #[test]
    fn timeout_expired() {
        let budget = ExecutionBudget {
            fuel_remaining: 1000,
            deadline: Instant::now() - std::time::Duration::from_secs(1),
            max_iterations: 1000,
            timeout_secs: 0,
        };
        assert!(budget.check_timeout().is_err());
    }

    #[test]
    fn trigger_default_allows_bounded_loops() {
        let mut budget = ExecutionBudget::trigger_default();
        for _ in 0..10_000 {
            budget.consume_iteration().unwrap();
        }
    }
}
