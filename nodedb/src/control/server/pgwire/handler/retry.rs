//! Transparent retry for `RetryableSchemaChanged`.
//!
//! When the planner tries to acquire a descriptor lease at a
//! version being drained by an in-flight DDL, the adapter
//! surfaces `crate::Error::RetryableSchemaChanged`. The pgwire
//! handler catches this and retries the whole statement a
//! bounded number of times with backoff — by which point either
//! the drain has completed and the new descriptor version is
//! readable, or the retry budget is exhausted and a typed
//! `"schema changed"` error surfaces to the client.
//!
//! The retry is intentionally **dumb**: it re-runs the entire
//! `plan_sql_with_rls_returning` call, including parsing. A
//! smarter implementation would hold onto the parsed AST and
//! only re-resolve. That's a future optimisation — for the
//! common drain case (sub-second drains on clusters with short
//! query lifetimes) the extra parse cost is negligible.
//!
//! ## Retry budget
//!
//! Three attempts total with 50ms, 100ms, 200ms backoff between
//! them — roughly 350ms of tolerance for a drain to complete.
//! The `DEFAULT_DRAIN_TIMEOUT` from `metadata_proposer` is 35s,
//! so in practice either drain completes within our retry budget
//! (the proposer is actively draining and is probably close to
//! done by the time we observe it) or drain is stuck and our
//! error helps the operator diagnose.

use std::time::Duration;

use crate::error::Error;

/// Maximum number of attempts (including the initial call).
const MAX_ATTEMPTS: usize = 3;

/// Backoff durations BETWEEN attempts. `BACKOFFS[i]` is the sleep
/// duration before attempt `i + 1`. Length must be
/// `MAX_ATTEMPTS - 1`.
const BACKOFFS: [Duration; MAX_ATTEMPTS - 1] =
    [Duration::from_millis(50), Duration::from_millis(100)];

/// Run `op` up to `MAX_ATTEMPTS` times. Retries only on
/// `Error::RetryableSchemaChanged`. Any other error (including
/// `Error::PlanError`) is returned immediately on the first
/// attempt. Returns the last error observed if every attempt
/// surfaced `RetryableSchemaChanged`.
///
/// The closure takes no arguments — callers capture whatever
/// context (sql text, tenant_id, security context) they need
/// via move semantics. The closure is `async` so it can
/// `.await` the planner.
pub async fn retry_on_schema_change<F, Fut, T>(mut op: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut last_err: Option<Error> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(Error::RetryableSchemaChanged { descriptor }) => {
                tracing::debug!(
                    attempt,
                    descriptor = %descriptor,
                    "pgwire: retrying plan after schema change"
                );
                last_err = Some(Error::RetryableSchemaChanged { descriptor });
                if let Some(backoff) = BACKOFFS.get(attempt) {
                    tokio::time::sleep(*backoff).await;
                }
            }
            Err(other) => return Err(other),
        }
    }
    // Exhausted retries — surface the last RetryableSchemaChanged.
    Err(last_err.unwrap_or_else(|| Error::PlanError {
        detail: "retry_on_schema_change: no attempts recorded".into(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn first_attempt_success() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, Error> = retry_on_schema_change(|| {
            let c = calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(c as i32) }
        })
        .await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_on_schema_change_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let result: Result<&str, Error> = retry_on_schema_change(|| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(Error::RetryableSchemaChanged {
                        descriptor: format!("attempt {n}"),
                    })
                } else {
                    Ok("done")
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn surfaces_error_after_budget_exhausted() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), Error> = retry_on_schema_change(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::RetryableSchemaChanged {
                    descriptor: "orders".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn non_retryable_error_surfaces_immediately() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), Error> = retry_on_schema_change(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::PlanError {
                    detail: "syntax error".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::PlanError { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
