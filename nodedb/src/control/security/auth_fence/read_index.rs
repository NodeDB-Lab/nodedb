// SPDX-License-Identifier: BUSL-1.1

//! Coalesce concurrent read-index requests for one Raft group.
//!
//! A read index may answer a request only when the probe that produced it
//! started after the request arrived. A probe that started earlier can carry
//! a commit index below an entry acknowledged just before the request. So a
//! request that finds a probe in flight waits for the next one, and one probe
//! then answers every request that arrived while the previous probe ran.

use std::future::Future;
use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::Notify;

use crate::control::cluster::read_index::ReadIndexRefusal;

/// Probe numbering and the last answer.
#[derive(Debug, Default)]
struct CoalescerState {
    /// The probe running now, if any.
    in_flight: Option<u64>,
    /// The highest probe that finished.
    completed: u64,
    /// The answer of probe `completed`.
    result: Option<Result<u64, ReadIndexRefusal>>,
}

/// Coalesces read-index probes for one group.
#[derive(Debug, Default)]
pub struct ReadIndexCoalescer {
    state: Mutex<CoalescerState>,
    done: Notify,
}

/// Clears the in-flight probe when its runner stops without an answer, so a
/// cancelled runner never blocks later requests.
struct RunGuard<'a> {
    coalescer: &'a ReadIndexCoalescer,
    probe: u64,
    finished: bool,
}

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        {
            let mut state = self.coalescer.lock();
            if state.in_flight == Some(self.probe) {
                state.in_flight = None;
            }
        }
        self.coalescer.done.notify_waiters();
    }
}

impl ReadIndexCoalescer {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CoalescerState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A read index taken by a probe that started after this call, running
    /// `probe` when no such probe is in flight. Refuses with a timeout once
    /// `deadline` passes.
    pub async fn read_index<F, Fut>(
        &self,
        deadline: Instant,
        probe: F,
    ) -> Result<u64, ReadIndexRefusal>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<u64, ReadIndexRefusal>>,
    {
        let started = Instant::now();
        let target = {
            let state = self.lock();
            match state.in_flight {
                Some(running) => running + 1,
                None => state.completed + 1,
            }
        };
        loop {
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let run = {
                let mut state = self.lock();
                if state.completed >= target
                    && let Some(result) = state.result
                {
                    return result;
                }
                if state.in_flight.is_none() {
                    state.in_flight = Some(target);
                    true
                } else {
                    false
                }
            };

            if run {
                let mut guard = RunGuard {
                    coalescer: self,
                    probe: target,
                    finished: false,
                };
                let result = probe().await;
                {
                    let mut state = self.lock();
                    state.completed = state.completed.max(target);
                    state.result = Some(result);
                    state.in_flight = None;
                }
                guard.finished = true;
                self.done.notify_waiters();
                return result;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ReadIndexRefusal::Timeout {
                    waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                });
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[tokio::test]
    async fn a_request_with_no_probe_in_flight_runs_one() {
        let coalescer = ReadIndexCoalescer::new();
        let probes = AtomicU64::new(0);
        let index = coalescer
            .read_index(deadline(), || async {
                Ok(probes.fetch_add(1, Ordering::SeqCst) + 10)
            })
            .await;
        assert_eq!(index, Ok(10));
        assert_eq!(probes.load(Ordering::SeqCst), 1);
    }

    /// A request that arrives while a probe runs never takes that probe's
    /// answer: it waits for a probe that started after it.
    #[tokio::test]
    async fn a_request_arriving_during_a_probe_waits_for_the_next_one() {
        let coalescer = Arc::new(ReadIndexCoalescer::new());
        let release = Arc::new(Notify::new());
        let probes = Arc::new(AtomicU64::new(0));

        let first = {
            let coalescer = Arc::clone(&coalescer);
            let release = Arc::clone(&release);
            let probes = Arc::clone(&probes);
            tokio::spawn(async move {
                coalescer
                    .read_index(deadline(), || {
                        let release = Arc::clone(&release);
                        let probes = Arc::clone(&probes);
                        async move {
                            let n = probes.fetch_add(1, Ordering::SeqCst) + 1;
                            if n == 1 {
                                release.notified().await;
                            }
                            Ok(n * 100)
                        }
                    })
                    .await
            })
        };
        while probes.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        let second = {
            let coalescer = Arc::clone(&coalescer);
            let probes = Arc::clone(&probes);
            tokio::spawn(async move {
                coalescer
                    .read_index(deadline(), || {
                        let probes = Arc::clone(&probes);
                        async move { Ok((probes.fetch_add(1, Ordering::SeqCst) + 1) * 100) }
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        release.notify_one();

        assert_eq!(first.await.expect("first"), Ok(100));
        assert_eq!(second.await.expect("second"), Ok(200));
        assert_eq!(probes.load(Ordering::SeqCst), 2);
    }

    /// A runner dropped mid-probe leaves no probe marked in flight.
    #[tokio::test]
    async fn a_cancelled_runner_does_not_block_later_requests() {
        let coalescer = Arc::new(ReadIndexCoalescer::new());
        let stuck = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(
                async move { coalescer.read_index(deadline(), std::future::pending).await },
            )
        };
        tokio::task::yield_now().await;
        stuck.abort();
        let _ = stuck.await;

        let index = coalescer.read_index(deadline(), || async { Ok(7) }).await;
        assert_eq!(index, Ok(7));
    }

    #[tokio::test]
    async fn a_request_past_its_deadline_times_out() {
        let coalescer = Arc::new(ReadIndexCoalescer::new());
        let holder = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(
                async move { coalescer.read_index(deadline(), std::future::pending).await },
            )
        };
        tokio::task::yield_now().await;
        let refused = coalescer
            .read_index(Instant::now() + Duration::from_millis(20), || async {
                Ok(1)
            })
            .await;
        assert!(matches!(refused, Err(ReadIndexRefusal::Timeout { .. })));
        holder.abort();
    }
}
