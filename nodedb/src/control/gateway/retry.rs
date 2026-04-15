//! Typed `NotLeader` retry with 3-attempt budget + 50/100/200 ms backoff.
//!
//! When a remote dispatch returns `Error::NotLeader`, the retry helper:
//! 1. Extracts the hinted new leader from the error.
//! 2. Updates the routing table entry for the affected group.
//! 3. Sleeps for the appropriate backoff duration.
//! 4. Re-invokes the closure.
//!
//! If the hinted leader is unknown (no hint), we still retry after sleep
//! without updating the routing table — a subsequent routing lookup will
//! re-read the table from the current routing state.
//!
//! After `MAX_RETRIES` attempts the final `NotLeader` error is propagated.

use std::future::Future;
use std::sync::RwLock;

use tokio::time::{Duration, sleep};
use tracing::debug;

use nodedb_cluster::RoutingTable;

use crate::Error;

/// Maximum number of dispatch attempts (initial + 2 retries = 3 total).
pub const MAX_RETRIES: usize = 3;

/// Backoff durations for each retry attempt.
const BACKOFF_MS: [u64; MAX_RETRIES] = [50, 100, 200];

/// Execute `f` up to `MAX_RETRIES` times, retrying on `Error::NotLeader`.
///
/// `f` receives the current attempt index (0-based).
///
/// On `NotLeader` with a hinted leader, the routing table is updated before
/// the next retry so the caller's routing decision changes. On non-`NotLeader`
/// errors the error is propagated immediately without retry.
pub async fn retry_not_leader<F, Fut, T>(
    routing: Option<&RwLock<RoutingTable>>,
    f: F,
) -> Result<T, Error>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut last_err = None;
    for (attempt, &backoff_ms) in BACKOFF_MS.iter().enumerate() {
        match f(attempt).await {
            Ok(v) => return Ok(v),
            Err(Error::NotLeader {
                vshard_id,
                leader_node,
                ..
            }) => {
                debug!(
                    attempt,
                    vshard_id = vshard_id.as_u16(),
                    leader_node,
                    "gateway: NotLeader — will retry with new leader hint"
                );

                // Update routing table if we have a hint and a table.
                if let (true, Some(rt)) = (leader_node != 0, routing)
                    && let Ok(mut table) = rt.write()
                    && let Ok(group_id) = table.group_for_vshard(vshard_id.as_u16())
                {
                    table.set_leader(group_id, leader_node);
                }

                if attempt + 1 < MAX_RETRIES {
                    sleep(Duration::from_millis(backoff_ms)).await;
                }

                last_err = Some(Error::NotLeader {
                    vshard_id,
                    leader_node,
                    leader_addr: String::new(),
                });
            }
            Err(other) => return Err(other),
        }
    }

    Err(last_err.unwrap_or(Error::Internal {
        detail: "retry_not_leader exhausted all attempts".into(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::types::VShardId;

    #[tokio::test]
    async fn success_on_first_attempt() {
        let result = retry_not_leader(None, |_attempt| async { Ok::<u32, Error>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn success_on_second_attempt() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&call_count);
        let result = retry_not_leader(None, move |_attempt| {
            let c = Arc::clone(&count);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(Error::NotLeader {
                        vshard_id: VShardId::new(0),
                        leader_node: 2,
                        leader_addr: "10.0.0.2:9400".into(),
                    })
                } else {
                    Ok::<u32, Error>(99)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exhausts_retries_returns_not_leader() {
        let result = retry_not_leader(None, |_| async {
            Err::<u32, Error>(Error::NotLeader {
                vshard_id: VShardId::new(1),
                leader_node: 0,
                leader_addr: String::new(),
            })
        })
        .await;
        assert!(matches!(result, Err(Error::NotLeader { .. })));
    }

    #[tokio::test]
    async fn non_not_leader_error_propagates_immediately() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&call_count);
        let result = retry_not_leader(None, move |_| {
            let c = Arc::clone(&count);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<u32, Error>(Error::BadRequest {
                    detail: "bad".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::BadRequest { .. })));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn routing_table_updated_on_not_leader_hint() {
        let table = RoutingTable::uniform(1, &[1, 2], 2);
        let rt = Arc::new(RwLock::new(table));
        let rt_clone = Arc::clone(&rt);

        let call_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&call_count);

        let _ = retry_not_leader(Some(&*rt_clone), move |_| {
            let c = Arc::clone(&count);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(Error::NotLeader {
                        vshard_id: VShardId::new(0),
                        leader_node: 2,
                        leader_addr: "addr".into(),
                    })
                } else {
                    Ok::<(), Error>(())
                }
            }
        })
        .await;

        let table = rt.read().unwrap();
        assert_eq!(table.leader_for_vshard(0).unwrap(), 2);
    }
}
