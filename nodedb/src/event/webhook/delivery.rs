// SPDX-License-Identifier: BUSL-1.1

//! Webhook delivery task: background Tokio task per webhook-enabled stream.
//!
//! Consumes events from the stream's buffer using an internal consumer group
//! (`_webhook:<stream_name>`), POSTs each event batch to the URL, and commits
//! offsets on success. In a cluster only the node that holds the leader lease
//! of the stream's owning group delivers. It reads every partition, from its
//! own buffer and from the members of groups it does not replicate. See
//! [`crate::event::cdc::sink_owner`] and [`deliver_event`] for the fencing and
//! idempotency headers. Failed deliveries retry with exponential backoff.
//!
//! The URL is user-supplied, so every POST goes through the egress client:
//! the URL passes the `[egress]` policy, every resolved address is checked
//! at connect time, and no redirect is followed. A URL the policy refuses
//! stops the task, since no retry can deliver to it.

use std::sync::Arc;
use std::time::Duration;

use sonic_rs;

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::control::security::egress::{
    EgressClientOptions, EgressFault, SystemLookup, classify, egress_client,
};
use crate::control::security::redaction::RedactionStore;
use crate::control::state::SharedState;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::sink_owner::{SinkFence, register_sink_groups, sink_lease, webhook_group};

use super::types::WebhookConfig;

/// Spawn a webhook delivery task for a single stream.
///
/// Returns a handle that can be used to abort the task.
pub fn spawn_delivery_task(
    state: Arc<SharedState>,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    stream_name: String,
    config: WebhookConfig,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let group_name = webhook_group(&stream_name);

    // The replicated offset commit names a registered group. Installing the
    // stream registered it on every node; this covers a stream registered
    // before its sink task started.
    if let Some(def) = state
        .stream_registry
        .get(database_id, tenant_id, &stream_name)
    {
        register_sink_groups(&state.group_registry, &def);
    }

    tokio::spawn(async move {
        delivery_loop(
            state,
            database_id,
            tenant_id,
            stream_name,
            group_name,
            config,
            shutdown,
        )
        .await;
    })
}

/// The main delivery loop.
async fn delivery_loop(
    state: Arc<SharedState>,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    stream_name: String,
    group_name: String,
    config: WebhookConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(
        stream = %stream_name,
        url = %config.url,
        "webhook delivery task started"
    );

    let policy = Arc::clone(state.remote_sources.egress());
    if let Err(e) = policy.check_url(&config.url) {
        tracing::error!(
            stream = %stream_name,
            error = %e,
            "webhook delivery stopped: the URL fails the [egress] policy"
        );
        return;
    }
    let timeout = Duration::from_secs(config.timeout_secs);
    let client = match egress_client(
        policy,
        Arc::new(SystemLookup),
        EgressClientOptions {
            timeout: Some(timeout),
            connect_timeout: timeout,
            read_timeout: timeout,
            max_redirects: 0,
        },
    ) {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(
                stream = %stream_name,
                error = %e,
                "webhook delivery stopped: the HTTP client cannot be built"
            );
            return;
        }
    };

    loop {
        if *shutdown.borrow() {
            debug!(stream = %stream_name, "webhook delivery task shutting down");
            return;
        }

        // Only the lease holder of the stream's owning group delivers.
        let Some(lease) = sink_lease(&state, database_id, &stream_name) else {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                _ = shutdown.changed() => {}
            }
            continue;
        };
        let fence = SinkFence {
            state: &state,
            database_id,
            stream_name: &stream_name,
            lease,
        };

        // Read every partition after the internal consumer group's offsets:
        // local ones from this node's buffer, the rest from their members.
        let consume_params = crate::event::cdc::consume::ConsumeParams {
            database_id,
            tenant_id,
            stream_name: &stream_name,
            group_name: &group_name,
            partition: None,
            limit: 100, // Batch size per delivery cycle.
        };

        let result = crate::event::cdc::consume::consume_for_sink(&state, &consume_params).await;

        match result {
            Ok(consume_result) if !consume_result.events.is_empty() => {
                let batch_size = consume_result.events.len();
                trace!(
                    stream = %stream_name,
                    batch_size,
                    "delivering webhook batch"
                );

                // The destination is subscribed on behalf of the principal that
                // created the stream, whose roles the subscription record
                // carries. Without that record there is no scope to evaluate a
                // column redaction policy against, so nothing is delivered.
                let Some(mut subscriber) =
                    CdcSubscriberScope::for_stream(&state, database_id, tenant_id, &stream_name)
                else {
                    warn!(
                        stream = %stream_name,
                        "webhook delivery: change stream is not registered — \
                         refusing to deliver events with no subscriber scope"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };

                // POST each event individually (not batched — simpler retry semantics).
                let delivered = deliver_batch(BatchDelivery {
                    client: &client,
                    config: &config,
                    stream_name: &stream_name,
                    store: &state.redaction,
                    subscriber: &mut subscriber,
                    events: &consume_result.events,
                    fence: &fence,
                })
                .await;

                // Commit offsets for the events this cycle finished with, only
                // while this node still holds the batch's lease. A later owner
                // redelivers the rest under the same idempotency keys.
                if delivered > 0 && fence.holds() {
                    // Commit the exact composite position of the last
                    // processed event in each partition, taken from the batch
                    // as consumed: an event withheld by a redaction rule is
                    // finished with too, and must not be redelivered forever.
                    // One replicated commit per delivery tick carries every
                    // partition, so a delivery task on any node resumes here.
                    let offsets = crate::event::cdc::consume::batch_tails(
                        &consume_result.events[..delivered],
                    )
                    .into_iter()
                    .map(|(partition_id, offset)| {
                        crate::event::cdc::consumer_group::PartitionOffset::new(
                            partition_id,
                            offset,
                        )
                    })
                    .collect();
                    if let Err(e) = crate::control::server::shared::ddl::neutral::consumer_group::commit::commit_group_offsets(
                        &state,
                        database_id,
                        tenant_id,
                        &stream_name,
                        &group_name,
                        offsets,
                    )
                    .await
                    {
                        warn!(
                            stream = %stream_name,
                            error = %e.message,
                            "failed to commit webhook offset"
                        );
                    }
                    trace!(
                        stream = %stream_name,
                        delivered,
                        total = batch_size,
                        "webhook batch delivered"
                    );
                }

                // If we delivered everything, immediately try again (more may have arrived).
                if delivered == batch_size {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            Ok(_) => {
                // No events — wait before polling again.
            }
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "webhook delivery: consume error"
                );
            }
        }

        // Poll interval: 200ms for reasonable latency without busy-spinning.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// One batch delivery's inputs.
struct BatchDelivery<'a> {
    client: &'a reqwest::Client,
    config: &'a WebhookConfig,
    stream_name: &'a str,
    store: &'a RedactionStore,
    subscriber: &'a mut CdcSubscriberScope,
    events: &'a [Arc<CdcEvent>],
    fence: &'a SinkFence<'a>,
}

/// POST a batch, redacting each event for the subscriber first.
///
/// Returns how many leading events of the batch this cycle finished with, which
/// is what the offset commit advances over. An event a rule covers but whose
/// payload could not be rewritten is skipped rather than POSTed in the clear,
/// and still counts as finished so the cursor moves past it instead of
/// redelivering it forever. Delivery stops at the first POST failure, and at
/// the first POST this node no longer holds the lease for, so the next cycle
/// or the next owner retries from the last committed offset.
async fn deliver_batch(delivery: BatchDelivery<'_>) -> usize {
    let BatchDelivery {
        client,
        config,
        stream_name,
        store,
        subscriber,
        events,
        fence,
    } = delivery;
    let mut finished = 0usize;
    for event in events {
        if let Some(event) = subscriber.apply(store, event) {
            let target = DeliveryTarget {
                client,
                config,
                stream_name,
                fence,
            };
            if !deliver_event(&target, &event).await {
                break;
            }
        }
        finished += 1;
    }
    finished
}

/// Where and under which lease one event is POSTed.
struct DeliveryTarget<'a> {
    client: &'a reqwest::Client,
    config: &'a WebhookConfig,
    stream_name: &'a str,
    fence: &'a SinkFence<'a>,
}

/// POST a single event to the webhook URL. Returns true on success.
/// Retries with exponential backoff on failure. Every attempt first checks
/// that this node still holds the batch's lease, and returns false without
/// POSTing when it does not.
///
/// Every request carries two headers an endpoint uses to apply each event
/// once:
/// - `X-Idempotency-Key`: `<partition>:<epoch>:<index>:<sequence>`, the
///   event's partition and position. Every owner and every retry sends the
///   same key for the same event, so the endpoint drops a key it has seen.
/// - `X-Fencing-Token`: the Raft term of the owning group's leader lease the
///   delivering node holds. It rises with each new owner. An endpoint that
///   keeps the highest token it accepted rejects a lower one: that request
///   comes from an owner whose lease a later owner replaced.
async fn deliver_event(target: &DeliveryTarget<'_>, event: &CdcEvent) -> bool {
    let DeliveryTarget {
        client,
        config,
        stream_name,
        fence,
    } = *target;
    let body = match sonic_rs::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                error = %e,
                stream = stream_name,
                row_id = %event.row_id,
                "failed to serialize webhook event, skipping"
            );
            return false;
        }
    };
    let idempotency_key = format!("{}:{}", event.partition, event.offset_token());
    let fencing_token = fence.lease.term.to_string();

    for attempt in 0..=config.max_retries {
        if !fence.holds() {
            debug!(
                stream = stream_name,
                lsn = event.lsn,
                "webhook delivery stopped: this node no longer holds the sink lease"
            );
            return false;
        }
        let mut request = client
            .post(&config.url)
            .header("Content-Type", "application/json")
            .header("X-Idempotency-Key", &idempotency_key)
            .header("X-Fencing-Token", &fencing_token)
            .header("X-Event-Sequence", event.sequence.to_string())
            .header("X-Stream-Name", stream_name)
            .header("X-Partition", event.partition.to_string())
            .header("X-LSN", event.lsn.to_string())
            .header("X-CDC-Offset", event.offset_token());

        // Add custom headers.
        for (key, value) in &config.headers {
            request = request.header(key, value);
        }

        match request.body(body.clone()).send().await {
            Ok(response) if response.status().is_success() => {
                trace!(
                    stream = stream_name,
                    lsn = event.lsn,
                    status = response.status().as_u16(),
                    "webhook delivered"
                );
                return true;
            }
            Ok(response) => {
                let status = response.status().as_u16();
                // 4xx errors (except 429) are permanent — don't retry.
                if (400..500).contains(&status) && status != 429 {
                    warn!(
                        stream = stream_name,
                        lsn = event.lsn,
                        status,
                        attempt,
                        "webhook rejected with client error, not retrying"
                    );
                    return false;
                }
                warn!(
                    stream = stream_name,
                    lsn = event.lsn,
                    status,
                    attempt,
                    max_retries = config.max_retries,
                    "webhook delivery failed, retrying"
                );
            }
            Err(e) => {
                if let EgressFault::Refused(refused) = classify(&e) {
                    warn!(
                        stream = stream_name,
                        lsn = event.lsn,
                        error = %refused,
                        "webhook refused by the egress policy, not retrying"
                    );
                    return false;
                }
                warn!(
                    stream = stream_name,
                    lsn = event.lsn,
                    error = %e.without_url(),
                    attempt,
                    max_retries = config.max_retries,
                    "webhook delivery error, retrying"
                );
            }
        }

        if attempt < config.max_retries {
            tokio::time::sleep(backoff_delay(attempt)).await;
        }
    }

    warn!(
        stream = stream_name,
        lsn = event.lsn,
        max_retries = config.max_retries,
        "webhook delivery exhausted retries"
    );
    false
}

/// Compute exponential backoff delay for a given retry attempt.
///
/// Formula: min(100ms * 2^attempt, 10s).
fn backoff_delay(attempt: u32) -> Duration {
    Duration::from_millis(100 * 2u64.saturating_pow(attempt)).min(Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_exponential_with_cap() {
        assert_eq!(backoff_delay(0), Duration::from_millis(100));
        assert_eq!(backoff_delay(1), Duration::from_millis(200));
        assert_eq!(backoff_delay(2), Duration::from_millis(400));
        assert_eq!(backoff_delay(3), Duration::from_millis(800));
        assert_eq!(backoff_delay(4), Duration::from_millis(1600));
        assert_eq!(backoff_delay(5), Duration::from_millis(3200));
        assert_eq!(backoff_delay(6), Duration::from_millis(6400));
        // Capped at 10s.
        assert_eq!(backoff_delay(7), Duration::from_secs(10));
        assert_eq!(backoff_delay(10), Duration::from_secs(10));
        assert_eq!(backoff_delay(32), Duration::from_secs(10));
    }

    /// A webhook URL is user-supplied. One the egress policy refuses stops
    /// the task before any request leaves the server.
    #[tokio::test]
    async fn a_url_the_egress_policy_refuses_is_never_contacted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("webhook.wal")).expect("wal"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let (_tx, shutdown) = watch::channel(false);
        for url in [
            format!("http://127.0.0.1:{port}/hook"),
            format!("http://localhost:{port}/hook"),
            "https://169.254.169.254/latest/meta-data/".to_string(),
        ] {
            let task = spawn_delivery_task(
                Arc::clone(&state),
                crate::types::DatabaseId::DEFAULT,
                1,
                "ssrf_stream".into(),
                WebhookConfig {
                    url: url.clone(),
                    ..WebhookConfig::default()
                },
                shutdown.clone(),
            );
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap_or_else(|_| panic!("{url}: the task did not stop"))
                .expect("the task ends cleanly");
        }
        let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(accepted.is_err(), "a refused webhook reached the listener");
    }

    #[test]
    fn webhook_group_name_format() {
        assert_eq!(webhook_group("orders_stream"), "_webhook:orders_stream");
    }
}
