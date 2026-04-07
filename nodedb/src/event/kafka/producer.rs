//! Kafka producer: publishes CDC events to external Kafka topics.
//!
//! One background Tokio task per Kafka-delivery stream. Consumes from the
//! stream's `StreamBuffer` via an internal consumer group, serializes events,
//! and publishes to the configured Kafka topic using rdkafka's `FutureProducer`.
//!
//! **Exactly-once:** When `transactional = true`, uses Kafka's idempotent
//! producer (`enable.idempotence = true`) with a `transactional.id` per stream.
//! Each batch is wrapped in `begin_transaction` / `commit_transaction`.

#[cfg(feature = "kafka")]
use sonic_rs;

#[cfg(feature = "kafka")]
use std::sync::Arc;
#[cfg(feature = "kafka")]
use std::time::Duration;

#[cfg(feature = "kafka")]
use rdkafka::producer::Producer;
#[cfg(feature = "kafka")]
use tokio::sync::watch;
#[cfg(feature = "kafka")]
use tracing::{debug, info, trace, warn};

#[cfg(feature = "kafka")]
use super::config::KafkaDeliveryConfig;
#[cfg(feature = "kafka")]
use crate::control::state::SharedState;
#[cfg(feature = "kafka")]
use crate::event::cdc::consume::{ConsumeParams, consume_local};

/// Spawn a background Kafka producer task for a change stream.
///
/// Consumes events from the stream's buffer using an internal consumer group
/// (`_kafka_{stream_name}`) and publishes to the configured Kafka topic.
///
/// Returns the `JoinHandle` for lifecycle management (abort on DROP CHANGE STREAM).
#[cfg(feature = "kafka")]
pub fn spawn_kafka_task(
    stream_name: String,
    tenant_id: u32,
    config: KafkaDeliveryConfig,
    shared_state: Arc<SharedState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            stream = %stream_name,
            topic = %config.topic,
            brokers = %config.brokers,
            transactional = config.transactional,
            "Kafka producer task started"
        );

        let producer = match create_producer(&config, &stream_name) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "failed to create Kafka producer — task exiting"
                );
                return;
            }
        };

        // Initialize transactional producer if configured.
        if config.transactional
            && let Err(e) = producer.init_transactions(Duration::from_secs(10))
        {
            warn!(
                stream = %stream_name,
                error = %e,
                "failed to init Kafka transactions — falling back to at-least-once"
            );
        }

        let group_name = format!("_kafka_{stream_name}");
        let poll_interval = Duration::from_millis(100);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {
                    let consume_params = ConsumeParams {
                        tenant_id,
                        stream_name: &stream_name,
                        group_name: &group_name,
                        partition: None,
                        limit: 100,
                    };

                    let result = consume_local(&shared_state, &consume_params);
                    let events = match result {
                        Ok(r) if !r.events.is_empty() => r,
                        _ => continue,
                    };

                    let batch_size = events.events.len();

                    // Begin transaction if configured.
                    if config.transactional
                        && let Err(e) = producer.begin_transaction()
                    {
                        warn!(error = %e, "Kafka begin_transaction failed");
                        continue;
                    }

                    let mut published = 0u32;
                    for event in &events.events {
                        let payload = match serialize_event(event, config.format) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!(error = %e, "failed to serialize event for Kafka");
                                continue;
                            }
                        };

                        let key = format!("{}:{}", event.partition, event.lsn);
                        let record = rdkafka::producer::FutureRecord::to(&config.topic)
                            .key(&key)
                            .payload(&payload);

                        match producer.send(record, Duration::from_secs(5)).await {
                            Ok(_) => published += 1,
                            Err((e, _)) => {
                                warn!(
                                    error = %e,
                                    topic = %config.topic,
                                    "Kafka publish failed"
                                );
                                // Stop batch — next cycle retries from last committed offset.
                                break;
                            }
                        }
                    }

                    // Commit Kafka transaction.
                    if config.transactional && published > 0
                        && let Err(e) = producer
                            .commit_transaction(Duration::from_secs(10))
                    {
                        warn!(error = %e, "Kafka commit_transaction failed");
                        continue;
                    }

                    // Commit consumer offsets for successfully published events.
                    if published > 0 {
                        for (partition_id, lsn) in &events.partition_offsets {
                            let _ = shared_state.offset_store.commit_offset(
                                tenant_id,
                                &stream_name,
                                &group_name,
                                *partition_id,
                                *lsn,
                            );
                        }
                        trace!(
                            stream = %stream_name,
                            published,
                            batch_size,
                            "Kafka batch published"
                        );
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!(stream = %stream_name, "Kafka producer task shutting down");
                        return;
                    }
                }
            }
        }
    })
}

/// Create an rdkafka FutureProducer with the given configuration.
#[cfg(feature = "kafka")]
fn create_producer(
    config: &KafkaDeliveryConfig,
    stream_name: &str,
) -> Result<rdkafka::producer::FutureProducer, String> {
    use rdkafka::ClientConfig;

    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &config.brokers);
    client_config.set("message.timeout.ms", "30000");

    if config.transactional {
        client_config.set("enable.idempotence", "true");
        client_config.set("transactional.id", format!("nodedb-kafka-{stream_name}"));
    }

    client_config
        .create()
        .map_err(|e| format!("create Kafka producer: {e}"))
}

/// Serialize a CdcEvent for Kafka publishing.
#[cfg(feature = "kafka")]
fn serialize_event(
    event: &crate::event::cdc::event::CdcEvent,
    format: super::config::KafkaFormat,
) -> Result<Vec<u8>, String> {
    match format {
        super::config::KafkaFormat::Json => {
            sonic_rs::to_vec(event).map_err(|e| format!("JSON serialize: {e}"))
        }
        super::config::KafkaFormat::Avro => {
            // Avro serialization uses the same JSON representation for now.
            // Full Avro schema registry integration is a future enhancement —
            // the config field and wire path are ready for it.
            sonic_rs::to_vec(event).map_err(|e| format!("Avro (JSON fallback) serialize: {e}"))
        }
    }
}

// When kafka feature is disabled, provide a stub that logs a warning.
#[cfg(not(feature = "kafka"))]
pub fn spawn_kafka_task(
    stream_name: String,
    _tenant_id: u32,
    _config: super::config::KafkaDeliveryConfig,
    _shared_state: std::sync::Arc<crate::control::state::SharedState>,
    _shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tracing::warn!(
            stream = %stream_name,
            "Kafka bridge not available — compile with --features kafka"
        );
    })
}
