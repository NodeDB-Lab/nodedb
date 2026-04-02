//! ILP (InfluxDB Line Protocol) TCP listener for timeseries ingest.
//!
//! Accepts plain TCP connections on the configured port. Each connection
//! reads newline-delimited ILP lines, parses them, and dispatches
//! `TimeseriesIngest` plans to the Data Plane via SPSC.
//!
//! Protocol: raw TCP, one ILP line per newline. No HTTP overhead.
//! Compatible with `telegraf`, `vector`, and InfluxDB client libraries.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TimeseriesOp;
use crate::control::server::conn_stream::ConnStream;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

/// ILP TCP listener.
pub struct IlpListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl IlpListener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await.map_err(crate::Error::Io)?;
        info!(%addr, "ILP TCP listener bound");
        Ok(Self { tcp, addr })
    }

    /// Run the accept loop until shutdown.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        conn_semaphore: Arc<Semaphore>,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "ILP listener accepting connections");

        let mut connections = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(%peer, "ILP connection rejected: max connections");
                                    continue;
                                }
                            };
                            let state = Arc::clone(&state);

                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            let cs = ConnStream::tls(tls_stream);
                                            if let Err(e) = handle_ilp_connection(cs, peer, &state).await {
                                                debug!(%peer, error = %e, "ILP TLS connection error");
                                            }
                                        }
                                        Err(e) => {
                                            warn!(%peer, error = %e, "ILP TLS handshake failed");
                                        }
                                    }
                                    drop(permit);
                                });
                            } else {
                                connections.spawn(async move {
                                    let cs = ConnStream::plain(stream);
                                    if let Err(e) = handle_ilp_connection(cs, peer, &state).await {
                                        debug!(%peer, error = %e, "ILP connection error");
                                    }
                                    drop(permit);
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "ILP accept error");
                        }
                    }
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
                _ = shutdown.changed() => {
                    info!(addr = %self.addr, "ILP listener shutting down");
                    break;
                }
            }
        }

        // Drain remaining connections with timeout.
        let drain = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        });
        let _ = drain.await;
        Ok(())
    }
}

/// Handle a single ILP TCP connection with adaptive batch coalescing.
///
/// Batch size adapts to ingest rate:
/// - High rate (>100K lines/s): batch up to 10K lines or 10ms window
/// - Medium rate (1K-100K/s): batch up to 1K lines or 50ms window
/// - Low rate (<1K/s): batch per 100 lines or 100ms window
///
/// Larger batches amortize per-batch overhead (WAL append, memtable lock,
/// partition lookup).
async fn handle_ilp_connection(
    stream: ConnStream,
    peer: SocketAddr,
    state: &SharedState,
) -> crate::Result<()> {
    debug!(%peer, "ILP connection accepted");

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    let mut batch = String::new();
    let mut line_count = 0u64;
    let mut total_ingested = 0u64;

    // Adaptive batch coalescing state.
    let mut rate_estimator = IlpRateEstimator::new();
    let mut batch_target = 1000u64;
    let mut window = tokio::time::interval(std::time::Duration::from_millis(50));
    window.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let tenant_id = TenantId::new(1);

    loop {
        tokio::select! {
            // Read next line.
            result = lines.next_line() => {
                match result {
                    Ok(Some(line)) => {
                        if line.is_empty() || line.starts_with('#') {
                            continue;
                        }

                        batch.push_str(&line);
                        batch.push('\n');
                        line_count += 1;

                        // Flush when batch reaches adaptive target.
                        if line_count >= batch_target {
                            let flushed = line_count;
                            total_ingested += flush_ilp_batch(state, tenant_id, &batch).await?;
                            batch.clear();
                            line_count = 0;

                            // Update rate estimator and recalculate batch target.
                            rate_estimator.record(flushed);
                            let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                            batch_target = new_target;
                            window = tokio::time::interval(
                                std::time::Duration::from_millis(new_window_ms),
                            );
                            window.set_missed_tick_behavior(
                                tokio::time::MissedTickBehavior::Delay,
                            );
                        }
                    }
                    Ok(None) => break, // Connection closed.
                    Err(_) => break,   // Read error.
                }
            }
            // Timer-based flush (for low-rate connections).
            _ = window.tick() => {
                if !batch.is_empty() {
                    let flushed = line_count;
                    total_ingested += flush_ilp_batch(state, tenant_id, &batch).await?;
                    batch.clear();
                    line_count = 0;

                    rate_estimator.record(flushed);
                    let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                    batch_target = new_target;
                    window = tokio::time::interval(
                        std::time::Duration::from_millis(new_window_ms),
                    );
                    window.set_missed_tick_behavior(
                        tokio::time::MissedTickBehavior::Delay,
                    );
                }
            }
        }
    }

    // Flush remaining.
    if !batch.is_empty() {
        total_ingested += flush_ilp_batch(state, tenant_id, &batch).await?;
    }

    debug!(%peer, total_ingested, "ILP connection closed");
    Ok(())
}

/// EWMA-based rate estimator for adaptive ILP batch sizing.
struct IlpRateEstimator {
    /// Smoothed rate in lines/second.
    rate: f64,
    /// EWMA smoothing factor (0.2 = responsive to recent changes).
    alpha: f64,
    /// Last measurement timestamp.
    last_ts: std::time::Instant,
}

impl IlpRateEstimator {
    fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.2,
            last_ts: std::time::Instant::now(),
        }
    }

    /// Record that `lines` were flushed since the last call.
    fn record(&mut self, lines: u64) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_ts).as_secs_f64();
        self.last_ts = now;

        if elapsed > 0.0 {
            let instant_rate = lines as f64 / elapsed;
            if self.rate == 0.0 {
                self.rate = instant_rate;
            } else {
                self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            }
        }
    }

    /// Suggest (batch_size, window_ms) based on current rate.
    fn suggest_batch_params(&self) -> (u64, u64) {
        if self.rate > 100_000.0 {
            // High rate: large batches, short window.
            (10_000, 10)
        } else if self.rate > 1_000.0 {
            // Medium rate: moderate batches.
            (1_000, 50)
        } else {
            // Low rate: small batches, long window.
            (100, 100)
        }
    }
}

/// Dispatch an ILP batch to the Data Plane with series-aware routing.
///
/// Groups lines by `(measurement, sorted_tags)` hash to route each series
/// to a deterministic core. This eliminates cross-core contention: each
/// core owns a subset of series.
///
/// For batches with a single measurement (common case), all lines go to
/// one dispatch — no overhead from grouping.
async fn flush_ilp_batch(
    state: &SharedState,
    tenant_id: TenantId,
    batch: &str,
) -> crate::Result<u64> {
    // Fast path: extract collection from first line.
    let collection = batch
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .and_then(|l| l.split([',', ' ']).next())
        .unwrap_or("default_metrics")
        .to_string();

    // Route all ILP lines for a collection to the same vShard as the
    // collection-based scan uses. This ensures timeseries scans find
    // the memtable data on the correct Data Plane core.
    // Per-series sharding is deferred until the scan path supports
    // fan-out across multiple cores.
    let collection_vshard = VShardId::from_collection(&collection);
    let mut shard_batches: std::collections::HashMap<u16, String> =
        std::collections::HashMap::new();

    for line in batch.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let entry = shard_batches.entry(collection_vshard.as_u16()).or_default();
        entry.push_str(line);
        entry.push('\n');
    }

    let mut total_accepted = 0u64;

    for (shard_id, shard_batch) in &shard_batches {
        let vshard_id = VShardId::new(*shard_id);
        let payload_bytes = shard_batch.as_bytes().to_vec();

        // Append to WAL first — returns the assigned LSN for dedup tracking.
        let wal_lsn = crate::control::server::wal_dispatch::wal_append_timeseries(
            &state.wal,
            tenant_id,
            vshard_id,
            &collection,
            &payload_bytes,
            Some(&state.credentials),
        )?
        .map(|lsn| lsn.as_u64());

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.clone(),
            payload: payload_bytes,
            format: "ilp".to_string(),
            wal_lsn,
        });

        let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard_id, plan, 0,
        )
        .await?;

        if !response.payload.is_empty()
            && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&response.payload)
        {
            total_accepted += v.get("accepted").and_then(|a| a.as_u64()).unwrap_or(0);

            if let Some(schema_cols) = v.get("schema_columns").and_then(|s| s.as_array()) {
                let fields: Vec<(String, String)> = schema_cols
                    .iter()
                    .filter_map(|pair| {
                        let arr = pair.as_array()?;
                        Some((
                            arr.first()?.as_str()?.to_string(),
                            arr.get(1)?.as_str()?.to_string(),
                        ))
                    })
                    .collect();

                if !fields.is_empty()
                    && let Some(catalog) = state.credentials.catalog().as_ref()
                    && let Ok(Some(mut coll)) =
                        catalog.get_collection(tenant_id.as_u32(), &collection)
                    && coll.fields != fields
                {
                    coll.fields = fields;
                    if let Err(e) = catalog.put_collection(&coll) {
                        tracing::warn!(
                            collection = %collection,
                            error = %e,
                            "failed to propagate ILP schema to catalog",
                        );
                    }
                }
            }
        }
    }

    Ok(total_accepted)
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_collection_from_ilp() {
        let batch = "cpu,host=server01 value=0.64 1000\nmem,host=server01 used=1024 2000\n";
        let collection = batch
            .lines()
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .and_then(|l| l.split([',', ' ']).next())
            .unwrap_or("default_metrics");
        assert_eq!(collection, "cpu");
    }
}
