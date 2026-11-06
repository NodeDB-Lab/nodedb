// SPDX-License-Identifier: BUSL-1.1

//! Cross-cluster QUIC link: one mirror's connection to one source replica at a
//! time, with its hello, reconnect backoff, and move to another seed.
//!
//! [`CrossClusterLink`] lives on the Control Plane. Each request runs on its own
//! bidi stream, so a long poll on one group never blocks another.

use std::net::SocketAddr;
use std::time::Duration;

use rand::RngExt as _;
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::error::MirrorError;
use super::handshake::{MIRROR_PROTOCOL_VERSION, MirrorHello, MirrorHelloAck};
use super::tail_wire::{
    MAX_REQUEST_FRAME, MAX_RESPONSE_FRAME, MAX_TAIL_WAIT_MS, MirrorRefusal, MirrorRequest,
    MirrorResponse, encode_frame, read_frame, write_frame,
};
use super::throttle::SendThrottle;

/// Base reconnect delay.
const RECONNECT_BASE_MS: u64 = 500;
/// Maximum reconnect delay.
const RECONNECT_MAX_MS: u64 = 30_000;
/// Jitter fraction: ±25 % of the current delay.
const JITTER_FRACTION: f64 = 0.25;
/// How long a request waits for its response, past the source's longest poll.
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(MAX_TAIL_WAIT_MS + 15_000);

/// What names one link.
#[derive(Debug, Clone)]
pub struct MirrorLinkConfig {
    /// The name this mirror knows the source cluster by.
    pub source_cluster: String,
    /// The source database's name.
    pub source_database: String,
    /// This cluster's id.
    pub mirror_cluster_id: u64,
    /// The link's id on the source.
    pub link_id: u64,
    /// The source's mirror listeners. A link moves on when a replica cannot
    /// answer.
    pub seeds: Vec<SocketAddr>,
    /// The name every source certificate carries.
    pub server_name: String,
}

#[derive(Debug, Default)]
struct LinkState {
    /// The next seed to dial.
    seed: usize,
    connection: Option<quinn::Connection>,
}

/// One mirror's connection to its source.
pub struct CrossClusterLink {
    config: MirrorLinkConfig,
    client_config: quinn::ClientConfig,
    endpoint: quinn::Endpoint,
    state: Mutex<LinkState>,
    /// Bytes-in-flight throttle of a bootstrap's transfer.
    pub throttle: SendThrottle,
}

impl CrossClusterLink {
    pub fn new(
        config: MirrorLinkConfig,
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        throttle: SendThrottle,
    ) -> Self {
        Self {
            config,
            client_config,
            endpoint,
            state: Mutex::new(LinkState::default()),
            throttle,
        }
    }

    /// The source cluster this link follows.
    pub fn source_cluster_id(&self) -> &str {
        &self.config.source_cluster
    }

    /// The link's id on the source.
    pub fn link_id(&self) -> u64 {
        self.config.link_id
    }

    /// How many replicas this link can try.
    pub fn seed_count(&self) -> usize {
        self.config.seeds.len()
    }

    /// Dial the current seed and run the hello. A refusal comes back as is:
    /// retrying it fixes nothing an operator did not fix.
    pub async fn connect(&self) -> Result<MirrorHelloAck, MirrorError> {
        let (address, seed) = {
            let state = self.state.lock().await;
            let seed = state.seed;
            let address =
                self.config
                    .seeds
                    .get(seed)
                    .copied()
                    .ok_or_else(|| MirrorError::Transport {
                        detail: format!(
                            "mirror link {} has no source seed to dial",
                            self.config.link_id
                        ),
                    })?;
            (address, seed)
        };
        let connection = self.dial(address).await?;
        let hello = MirrorRequest::Hello(MirrorHello {
            source_cluster: self.config.source_cluster.clone(),
            source_database: self.config.source_database.clone(),
            mirror_cluster_id: self.config.mirror_cluster_id,
            link_id: self.config.link_id,
            protocol_version: MIRROR_PROTOCOL_VERSION,
        });
        match exchange_on(&connection, &hello).await? {
            MirrorResponse::Hello(ack) => {
                let mut state = self.state.lock().await;
                if state.seed == seed {
                    state.connection = Some(connection);
                }
                Ok(ack)
            }
            MirrorResponse::Refused(refusal) => Err(MirrorError::Refused(refusal)),
            MirrorResponse::Tail(_) | MirrorResponse::Ack => {
                Err(MirrorError::Refused(MirrorRefusal::Malformed {
                    detail: "the source answered a hello with another response".to_string(),
                }))
            }
        }
    }

    /// Drop the connection and aim at the next seed.
    pub async fn next_seed(&self) {
        let mut state = self.state.lock().await;
        state.connection = None;
        let count = self.config.seeds.len().max(1);
        state.seed = (state.seed + 1) % count;
    }

    /// Run `request` on the live connection, connecting first when there is
    /// none. A transport error drops the connection.
    pub async fn exchange(&self, request: &MirrorRequest) -> Result<MirrorResponse, MirrorError> {
        let live = self.state.lock().await.connection.clone();
        let connection = match live {
            Some(connection) => connection,
            None => {
                self.connect().await?;
                self.state.lock().await.connection.clone().ok_or_else(|| {
                    MirrorError::Transport {
                        detail: "the mirror link moved seeds while it connected".to_string(),
                    }
                })?
            }
        };
        let answered = exchange_on(&connection, request).await;
        if matches!(answered, Err(MirrorError::Transport { .. })) {
            self.state.lock().await.connection = None;
        }
        answered
    }

    /// Reconnect with exponential backoff, trying each seed in turn. Only a
    /// refusal the next attempt can clear retries.
    pub async fn schedule_reconnect(&self) -> Result<MirrorHelloAck, MirrorError> {
        self.state.lock().await.connection = None;
        self.throttle.reset();
        let mut delay_ms = RECONNECT_BASE_MS;
        loop {
            let sleep_ms = delay_ms.saturating_add_signed(jitter_for(delay_ms));
            info!(
                source_cluster = %self.config.source_cluster,
                sleep_ms,
                "mirror link: reconnecting after disconnect"
            );
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            match self.connect().await {
                Ok(ack) => return Ok(ack),
                Err(MirrorError::Refused(refusal)) if !refusal.is_transient() => {
                    return Err(MirrorError::Refused(refusal));
                }
                Err(error) => {
                    warn!(
                        source_cluster = %self.config.source_cluster,
                        %error,
                        "mirror link: connect failed, will retry on the next seed"
                    );
                }
            }
            self.next_seed().await;
            delay_ms = (delay_ms * 2).min(RECONNECT_MAX_MS);
        }
    }

    async fn dial(&self, address: SocketAddr) -> Result<quinn::Connection, MirrorError> {
        self.endpoint
            .connect_with(
                self.client_config.clone(),
                address,
                &self.config.server_name,
            )
            .map_err(|e| MirrorError::Transport {
                detail: format!("connect to source {address}: {e}"),
            })?
            .await
            .map_err(|e| MirrorError::Transport {
                detail: format!("QUIC handshake with source {address}: {e}"),
            })
    }
}

/// One request and one response on a fresh stream of `connection`. The source's
/// frame is bounded before the read allocates, and the wait is bounded too.
async fn exchange_on(
    connection: &quinn::Connection,
    request: &MirrorRequest,
) -> Result<MirrorResponse, MirrorError> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| MirrorError::Transport {
            detail: format!("open a mirror stream: {e}"),
        })?;
    let payload = encode_frame(request, MAX_REQUEST_FRAME)?;
    write_frame(&mut send, &payload).await?;
    send.finish().map_err(|e| MirrorError::Transport {
        detail: format!("finish a mirror request: {e}"),
    })?;
    let bytes = tokio::time::timeout(RESPONSE_TIMEOUT, read_frame(&mut recv, MAX_RESPONSE_FRAME))
        .await
        .map_err(|_| MirrorError::Transport {
            detail: format!(
                "the source sent no response within {} ms",
                RESPONSE_TIMEOUT.as_millis()
            ),
        })??;
    zerompk::from_msgpack(&bytes).map_err(|e| MirrorError::HandshakeCodec {
        detail: format!("decode a mirror response: {e}"),
    })
}

/// Compute ±`JITTER_FRACTION` jitter for a given delay, returning a signed
/// offset.
fn jitter_for(delay_ms: u64) -> i64 {
    let max = (delay_ms as f64 * JITTER_FRACTION) as i64;
    if max == 0 {
        return 0;
    }
    rand::rng().random_range(-max..=max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_bounds() {
        for delay in [500u64, 1000, 5000, 30_000] {
            for _ in 0..200 {
                let j = jitter_for(delay);
                let max = (delay as f64 * JITTER_FRACTION) as i64;
                assert!(
                    j.abs() <= max,
                    "jitter {j} out of bounds ±{max} for delay {delay}"
                );
            }
        }
    }

    #[test]
    fn backoff_capped_at_max() {
        let mut d: u64 = RECONNECT_BASE_MS;
        for _ in 0..30 {
            d = (d * 2).min(RECONNECT_MAX_MS);
        }
        assert_eq!(d, RECONNECT_MAX_MS);
    }
}
