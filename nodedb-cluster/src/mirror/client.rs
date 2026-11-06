// SPDX-License-Identifier: BUSL-1.1

//! The mirror's view of its source: hello, fetch and ack, across replicas.
//!
//! Committed indexes are the same on every replica, so a fetch can read any
//! one. A replica can answer `LogTruncated` alone: a snapshot install moves one
//! replica's log start past a position its peers still hold. So a fetch tries
//! every seed before it reports truncation, and it reports truncation only once
//! no replica can still answer. An unreached seed can still hold the entries, so
//! that outcome stays a transport error, which a later fetch retries.

use std::sync::Arc;

use super::bootstrap::CrossClusterSnapshotEnvelope;
use super::error::MirrorError;
use super::handshake::MirrorHelloAck;
use super::link::CrossClusterLink;
use super::tail_wire::{
    MirrorAck, MirrorBootstrapRequest, MirrorRefusal, MirrorRequest, MirrorResponse,
    MirrorTailBatch, MirrorTailRequest,
};

/// What every seed of one fetch answered, short of an answer the fetch returns.
#[derive(Debug, Default)]
struct Sweep {
    /// The lowest log start any replica reported. It says how far the mirror
    /// trails the nearest replica.
    truncated_at: Option<u64>,
    last_error: Option<MirrorError>,
    unreachable: bool,
}

impl Sweep {
    /// Note one seed's failure. `Some` ends the sweep: the refusal is the same
    /// on every replica.
    fn note(&mut self, error: MirrorError) -> Option<MirrorError> {
        match error {
            MirrorError::Refused(MirrorRefusal::LogTruncated {
                first_available, ..
            }) => {
                let lowest = self
                    .truncated_at
                    .map_or(first_available, |held| held.min(first_available));
                self.truncated_at = Some(lowest);
                None
            }
            MirrorError::Refused(MirrorRefusal::NotHosted { .. }) => None,
            MirrorError::Refused(refusal @ MirrorRefusal::Unavailable { .. }) => {
                self.unreachable = true;
                self.last_error = Some(MirrorError::Refused(refusal));
                None
            }
            refused @ MirrorError::Refused(_) => Some(refused),
            other => {
                self.unreachable = true;
                self.last_error = Some(other);
                None
            }
        }
    }

    /// What the fetch reports after every seed.
    fn conclude(self, group: u64) -> MirrorError {
        if !self.unreachable
            && let Some(first_available) = self.truncated_at
        {
            return MirrorError::Refused(MirrorRefusal::LogTruncated {
                group,
                first_available,
            });
        }
        self.last_error
            .unwrap_or(MirrorError::Refused(MirrorRefusal::NotHosted { group }))
    }
}

/// The mirror's client of one source. The applier drives it per database.
#[derive(Clone)]
pub struct MirrorSourceClient {
    link: Arc<CrossClusterLink>,
}

impl MirrorSourceClient {
    pub fn new(link: Arc<CrossClusterLink>) -> Self {
        Self { link }
    }

    /// Run the hello on a fresh connection.
    pub async fn hello(&self) -> Result<MirrorHelloAck, MirrorError> {
        self.link.connect().await
    }

    /// Say whether each later hello asks for a new pin.
    pub fn set_fresh(&self, fresh: bool) {
        self.link.set_fresh(fresh);
    }

    /// The link's id on the source.
    pub fn link_id(&self) -> u64 {
        self.link.link_id()
    }

    /// Give back the link's pin on the source. Any replica answers: the removal
    /// replicates.
    pub async fn release(&self, link_id: u64) -> Result<(), MirrorError> {
        match self
            .link
            .exchange(&MirrorRequest::Release { link_id })
            .await?
        {
            MirrorResponse::Ack => Ok(()),
            MirrorResponse::Refused(refusal) => Err(MirrorError::Refused(refusal)),
            MirrorResponse::Hello(_) | MirrorResponse::Tail(_) | MirrorResponse::Bootstrap(_) => {
                Err(MirrorError::Refused(MirrorRefusal::Malformed {
                    detail: "the source answered a release with another response".to_string(),
                }))
            }
        }
    }

    /// Fetch the batch `request` names from the first replica that can answer.
    pub async fn tail(&self, request: &MirrorTailRequest) -> Result<MirrorTailBatch, MirrorError> {
        let wire = MirrorRequest::Tail(request.clone());
        let mut sweep = Sweep::default();
        for _ in 0..self.link.seed_count().max(1) {
            match self.link.exchange(&wire).await.and_then(tail_answer) {
                Ok(batch) => {
                    check_progress(request, &batch)?;
                    return Ok(batch);
                }
                Err(error) => {
                    if let Some(final_error) = sweep.note(error) {
                        return Err(final_error);
                    }
                }
            }
            self.link.next_seed().await;
        }
        Err(sweep.conclude(request.group))
    }

    /// Report the mirror's progress to any replica: the pin replicates.
    pub async fn ack(&self, ack: &MirrorAck) -> Result<(), MirrorError> {
        match self.link.exchange(&MirrorRequest::Ack(ack.clone())).await? {
            MirrorResponse::Ack => Ok(()),
            MirrorResponse::Refused(refusal) => Err(MirrorError::Refused(refusal)),
            MirrorResponse::Hello(_) | MirrorResponse::Tail(_) | MirrorResponse::Bootstrap(_) => {
                Err(MirrorError::Refused(MirrorRefusal::Malformed {
                    detail: "the source answered an ack with another response".to_string(),
                }))
            }
        }
    }

    /// Fetch a chunk of the link's capture from `offset`, or `None` while the
    /// source still takes it.
    pub async fn bootstrap_chunk(
        &self,
        link_id: u64,
        offset: u64,
    ) -> Result<Option<CrossClusterSnapshotEnvelope>, MirrorError> {
        let request = MirrorRequest::Bootstrap(MirrorBootstrapRequest { link_id, offset });
        match self.link.exchange(&request).await? {
            MirrorResponse::Bootstrap(chunk) => Ok(chunk.map(|boxed| *boxed)),
            MirrorResponse::Refused(refusal) => Err(MirrorError::Refused(refusal)),
            MirrorResponse::Hello(_) | MirrorResponse::Tail(_) | MirrorResponse::Ack => {
                Err(MirrorError::Refused(MirrorRefusal::Malformed {
                    detail: "the source answered a bootstrap with another response".to_string(),
                }))
            }
        }
    }

    /// Move to the next replica. A capture lives on one replica, so a transfer
    /// that lost its replica starts over on another.
    pub async fn next_seed(&self) {
        self.link.next_seed().await;
    }
}

fn tail_answer(response: MirrorResponse) -> Result<MirrorTailBatch, MirrorError> {
    match response {
        MirrorResponse::Tail(batch) => Ok(batch),
        MirrorResponse::Refused(refusal) => Err(MirrorError::Refused(refusal)),
        MirrorResponse::Hello(_) | MirrorResponse::Ack | MirrorResponse::Bootstrap(_) => {
            Err(MirrorError::Refused(MirrorRefusal::Malformed {
                detail: "the source answered a fetch with another response".to_string(),
            }))
        }
    }
}

/// Refuse a batch that moves backward or names an index it did not cover. Such
/// a batch re-applies or skips source entries.
fn check_progress(request: &MirrorTailRequest, batch: &MirrorTailBatch) -> Result<(), MirrorError> {
    let in_range = batch
        .entries
        .iter()
        .all(|entry| entry.index >= request.from_index && entry.index < batch.next_index);
    if batch.next_index < request.from_index || !in_range {
        return Err(MirrorError::Refused(MirrorRefusal::Malformed {
            detail: format!(
                "group {} batch from {} ends at {} with entries outside it",
                request.group, request.from_index, batch.next_index
            ),
        }));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::sync::watch;

    use super::*;
    use crate::mirror::allowlist::{
        MirrorAllowlist, MirrorClient, make_mirror_client_config, make_mirror_server_config,
    };
    use crate::mirror::handshake::MirrorHello;
    use crate::mirror::link::MirrorLinkConfig;
    use crate::mirror::listener::{MirrorSourceService, serve_mirror_source};
    use crate::mirror::tail_wire::{MirrorBootstrapRequest, MirrorTailEntry};
    use crate::mirror::throttle::SendThrottle;
    use crate::transport::config::{
        SNI_HOSTNAME, TlsCredentials, generate_node_credentials, issue_leaf_for_sans,
    };

    /// A replica whose every fetch answers one way.
    struct Replica {
        answer: Result<MirrorTailBatch, MirrorRefusal>,
        fetches: AtomicU32,
    }

    #[async_trait::async_trait]
    impl MirrorSourceService for Replica {
        async fn hello(
            &self,
            _client: &MirrorClient,
            hello: MirrorHello,
        ) -> Result<MirrorHelloAck, MirrorRefusal> {
            Ok(MirrorHelloAck {
                source_cluster: hello.source_cluster,
                source_cluster_id: 1,
                source_database_id: 1024,
                timeline: 0,
                groups: vec![0, 1],
            })
        }

        async fn tail(
            &self,
            _client: &MirrorClient,
            _request: MirrorTailRequest,
        ) -> Result<MirrorTailBatch, MirrorRefusal> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.answer.clone()
        }

        async fn ack(&self, _client: &MirrorClient, _ack: MirrorAck) -> Result<(), MirrorRefusal> {
            Ok(())
        }

        async fn bootstrap(
            &self,
            _client: &MirrorClient,
            _request: MirrorBootstrapRequest,
        ) -> Result<Option<CrossClusterSnapshotEnvelope>, MirrorRefusal> {
            Ok(None)
        }

        async fn release(
            &self,
            _client: &MirrorClient,
            _link_id: u64,
        ) -> Result<(), MirrorRefusal> {
            Ok(())
        }
    }

    struct Fixture {
        client: MirrorSourceClient,
        replicas: Vec<Arc<Replica>>,
        _stops: Vec<watch::Sender<bool>>,
    }

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    fn spawn_replica(
        creds: &TlsCredentials,
        client_spki: [u8; 32],
        replica: Arc<Replica>,
    ) -> (SocketAddr, watch::Sender<bool>) {
        let allowlist = Arc::new(MirrorAllowlist::new([MirrorClient {
            name: "dr".to_string(),
            spki: client_spki,
            databases: vec!["orders".to_string()],
        }]));
        let config = make_mirror_server_config(creds, Arc::clone(&allowlist)).expect("config");
        let endpoint = quinn::Endpoint::server(config, loopback()).expect("bind");
        let addr = endpoint.local_addr().expect("addr");
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(serve_mirror_source(endpoint, allowlist, replica, stopped));
        (addr, stop)
    }

    /// A client whose seeds are one replica per answer, plus `dead` seeds no
    /// listener holds.
    fn fixture(answers: Vec<Result<MirrorTailBatch, MirrorRefusal>>, dead: usize) -> Fixture {
        let (ca, server) = generate_node_credentials("source").expect("creds");
        let client_creds = issue_leaf_for_sans(&ca, &["dr"]).expect("leaf");
        let mut seeds = Vec::new();
        let mut replicas = Vec::new();
        let mut stops = Vec::new();
        for answer in answers {
            let replica = Arc::new(Replica {
                answer,
                fetches: AtomicU32::new(0),
            });
            let (addr, stop) = spawn_replica(&server, client_creds.spki_pin, Arc::clone(&replica));
            seeds.push(addr);
            replicas.push(replica);
            stops.push(stop);
        }
        for _ in 0..dead {
            let unused = std::net::UdpSocket::bind(loopback()).expect("reserve a port");
            seeds.push(unused.local_addr().expect("addr"));
        }
        let endpoint = quinn::Endpoint::client(loopback()).expect("client endpoint");
        let mut client_config = make_mirror_client_config(&client_creds).expect("client config");
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(std::time::Duration::from_millis(300)).expect("idle"),
        ));
        client_config.transport_config(Arc::new(transport));
        let link = CrossClusterLink::new(
            MirrorLinkConfig {
                source_cluster: "prod".to_string(),
                source_database: "orders".to_string(),
                mirror_cluster_id: 2,
                link_id: 7,
                mode: nodedb_types::MirrorMode::Async,
                seeds,
                server_name: SNI_HOSTNAME.to_string(),
            },
            endpoint,
            client_config,
            SendThrottle::default_cap(),
        );
        Fixture {
            client: MirrorSourceClient::new(Arc::new(link)),
            replicas,
            _stops: stops,
        }
    }

    fn request() -> MirrorTailRequest {
        MirrorTailRequest {
            link_id: 7,
            group: 1,
            from_index: 5,
            max_bytes: 1024,
            wait_ms: 0,
        }
    }

    fn batch(next_index: u64) -> MirrorTailBatch {
        MirrorTailBatch {
            entries: vec![MirrorTailEntry {
                index: 5,
                data: vec![1],
            }],
            next_index,
            commit_index: 9,
            source_hlc_ns: 1,
            first_available: 1,
        }
    }

    fn truncated(first_available: u64) -> MirrorRefusal {
        MirrorRefusal::LogTruncated {
            group: 1,
            first_available,
        }
    }

    /// One replica installed a snapshot and lost the range. Its peer still
    /// holds it, so the fetch succeeds and reports no truncation.
    #[tokio::test]
    async fn a_truncated_replica_falls_over_to_one_that_holds_the_range() {
        let fixture = fixture(vec![Err(truncated(50)), Ok(batch(6))], 0);
        let read = fixture.client.tail(&request()).await.expect("fetched");
        assert_eq!(read.next_index, 6);
        assert_eq!(fixture.replicas[0].fetches.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.replicas[1].fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn truncation_reports_only_once_every_replica_answered_it() {
        let fixture = fixture(vec![Err(truncated(50)), Err(truncated(40))], 0);
        let error = fixture
            .client
            .tail(&request())
            .await
            .expect_err("truncated");
        assert!(matches!(
            error,
            MirrorError::Refused(MirrorRefusal::LogTruncated {
                group: 1,
                first_available: 40
            })
        ));
        assert_eq!(fixture.replicas[1].fetches.load(Ordering::SeqCst), 1);
    }

    /// An unreached seed can still hold the range, so a fetch reports a transport
    /// error, and the mirror keeps its copy.
    #[tokio::test]
    async fn a_dead_seed_keeps_truncation_from_reporting() {
        let fixture = fixture(vec![Err(truncated(50))], 1);
        let error = fixture
            .client
            .tail(&request())
            .await
            .expect_err("no answer");
        assert!(matches!(error, MirrorError::Transport { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn a_replica_that_left_the_group_falls_over() {
        let fixture = fixture(
            vec![Err(MirrorRefusal::NotHosted { group: 1 }), Ok(batch(6))],
            0,
        );
        assert!(fixture.client.tail(&request()).await.is_ok());
    }

    /// A refusal every replica repeats ends the fetch on the first.
    #[tokio::test]
    async fn an_expired_pin_stops_at_the_first_replica() {
        let fixture = fixture(
            vec![Err(MirrorRefusal::PinExpired { link_id: 7 }), Ok(batch(6))],
            0,
        );
        let error = fixture.client.tail(&request()).await.expect_err("expired");
        assert!(matches!(
            error,
            MirrorError::Refused(MirrorRefusal::PinExpired { link_id: 7 })
        ));
        assert_eq!(fixture.replicas[1].fetches.load(Ordering::SeqCst), 0);
    }

    /// A batch that moves backward is refused, so the mirror never re-applies.
    #[test]
    fn a_batch_that_moves_backward_is_refused() {
        assert!(check_progress(&request(), &batch(6)).is_ok());
        assert!(check_progress(&request(), &batch(4)).is_err());
        assert!(
            check_progress(&request(), &batch(5)).is_err(),
            "the entry at 5 sits outside a batch ending at 5"
        );
    }

    #[test]
    fn a_sweep_with_nothing_reports_not_hosted() {
        assert!(matches!(
            Sweep::default().conclude(3),
            MirrorError::Refused(MirrorRefusal::NotHosted { group: 3 })
        ));
    }
}
