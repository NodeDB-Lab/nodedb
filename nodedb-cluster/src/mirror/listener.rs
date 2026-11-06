// SPDX-License-Identifier: BUSL-1.1

//! The source's mirror listener.
//!
//! This is the trust boundary between two clusters. Its order of checks is the
//! contract:
//!
//! 1. TLS: the client's certificate chains to the mirror CA, and its key is in
//!    the allowlist. Any other client fails the handshake.
//! 2. The connection reads its client from the handshake's certificate. No
//!    request names who is asking.
//! 3. Every frame's length is bounded before it allocates, and every read has a
//!    deadline, so a slow or oversized peer holds one stream at most.
//! 4. Every decoded request is validated, then the service answers it. The
//!    service owns the database check and the filter.
//!
//! The listener reads no log itself, so it holds no Raft or Data Plane state.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
use tracing::{debug, warn};

use super::allowlist::{MirrorAllowlist, MirrorClient};
use super::error::MirrorError;
use super::handshake::{MirrorHello, MirrorHelloAck};
use super::tail_wire::{
    MAX_REQUEST_FRAME, MAX_RESPONSE_FRAME, MirrorAck, MirrorRefusal, MirrorRequest, MirrorResponse,
    MirrorTailBatch, MirrorTailRequest, encode_frame, read_frame, write_frame,
};

/// How long a client gets to send its request.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// The most connections one node serves.
const MAX_CONNECTIONS: usize = 64;
/// QUIC close code of a connection whose client the allowlist does not name.
const CLOSE_NOT_ALLOWED: u32 = 1;

/// What a source answers an authenticated client with.
#[async_trait::async_trait]
pub trait MirrorSourceService: Send + Sync + 'static {
    async fn hello(
        &self,
        client: &MirrorClient,
        hello: MirrorHello,
    ) -> Result<MirrorHelloAck, MirrorRefusal>;

    async fn tail(
        &self,
        client: &MirrorClient,
        request: MirrorTailRequest,
    ) -> Result<MirrorTailBatch, MirrorRefusal>;

    async fn ack(&self, client: &MirrorClient, ack: MirrorAck) -> Result<(), MirrorRefusal>;
}

/// Bind the listener's endpoint at `addr`.
pub fn bind_mirror_endpoint(
    addr: std::net::SocketAddr,
    creds: &crate::transport::config::TlsCredentials,
    allowlist: Arc<MirrorAllowlist>,
) -> crate::error::Result<quinn::Endpoint> {
    let config = super::allowlist::make_mirror_server_config(creds, allowlist)?;
    quinn::Endpoint::server(config, addr).map_err(|e| crate::error::ClusterError::Transport {
        detail: format!("bind the mirror listener at {addr}: {e}"),
    })
}

/// Serve `endpoint` until `shutdown` turns true.
pub async fn serve_mirror_source(
    endpoint: quinn::Endpoint,
    allowlist: Arc<MirrorAllowlist>,
    service: Arc<dyn MirrorSourceService>,
    mut shutdown: watch::Receiver<bool>,
) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let incoming = tokio::select! {
            incoming = endpoint.accept() => incoming,
            _ = shutdown.changed() => None,
        };
        if *shutdown.borrow() {
            break;
        }
        let Some(incoming) = incoming else {
            break;
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            warn!(
                max = MAX_CONNECTIONS,
                "mirror listener: refused a connection past the bound"
            );
            incoming.refuse();
            continue;
        };
        let allowlist = Arc::clone(&allowlist);
        let service = Arc::clone(&service);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _slot = slot;
            serve_connection(incoming, allowlist, service, shutdown).await;
        });
    }
    endpoint.close(0u32.into(), b"shutdown");
}

/// The allowlisted client of `connection`, read from its TLS identity.
fn client_of(
    connection: &quinn::Connection,
    allowlist: &MirrorAllowlist,
) -> Option<Arc<MirrorClient>> {
    let identity = connection.peer_identity()?;
    let chain = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .ok()?;
    allowlist.client_of(chain.first()?.as_ref())
}

async fn serve_connection(
    incoming: quinn::Incoming,
    allowlist: Arc<MirrorAllowlist>,
    service: Arc<dyn MirrorSourceService>,
    mut shutdown: watch::Receiver<bool>,
) {
    let connection = match incoming.await {
        Ok(connection) => connection,
        Err(error) => {
            debug!(%error, "mirror listener: a client failed the handshake");
            return;
        }
    };
    // The verifier passed this client. The check stays here too, so a verifier
    // change never opens a stream for a client nobody named.
    let Some(client) = client_of(&connection, &allowlist) else {
        warn!("mirror listener: a connection holds no allowlisted identity");
        connection.close(CLOSE_NOT_ALLOWED.into(), b"not allowed");
        return;
    };
    loop {
        let stream = tokio::select! {
            stream = connection.accept_bi() => stream,
            _ = shutdown.changed() => return,
        };
        let (mut send, mut recv) = match stream {
            Ok(stream) => stream,
            Err(error) => {
                debug!(%error, client = %client.name, "mirror listener: a connection ended");
                return;
            }
        };
        let client = Arc::clone(&client);
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            let response = answer(&mut recv, service.as_ref(), &client).await;
            if let Err(error) = reply(&mut send, &response).await {
                debug!(%error, client = %client.name, "mirror listener: a reply failed");
            }
        });
    }
}

/// Read one request from `reader` and answer it. Every failure is a refusal,
/// so the client learns why its request went nowhere.
async fn answer<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    service: &dyn MirrorSourceService,
    client: &MirrorClient,
) -> MirrorResponse {
    let read =
        tokio::time::timeout(REQUEST_READ_TIMEOUT, read_frame(reader, MAX_REQUEST_FRAME)).await;
    let bytes = match read {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(MirrorError::Refused(refusal))) => return MirrorResponse::Refused(refusal),
        Ok(Err(error)) => {
            return MirrorResponse::Refused(MirrorRefusal::Malformed {
                detail: error.to_string(),
            });
        }
        Err(_) => {
            return MirrorResponse::Refused(MirrorRefusal::Malformed {
                detail: format!("no request within {}s", REQUEST_READ_TIMEOUT.as_secs()),
            });
        }
    };
    let request: MirrorRequest = match zerompk::from_msgpack(&bytes) {
        Ok(request) => request,
        Err(error) => {
            return MirrorResponse::Refused(MirrorRefusal::Malformed {
                detail: format!("decode a request: {error}"),
            });
        }
    };
    if let Err(refusal) = request.validate() {
        return MirrorResponse::Refused(refusal);
    }
    let answered = match request {
        MirrorRequest::Hello(hello) => service
            .hello(client, hello)
            .await
            .map(MirrorResponse::Hello),
        MirrorRequest::Tail(tail) => service.tail(client, tail).await.map(MirrorResponse::Tail),
        MirrorRequest::Ack(ack) => service.ack(client, ack).await.map(|()| MirrorResponse::Ack),
    };
    answered.unwrap_or_else(MirrorResponse::Refused)
}

/// Write `response` and finish the stream. A batch past the frame bound answers
/// a refusal, so the client never reads a frame it would refuse.
async fn reply(send: &mut quinn::SendStream, response: &MirrorResponse) -> Result<(), MirrorError> {
    let payload = match encode_frame(response, MAX_RESPONSE_FRAME) {
        Ok(payload) => payload,
        Err(MirrorError::Refused(refusal)) => {
            encode_frame(&MirrorResponse::Refused(refusal), MAX_RESPONSE_FRAME)?
        }
        Err(error) => return Err(error),
    };
    write_frame(send, &payload).await?;
    send.finish().map_err(|e| MirrorError::Transport {
        detail: format!("finish a mirror response: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::mirror::allowlist::{make_mirror_client_config, make_mirror_server_config};
    use crate::mirror::handshake::MIRROR_PROTOCOL_VERSION;
    use crate::mirror::link::{CrossClusterLink, MirrorLinkConfig};
    use crate::mirror::throttle::SendThrottle;
    use crate::transport::config::{
        SNI_HOSTNAME, TlsCredentials, generate_node_credentials, issue_leaf_for_sans,
    };

    /// Counts every call, so a test proves a refused client reached nothing.
    #[derive(Default)]
    struct CountingService {
        calls: AtomicU32,
    }

    #[async_trait::async_trait]
    impl MirrorSourceService for CountingService {
        async fn hello(
            &self,
            client: &MirrorClient,
            hello: MirrorHello,
        ) -> Result<MirrorHelloAck, MirrorRefusal> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(MirrorHelloAck {
                source_cluster: hello.source_cluster,
                source_cluster_id: 1,
                source_database_id: u64::from(client.allows(&hello.source_database)),
                timeline: 0,
                groups: vec![0],
            })
        }

        async fn tail(
            &self,
            _client: &MirrorClient,
            request: MirrorTailRequest,
        ) -> Result<MirrorTailBatch, MirrorRefusal> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(MirrorRefusal::LogTruncated {
                group: request.group,
                first_available: 9,
            })
        }

        async fn ack(&self, _client: &MirrorClient, _ack: MirrorAck) -> Result<(), MirrorRefusal> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Source {
        addr: SocketAddr,
        service: Arc<CountingService>,
        _stop: watch::Sender<bool>,
    }

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    /// A source whose server identity and client CA come from `creds`, with
    /// one allowlisted client `client_spki`.
    fn source(creds: &TlsCredentials, client_spki: [u8; 32]) -> Source {
        let allowlist = Arc::new(MirrorAllowlist::new([MirrorClient {
            name: "dr".to_string(),
            spki: client_spki,
            databases: vec!["orders".to_string()],
        }]));
        let config = make_mirror_server_config(creds, Arc::clone(&allowlist)).expect("config");
        let endpoint = quinn::Endpoint::server(config, loopback()).expect("bind");
        let addr = endpoint.local_addr().expect("addr");
        let service = Arc::new(CountingService::default());
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(serve_mirror_source(
            endpoint,
            allowlist,
            Arc::clone(&service) as Arc<dyn MirrorSourceService>,
            stopped,
        ));
        Source {
            addr,
            service,
            _stop: stop,
        }
    }

    fn link(addr: SocketAddr, creds: &TlsCredentials) -> CrossClusterLink {
        let client_config = make_mirror_client_config(creds).expect("client config");
        link_with(addr, client_config)
    }

    fn link_with(addr: SocketAddr, client_config: quinn::ClientConfig) -> CrossClusterLink {
        let endpoint = quinn::Endpoint::client(loopback()).expect("client endpoint");
        CrossClusterLink::new(
            MirrorLinkConfig {
                source_cluster: "prod".to_string(),
                source_database: "orders".to_string(),
                mirror_cluster_id: 2,
                link_id: 7,
                seeds: vec![addr],
                server_name: SNI_HOSTNAME.to_string(),
            },
            endpoint,
            client_config,
            SendThrottle::default_cap(),
        )
    }

    fn sibling_creds(ca: &nexar::transport::tls::ClusterCa, name: &str) -> TlsCredentials {
        issue_leaf_for_sans(ca, &[name]).expect("leaf")
    }

    #[tokio::test]
    async fn an_allowlisted_client_reaches_the_service() {
        let (ca, server) = generate_node_credentials("source").expect("creds");
        let client = sibling_creds(&ca, "dr");
        let source = source(&server, client.spki_pin);
        let ack = link(source.addr, &client).connect().await.expect("hello");
        assert_eq!(ack.source_cluster, "prod");
        assert_eq!(ack.source_database_id, 1, "the service read the client");
        assert_eq!(source.service.calls.load(Ordering::SeqCst), 1);
    }

    /// The CA signed this client, and the allowlist does not name its key.
    #[tokio::test]
    async fn an_unlisted_key_never_reaches_the_service() {
        let (ca, server) = generate_node_credentials("source").expect("creds");
        let listed = sibling_creds(&ca, "dr");
        let unlisted = sibling_creds(&ca, "intruder");
        let source = source(&server, listed.spki_pin);
        assert!(link(source.addr, &unlisted).connect().await.is_err());
        assert_eq!(source.service.calls.load(Ordering::SeqCst), 0);
    }

    /// The key is listed, and another CA signed its certificate.
    #[tokio::test]
    async fn a_certificate_of_another_ca_never_reaches_the_service() {
        let (_ca, server) = generate_node_credentials("source").expect("creds");
        let (_other_ca, mut foreign) = generate_node_credentials("foreign").expect("creds");
        let spki = foreign.spki_pin;
        foreign.ca_cert = server.ca_cert.clone();
        let source = source(&server, spki);
        assert!(link(source.addr, &foreign).connect().await.is_err());
        assert_eq!(source.service.calls.load(Ordering::SeqCst), 0);
    }

    /// No certificate at all: the allowlist has nothing to check.
    #[tokio::test]
    async fn a_client_with_no_certificate_never_reaches_the_service() {
        let (_ca, server) = generate_node_credentials("source").expect("creds");
        let source = source(&server, [7; 32]);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(server.ca_cert.clone()).expect("root");
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![crate::mirror::allowlist::ALPN_NODEDB_MIRROR.to_vec()];
        let crypto =
            quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls)).expect("crypto");
        let config = quinn::ClientConfig::new(Arc::new(crypto));
        assert!(link_with(source.addr, config).connect().await.is_err());
        assert_eq!(source.service.calls.load(Ordering::SeqCst), 0);
    }

    /// A raft client's ALPN never opens this listener.
    #[tokio::test]
    async fn another_protocols_alpn_is_refused() {
        let (ca, server) = generate_node_credentials("source").expect("creds");
        let client = sibling_creds(&ca, "dr");
        let source = source(&server, client.spki_pin);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(server.ca_cert.clone()).expect("root");
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![client.cert.clone()], client.key.clone_key())
        .expect("auth");
        tls.alpn_protocols = vec![crate::transport::config::ALPN_NODEDB_RAFT.to_vec()];
        let crypto =
            quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls)).expect("crypto");
        let config = quinn::ClientConfig::new(Arc::new(crypto));
        assert!(link_with(source.addr, config).connect().await.is_err());
        assert_eq!(source.service.calls.load(Ordering::SeqCst), 0);
    }

    fn entry() -> MirrorClient {
        MirrorClient {
            name: "dr".to_string(),
            spki: [0; 32],
            databases: vec!["orders".to_string()],
        }
    }

    async fn answered(bytes: Vec<u8>) -> (MirrorResponse, u32) {
        let service = CountingService::default();
        let response = answer(&mut bytes.as_slice(), &service, &entry()).await;
        (response, service.calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_before_the_service() {
        let (response, calls) = answered(u32::MAX.to_be_bytes().to_vec()).await;
        assert!(matches!(
            response,
            MirrorResponse::Refused(MirrorRefusal::FrameTooLarge { .. })
        ));
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn an_undecodable_frame_is_refused_before_the_service() {
        let mut bytes = 3u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0xc1, 0xc1, 0xc1]);
        let (response, calls) = answered(bytes).await;
        assert!(matches!(
            response,
            MirrorResponse::Refused(MirrorRefusal::Malformed { .. })
        ));
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn an_invalid_request_is_refused_before_the_service() {
        let request = MirrorRequest::Hello(MirrorHello {
            source_cluster: "x".repeat(4_000),
            source_database: "orders".to_string(),
            mirror_cluster_id: 2,
            link_id: 7,
            protocol_version: MIRROR_PROTOCOL_VERSION,
        });
        let payload = encode_frame(&request, MAX_REQUEST_FRAME).expect("encode");
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &payload).await.expect("frame");
        let (response, calls) = answered(bytes).await;
        assert!(matches!(
            response,
            MirrorResponse::Refused(MirrorRefusal::Malformed { .. })
        ));
        assert_eq!(calls, 0);
    }

    /// A client that sends nothing holds its stream until the read deadline,
    /// then gets a refusal.
    #[tokio::test(start_paused = true)]
    async fn a_silent_client_is_refused_at_the_read_deadline() {
        let (mut silent, _held_open) = tokio::io::duplex(16);
        let service = CountingService::default();
        let response = answer(&mut silent, &service, &entry()).await;
        assert!(matches!(
            response,
            MirrorResponse::Refused(MirrorRefusal::Malformed { .. })
        ));
        assert_eq!(service.calls.load(Ordering::SeqCst), 0);
    }

    /// A batch larger than the frame bound answers a refusal.
    #[test]
    fn a_response_past_the_bound_encodes_as_a_refusal() {
        let huge = MirrorResponse::Tail(MirrorTailBatch {
            entries: vec![crate::mirror::tail_wire::MirrorTailEntry {
                index: 1,
                data: vec![0; 64],
            }],
            next_index: 2,
            commit_index: 2,
            source_hlc_ns: 0,
            first_available: 1,
        });
        assert!(matches!(
            encode_frame(&huge, 8),
            Err(MirrorError::Refused(MirrorRefusal::FrameTooLarge { .. }))
        ));
    }
}
