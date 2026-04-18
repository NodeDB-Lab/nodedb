//! Host-side glue for the cluster bootstrap listener (L.4).
//!
//! Wires `nodedb-cluster`'s transport-only listener to the local
//! node's loaded CA + cluster secret so it can verify join tokens
//! and mint per-node leaf certs.

use std::net::SocketAddr;
use std::sync::Arc;

use nodedb_cluster::bootstrap_listener::{
    BootstrapCredsRequest, BootstrapCredsResponse, BootstrapHandler,
};

use crate::ctl::join_token::verify_token;

/// Binds the local node's TLS material (CA key + cluster secret)
/// to the generic listener handler. Constructed in `main.rs` once
/// the node has loaded creds; passed into `spawn_listener`.
pub struct HostBootstrapHandler {
    /// Persisted CA used to issue leaf certs for new joiners. Holds
    /// the key pair via `ClusterCa::from_der`.
    ca: Arc<nexar::transport::tls::ClusterCa>,
    /// 32-byte HMAC key used to verify join tokens.
    cluster_secret: [u8; 32],
}

impl HostBootstrapHandler {
    pub fn new(ca: Arc<nexar::transport::tls::ClusterCa>, cluster_secret: [u8; 32]) -> Self {
        Self { ca, cluster_secret }
    }

    fn issue(&self, node_id: u64) -> Result<BootstrapCredsResponse, String> {
        let node_san = format!("node-{node_id}");
        let creds = nodedb_cluster::issue_leaf_for_sans(
            &self.ca,
            &[&node_san, nodedb_cluster::transport::config::SNI_HOSTNAME],
        )
        .map_err(|e| format!("issue leaf: {e}"))?;
        // Preserve the cluster's *existing* secret. `issue_leaf_for_sans`
        // generates a fresh one for the returned bundle; overwrite so
        // the joiner shares the same MAC key as the rest of the cluster.
        Ok(BootstrapCredsResponse {
            ok: true,
            error: String::new(),
            ca_cert_der: self.ca.cert_der().to_vec(),
            node_cert_der: creds.cert.to_vec(),
            node_key_der: creds.key.secret_der().to_vec(),
            cluster_secret: self.cluster_secret.to_vec(),
        })
    }
}

impl BootstrapHandler for HostBootstrapHandler {
    fn handle<'a>(
        &'a self,
        req: BootstrapCredsRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BootstrapCredsResponse> + Send + 'a>>
    {
        Box::pin(async move {
            let (token_node, _expiry) = match verify_token(&req.token_hex, &self.cluster_secret) {
                Ok(v) => v,
                Err(e) => return BootstrapCredsResponse::error(format!("token: {e}")),
            };
            if token_node != req.node_id {
                return BootstrapCredsResponse::error(format!(
                    "node id mismatch: token bound to {token_node}, request claims {}",
                    req.node_id
                ));
            }
            match self.issue(req.node_id) {
                Ok(resp) => resp,
                Err(e) => BootstrapCredsResponse::error(e),
            }
        })
    }
}

/// Convenience: spawn the listener with a handler built from the
/// node's loaded `TlsCredentials` + a reloaded `ClusterCa`. Callers
/// pass the data-dir path so the loader can find `ca.key`.
pub fn spawn(
    listen_addr: SocketAddr,
    ca_cert_der: &nodedb_cluster::transport::pki_types::CertificateDer<'_>,
    ca_key_pkcs8_der: &[u8],
    cluster_secret: [u8; 32],
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let ca =
        nexar::transport::tls::ClusterCa::from_der(ca_key_pkcs8_der, ca_cert_der).map_err(|e| {
            crate::Error::Config {
                detail: format!("bootstrap listener: load CA: {e}"),
            }
        })?;
    let handler = Arc::new(HostBootstrapHandler::new(Arc::new(ca), cluster_secret));
    nodedb_cluster::bootstrap_listener::spawn_listener(listen_addr, handler, shutdown).map_err(
        |e| crate::Error::Config {
            detail: format!("bootstrap listener spawn: {e}"),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    fn mint_token(secret: &[u8; 32], for_node: u64, ttl_secs: u64) -> String {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + ttl_secs;
        let mut body = Vec::new();
        body.extend_from_slice(&for_node.to_le_bytes());
        body.extend_from_slice(&expiry.to_le_bytes());
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(&body);
        body.extend_from_slice(&mac.finalize().into_bytes());
        use std::fmt::Write as _;
        let mut hex = String::new();
        for b in &body {
            let _ = write!(hex, "{b:02x}");
        }
        hex
    }

    #[tokio::test]
    async fn end_to_end_fetch_creds_roundtrip() {
        // 1. Bootstrap a local CA on the "server" side.
        let (ca, _creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["node-1", "nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let ca_key = ca.key_pair_pkcs8_der();
        let cluster_secret = [0x42u8; 32];

        // 2. Spawn the listener.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            &ca_cert,
            &ca_key,
            cluster_secret,
            rx,
        )
        .unwrap();

        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(30)).await;

        // 3. Mint a fresh token for node 7 and fetch creds.
        let token = mint_token(&cluster_secret, 7, 60);
        let resp = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &token,
            7,
            Duration::from_secs(3),
        )
        .await
        .unwrap();

        assert!(resp.ok);
        assert!(!resp.ca_cert_der.is_empty());
        assert!(!resp.node_cert_der.is_empty());
        assert!(!resp.node_key_der.is_empty());
        assert_eq!(resp.cluster_secret, cluster_secret.to_vec());
        // Delivered CA cert matches the server's.
        assert_eq!(resp.ca_cert_der, ca_cert.as_ref().to_vec());

        // 4. Shutdown.
        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }

    #[tokio::test]
    async fn rejects_bad_token() {
        let (ca, _creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let ca_key = ca.key_pair_pkcs8_der();
        let cluster_secret = [0x55u8; 32];

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            &ca_cert,
            &ca_key,
            cluster_secret,
            rx,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Wrong secret → invalid MAC.
        let bad_token = mint_token(&[0xAAu8; 32], 1, 60);
        let err = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &bad_token,
            1,
            Duration::from_secs(3),
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("token"), "got: {msg}");

        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }

    #[tokio::test]
    async fn rejects_node_id_mismatch() {
        let (ca, _creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let ca_key = ca.key_pair_pkcs8_der();
        let cluster_secret = [0x99u8; 32];

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            &ca_cert,
            &ca_key,
            cluster_secret,
            rx,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Token for node 2, request claims node 3.
        let token = mint_token(&cluster_secret, 2, 60);
        let err = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &token,
            3,
            Duration::from_secs(3),
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("node id mismatch"), "got: {msg}");

        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }
}
