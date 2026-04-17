//! Cluster TLS credential resolution.
//!
//! Resolves a [`TransportCredentials`] from the operator's [`ClusterSettings`]
//! (explicit file paths, `insecure_transport` opt-out, or auto-bootstrap).
//!
//! **Resolution order:**
//!
//! 1. `cluster.insecure_transport = true` → [`TransportCredentials::Insecure`]
//!    (emits a loud startup warning inside the transport constructor).
//! 2. `cluster.tls = { cert, key, ca, [crl] }` set → load PEM files, wrap in
//!    [`TransportCredentials::Mtls`].
//! 3. On-disk creds under `data_dir/tls/` from a previous run → load.
//! 4. This node is the bootstrapping node (first seed, force_bootstrap, or
//!    single-node) and no creds exist → auto-generate a cluster CA plus a
//!    node cert, persist under `data_dir/tls/`, return as
//!    [`TransportCredentials::Mtls`].
//! 5. Otherwise (joining node, no creds provisioned) → fail with a clear
//!    error. L.4 delivers CA + node creds through the join RPC.
//!
//! [`ClusterSettings`]: crate::config::server::ClusterSettings
//! [`TransportCredentials`]: nodedb_cluster::TransportCredentials
//! [`TransportCredentials::Insecure`]: nodedb_cluster::TransportCredentials::Insecure
//! [`TransportCredentials::Mtls`]: nodedb_cluster::TransportCredentials::Mtls

use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use nodedb_cluster::transport::pki_types::{
    CertificateDer, CertificateRevocationListDer, PrivateKeyDer,
};
use nodedb_cluster::{TlsCredentials, TransportCredentials, generate_node_credentials};
use tracing::{info, warn};

use crate::config::server::{ClusterSettings, TlsPaths};

/// Standard relative path under the data dir where auto-bootstrapped and
/// auto-loaded TLS material lives.
pub const TLS_SUBDIR: &str = "tls";
const NODE_CERT_FILE: &str = "node.crt";
const NODE_KEY_FILE: &str = "node.key";
const CA_CERT_FILE: &str = "ca.crt";
/// Cluster-wide HMAC key used by the authenticated Raft frame envelope.
/// Persisted as raw 32 bytes (no PEM framing) with 0600 perms.
const CLUSTER_SECRET_FILE: &str = "cluster_secret.bin";
const CLUSTER_SECRET_LEN: usize = 32;

/// Resolve [`TransportCredentials`] for this node from operator settings
/// and on-disk state. See module docs for resolution order.
pub fn resolve_credentials(
    settings: &ClusterSettings,
    data_dir: &Path,
) -> crate::Result<TransportCredentials> {
    if settings.insecure_transport {
        warn!(
            node_id = settings.node_id,
            "cluster.insecure_transport = true — channel authentication DISABLED. \
             This is safe only on fully isolated private networks."
        );
        return Ok(TransportCredentials::Insecure);
    }

    let tls_dir = data_dir.join(TLS_SUBDIR);

    if let Some(paths) = &settings.tls {
        let creds = load_from_paths(paths, &tls_dir)?;
        info!(
            node_id = settings.node_id,
            cert = %paths.cert.display(),
            "cluster TLS credentials loaded from operator-provided paths"
        );
        return Ok(TransportCredentials::Mtls(creds));
    }

    if tls_dir.join(NODE_CERT_FILE).exists()
        && tls_dir.join(NODE_KEY_FILE).exists()
        && tls_dir.join(CA_CERT_FILE).exists()
        && tls_dir.join(CLUSTER_SECRET_FILE).exists()
    {
        let creds = load_from_data_dir(&tls_dir)?;
        info!(
            node_id = settings.node_id,
            dir = %tls_dir.display(),
            "cluster TLS credentials loaded from data dir"
        );
        return Ok(TransportCredentials::Mtls(creds));
    }

    if is_bootstrapping_node(settings) {
        let creds = bootstrap_credentials(settings, &tls_dir)?;
        info!(
            node_id = settings.node_id,
            dir = %tls_dir.display(),
            "bootstrapped new cluster CA + node credentials"
        );
        return Ok(TransportCredentials::Mtls(creds));
    }

    Err(crate::Error::Config {
        detail: format!(
            "cluster TLS credentials not found for node {node_id}. This node is not the \
             bootstrapping node, so it must receive credentials out of band. Either set \
             [cluster.tls] in the config pointing at PEM files, or set \
             cluster.insecure_transport = true for an isolated-network dev cluster.",
            node_id = settings.node_id
        ),
    })
}

/// A node is treated as a bootstrapper if either:
/// - `force_bootstrap = true` (operator override), or
/// - it is the lexicographically smallest seed and its `listen` address is
///   among the seeds (the standard single-node / first-in-cluster path).
fn is_bootstrapping_node(settings: &ClusterSettings) -> bool {
    if settings.force_bootstrap {
        return true;
    }
    let mut seeds = settings.seed_nodes.clone();
    seeds.sort();
    seeds.first() == Some(&settings.listen) && seeds.contains(&settings.listen)
}

fn load_from_paths(paths: &TlsPaths, tls_dir: &Path) -> crate::Result<TlsCredentials> {
    let cert = read_single_cert(&paths.cert)?;
    let key = read_private_key(&paths.key)?;
    let ca_cert = read_single_cert(&paths.ca)?;
    let crls = match &paths.crl {
        Some(p) => read_crls(p)?,
        None => Vec::new(),
    };
    let secret_path = paths
        .cluster_secret
        .clone()
        .unwrap_or_else(|| tls_dir.join(CLUSTER_SECRET_FILE));
    let cluster_secret = read_cluster_secret(&secret_path)?;
    Ok(TlsCredentials {
        cert,
        key,
        ca_cert,
        crls,
        cluster_secret,
    })
}

fn load_from_data_dir(tls_dir: &Path) -> crate::Result<TlsCredentials> {
    let cert = read_single_cert(&tls_dir.join(NODE_CERT_FILE))?;
    let key = read_private_key(&tls_dir.join(NODE_KEY_FILE))?;
    let ca_cert = read_single_cert(&tls_dir.join(CA_CERT_FILE))?;
    let cluster_secret = read_cluster_secret(&tls_dir.join(CLUSTER_SECRET_FILE))?;
    Ok(TlsCredentials {
        cert,
        key,
        ca_cert,
        crls: Vec::new(),
        cluster_secret,
    })
}

fn bootstrap_credentials(
    settings: &ClusterSettings,
    tls_dir: &Path,
) -> crate::Result<TlsCredentials> {
    fs::create_dir_all(tls_dir).map_err(|e| crate::Error::Config {
        detail: format!("create tls dir {}: {e}", tls_dir.display()),
    })?;

    // SAN must match the SNI hostname used by the Raft QUIC client
    // (`transport::config::SNI_HOSTNAME`, fixed at "nodedb") or rustls
    // rejects the handshake. Per-node identity binding via SAN is a
    // follow-up (L.4) that will add issuer support for multiple SANs —
    // for L.1 every node uses the shared cluster SAN and identity lives
    // in the envelope's from_node_id (L.2).
    let _desired_node_san = format!("node-{}", settings.node_id);
    let (ca, creds) = generate_node_credentials(nodedb_cluster::transport::config::SNI_HOSTNAME)
        .map_err(|e| crate::Error::Config {
            detail: format!("bootstrap cluster CA: {e}"),
        })?;

    write_pem_cert(&tls_dir.join(CA_CERT_FILE), ca.cert_der().as_ref())?;
    write_pem_cert(&tls_dir.join(NODE_CERT_FILE), creds.cert.as_ref())?;
    write_pem_private_key(&tls_dir.join(NODE_KEY_FILE), creds.key.secret_der())?;
    write_cluster_secret(&tls_dir.join(CLUSTER_SECRET_FILE), &creds.cluster_secret)?;

    Ok(creds)
}

fn read_cluster_secret(path: &Path) -> crate::Result<[u8; CLUSTER_SECRET_LEN]> {
    ensure_secret_file_perms(path)?;
    let bytes = fs::read(path).map_err(|e| crate::Error::Config {
        detail: format!("read cluster secret {}: {e}", path.display()),
    })?;
    if bytes.len() != CLUSTER_SECRET_LEN {
        return Err(crate::Error::Config {
            detail: format!(
                "cluster secret {} has {} bytes, expected {CLUSTER_SECRET_LEN}",
                path.display(),
                bytes.len()
            ),
        });
    }
    let mut out = [0u8; CLUSTER_SECRET_LEN];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn write_cluster_secret(path: &PathBuf, secret: &[u8; CLUSTER_SECRET_LEN]) -> crate::Result<()> {
    fs::write(path, secret).map_err(|e| crate::Error::Config {
        detail: format!("write cluster secret {}: {e}", path.display()),
    })?;
    set_private_key_perms(path)
}

fn read_single_cert(path: &Path) -> crate::Result<CertificateDer<'static>> {
    let bytes = fs::read(path).map_err(|e| crate::Error::Config {
        detail: format!("read cert {}: {e}", path.display()),
    })?;
    let mut reader = BufReader::new(&bytes[..]);
    let mut iter = rustls_pemfile::certs(&mut reader);
    match iter.next() {
        Some(Ok(cert)) => Ok(cert),
        Some(Err(e)) => Err(crate::Error::Config {
            detail: format!("parse cert {}: {e}", path.display()),
        }),
        None => Err(crate::Error::Config {
            detail: format!("cert file {} contains no PEM certificates", path.display()),
        }),
    }
}

fn read_private_key(path: &Path) -> crate::Result<PrivateKeyDer<'static>> {
    ensure_secret_file_perms(path)?;
    let bytes = fs::read(path).map_err(|e| crate::Error::Config {
        detail: format!("read key {}: {e}", path.display()),
    })?;
    let mut reader = BufReader::new(&bytes[..]);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| crate::Error::Config {
            detail: format!("parse key {}: {e}", path.display()),
        })?
        .ok_or_else(|| crate::Error::Config {
            detail: format!("key file {} contains no PEM private key", path.display()),
        })
}

fn read_crls(path: &Path) -> crate::Result<Vec<CertificateRevocationListDer<'static>>> {
    nodedb_cluster::load_crls_from_pem(path).map_err(|e| crate::Error::Config {
        detail: format!("load CRL {}: {e}", path.display()),
    })
}

fn write_pem_cert(path: &PathBuf, der: &[u8]) -> crate::Result<()> {
    let pem = pem_encode("CERTIFICATE", der);
    fs::write(path, pem).map_err(|e| crate::Error::Config {
        detail: format!("write {}: {e}", path.display()),
    })
}

fn write_pem_private_key(path: &PathBuf, der: &[u8]) -> crate::Result<()> {
    let pem = pem_encode("PRIVATE KEY", der);
    fs::write(path, pem).map_err(|e| crate::Error::Config {
        detail: format!("write {}: {e}", path.display()),
    })?;
    set_private_key_perms(path)
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::with_capacity(b64.len() + 64);
    out.push_str("-----BEGIN ");
    out.push_str(label);
    out.push_str("-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str("-----END ");
    out.push_str(label);
    out.push_str("-----\n");
    out
}

/// Tighten private-key file mode to 0600. No-op on non-Unix.
fn set_private_key_perms(path: &PathBuf) -> crate::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, perms).map_err(|e| crate::Error::Config {
            detail: format!("chmod 0600 {}: {e}", path.display()),
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Refuse to start if a secret file is world- or group-readable.
/// Hard-fail is the only acceptable behaviour here — a warn+continue
/// makes the cluster run in a dangerous configuration that the operator
/// believes is safe.
///
/// No-op on non-Unix (Windows ACL enforcement is out of scope for L.5).
fn ensure_secret_file_perms(path: &Path) -> crate::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|e| crate::Error::Config {
            detail: format!("stat {}: {e}", path.display()),
        })?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(crate::Error::Config {
                detail: format!(
                    "secret file {} has mode {:04o}; refusing to start. Set to 0600 \
                     (chmod 600 {}) and restart.",
                    path.display(),
                    mode,
                    path.display()
                ),
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn settings(node_id: u64, listen: SocketAddr, seeds: Vec<SocketAddr>) -> ClusterSettings {
        ClusterSettings {
            node_id,
            listen,
            seed_nodes: seeds,
            num_groups: 1,
            replication_factor: 1,
            force_bootstrap: false,
            tls: None,
            insecure_transport: false,
        }
    }

    #[test]
    fn insecure_flag_short_circuits() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = settings(1, "127.0.0.1:9400".parse().unwrap(), vec![]);
        s.insecure_transport = true;
        let creds = resolve_credentials(&s, dir.path()).unwrap();
        assert!(creds.is_insecure());
    }

    #[test]
    fn bootstraps_first_seed_when_no_creds_provided() {
        let dir = tempfile::tempdir().unwrap();
        let addr: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(1, addr, vec![addr]);
        let creds = resolve_credentials(&s, dir.path()).unwrap();
        assert!(!creds.is_insecure());
        assert!(dir.path().join(TLS_SUBDIR).join(CA_CERT_FILE).exists());
        assert!(dir.path().join(TLS_SUBDIR).join(NODE_CERT_FILE).exists());
        assert!(dir.path().join(TLS_SUBDIR).join(NODE_KEY_FILE).exists());
    }

    #[test]
    fn reloads_credentials_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let addr: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(1, addr, vec![addr]);
        // First run bootstraps.
        let _ = resolve_credentials(&s, dir.path()).unwrap();
        // Second run loads from disk without re-generating.
        let ca_before = fs::read(dir.path().join(TLS_SUBDIR).join(CA_CERT_FILE)).unwrap();
        let creds2 = resolve_credentials(&s, dir.path()).unwrap();
        assert!(!creds2.is_insecure());
        let ca_after = fs::read(dir.path().join(TLS_SUBDIR).join(CA_CERT_FILE)).unwrap();
        assert_eq!(
            ca_before, ca_after,
            "CA should not be regenerated on restart"
        );
    }

    #[test]
    fn joining_node_without_creds_errors() {
        let dir = tempfile::tempdir().unwrap();
        let self_addr: SocketAddr = "10.0.0.2:9400".parse().unwrap();
        let first_seed: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(2, self_addr, vec![first_seed, self_addr]);
        let err = resolve_credentials(&s, dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("TLS credentials not found"),
            "expected missing-creds error, got: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bootstrapped_key_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let addr: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(1, addr, vec![addr]);
        let _ = resolve_credentials(&s, dir.path()).unwrap();
        let key_path = dir.path().join(TLS_SUBDIR).join(NODE_KEY_FILE);
        let mode = fs::metadata(&key_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "key file must be 0600, got {:o}", mode);
        let secret_path = dir.path().join(TLS_SUBDIR).join(CLUSTER_SECRET_FILE);
        let mode = fs::metadata(&secret_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "cluster_secret.bin must be 0600, got {:o}",
            mode
        );
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_load_world_readable_key() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let addr: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(1, addr, vec![addr]);
        // First run bootstraps with tight perms.
        let _ = resolve_credentials(&s, dir.path()).unwrap();
        // Simulate an operator (or a careless tarball) loosening perms.
        let key_path = dir.path().join(TLS_SUBDIR).join(NODE_KEY_FILE);
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();
        // Second run must refuse to load.
        let err = resolve_credentials(&s, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to start") && msg.contains("0644"),
            "expected strict-perms error, got: {msg}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_load_group_readable_cluster_secret() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let addr: SocketAddr = "10.0.0.1:9400".parse().unwrap();
        let s = settings(1, addr, vec![addr]);
        let _ = resolve_credentials(&s, dir.path()).unwrap();
        let secret_path = dir.path().join(TLS_SUBDIR).join(CLUSTER_SECRET_FILE);
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o640)).unwrap();
        let err = resolve_credentials(&s, dir.path()).unwrap_err();
        assert!(err.to_string().contains("refusing to start"));
    }
}
