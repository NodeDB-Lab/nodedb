//! `nodedb regen-certs` — reissue the per-node certificate under the
//! existing CA.
//!
//! Loads `data_dir/tls/{ca.crt,ca.key}`, issues a fresh leaf bound to
//! SANs `node-<id>` + `"nodedb"`, and writes `node.crt` + `node.key`
//! (0600). The `cluster_secret.bin` is untouched so the envelope MAC
//! key stays stable across reissue. The CA itself is unchanged —
//! other nodes' trust store needs no update.
//!
//! Used after a node-cert compromise / near-expiry. For a CA
//! rotation, use `nodedb rotate-ca --stage` + `--finalize`.

use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use nodedb_cluster::issue_leaf_for_sans;
use nodedb_cluster::transport::config::SNI_HOSTNAME;
use nodedb_cluster::transport::pki_types::CertificateDer;

use crate::control::cluster::pem_io;

pub fn run(data_dir: &Path, node_id: u64) -> Result<(), String> {
    let tls_dir = data_dir.join("tls");
    let ca_cert_path = tls_dir.join("ca.crt");
    let ca_key_path = tls_dir.join("ca.key");

    if !ca_cert_path.exists() {
        return Err(format!(
            "CA cert not found at {}. This node was not bootstrapped as a CA authority; \
             use `rotate-ca --stage` to introduce a new CA cluster-wide instead.",
            ca_cert_path.display()
        ));
    }
    if !ca_key_path.exists() {
        return Err(format!(
            "CA private key not found at {}. Either the bootstrap predates CA-key \
             persistence (upgrade path: `rotate-ca --stage` + `--finalize` to swap to \
             a fresh CA with a persisted key), or the key was deliberately removed \
             after bootstrap. Without it, a same-CA reissue is impossible.",
            ca_key_path.display()
        ));
    }

    ensure_ca_key_perms(&ca_key_path)?;

    let ca_cert_bytes = fs::read(&ca_cert_path)
        .map_err(|e| format!("read ca.crt {}: {e}", ca_cert_path.display()))?;
    let ca_cert_der = parse_single_cert_pem(&ca_cert_bytes)
        .map_err(|e| format!("parse ca.crt {}: {e}", ca_cert_path.display()))?;

    let ca_key_bytes = fs::read(&ca_key_path)
        .map_err(|e| format!("read ca.key {}: {e}", ca_key_path.display()))?;
    let ca_key_der = parse_private_key_pem(&ca_key_bytes)
        .map_err(|e| format!("parse ca.key {}: {e}", ca_key_path.display()))?;

    let ca = nexar::transport::tls::ClusterCa::from_der(&ca_key_der, &ca_cert_der)
        .map_err(|e| format!("reload CA: {e}"))?;

    let node_san = format!("node-{node_id}");
    let creds = issue_leaf_for_sans(&ca, &[&node_san, SNI_HOSTNAME])
        .map_err(|e| format!("issue new leaf: {e}"))?;

    // Atomic-ish write: stage under `.new` then rename.
    let node_cert_path = tls_dir.join("node.crt");
    let node_key_path = tls_dir.join("node.key");
    let cert_tmp = with_suffix(&node_cert_path, ".new");
    let key_tmp = with_suffix(&node_key_path, ".new");

    write_pem_cert(&cert_tmp, creds.cert.as_ref())
        .map_err(|e| format!("write {}: {e}", cert_tmp.display()))?;
    write_pem_key(&key_tmp, creds.key.secret_der())
        .map_err(|e| format!("write {}: {e}", key_tmp.display()))?;

    fs::rename(&cert_tmp, &node_cert_path).map_err(|e| {
        format!(
            "rename {} → {}: {e}",
            cert_tmp.display(),
            node_cert_path.display()
        )
    })?;
    fs::rename(&key_tmp, &node_key_path).map_err(|e| {
        format!(
            "rename {} → {}: {e}",
            key_tmp.display(),
            node_key_path.display()
        )
    })?;

    println!("reissued node cert:");
    println!("  node_id:   {node_id}");
    println!("  SANs:      [{node_san}, {SNI_HOSTNAME}]");
    println!("  cert:      {}", node_cert_path.display());
    println!("  key:       {} (0600)", node_key_path.display());
    println!();
    println!("restart the node to pick up the new cert. CA + cluster_secret unchanged;");
    println!("no action required on peer nodes.");
    Ok(())
}

fn parse_single_cert_pem(bytes: &[u8]) -> Result<CertificateDer<'static>, String> {
    let mut reader = BufReader::new(bytes);
    let mut iter = rustls_pemfile::certs(&mut reader);
    match iter.next() {
        Some(Ok(cert)) => Ok(cert),
        Some(Err(e)) => Err(format!("pem: {e}")),
        None => Err("no CERTIFICATE block".into()),
    }
}

fn parse_private_key_pem(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut reader = BufReader::new(bytes);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("pem: {e}"))?
        .ok_or_else(|| "no PRIVATE KEY block".to_string())?;
    Ok(key.secret_der().to_vec())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

// PEM writers live in `crate::control::cluster::pem_io`. Keep thin
// aliases inside this module so the atomic-write staging block below
// reads the same way it did before the extraction.
fn write_pem_cert(path: &Path, der: &[u8]) -> std::io::Result<()> {
    pem_io::write_pem_cert(path, der)
}
fn write_pem_key(path: &Path, der: &[u8]) -> std::io::Result<()> {
    pem_io::write_pem_private_key(path, der)
}

fn ensure_ca_key_perms(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|e| format!("stat {}: {e}", path.display()))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(format!(
                "CA key {} has mode {:04o}; refuse to use. chmod 600 {} and retry.",
                path.display(),
                mode,
                path.display()
            ));
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

    #[test]
    fn regen_reissues_cert_under_existing_ca() {
        // Bootstrap a fresh CA, persist ca.crt + ca.key + node.{crt,key},
        // then call `run` and verify node.crt changed while ca.crt stayed
        // identical.
        let td = tempfile::tempdir().unwrap();
        let data_dir = td.path();
        let tls_dir = data_dir.join("tls");
        fs::create_dir_all(&tls_dir).unwrap();

        let (ca, creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["node-1", SNI_HOSTNAME]).unwrap();
        write_pem_cert(&tls_dir.join("ca.crt"), ca.cert_der().as_ref()).unwrap();
        write_pem_key(&tls_dir.join("ca.key"), &ca.key_pair_pkcs8_der()).unwrap();
        write_pem_cert(&tls_dir.join("node.crt"), creds.cert.as_ref()).unwrap();
        write_pem_key(&tls_dir.join("node.key"), creds.key.secret_der()).unwrap();
        // cluster_secret.bin isn't needed by regen-certs but exists in a real deployment.
        fs::write(tls_dir.join("cluster_secret.bin"), [0u8; 32]).unwrap();
        pem_io::set_private_key_perms(&tls_dir.join("cluster_secret.bin")).unwrap();

        let old_ca_bytes = fs::read(tls_dir.join("ca.crt")).unwrap();
        let old_node_bytes = fs::read(tls_dir.join("node.crt")).unwrap();

        run(data_dir, 1).unwrap();

        let new_ca_bytes = fs::read(tls_dir.join("ca.crt")).unwrap();
        let new_node_bytes = fs::read(tls_dir.join("node.crt")).unwrap();

        assert_eq!(old_ca_bytes, new_ca_bytes, "ca.crt must be unchanged");
        assert_ne!(old_node_bytes, new_node_bytes, "node.crt must be reissued");
    }

    #[test]
    fn regen_errors_when_ca_key_missing() {
        let td = tempfile::tempdir().unwrap();
        let data_dir = td.path();
        let tls_dir = data_dir.join("tls");
        fs::create_dir_all(&tls_dir).unwrap();
        let (ca, _) = nodedb_cluster::generate_node_credentials_multi_san(&["node-1"]).unwrap();
        write_pem_cert(&tls_dir.join("ca.crt"), ca.cert_der().as_ref()).unwrap();
        // Deliberately no ca.key.
        let err = run(data_dir, 1).unwrap_err();
        assert!(err.contains("CA private key not found"), "got: {err}");
    }
}
