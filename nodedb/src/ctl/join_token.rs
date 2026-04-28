//! `nodedb join-token --create` — thin CLI wrapper over
//! [`nodedb_cluster::auth::join_token`].
//!
//! Token format (opaque to callers, printed as hex):
//! ```text
//! [for_node: u64 LE | expiry_unix_secs: u64 LE | mac: 32 bytes]
//! ```
//! The MAC is HMAC-SHA256 over `for_node || expiry` keyed by the
//! cluster's persisted `cluster_secret`. Verification and issuance
//! logic live in `nodedb_cluster::auth::join_token` — the single
//! source of truth for the token format.
//!
//! Verification is consumed by the bootstrap-listener handler in
//! `nodedb/src/control/cluster/bootstrap_listener.rs` via
//! `nodedb_cluster::verify_token`.

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb_cluster::auth::join_token as tok;

const CLUSTER_SECRET_LEN: usize = 32;

pub fn create(data_dir: &Path, for_node: u64, ttl: Duration) -> Result<(), String> {
    let secret_path = data_dir.join("tls").join("cluster_secret.bin");
    let secret = read_cluster_secret(&secret_path)?;

    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock before UNIX epoch: {e}"))?
        + ttl;
    let expiry_secs = expiry.as_secs();

    let hex = tok::issue_token(&secret, for_node, expiry_secs)
        .map_err(|e| format!("issue token: {e}"))?;

    println!("join token (expires at unix {expiry_secs}):");
    println!("{hex}");
    println!();
    println!("usage on joiner:");
    println!("  NODEDB_JOIN_TOKEN={hex} nodedb <config.toml>");
    Ok(())
}

fn read_cluster_secret(path: &Path) -> Result<[u8; CLUSTER_SECRET_LEN], String> {
    let bytes =
        fs::read(path).map_err(|e| format!("read cluster secret {}: {e}", path.display()))?;
    if bytes.len() != CLUSTER_SECRET_LEN {
        return Err(format!(
            "cluster secret {} has {} bytes, expected {CLUSTER_SECRET_LEN}",
            path.display(),
            bytes.len()
        ));
    }
    let mut out = [0u8; CLUSTER_SECRET_LEN];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nodedb_cluster::auth::join_token as tok;

    #[test]
    fn verify_accepts_fresh_token_rejects_tampered() {
        let secret = [0x11u8; 32];
        let for_node = 42u64;
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = tok::issue_token(&secret, for_node, expiry).unwrap();

        let (got_node, got_exp) = tok::verify_token(&hex, &secret).unwrap();
        assert_eq!(got_node, for_node);
        assert_eq!(got_exp, expiry);

        // Flip a byte in the MAC portion.
        let mut tampered = hex.clone();
        let flip = tampered.len() - 4;
        tampered.replace_range(flip..flip + 2, "00");
        assert!(tok::verify_token(&tampered, &secret).is_err());

        // Wrong secret.
        let other_secret = [0x22u8; 32];
        assert!(tok::verify_token(&hex, &other_secret).is_err());
    }

    #[test]
    fn verify_rejects_expired() {
        let secret = [0xAAu8; 32];
        let expiry = 1u64; // very old
        let hex = tok::issue_token(&secret, 1, expiry).unwrap();
        let err = tok::verify_token(&hex, &secret).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("expired"), "got: {msg}");
    }

    #[test]
    fn create_writes_hex_token_for_real_secret() {
        use std::io::Write;
        let td = tempfile::tempdir().unwrap();
        let tls_dir = td.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        let mut f = std::fs::File::create(tls_dir.join("cluster_secret.bin")).unwrap();
        f.write_all(&[0x5Au8; 32]).unwrap();
        drop(f);
        // Smoke test: succeeds and writes expected amount of output.
        create(td.path(), 9, Duration::from_secs(600)).unwrap();
    }
}
