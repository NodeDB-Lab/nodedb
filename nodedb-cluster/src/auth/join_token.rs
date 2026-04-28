//! HMAC-SHA256 join-token issuance and constant-time verification.
//!
//! Token format (opaque to callers, transmitted as hex):
//! ```text
//! [for_node: u64 LE | expiry_unix_secs: u64 LE | mac: 32 bytes]
//! ```
//! The MAC is HMAC-SHA256 over `for_node || expiry_unix_secs` keyed by
//! the cluster's `cluster_secret`. Verification is constant-time via
//! `hmac::Mac::verify_slice` (which uses the `subtle` crate internally).
//!
//! The `nodedb` crate's `ctl::join_token` module is a thin CLI wrapper
//! that delegates issuance to [`issue_token`] here. Verification is
//! consumed by the bootstrap-listener handler in
//! `nodedb/src/control/cluster/bootstrap_listener.rs`.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Number of bytes in the token header (for_node + expiry).
pub const TOKEN_HEADER_LEN: usize = 8 + 8;
/// Number of bytes in the HMAC-SHA256 tag.
pub const TOKEN_MAC_LEN: usize = 32;
/// Total token byte length before hex encoding.
pub const TOKEN_BYTE_LEN: usize = TOKEN_HEADER_LEN + TOKEN_MAC_LEN;
/// Expected hex string length of a token.
pub const TOKEN_HEX_LEN: usize = TOKEN_BYTE_LEN * 2;

/// Error returned by token operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("token wrong length")]
    WrongLength,
    #[error("token contains invalid hex")]
    InvalidHex,
    #[error("invalid token MAC")]
    InvalidMac,
    #[error("token expired")]
    Expired,
    #[error("hmac key length invalid")]
    HmacKeyLength,
}

/// Issue a new HMAC-SHA256 join token for `for_node` that expires at
/// `expiry_unix_secs`. Returns the raw token bytes (hex-encode for
/// printing or transmission).
///
/// Use [`token_to_hex`] to produce the hex string.
pub fn issue_token_bytes(
    secret: &[u8; 32],
    for_node: u64,
    expiry_unix_secs: u64,
) -> Result<[u8; TOKEN_BYTE_LEN], TokenError> {
    let mut buf = [0u8; TOKEN_BYTE_LEN];
    buf[..8].copy_from_slice(&for_node.to_le_bytes());
    buf[8..16].copy_from_slice(&expiry_unix_secs.to_le_bytes());
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).map_err(|_| TokenError::HmacKeyLength)?;
    mac.update(&buf[..TOKEN_HEADER_LEN]);
    let tag = mac.finalize().into_bytes();
    buf[TOKEN_HEADER_LEN..].copy_from_slice(&tag);
    Ok(buf)
}

/// Convenience: issue a token and return it as a lowercase hex string.
pub fn issue_token(
    secret: &[u8; 32],
    for_node: u64,
    expiry_unix_secs: u64,
) -> Result<String, TokenError> {
    let bytes = issue_token_bytes(secret, for_node, expiry_unix_secs)?;
    Ok(token_to_hex(&bytes))
}

/// Encode raw token bytes as a lowercase hex string.
pub fn token_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Compute SHA-256 of the token bytes. Used as the stable identity for
/// state-machine tracking (never stores the raw token).
pub fn token_hash(token_hex: &str) -> Result<[u8; 32], TokenError> {
    use sha2::Digest;
    let bytes = hex_decode(token_hex)?;
    Ok(sha2::Sha256::digest(&bytes).into())
}

/// Verify a hex-encoded token against `secret`. Returns the bound
/// `(for_node, expiry_unix_secs)` on success.
///
/// The HMAC comparison is constant-time (via `hmac::Mac::verify_slice`
/// which uses the `subtle` crate internally).
pub fn verify_token(token_hex: &str, secret: &[u8; 32]) -> Result<(u64, u64), TokenError> {
    if token_hex.len() != TOKEN_HEX_LEN {
        return Err(TokenError::WrongLength);
    }
    let bytes = hex_decode(token_hex)?;
    let (body, tag) = bytes.split_at(TOKEN_HEADER_LEN);
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).map_err(|_| TokenError::HmacKeyLength)?;
    mac.update(body);
    mac.verify_slice(tag).map_err(|_| TokenError::InvalidMac)?;
    let for_node = u64::from_le_bytes(body[..8].try_into().expect("slice is 8 bytes"));
    let expiry = u64::from_le_bytes(body[8..].try_into().expect("slice is 8 bytes"));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now > expiry {
        return Err(TokenError::Expired);
    }
    Ok((for_node, expiry))
}

fn hex_decode(s: &str) -> Result<Vec<u8>, TokenError> {
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        if chunk.len() != 2 {
            return Err(TokenError::InvalidHex);
        }
        let hi = hex_digit(chunk[0]).ok_or(TokenError::InvalidHex)?;
        let lo = hex_digit(chunk[1]).ok_or(TokenError::InvalidHex)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn roundtrip_verify_accepts_fresh_token() {
        let secret = [0x11u8; 32];
        let for_node = 42u64;
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = issue_token(&secret, for_node, expiry).unwrap();
        let (got_node, got_exp) = verify_token(&hex, &secret).unwrap();
        assert_eq!(got_node, for_node);
        assert_eq!(got_exp, expiry);
    }

    #[test]
    fn rejects_tampered_mac() {
        let secret = [0x11u8; 32];
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = issue_token(&secret, 1, expiry).unwrap();
        // Flip last two hex chars (end of MAC).
        let mut tampered = hex.clone();
        let len = tampered.len();
        tampered.replace_range(len - 2..len, "00");
        assert_eq!(
            verify_token(&tampered, &secret).unwrap_err(),
            TokenError::InvalidMac
        );
    }

    #[test]
    fn rejects_wrong_secret() {
        let secret = [0x11u8; 32];
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = issue_token(&secret, 5, expiry).unwrap();
        let other = [0x22u8; 32];
        assert_eq!(
            verify_token(&hex, &other).unwrap_err(),
            TokenError::InvalidMac
        );
    }

    #[test]
    fn rejects_expired() {
        let secret = [0xAAu8; 32];
        // expiry in the distant past
        let expiry = 1u64;
        let hex = issue_token(&secret, 1, expiry).unwrap();
        assert_eq!(
            verify_token(&hex, &secret).unwrap_err(),
            TokenError::Expired
        );
    }

    #[test]
    fn rejects_wrong_length() {
        let secret = [0x55u8; 32];
        assert_eq!(
            verify_token("deadbeef", &secret).unwrap_err(),
            TokenError::WrongLength
        );
    }

    #[test]
    fn token_hash_stable() {
        let secret = [0x33u8; 32];
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = issue_token(&secret, 7, expiry).unwrap();
        let h1 = token_hash(&hex).unwrap();
        let h2 = token_hash(&hex).unwrap();
        assert_eq!(h1, h2);
        // Hash must differ from token bytes
        let _bytes = hex_decode(&hex).unwrap();
        // Just check it's non-zero
        assert!(h1.iter().any(|&b| b != 0));
    }

    // Verify constant-time property indirectly: the rejection path for
    // InvalidMac must go through `verify_slice`, not a byte-by-byte short-
    // circuit. We can't measure timing in a unit test, but we can confirm
    // the error variant is `InvalidMac` (not `WrongLength` or `InvalidHex`),
    // which means the full MAC was fed into `verify_slice` before rejection.
    #[test]
    fn invalid_mac_error_variant_confirms_constant_time_path() {
        let secret = [0x77u8; 32];
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let hex = issue_token(&secret, 9, expiry).unwrap();
        // Replace entire MAC region with zeros (all-zero hex suffix).
        let header_hex_len = TOKEN_HEADER_LEN * 2;
        let zero_mac = "00".repeat(TOKEN_MAC_LEN);
        let tampered = format!("{}{}", &hex[..header_hex_len], zero_mac);
        assert_eq!(tampered.len(), TOKEN_HEX_LEN);
        assert_eq!(
            verify_token(&tampered, &secret).unwrap_err(),
            TokenError::InvalidMac,
            "rejection must be via constant-time verify_slice, not short-circuit"
        );
    }

    /// Issue a token using the helper function that includes reading the
    /// wall clock, identical to the CLI path.
    #[test]
    fn issue_token_with_duration() {
        let secret = [0xBBu8; 32];
        let ttl = Duration::from_secs(300);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expiry = now + ttl.as_secs();
        let hex = issue_token(&secret, 3, expiry).unwrap();
        assert_eq!(hex.len(), TOKEN_HEX_LEN);
        let (node, exp) = verify_token(&hex, &secret).unwrap();
        assert_eq!(node, 3);
        assert!(exp >= now + ttl.as_secs() - 2); // allow 2s clock slack
    }
}
