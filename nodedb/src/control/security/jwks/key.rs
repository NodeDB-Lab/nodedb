// SPDX-License-Identifier: BUSL-1.1

//! JWK (JSON Web Key) parsing and verification key abstraction.
//!
//! Parses JWKS JSON responses into `VerificationKey` values that can
//! verify JWT signatures. Supports RSA (RS256) and EC (ES256, ES384).

use crate::control::security::util::base64_url_decode;

/// A parsed verification key extracted from a JWK.
#[derive(Debug, Clone)]
pub struct VerificationKey {
    /// Key ID (`kid` claim in JWK and JWT header).
    pub kid: String,
    /// Algorithm this key is intended for (`alg` field in JWK).
    pub algorithm: String,
    /// The actual key material.
    pub key_type: KeyType,
}

/// Key material for signature verification.
#[derive(Debug, Clone)]
pub enum KeyType {
    /// RSA public key (DER-encoded PKCS#8).
    Rsa(Vec<u8>),
    /// P-256 public key (SEC1 uncompressed point).
    EcP256(Vec<u8>),
    /// P-384 public key (SEC1 uncompressed point).
    EcP384(Vec<u8>),
}

/// A JWKS response: `{"keys": [...]}`.
#[derive(Debug, serde::Deserialize)]
pub struct JwksResponse {
    pub keys: Vec<JwkEntry>,
}

/// A single JWK entry from the JWKS `keys` array.
#[derive(Debug, serde::Deserialize)]
pub struct JwkEntry {
    /// Key type: "RSA" or "EC".
    pub kty: String,
    /// Key ID.
    #[serde(default)]
    pub kid: String,
    /// Algorithm hint.
    #[serde(default)]
    pub alg: String,
    /// Key use: "sig" for signature verification.
    #[serde(default, rename = "use")]
    pub key_use: String,

    // RSA fields.
    #[serde(default)]
    pub n: String,
    #[serde(default)]
    pub e: String,

    // EC fields.
    #[serde(default)]
    pub crv: String,
    #[serde(default)]
    pub x: String,
    #[serde(default)]
    pub y: String,
}

/// Parse a JWK entry into a `VerificationKey`.
///
/// Returns `None` for keys that aren't signature keys (e.g., `use: "enc"`)
/// or use unsupported algorithms.
pub fn parse_jwk(entry: &JwkEntry) -> Option<VerificationKey> {
    // Skip non-signature keys.
    if !entry.key_use.is_empty() && entry.key_use != "sig" {
        return None;
    }

    match entry.kty.as_str() {
        "RSA" => parse_rsa_jwk(entry),
        "EC" => parse_ec_jwk(entry),
        _ => None,
    }
}

/// Parse an RSA JWK into a DER-encoded public key.
fn parse_rsa_jwk(entry: &JwkEntry) -> Option<VerificationKey> {
    let n_bytes = base64_url_decode(&entry.n)?;
    let e_bytes = base64_url_decode(&entry.e)?;

    // Build DER-encoded RSA public key (PKCS#1 RSAPublicKey).
    // ASN.1: SEQUENCE { INTEGER n, INTEGER e }
    let n_der = to_der_integer(&n_bytes);
    let e_der = to_der_integer(&e_bytes);

    let mut seq_content = Vec::new();
    seq_content.extend_from_slice(&n_der);
    seq_content.extend_from_slice(&e_der);

    let mut pkcs1_der = Vec::new();
    pkcs1_der.push(0x30); // SEQUENCE tag
    encode_der_length(&mut pkcs1_der, seq_content.len());
    pkcs1_der.extend_from_slice(&seq_content);

    let alg = if entry.alg.is_empty() {
        "RS256".to_string()
    } else {
        entry.alg.clone()
    };

    Some(VerificationKey {
        kid: entry.kid.clone(),
        algorithm: alg,
        key_type: KeyType::Rsa(pkcs1_der),
    })
}

/// Parse an EC JWK (P-256 or P-384) into SEC1 uncompressed point.
fn parse_ec_jwk(entry: &JwkEntry) -> Option<VerificationKey> {
    let x_bytes = base64_url_decode(&entry.x)?;
    let y_bytes = base64_url_decode(&entry.y)?;

    // SEC1 uncompressed point: 0x04 || x || y
    let mut point = Vec::with_capacity(1 + x_bytes.len() + y_bytes.len());
    point.push(0x04);
    point.extend_from_slice(&x_bytes);
    point.extend_from_slice(&y_bytes);

    let (key_type, default_alg) = match entry.crv.as_str() {
        "P-256" => (KeyType::EcP256(point), "ES256"),
        "P-384" => (KeyType::EcP384(point), "ES384"),
        _ => return None,
    };

    let alg = if entry.alg.is_empty() {
        default_alg.to_string()
    } else {
        entry.alg.clone()
    };

    Some(VerificationKey {
        kid: entry.kid.clone(),
        algorithm: alg,
        key_type,
    })
}

/// Verify a JWT signature using a `VerificationKey`.
///
/// `signing_input` is `header.payload` (the part that was signed).
/// `signature` is the raw decoded signature bytes.
pub fn verify_signature(key: &VerificationKey, signing_input: &[u8], signature: &[u8]) -> bool {
    match &key.key_type {
        KeyType::Rsa(der) => verify_rsa_sha256(der, signing_input, signature),
        KeyType::EcP256(point) => verify_ec_p256(point, signing_input, signature),
        KeyType::EcP384(point) => verify_ec_p384(point, signing_input, signature),
    }
}

/// RSA-SHA256 (RS256) verification.
fn verify_rsa_sha256(pkcs1_der: &[u8], message: &[u8], signature: &[u8]) -> bool {
    use rsa::Pkcs1v15Sign;

    // Try PKCS#1 first (what we build from JWK n,e), then PKCS#8.
    let rsa_key = if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs1::DecodeRsaPublicKey>::from_pkcs1_der(pkcs1_der)
    {
        key
    } else if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs8::DecodePublicKey>::from_public_key_der(pkcs1_der)
    {
        key
    } else {
        return false;
    };

    let digest = {
        use sha2::Digest;
        sha2::Sha256::digest(message)
    };
    let scheme = Pkcs1v15Sign::new::<sha2::Sha256>();
    rsa_key.verify(scheme, &digest, signature).is_ok()
}

/// ES256 (P-256 + SHA-256) verification.
fn verify_ec_p256(sec1_point: &[u8], message: &[u8], signature: &[u8]) -> bool {
    use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};

    let vk = match VerifyingKey::from_sec1_bytes(sec1_point) {
        Ok(k) => k,
        Err(_) => return false,
    };

    // JWT ES256 signatures are raw r||s (32+32 = 64 bytes), not DER.
    let sig = match Signature::from_slice(signature) {
        Ok(s) => s,
        Err(_) => return false,
    };

    vk.verify(message, &sig).is_ok()
}

/// ES384 (P-384 + SHA-384) verification.
fn verify_ec_p384(sec1_point: &[u8], message: &[u8], signature: &[u8]) -> bool {
    use p384::ecdsa::{Signature, VerifyingKey, signature::Verifier};

    let vk = match VerifyingKey::from_sec1_bytes(sec1_point) {
        Ok(k) => k,
        Err(_) => return false,
    };

    // JWT ES384 signatures are raw r||s (48+48 = 96 bytes).
    let sig = match Signature::from_slice(signature) {
        Ok(s) => s,
        Err(_) => return false,
    };

    vk.verify(message, &sig).is_ok()
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Encode a byte slice as a DER INTEGER.
fn to_der_integer(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x02); // INTEGER tag

    // If high bit set, prepend 0x00 to keep positive.
    let needs_pad = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let len = bytes.len() + if needs_pad { 1 } else { 0 };
    encode_der_length(&mut out, len);
    if needs_pad {
        out.push(0x00);
    }
    out.extend_from_slice(bytes);
    out
}

/// Encode a DER length.
fn encode_der_length(out: &mut Vec<u8>, len: usize) {
    if len < 0x80 {
        out.push(len as u8);
    } else if len <= 0xFF {
        out.push(0x81);
        out.push(len as u8);
    } else {
        out.push(0x82);
        out.push((len >> 8) as u8);
        out.push(len as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rsa_jwk_entry() {
        let entry = JwkEntry {
            kty: "RSA".into(),
            kid: "rsa-key-1".into(),
            alg: "RS256".into(),
            key_use: "sig".into(),
            // Minimal RSA key components (not a real key, just structure test).
            n: "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw".into(),
            e: "AQAB".into(),
            crv: String::new(),
            x: String::new(),
            y: String::new(),
        };

        let key = parse_jwk(&entry).unwrap();
        assert_eq!(key.kid, "rsa-key-1");
        assert_eq!(key.algorithm, "RS256");
        assert!(matches!(key.key_type, KeyType::Rsa(_)));
    }

    #[test]
    fn parse_ec_p256_jwk_entry() {
        let entry = JwkEntry {
            kty: "EC".into(),
            kid: "ec-key-1".into(),
            alg: "ES256".into(),
            key_use: "sig".into(),
            n: String::new(),
            e: String::new(),
            crv: "P-256".into(),
            // Example P-256 coordinates (32 bytes each, base64url).
            x: "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU".into(),
            y: "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0".into(),
        };

        let key = parse_jwk(&entry).unwrap();
        assert_eq!(key.kid, "ec-key-1");
        assert_eq!(key.algorithm, "ES256");
        assert!(matches!(key.key_type, KeyType::EcP256(_)));
    }

    const SIGNING_INPUT: &[u8] = b"eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0";

    fn ec_key(key_type: KeyType, algorithm: &str) -> VerificationKey {
        VerificationKey {
            kid: "ec-key".into(),
            algorithm: algorithm.into(),
            key_type,
        }
    }

    #[test]
    fn es256_signature_round_trip() {
        use p256::ecdsa::{Signature, SigningKey, signature::Signer};

        let signing = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let point = signing.verifying_key().to_sec1_point(false);
        let key = ec_key(KeyType::EcP256(point.as_bytes().to_vec()), "ES256");
        let signature: Signature = signing.sign(SIGNING_INPUT);
        let raw = signature.to_bytes();

        assert!(verify_signature(&key, SIGNING_INPUT, &raw));
        assert!(!verify_signature(&key, b"tampered", &raw));
        let mut flipped = raw.to_vec();
        flipped[0] ^= 1;
        assert!(!verify_signature(&key, SIGNING_INPUT, &flipped));
        assert!(!verify_signature(&key, SIGNING_INPUT, &raw[..63]));
    }

    #[test]
    fn es384_signature_round_trip() {
        use p384::ecdsa::{Signature, SigningKey, signature::Signer};

        let signing = SigningKey::from_slice(&[9u8; 48]).unwrap();
        let point = signing.verifying_key().to_sec1_point(false);
        let key = ec_key(KeyType::EcP384(point.as_bytes().to_vec()), "ES384");
        let signature: Signature = signing.sign(SIGNING_INPUT);
        let raw = signature.to_bytes();

        assert!(verify_signature(&key, SIGNING_INPUT, &raw));
        assert!(!verify_signature(&key, b"tampered", &raw));
        let mut flipped = raw.to_vec();
        flipped[0] ^= 1;
        assert!(!verify_signature(&key, SIGNING_INPUT, &flipped));
    }

    #[test]
    fn ec_point_off_the_curve_is_refused() {
        let mut point = vec![0x04];
        point.extend_from_slice(&[1u8; 64]);
        let key = ec_key(KeyType::EcP256(point), "ES256");
        assert!(!verify_signature(&key, SIGNING_INPUT, &[1u8; 64]));
    }

    #[test]
    fn skip_encryption_keys() {
        let entry = JwkEntry {
            kty: "RSA".into(),
            kid: "enc-key".into(),
            alg: "RSA-OAEP".into(),
            key_use: "enc".into(),
            n: "AQAB".into(),
            e: "AQAB".into(),
            crv: String::new(),
            x: String::new(),
            y: String::new(),
        };
        assert!(parse_jwk(&entry).is_none());
    }

    #[test]
    fn skip_unknown_curve() {
        let entry = JwkEntry {
            kty: "EC".into(),
            kid: "ed-key".into(),
            alg: "EdDSA".into(),
            key_use: "sig".into(),
            n: String::new(),
            e: String::new(),
            crv: "Ed25519".into(),
            x: "abc".into(),
            y: "def".into(),
        };
        assert!(parse_jwk(&entry).is_none());
    }
}
