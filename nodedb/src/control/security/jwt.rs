// SPDX-License-Identifier: BUSL-1.1

//! JWT (JSON Web Token) bearer token authentication.
//!
//! Validates JWTs presented as `Authorization: Bearer <token>` headers
//! or as `password` in pgwire authentication. Supports:
//!
//! - HS256 (HMAC-SHA256) for shared-secret deployments
//! - RS256 (RSA-SHA256) for public-key deployments
//! - Token expiration (`exp` claim)
//! - Tenant isolation (`tenant_id` claim)
//! - Role mapping (`roles` claim → NodeDB roles)
//!
//! The JWT secret/public key is configured per cluster. Tokens are
//! stateless — no server-side session storage required.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::debug;

use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity, Role, roles_from_external_claims};

/// JWT validation configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC secret for HS256 verification (raw bytes).
    /// If empty, HS256 is disabled.
    pub hmac_secret: Vec<u8>,
    /// RSA public key for RS256 verification (DER-encoded PKCS#8 or PKCS#1).
    /// If empty, RS256 is disabled.
    pub rsa_public_key_der: Vec<u8>,
    /// Expected issuer (`iss` claim). Empty = don't validate.
    pub expected_issuer: String,
    /// Expected audience (`aud` claim). Empty = don't validate.
    pub expected_audience: String,
    /// Clock skew tolerance in seconds for `exp`/`nbf` validation.
    pub clock_skew_seconds: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            hmac_secret: Vec::new(),
            rsa_public_key_der: Vec::new(),
            expected_issuer: String::new(),
            expected_audience: String::new(),
            clock_skew_seconds: 60,
        }
    }
}

/// JWT header (the first base64url-encoded segment).
#[derive(Debug, serde::Deserialize)]
struct JwtHeader {
    /// Algorithm: "HS256" or "RS256".
    alg: String,
}

/// Decoded JWT claims (the payload after verification).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct JwtClaims {
    /// Subject: typically user_id or username.
    pub sub: String,
    /// Tenant ID.
    #[serde(default)]
    pub tenant_id: u64,
    /// Roles as string array.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Expiration time (Unix timestamp).
    #[serde(default)]
    pub exp: u64,
    /// Not-before time (Unix timestamp).
    #[serde(default)]
    pub nbf: u64,
    /// Issued-at time.
    #[serde(default)]
    pub iat: u64,
    /// Issuer.
    #[serde(default)]
    pub iss: String,
    /// Audience.
    #[serde(default)]
    pub aud: String,
    /// User ID (NodeDB-specific claim).
    #[serde(default)]
    pub user_id: u64,
    /// Legacy external superuser assertion.
    ///
    /// Parsed for compatibility and security telemetry, but never grants
    /// NodeDB superuser authority.
    #[serde(default)]
    pub is_superuser: bool,
    /// Extended claims not covered by the standard fields above.
    ///
    /// Captures provider-specific claims (email, org_id, groups, permissions,
    /// status, metadata) that `AuthContext::from_jwt()` maps to session
    /// variables. Different providers use different claim names — the
    /// `[auth.jwt.claims]` config section remaps them.
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// JWT validator.
pub struct JwtValidator {
    config: JwtConfig,
}

impl JwtValidator {
    pub fn new(config: JwtConfig) -> Self {
        Self { config }
    }

    /// Validate a JWT token string and extract the authenticated identity.
    ///
    /// Performs:
    /// 1. Base64 decode header + payload + signature
    /// 2. HMAC-SHA256 signature verification (if configured)
    /// 3. Expiration check (`exp` claim)
    /// 4. Issuer/audience validation (if configured)
    /// 5. Map claims → `AuthenticatedIdentity`
    pub fn validate(&self, token: &str) -> Result<AuthenticatedIdentity, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::MalformedToken);
        }

        // Decode header to determine algorithm.
        let header_bytes = base64_url_decode(parts[0]).ok_or(JwtError::DecodingError)?;
        let header: JwtHeader =
            sonic_rs::from_slice(&header_bytes).map_err(|_| JwtError::InvalidClaims)?;

        // Decode payload (middle part). We verify signature separately.
        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        let claims: JwtClaims =
            sonic_rs::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)?;

        // Verify signature based on algorithm declared in header.
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let signature_bytes = base64_url_decode(parts[2]).ok_or(JwtError::DecodingError)?;

        match header.alg.as_str() {
            "HS256" => {
                if self.config.hmac_secret.is_empty() {
                    return Err(JwtError::UnsupportedAlgorithm);
                }
                if !verify_hmac_sha256(
                    &self.config.hmac_secret,
                    signing_input.as_bytes(),
                    &signature_bytes,
                ) {
                    return Err(JwtError::InvalidSignature);
                }
            }
            "RS256" => {
                if self.config.rsa_public_key_der.is_empty() {
                    return Err(JwtError::UnsupportedAlgorithm);
                }
                if !verify_rsa_sha256(
                    &self.config.rsa_public_key_der,
                    signing_input.as_bytes(),
                    &signature_bytes,
                ) {
                    return Err(JwtError::InvalidSignature);
                }
            }
            _ => return Err(JwtError::UnsupportedAlgorithm),
        }

        // Check expiration.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if claims.exp > 0 && now > claims.exp + self.config.clock_skew_seconds {
            return Err(JwtError::Expired);
        }
        if claims.nbf > 0 && now + self.config.clock_skew_seconds < claims.nbf {
            return Err(JwtError::NotYetValid);
        }

        // Validate issuer.
        if !self.config.expected_issuer.is_empty() && claims.iss != self.config.expected_issuer {
            return Err(JwtError::InvalidIssuer);
        }

        // Validate audience.
        if !self.config.expected_audience.is_empty() && claims.aud != self.config.expected_audience
        {
            return Err(JwtError::InvalidAudience);
        }

        // External tokens may supply ordinary roles, but NodeDB owns the
        // non-assertable superuser privilege boundary.
        let roles: Vec<Role> = roles_from_external_claims(&claims.roles, claims.is_superuser);

        let username = if claims.sub.is_empty() {
            format!("jwt_user_{}", claims.user_id)
        } else {
            claims.sub.clone()
        };

        debug!(
            username = %username,
            tenant_id = claims.tenant_id,
            roles = ?roles,
            "JWT validated"
        );

        Ok(AuthenticatedIdentity {
            user_id: claims.user_id,
            username,
            tenant_id: TenantId::new(claims.tenant_id),
            auth_method: AuthMethod::OidcBearer,
            roles,
            is_superuser: false,
            default_database: None,
            accessible_databases: AuthenticatedIdentity::default_database_set(false),
        })
    }

    /// Check if JWT authentication is configured (has a secret or public key).
    pub fn is_configured(&self) -> bool {
        !self.config.hmac_secret.is_empty() || !self.config.rsa_public_key_der.is_empty()
    }
}

/// JWT validation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    #[error("malformed JWT token")]
    MalformedToken,
    #[error("invalid JWT claims")]
    InvalidClaims,
    #[error("JWT signature verification failed")]
    InvalidSignature,
    #[error("JWT token expired")]
    Expired,
    #[error("JWT token not yet valid")]
    NotYetValid,
    #[error("JWT issuer mismatch")]
    InvalidIssuer,
    #[error("JWT audience mismatch")]
    InvalidAudience,
    #[error("JWT base64 decoding error")]
    DecodingError,
    #[error("JWT algorithm not supported or not configured")]
    UnsupportedAlgorithm,
}

/// Verify HMAC-SHA256 signature.
fn verify_hmac_sha256(secret: &[u8], message: &[u8], expected_signature: &[u8]) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac = match HmacSha256::new_from_slice(secret) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(message);
    mac.verify_slice(expected_signature).is_ok()
}

/// Verify RSA-SHA256 (RS256) signature using a DER-encoded public key.
///
/// Supports both PKCS#1 (RSAPublicKey) and PKCS#8 (SubjectPublicKeyInfo) formats.
fn verify_rsa_sha256(public_key_der: &[u8], message: &[u8], signature: &[u8]) -> bool {
    use rsa::Pkcs1v15Sign;

    // Try PKCS#8 first, then PKCS#1.
    let rsa_key = if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs8::DecodePublicKey>::from_public_key_der(public_key_der)
    {
        key
    } else if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs1::DecodeRsaPublicKey>::from_pkcs1_der(public_key_der)
    {
        key
    } else {
        return false;
    };

    // Hash the message with SHA-256.
    use sha2::Digest;
    let digest = sha2::Sha256::digest(message);

    // Verify PKCS#1 v1.5 signature.
    let scheme = Pkcs1v15Sign::new::<sha2::Sha256>();
    rsa_key.verify(scheme, &digest, signature).is_ok()
}

/// Load an RSA public key from a PEM file (for JwtConfig initialization).
///
/// Accepts PEM files with either `BEGIN PUBLIC KEY` (PKCS#8) or
/// `BEGIN RSA PUBLIC KEY` (PKCS#1) headers.
pub fn load_rsa_public_key_pem(pem_path: &std::path::Path) -> Result<Vec<u8>, JwtError> {
    let pem_data = std::fs::read(pem_path).map_err(|_| JwtError::DecodingError)?;
    let parsed = pem::parse(&pem_data).map_err(|_| JwtError::DecodingError)?;
    match parsed.tag() {
        "PUBLIC KEY" | "RSA PUBLIC KEY" => Ok(parsed.into_contents()),
        _ => Err(JwtError::DecodingError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_claims() {
        // A minimal JWT payload (base64url encoded).
        let payload =
            r#"{"sub":"alice","tenant_id":1,"roles":["readwrite"],"exp":9999999999,"user_id":42}"#;
        let claims: JwtClaims = sonic_rs::from_str(payload).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.tenant_id, 1);
        assert_eq!(claims.user_id, 42);
        assert_eq!(claims.roles, vec!["readwrite"]);
    }

    #[test]
    fn malformed_token_rejected() {
        let validator = JwtValidator::new(JwtConfig::default());
        let result = validator.validate("not-a-jwt");
        assert_eq!(result.err(), Some(JwtError::MalformedToken));
    }

    #[test]
    fn base64url_decode_works() {
        let encoded = base64_url_encode(b"hello world");
        let decoded = base64_url_decode(&encoded).unwrap();
        assert_eq!(decoded, b"hello world");
    }

    fn base64_url_encode(data: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
    }

    /// Bit length used for RSA keys generated in this test module.
    /// Production validates whatever the operator configures; the
    /// signing/verification logic doesn't care about strength, so we
    /// use 1024 here to keep `RsaPrivateKey::new` from dominating the
    /// test runtime. RSA-1024 keygen is ~10x faster than RSA-2048
    /// without changing what these tests actually exercise.
    const TEST_RSA_BITS: usize = 1024;

    fn validate_rs256_payload(payload_json: &str) -> AuthenticatedIdentity {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        let mut rng = rsa::rand_core::OsRng;
        let private_key = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let public_key = rsa::RsaPublicKey::from(&private_key);
        let pub_der = {
            use rsa::pkcs8::EncodePublicKey;
            public_key.to_public_key_der().unwrap().as_ref().to_vec()
        };
        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = base64_url_encode(payload_json.as_bytes());
        let signing_input = format!("{header}.{payload}");
        let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
        let signature: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            base64_url_encode(&signature.to_bytes())
        );
        let validator = JwtValidator::new(JwtConfig {
            rsa_public_key_der: pub_der,
            ..Default::default()
        });
        validator.validate(&token).unwrap()
    }

    #[test]
    fn externally_asserted_superuser_flag_does_not_grant_superuser() {
        let identity = validate_rs256_payload(
            r#"{"sub":"alice","tenant_id":2,"roles":["readwrite"],"is_superuser":true,"exp":9999999999,"user_id":99}"#,
        );

        assert!(!identity.is_superuser);
        assert!(!identity.roles.contains(&Role::Superuser));
        assert!(identity.roles.contains(&Role::ReadWrite));
        assert!(!identity.can_access_database(nodedb_types::id::DatabaseId::new(9_999)));
    }

    #[test]
    fn externally_asserted_superuser_role_does_not_grant_superuser() {
        let identity = validate_rs256_payload(
            r#"{"sub":"alice","tenant_id":2,"roles":["superuser","readonly"],"is_superuser":false,"exp":9999999999,"user_id":99}"#,
        );

        assert!(!identity.is_superuser);
        assert!(!identity.roles.contains(&Role::Superuser));
        assert!(identity.roles.contains(&Role::ReadOnly));
    }

    #[test]
    fn rs256_roundtrip() {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        // Generate a test RSA key pair.
        let mut rng = rsa::rand_core::OsRng;
        let private_key = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let public_key = rsa::RsaPublicKey::from(&private_key);

        // Export public key as DER (PKCS#8).
        let pub_der = {
            use rsa::pkcs8::EncodePublicKey;
            public_key.to_public_key_der().unwrap().as_ref().to_vec()
        };

        // Build JWT manually.
        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload_json =
            r#"{"sub":"bob","tenant_id":2,"roles":["admin"],"exp":9999999999,"user_id":99}"#;
        let payload = base64_url_encode(payload_json.as_bytes());
        let signing_input = format!("{header}.{payload}");

        // Sign with RSA PKCS#1 v1.5.
        let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
        let sig: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = base64_url_encode(&sig.to_bytes());

        let token = format!("{signing_input}.{sig_b64}");

        // Validate.
        let config = JwtConfig {
            rsa_public_key_der: pub_der,
            ..Default::default()
        };
        let validator = JwtValidator::new(config);
        let identity = validator.validate(&token).unwrap();
        assert_eq!(identity.username, "bob");
        assert_eq!(identity.tenant_id, TenantId::new(2));
        assert_eq!(identity.user_id, 99);
    }

    #[test]
    fn rs256_wrong_key_rejected() {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        let mut rng = rsa::rand_core::OsRng;
        let key1 = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let key2 = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let pub2 = rsa::RsaPublicKey::from(&key2);

        let pub2_der = {
            use rsa::pkcs8::EncodePublicKey;
            pub2.to_public_key_der().unwrap().as_ref().to_vec()
        };

        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","exp":9999999999}"#);
        let signing_input = format!("{header}.{payload}");

        // Sign with key1.
        let signing_key = SigningKey::<sha2::Sha256>::new(key1);
        let sig: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = base64_url_encode(&sig.to_bytes());
        let token = format!("{signing_input}.{sig_b64}");

        // Verify with key2 — should fail.
        let config = JwtConfig {
            rsa_public_key_der: pub2_der,
            ..Default::default()
        };
        let validator = JwtValidator::new(config);
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::InvalidSignature)
        );
    }

    #[test]
    fn unsupported_algorithm_rejected() {
        let header = base64_url_encode(br#"{"alg":"ES256"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","exp":9999999999}"#);
        let sig = base64_url_encode(b"fakesig");
        let token = format!("{header}.{payload}.{sig}");

        let validator = JwtValidator::new(JwtConfig::default());
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::UnsupportedAlgorithm)
        );
    }
}
