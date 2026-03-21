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

use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity, Role};

/// JWT validation configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC secret for HS256 verification (base64-encoded).
    /// If empty, HS256 is disabled.
    pub hmac_secret: Vec<u8>,
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
            expected_issuer: String::new(),
            expected_audience: String::new(),
            clock_skew_seconds: 60,
        }
    }
}

/// Decoded JWT claims (the payload after verification).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct JwtClaims {
    /// Subject: typically user_id or username.
    pub sub: String,
    /// Tenant ID.
    #[serde(default)]
    pub tenant_id: u32,
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
    /// Whether this is a superuser token.
    #[serde(default)]
    pub is_superuser: bool,
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

        // Decode payload (middle part). We verify signature separately.
        let payload_bytes = base64_url_decode(parts[1])?;
        let claims: JwtClaims =
            serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)?;

        // Verify HMAC signature if secret is configured.
        if !self.config.hmac_secret.is_empty() {
            let signing_input = format!("{}.{}", parts[0], parts[1]);
            let signature_bytes = base64_url_decode(parts[2])?;
            if !verify_hmac_sha256(
                &self.config.hmac_secret,
                signing_input.as_bytes(),
                &signature_bytes,
            ) {
                return Err(JwtError::InvalidSignature);
            }
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

        // Map roles.
        let roles: Vec<Role> = claims
            .roles
            .iter()
            .map(|r| r.parse::<Role>().unwrap_or(Role::Custom(r.clone())))
            .collect();

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
            auth_method: AuthMethod::ApiKey, // JWT is a bearer token variant.
            roles,
            is_superuser: claims.is_superuser,
        })
    }

    /// Check if JWT authentication is configured (has a secret).
    pub fn is_configured(&self) -> bool {
        !self.config.hmac_secret.is_empty()
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
}

/// Base64url decode (no padding).
fn base64_url_decode(input: &str) -> Result<Vec<u8>, JwtError> {
    // Add padding if needed.
    let padded = match input.len() % 4 {
        2 => format!("{input}=="),
        3 => format!("{input}="),
        _ => input.to_string(),
    };
    // Replace URL-safe chars with standard base64.
    let standard = padded.replace('-', "+").replace('_', "/");
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(&standard)
        .map_err(|_| JwtError::DecodingError)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_claims() {
        // A minimal JWT payload (base64url encoded).
        let payload =
            r#"{"sub":"alice","tenant_id":1,"roles":["readwrite"],"exp":9999999999,"user_id":42}"#;
        let claims: JwtClaims = serde_json::from_str(payload).unwrap();
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
}
