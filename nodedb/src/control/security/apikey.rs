//! API key management — generation, verification, storage.
//!
//! Key format: `ndb_<key_id>_<secret>`
//! - key_id: 12 chars base62 (encodes random u64)
//! - secret: 43 chars base62 (encodes random 32 bytes)
//!
//! Storage: SHA-256 hash of secret in system catalog (redb).
//! The full key is only shown once at creation time.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::TenantId;

use super::catalog::{StoredApiKey, SystemCatalog};
use super::identity::{AuthMethod, AuthenticatedIdentity, Role};

/// A single scoped permission on a key: (permission, collection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyScope {
    pub permission: String,
    pub collection: String,
}

/// In-memory API key record.
#[derive(Debug, Clone)]
pub struct ApiKeyRecord {
    pub key_id: String,
    pub secret_hash: Vec<u8>,
    pub username: String,
    pub user_id: u64,
    pub tenant_id: TenantId,
    pub expires_at: u64,
    pub is_revoked: bool,
    pub created_at: u64,
    /// Permission scope restriction. Empty = inherit all user permissions.
    pub scope: Vec<KeyScope>,
}

impl ApiKeyRecord {
    fn to_stored(&self) -> StoredApiKey {
        StoredApiKey {
            key_id: self.key_id.clone(),
            secret_hash: self.secret_hash.clone(),
            username: self.username.clone(),
            user_id: self.user_id,
            tenant_id: self.tenant_id.as_u32(),
            expires_at: self.expires_at,
            is_revoked: self.is_revoked,
            created_at: self.created_at,
            scope: self
                .scope
                .iter()
                .map(|s| format!("{}:{}", s.permission, s.collection))
                .collect(),
        }
    }

    fn from_stored(s: StoredApiKey) -> Self {
        let scope = s
            .scope
            .iter()
            .filter_map(|s| {
                let (perm, coll) = s.split_once(':')?;
                Some(KeyScope {
                    permission: perm.to_string(),
                    collection: coll.to_string(),
                })
            })
            .collect();
        Self {
            key_id: s.key_id,
            secret_hash: s.secret_hash,
            username: s.username,
            user_id: s.user_id,
            tenant_id: TenantId::new(s.tenant_id),
            expires_at: s.expires_at,
            is_revoked: s.is_revoked,
            created_at: s.created_at,
            scope,
        }
    }

    /// Check if the key is currently valid (not revoked, not expired).
    pub fn is_valid(&self) -> bool {
        if self.is_revoked {
            return false;
        }
        if self.expires_at > 0 {
            let now = now_unix_secs();
            if now >= self.expires_at {
                return false;
            }
        }
        true
    }
}

/// API key store with in-memory cache.
///
/// Persistence is handled externally by whoever owns the SystemCatalog
/// (typically CredentialStore or SharedState). Call `persist_to()` and
/// `load_from()` to sync with the catalog.
pub struct ApiKeyStore {
    /// key_id → ApiKeyRecord
    keys: RwLock<HashMap<String, ApiKeyRecord>>,
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiKeyStore {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }

    /// Load API keys from the catalog into the cache.
    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored_keys = catalog.load_all_api_keys()?;
        let mut keys = self.keys.write().map_err(|e| crate::Error::Internal {
            detail: format!("api key lock poisoned: {e}"),
        })?;
        for stored in stored_keys {
            let record = ApiKeyRecord::from_stored(stored);
            keys.insert(record.key_id.clone(), record);
        }
        let count = keys.len();
        if count > 0 {
            tracing::info!(count, "loaded API keys from system catalog");
        }
        Ok(())
    }

    /// Clear the in-memory key map and re-run `load_from`.
    /// Used by the catalog recovery sanity checker to repair
    /// a divergent registry.
    pub(crate) fn clear_and_reload(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        {
            let mut keys = self.keys.write().map_err(|e| crate::Error::Internal {
                detail: format!("api key lock poisoned during repair: {e}"),
            })?;
            keys.clear();
        }
        self.load_from(catalog)
    }

    /// Persist a single key record to the catalog.
    fn persist_to(&self, catalog: &SystemCatalog, record: &ApiKeyRecord) -> crate::Result<()> {
        catalog.put_api_key(&record.to_stored())
    }

    /// Create a new API key for a user. Returns the full key string (shown once).
    ///
    /// `scope`: if non-empty, restricts the key to specific (permission, collection) pairs.
    /// An empty scope means the key inherits all of the user's permissions.
    pub fn create_key(
        &self,
        username: &str,
        user_id: u64,
        tenant_id: TenantId,
        expires_secs: u64,
        scope: Vec<KeyScope>,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<String> {
        let key_id = generate_key_id();
        let secret = generate_secret();
        let secret_hash = hash_secret(&secret);

        let expires_at = if expires_secs > 0 {
            now_unix_secs() + expires_secs
        } else {
            0
        };

        let record = ApiKeyRecord {
            key_id: key_id.clone(),
            secret_hash,
            username: username.to_string(),
            user_id,
            tenant_id,
            expires_at,
            is_revoked: false,
            created_at: now_unix_secs(),
            scope,
        };

        if let Some(catalog) = catalog {
            self.persist_to(catalog, &record)?;
        }

        let mut keys = self.keys.write().map_err(|e| crate::Error::Internal {
            detail: format!("api key lock poisoned: {e}"),
        })?;
        keys.insert(key_id.clone(), record);

        Ok(format!("ndb_{key_id}_{secret}"))
    }

    /// Verify an API key string. Returns the record if valid.
    pub fn verify_key(&self, token: &str) -> Option<ApiKeyRecord> {
        let (key_id, secret) = parse_token(token)?;

        let keys = self.keys.read().ok()?;
        let record = keys.get(key_id)?;

        if !record.is_valid() {
            return None;
        }

        let provided_hash = hash_secret(secret);
        if !constant_time_eq(&record.secret_hash, &provided_hash) {
            return None;
        }

        Some(record.clone())
    }

    /// Build an AuthenticatedIdentity from a verified API key.
    /// Uses the credential store to get the user's current roles.
    pub fn to_identity(
        &self,
        record: &ApiKeyRecord,
        roles: Vec<Role>,
        is_superuser: bool,
    ) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: record.user_id,
            username: record.username.clone(),
            tenant_id: record.tenant_id,
            auth_method: AuthMethod::ApiKey,
            roles,
            is_superuser,
        }
    }

    // ── Cluster replication hooks ──────────────────────────────────
    //
    // Power the `CatalogEntry::PutApiKey` / `DeleteApiKey` pipeline.
    // The pgwire handler on the leader calls `prepare_key` to build
    // the full `StoredApiKey` + return the secret-bearing token in
    // one step, proposes the `StoredApiKey` through raft, and every
    // node's applier calls `install_replicated_key` to upsert the
    // cache and `catalog.put_api_key` to write the redb.
    //
    // The plaintext secret NEVER travels through raft — only the
    // `secret_hash`. The proposing node's client is the only one
    // that ever sees the token.

    /// Build a `StoredApiKey` ready for replication + return the
    /// plaintext token the client will see. Generates the key_id
    /// and secret, hashes the secret, but does NOT insert into the
    /// in-memory cache or write to redb — the applier does that
    /// on every node after the raft commit.
    pub fn prepare_key(
        &self,
        username: &str,
        user_id: u64,
        tenant_id: TenantId,
        expires_secs: u64,
        scope: Vec<KeyScope>,
    ) -> (StoredApiKey, String) {
        let key_id = generate_key_id();
        let secret = generate_secret();
        let secret_hash = hash_secret(&secret);
        let expires_at = if expires_secs > 0 {
            now_unix_secs() + expires_secs
        } else {
            0
        };
        let stored = StoredApiKey {
            key_id: key_id.clone(),
            secret_hash,
            username: username.to_string(),
            user_id,
            tenant_id: tenant_id.as_u32(),
            expires_at,
            is_revoked: false,
            created_at: now_unix_secs(),
            scope: scope
                .iter()
                .map(|s| format!("{}:{}", s.permission, s.collection))
                .collect(),
        };
        let token = format!("ndb_{key_id}_{secret}");
        (stored, token)
    }

    /// Install a replicated `StoredApiKey` into the in-memory cache.
    /// Called by the production `MetadataCommitApplier` post-apply
    /// hook after the applier has written the record to local redb.
    pub fn install_replicated_key(&self, stored: &StoredApiKey) {
        let record = ApiKeyRecord::from_stored(stored.clone());
        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        keys.insert(stored.key_id.clone(), record);
    }

    /// Mark a replicated API key as revoked in the in-memory cache.
    /// Symmetric partner to `install_replicated_key` for the
    /// `CatalogEntry::DeleteApiKey` variant. The redb record stays
    /// in place with `is_revoked = true` so audit trails survive.
    pub fn install_replicated_revoke(&self, key_id: &str) {
        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        if let Some(record) = keys.get_mut(key_id) {
            record.is_revoked = true;
        }
    }

    /// Look up a replicated key by id. Used by handler pre-checks
    /// before proposing a revoke.
    pub fn get_key(&self, key_id: &str) -> Option<ApiKeyRecord> {
        let keys = self.keys.read().unwrap_or_else(|p| p.into_inner());
        keys.get(key_id).cloned()
    }

    /// Revoke an API key by key_id.
    pub fn revoke_key(&self, key_id: &str, catalog: Option<&SystemCatalog>) -> crate::Result<bool> {
        let mut keys = self.keys.write().map_err(|e| crate::Error::Internal {
            detail: format!("api key lock poisoned: {e}"),
        })?;

        if let Some(record) = keys.get_mut(key_id) {
            record.is_revoked = true;
            if let Some(catalog) = catalog {
                self.persist_to(catalog, record)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all keys for a user (does not return secrets).
    pub fn list_keys_for_user(&self, username: &str) -> Vec<ApiKeyRecord> {
        let keys = match self.keys.read() {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };
        keys.values()
            .filter(|k| k.username == username)
            .cloned()
            .collect()
    }

    /// List all keys (admin view).
    pub fn list_all_keys(&self) -> Vec<ApiKeyRecord> {
        let keys = match self.keys.read() {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };
        keys.values().cloned().collect()
    }
}

// ── Key generation ──────────────────────────────────────────────────

const BASE62: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn generate_key_id() -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    base62_encode(&bytes)
}

fn generate_secret() -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    base62_encode(&bytes)
}

fn base62_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        result.push(BASE62[(b >> 2) as usize % 62] as char);
        result.push(BASE62[(b & 0x03) as usize * 16 % 62] as char);
    }
    result
}

/// SHA-256 hash of an API key secret for storage.
///
/// API key secrets are high-entropy random tokens, so a fast cryptographic
/// hash (without key stretching) is appropriate — unlike passwords which
/// require Argon2id.
fn hash_secret(secret: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().to_vec()
}

fn parse_token(token: &str) -> Option<(&str, &str)> {
    let stripped = token.strip_prefix("ndb_")?;
    let underscore_pos = stripped.find('_')?;
    if underscore_pos == 0 || underscore_pos == stripped.len() - 1 {
        return None;
    }
    let key_id = &stripped[..underscore_pos];
    let secret = &stripped[underscore_pos + 1..];
    Some((key_id, secret))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_verify_key() {
        let store = ApiKeyStore::new();
        let token = store
            .create_key("alice", 1, TenantId::new(1), 0, vec![], None)
            .unwrap();

        assert!(token.starts_with("ndb_"));
        assert_eq!(token.matches('_').count(), 2); // ndb_<id>_<secret>

        let record = store.verify_key(&token).unwrap();
        assert_eq!(record.username, "alice");
        assert_eq!(record.user_id, 1);
    }

    #[test]
    fn invalid_token_rejected() {
        let store = ApiKeyStore::new();
        store
            .create_key("alice", 1, TenantId::new(1), 0, vec![], None)
            .unwrap();

        assert!(store.verify_key("ndb_wrong_secret").is_none());
        assert!(store.verify_key("garbage").is_none());
        assert!(store.verify_key("").is_none());
    }

    #[test]
    fn revoked_key_rejected() {
        let store = ApiKeyStore::new();
        let token = store
            .create_key("alice", 1, TenantId::new(1), 0, vec![], None)
            .unwrap();

        let (key_id, _) = parse_token(&token).unwrap();
        store.revoke_key(key_id, None).unwrap();

        assert!(store.verify_key(&token).is_none());
    }

    #[test]
    fn expired_key_rejected() {
        let store = ApiKeyStore::new();
        // Create with 0-second TTL (already expired).
        let key_id = generate_key_id();
        let secret = generate_secret();
        let secret_hash = hash_secret(&secret);

        let record = ApiKeyRecord {
            key_id: key_id.clone(),
            secret_hash,
            username: "bob".into(),
            user_id: 2,
            tenant_id: TenantId::new(1),
            expires_at: 1, // Unix timestamp 1 = 1970, definitely expired.
            is_revoked: false,
            created_at: 1,
            scope: vec![],
        };

        store.keys.write().unwrap().insert(key_id.clone(), record);

        let token = format!("ndb_{key_id}_{secret}");
        assert!(store.verify_key(&token).is_none());
    }

    #[test]
    fn list_keys_for_user() {
        let store = ApiKeyStore::new();
        store
            .create_key("alice", 1, TenantId::new(1), 0, vec![], None)
            .unwrap();
        store
            .create_key("alice", 1, TenantId::new(1), 0, vec![], None)
            .unwrap();
        store
            .create_key("bob", 2, TenantId::new(1), 0, vec![], None)
            .unwrap();

        assert_eq!(store.list_keys_for_user("alice").len(), 2);
        assert_eq!(store.list_keys_for_user("bob").len(), 1);
    }

    #[test]
    fn parse_token_format() {
        let (key_id, secret) = parse_token("ndb_abc123_secretpart").unwrap();
        assert_eq!(key_id, "abc123");
        assert_eq!(secret, "secretpart");

        assert!(parse_token("not_valid").is_none());
        assert!(parse_token("ndb__empty").is_none());
        assert!(parse_token("ndb_only_").is_none());
    }
}
