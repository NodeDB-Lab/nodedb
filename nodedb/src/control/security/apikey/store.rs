// SPDX-License-Identifier: BUSL-1.1

//! `ApiKeyStore` — in-memory cache, catalog persistence, raft replication hooks.
//!
//! Persistence is handled externally by whoever owns the `SystemCatalog`
//! (typically `CredentialStore` or `SharedState`). Call `persist_to()` and
//! `load_from()` to sync with the catalog.
//!
//! Cluster replication: leader handlers call `prepare_key` to mint the
//! `StoredApiKey` and the plaintext token in one step, propose the
//! `StoredApiKey` through raft (the secret hash, never the secret itself),
//! and every node's applier calls `install_replicated_key` to upsert the
//! cache plus `catalog.put_api_key` for redb durability.

use parking_lot::RwLock;
use std::collections::HashMap;

use crate::control::security::catalog::{StoredApiKey, SystemCatalog};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};

use super::record::{ApiKeyRecord, CreateKeyParams};
use super::token::{
    constant_time_eq, generate_key_id, generate_secret, hash_secret, now_unix_secs, parse_token,
};

/// API key store with in-memory cache.
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
        let mut keys = self.keys.write();
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
            let mut keys = self.keys.write();
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
    ///
    /// `accessible_databases`: if non-empty, restricts this key to those databases (must be
    /// a subset of the owner's database set). Empty means inherit from the owner at bind time.
    pub fn create_key(
        &self,
        params: CreateKeyParams<'_>,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<String> {
        let CreateKeyParams {
            username,
            user_id,
            tenant_id,
            expires_secs,
            scope,
            accessible_databases,
        } = params;
        let key_id = generate_key_id()?;
        let secret = generate_secret()?;
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
            accessible_databases,
        };

        if let Some(catalog) = catalog {
            self.persist_to(catalog, &record)?;
        }

        let mut keys = self.keys.write();
        keys.insert(key_id.clone(), record);

        Ok(format!("ndb_{key_id}.{secret}"))
    }

    /// Verify an API key string. Returns the record if valid.
    pub fn verify_key(&self, token: &str) -> Option<ApiKeyRecord> {
        let (key_id, secret) = parse_token(token)?;

        let keys = self.keys.read();
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
    /// The caller must compute and pass the effective `accessible_databases`
    /// (owner_set ∩ key_set) rather than letting this method guess.
    pub(crate) fn to_identity(
        &self,
        record: &ApiKeyRecord,
        roles: Vec<Role>,
        is_superuser: bool,
        accessible_databases: DatabaseSet,
    ) -> AuthenticatedIdentity {
        AuthenticatedIdentity::from_catalog_principal(
            crate::control::security::identity::CatalogPrincipal {
                user_id: record.user_id,
                username: record.username.clone(),
                tenant_id: record.tenant_id,
                auth_method: AuthMethod::ApiKey,
                roles,
                is_superuser,
                default_database: None,
                accessible_databases,
            },
        )
        .with_api_key_id(record.key_id.clone())
    }

    /// Build a `StoredApiKey` ready for replication + return the
    /// plaintext token the client will see. Generates the key_id
    /// and secret, hashes the secret, but does NOT insert into the
    /// in-memory cache or write to redb — the applier does that
    /// on every node after the raft commit.
    ///
    /// `accessible_databases`: subset of the owner's database set (validated by caller).
    /// Empty vec means "inherit from owner at bind time".
    pub fn prepare_key(
        &self,
        params: CreateKeyParams<'_>,
    ) -> crate::Result<(StoredApiKey, String)> {
        let CreateKeyParams {
            username,
            user_id,
            tenant_id,
            expires_secs,
            scope,
            accessible_databases,
        } = params;
        let key_id = generate_key_id()?;
        let secret = generate_secret()?;
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
            tenant_id: tenant_id.as_u64(),
            expires_at,
            is_revoked: false,
            created_at: now_unix_secs(),
            scope: scope
                .iter()
                .map(|s| format!("{}:{}", s.permission, s.collection))
                .collect(),
            accessible_databases: accessible_databases.iter().map(|id| id.as_u64()).collect(),
        };
        let token = format!("ndb_{key_id}.{secret}");
        Ok((stored, token))
    }

    /// Install a replicated `StoredApiKey` into the in-memory cache.
    /// Called by the production `MetadataCommitApplier` post-apply
    /// hook after the applier has written the record to local redb.
    pub fn install_replicated_key(&self, stored: &StoredApiKey) {
        let record = ApiKeyRecord::from_stored(stored.clone());
        let mut keys = self.keys.write();
        keys.insert(stored.key_id.clone(), record);
    }

    /// Mark a replicated API key as revoked in the in-memory cache.
    /// Symmetric partner to `install_replicated_key` for the
    /// `CatalogEntry::DeleteApiKey` variant. The redb record stays
    /// in place with `is_revoked = true` so audit trails survive.
    pub fn install_replicated_revoke(&self, key_id: &str) {
        let mut keys = self.keys.write();
        if let Some(record) = keys.get_mut(key_id) {
            record.is_revoked = true;
        }
    }

    /// Look up a replicated key by id. Used by handler pre-checks
    /// before proposing a revoke.
    pub fn get_key(&self, key_id: &str) -> Option<ApiKeyRecord> {
        let keys = self.keys.read();
        keys.get(key_id).cloned()
    }

    /// Whether the key exists and is neither revoked nor expired.
    /// Reads the record in place, without a clone.
    pub fn is_key_valid(&self, key_id: &str) -> bool {
        self.keys.read().get(key_id).is_some_and(|k| k.is_valid())
    }

    /// Revoke an API key by key_id.
    pub fn revoke_key(&self, key_id: &str, catalog: Option<&SystemCatalog>) -> crate::Result<bool> {
        let mut keys = self.keys.write();

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
        let keys = self.keys.read();
        keys.values()
            .filter(|k| k.username == username)
            .cloned()
            .collect()
    }

    /// List all keys (admin view).
    pub fn list_all_keys(&self) -> Vec<ApiKeyRecord> {
        let keys = self.keys.read();
        keys.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::KeyScope;
    use super::super::token::{generate_key_id, generate_secret, hash_secret, parse_token};
    use super::*;
    use crate::types::TenantId;

    fn empty_params(username: &str, user_id: u64) -> CreateKeyParams<'_> {
        CreateKeyParams {
            username,
            user_id,
            tenant_id: TenantId::new(1),
            expires_secs: 0,
            scope: vec![],
            accessible_databases: vec![],
        }
    }

    #[test]
    fn create_and_verify_key() {
        let store = ApiKeyStore::new();
        let token = store.create_key(empty_params("alice", 1), None).unwrap();

        // Format: ndb_<11 chars>.<43 chars> = 59 chars total.
        assert!(token.starts_with("ndb_"));
        assert_eq!(token.len(), 59);
        assert_eq!(token.chars().filter(|&c| c == '.').count(), 1);

        let record = store.verify_key(&token).unwrap();
        assert_eq!(record.username, "alice");
        assert_eq!(record.user_id, 1);
    }

    #[test]
    fn invalid_token_rejected() {
        let store = ApiKeyStore::new();
        store.create_key(empty_params("alice", 1), None).unwrap();

        // Underscore-separated (no dot) rejected.
        assert!(store.verify_key("ndb_wrongid_wrongsecret").is_none());
        // Garbage.
        assert!(store.verify_key("garbage").is_none());
        assert!(store.verify_key("").is_none());
        // Correct prefix but invalid base64url halves.
        assert!(store.verify_key("ndb_!!!.???").is_none());
        // Missing dot separator.
        assert!(store.verify_key("ndb_nodothere").is_none());
        // Wrong prefix.
        assert!(store.verify_key("nodedb_abc.def").is_none());
    }

    #[test]
    fn revoked_key_rejected() {
        let store = ApiKeyStore::new();
        let token = store.create_key(empty_params("alice", 1), None).unwrap();

        let (key_id, _) = parse_token(&token).unwrap();
        store.revoke_key(key_id, None).unwrap();

        assert!(store.verify_key(&token).is_none());
    }

    #[test]
    fn key_cache_remains_usable_after_panic_while_write_locked() {
        let store = ApiKeyStore::new();
        let token = store
            .create_key(empty_params("panic-safe", 9), None)
            .expect("create key");
        let (key_id, _) = parse_token(&token).expect("parse generated token");

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.keys.write();
            panic!("simulated interrupted API key cache update");
        }));
        assert!(outcome.is_err());
        assert!(store.verify_key(&token).is_some());
        assert!(store.revoke_key(key_id, None).expect("revoke key"));
        assert!(store.verify_key(&token).is_none());
    }

    #[test]
    fn expired_key_rejected() {
        let store = ApiKeyStore::new();
        let key_id = generate_key_id().expect("key id");
        let secret = generate_secret().expect("secret");
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
            accessible_databases: vec![],
        };

        store.keys.write().insert(key_id.clone(), record);

        let token = format!("ndb_{key_id}.{secret}");
        assert!(store.verify_key(&token).is_none());
    }

    #[test]
    fn list_keys_for_user_filters_by_owner() {
        let store = ApiKeyStore::new();
        store.create_key(empty_params("alice", 1), None).unwrap();
        store.create_key(empty_params("alice", 1), None).unwrap();
        store.create_key(empty_params("bob", 2), None).unwrap();

        assert_eq!(store.list_keys_for_user("alice").len(), 2);
        assert_eq!(store.list_keys_for_user("bob").len(), 1);
    }

    #[test]
    fn create_key_with_scope_records_it() {
        let store = ApiKeyStore::new();
        let scope = vec![KeyScope {
            permission: "read".into(),
            collection: "users".into(),
        }];
        let token = store
            .create_key(
                CreateKeyParams {
                    username: "alice",
                    user_id: 1,
                    tenant_id: TenantId::new(1),
                    expires_secs: 0,
                    scope: scope.clone(),
                    accessible_databases: vec![],
                },
                None,
            )
            .unwrap();
        let record = store.verify_key(&token).unwrap();
        assert_eq!(record.scope, scope);
    }
}
