// SPDX-License-Identifier: BUSL-1.1

//! JWKS key cache: in-memory by `kid` with optional disk persistence.
//!
//! Keys are stored per-provider. Cache supports:
//! - O(1) lookup by `kid`
//! - Bulk update on JWKS refresh (atomic swap)
//! - Disk persistence for offline fallback

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use parking_lot::RwLock;

use super::key::VerificationKey;

/// Per-provider cached key set.
struct ProviderKeys {
    /// kid → VerificationKey.
    keys: HashMap<String, VerificationKey>,
    /// When this keyset was last fetched.
    fetched_at: Instant,
    /// When we last attempted a re-fetch (for rate limiting).
    last_refetch_attempt: Instant,
}

/// Thread-safe JWKS key cache.
pub struct JwksCache {
    /// provider_name → cached keys.
    providers: RwLock<HashMap<String, ProviderKeys>>,
    /// Optional disk cache path for offline fallback.
    disk_path: Option<String>,
}

impl JwksCache {
    pub fn new(disk_path: Option<String>) -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            disk_path,
        }
    }

    /// Look up a verification key by provider name and `kid`.
    pub fn get(&self, provider: &str, kid: &str) -> Option<VerificationKey> {
        let providers = self.providers.read();
        providers
            .get(provider)
            .and_then(|pk| pk.keys.get(kid).cloned())
    }

    /// Check if a provider has any cached keys.
    pub fn has_provider(&self, provider: &str) -> bool {
        let providers = self.providers.read();
        providers.contains_key(provider)
    }

    /// Update all keys for a provider (atomic swap).
    pub fn update_provider(&self, provider: &str, keys: Vec<VerificationKey>) {
        let now = Instant::now();
        let key_map: HashMap<String, VerificationKey> =
            keys.into_iter().map(|k| (k.kid.clone(), k)).collect();

        let mut providers = self.providers.write();
        providers.insert(
            provider.to_string(),
            ProviderKeys {
                keys: key_map,
                fetched_at: now,
                last_refetch_attempt: now,
            },
        );

        // Persist to disk if configured.
        if let Some(ref path) = self.disk_path {
            let _ = self.persist_to_disk(path, &providers);
        }
    }

    /// Drop every cached key of `provider`: a catalog provider that is
    /// dropped, or whose JWKS endpoint changed, keeps no key.
    pub fn remove_provider(&self, provider: &str) {
        let mut providers = self.providers.write();
        if providers.remove(provider).is_some()
            && let Some(ref path) = self.disk_path
        {
            let _ = self.persist_to_disk(path, &providers);
        }
    }

    /// Check if enough time has passed since last re-fetch attempt.
    /// Used to rate-limit on-demand re-fetches for unknown `kid`.
    pub fn can_refetch(&self, provider: &str, min_interval_secs: u64) -> bool {
        let providers = self.providers.read();
        match providers.get(provider) {
            Some(pk) => pk.last_refetch_attempt.elapsed().as_secs() >= min_interval_secs,
            None => true, // No cache yet — allow fetch.
        }
    }

    /// Record a re-fetch attempt (even if it fails).
    pub fn mark_refetch_attempted(&self, provider: &str) {
        let mut providers = self.providers.write();
        if let Some(pk) = providers.get_mut(provider) {
            pk.last_refetch_attempt = Instant::now();
        }
    }

    /// Check if cached keys are stale (older than refresh interval).
    pub fn is_stale(&self, provider: &str, max_age_secs: u64) -> bool {
        let providers = self.providers.read();
        match providers.get(provider) {
            Some(pk) => pk.fetched_at.elapsed().as_secs() >= max_age_secs,
            None => true,
        }
    }

    /// Load cached keys from disk (offline fallback on startup).
    pub fn load_from_disk(&self) {
        let Some(ref path) = self.disk_path else {
            return;
        };

        let disk_path = Path::new(path);
        if !disk_path.exists() {
            return;
        }

        let data = match std::fs::read_to_string(disk_path) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "failed to read JWKS disk cache");
                return;
            }
        };

        let disk_cache: HashMap<String, Vec<DiskKeyEntry>> = match sonic_rs::from_str(&data) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "failed to parse JWKS disk cache");
                return;
            }
        };

        let now = Instant::now();
        let mut providers = self.providers.write();

        for (provider_name, entries) in disk_cache {
            let key_map: HashMap<String, VerificationKey> = entries
                .into_iter()
                .filter_map(|e| e.to_verification_key())
                .map(|k| (k.kid.clone(), k))
                .collect();

            if !key_map.is_empty() {
                tracing::info!(
                    provider = %provider_name,
                    keys = key_map.len(),
                    "loaded JWKS keys from disk cache"
                );
                providers.insert(
                    provider_name,
                    ProviderKeys {
                        keys: key_map,
                        fetched_at: now,
                        last_refetch_attempt: now,
                    },
                );
            }
        }
    }

    /// Persist current cache to disk.
    fn persist_to_disk(
        &self,
        path: &str,
        providers: &HashMap<String, ProviderKeys>,
    ) -> Result<(), std::io::Error> {
        let disk_cache: HashMap<String, Vec<DiskKeyEntry>> = providers
            .iter()
            .map(|(name, pk)| {
                let entries: Vec<DiskKeyEntry> = pk
                    .keys
                    .values()
                    .map(DiskKeyEntry::from_verification_key)
                    .collect();
                (name.clone(), entries)
            })
            .collect();

        let json = sonic_rs::to_string_pretty(&disk_cache).map_err(std::io::Error::other)?;

        // `jwks_cache_path` is one operator-supplied file path, so this is the
        // boundary where it splits into the (directory, name) pair the durable
        // writer takes. A path with no directory part or no final name is a
        // config error and is reported as one.
        let disk_path = Path::new(path);
        let (dir, name) = match (
            disk_path.parent(),
            disk_path.file_name().and_then(|n| n.to_str()),
        ) {
            (Some(dir), Some(name)) => (dir, name),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("jwks_cache_path is not a file path: {path}"),
                ));
            }
        };
        std::fs::create_dir_all(dir)?;
        nodedb_wal::segment::atomic_write_fsync(dir, name, json.as_bytes())
            .map_err(std::io::Error::other)?;
        Ok(())
    }
}

/// Serializable key entry for disk persistence.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DiskKeyEntry {
    kid: String,
    algorithm: String,
    key_type: String,
    /// Base64-encoded key material.
    key_data: String,
}

impl DiskKeyEntry {
    fn from_verification_key(key: &VerificationKey) -> Self {
        use base64::Engine;
        let (key_type, key_data) = match &key.key_type {
            super::key::KeyType::Rsa(der) => (
                "rsa".into(),
                base64::engine::general_purpose::STANDARD.encode(der),
            ),
            super::key::KeyType::EcP256(point) => (
                "ec_p256".into(),
                base64::engine::general_purpose::STANDARD.encode(point),
            ),
            super::key::KeyType::EcP384(point) => (
                "ec_p384".into(),
                base64::engine::general_purpose::STANDARD.encode(point),
            ),
        };
        Self {
            kid: key.kid.clone(),
            algorithm: key.algorithm.clone(),
            key_type,
            key_data,
        }
    }

    fn to_verification_key(&self) -> Option<VerificationKey> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.key_data)
            .ok()?;
        let key_type = match self.key_type.as_str() {
            "rsa" => super::key::KeyType::Rsa(bytes),
            "ec_p256" => super::key::KeyType::EcP256(bytes),
            "ec_p384" => super::key::KeyType::EcP384(bytes),
            _ => return None,
        };
        Some(VerificationKey {
            kid: self.kid.clone(),
            algorithm: self.algorithm.clone(),
            key_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::key::{KeyType, VerificationKey};
    use super::*;

    fn test_key(kid: &str) -> VerificationKey {
        VerificationKey {
            kid: kid.into(),
            algorithm: "RS256".into(),
            key_type: KeyType::Rsa(vec![0x30, 0x82, 0x01]),
        }
    }

    #[test]
    fn lookup_survives_panic_while_provider_cache_is_write_locked() {
        let cache = JwksCache::new(None);
        cache.update_provider("provider", vec![test_key("kid")]);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _providers = cache.providers.write();
            panic!("simulated interrupted JWKS refresh");
        }));
        assert!(result.is_err());
        assert!(cache.get("provider", "kid").is_some());
    }

    #[test]
    fn cache_get_and_update() {
        let cache = JwksCache::new(None);
        assert!(cache.get("provider1", "key1").is_none());

        cache.update_provider("provider1", vec![test_key("key1"), test_key("key2")]);

        assert!(cache.get("provider1", "key1").is_some());
        assert!(cache.get("provider1", "key2").is_some());
        assert!(cache.get("provider1", "key3").is_none());
        assert!(cache.get("other", "key1").is_none());
    }

    #[test]
    fn refetch_rate_limiting() {
        let cache = JwksCache::new(None);
        cache.update_provider("p1", vec![test_key("k1")]);

        // Just updated — can't refetch within 60s.
        assert!(!cache.can_refetch("p1", 60));

        // Can refetch with 0s interval.
        assert!(cache.can_refetch("p1", 0));

        // Unknown provider — always allowed.
        assert!(cache.can_refetch("unknown", 60));
    }

    #[test]
    fn staleness_check() {
        let cache = JwksCache::new(None);
        assert!(cache.is_stale("p1", 3600)); // No cache = stale.

        cache.update_provider("p1", vec![test_key("k1")]);
        assert!(!cache.is_stale("p1", 3600)); // Just updated = fresh.
        assert!(cache.is_stale("p1", 0)); // 0s max age = always stale.
    }

    #[test]
    fn disk_roundtrip() {
        let key = test_key("disk-key-1");
        let disk = DiskKeyEntry::from_verification_key(&key);
        let restored = disk.to_verification_key().unwrap();
        assert_eq!(restored.kid, "disk-key-1");
        assert_eq!(restored.algorithm, "RS256");
    }
}
