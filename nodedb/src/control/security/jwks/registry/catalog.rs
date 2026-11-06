// SPDX-License-Identifier: BUSL-1.1

//! Register and unregister catalog OIDC providers.
//!
//! Every node applies a `PutOidcProvider` / `DeleteOidcProvider` entry, and
//! boot registers every provider the catalog holds. A registered provider
//! joins the periodic refresh, and its keys are fetched once at
//! registration so its first token needs no on-demand fetch. A provider
//! that is dropped, or whose JWKS endpoint changed, loses its cached keys.

use std::sync::Arc;

use super::cache_identity::catalog_cache_identity;
use super::state::JwksRegistry;
use super::targets::RefreshTarget;
use crate::control::security::catalog::StoredOidcProvider;
use crate::control::security::jwks::fetch;

impl JwksRegistry {
    /// Register catalog provider `name` with its JWKS endpoint, replacing a
    /// previous registration of the same name. Returns the target whose
    /// keys the caller fetches with [`Self::prefetch`].
    pub fn register_catalog_provider(&self, name: &str, jwks_uri: &str) -> RefreshTarget {
        let target = RefreshTarget {
            cache_identity: catalog_cache_identity(name, jwks_uri),
            provider_name: name.to_string(),
            jwks_url: jwks_uri.to_string(),
        };
        let replaced = self.targets.write().put_catalog(target.clone());
        if let Some(old) = replaced
            && old.cache_identity != target.cache_identity
        {
            self.cache.remove_provider(&old.cache_identity);
        }
        target
    }

    /// Unregister catalog provider `name` and drop its cached keys.
    pub fn unregister_catalog_provider(&self, name: &str) {
        let removed = self.targets.write().remove_catalog(name);
        if let Some(old) = removed {
            self.cache.remove_provider(&old.cache_identity);
        }
    }

    /// Make the registered catalog providers exactly `providers`.
    ///
    /// Boot and a metadata-image reload both run this. Registration is
    /// synchronous. The key fetches run detached, so an unreachable provider
    /// holds up neither, and a token that arrives first fetches its keys
    /// itself. A provider the list no longer holds loses its cached keys.
    pub fn sync_catalog_providers(self: &Arc<Self>, providers: &[StoredOidcProvider]) {
        let stale: Vec<String> = self
            .targets
            .read()
            .catalog_names()
            .into_iter()
            .filter(|name| !providers.iter().any(|p| p.provider_name == *name))
            .collect();
        for name in &stale {
            self.unregister_catalog_provider(name);
        }
        for provider in providers {
            let target =
                self.register_catalog_provider(&provider.provider_name, &provider.jwks_uri);
            let registry = Arc::clone(self);
            tokio::spawn(async move {
                registry.prefetch(&target).await;
            });
        }
    }

    /// Fetch the keys of `target` into the cache. A failed fetch leaves the
    /// cache unchanged; the first token with an unknown `kid` fetches again.
    pub async fn prefetch(&self, target: &RefreshTarget) -> usize {
        fetch::fetch_and_cache(
            &target.cache_identity,
            &target.provider_name,
            &target.jwks_url,
            &self.cache,
            &self.policy,
        )
        .await
    }

    /// Whether catalog provider `name` is registered on this node.
    pub fn has_catalog_provider(&self, name: &str) -> bool {
        self.targets.read().has_catalog(name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::auth::JwtAuthConfig;
    use crate::control::security::egress::EgressPolicy;

    fn stored(name: &str, jwks_uri: &str) -> StoredOidcProvider {
        StoredOidcProvider {
            provider_name: name.to_string(),
            issuer: format!("https://{name}.example"),
            jwks_uri: jwks_uri.to_string(),
            audience: None,
            tenant_id: 1,
            claim_mapping: Vec::new(),
            created_at_lsn: 1,
        }
    }

    /// A metadata image replaces the catalog with no entry to apply, so a sync
    /// must add new providers and drop the ones the image no longer holds.
    #[tokio::test]
    async fn a_sync_holds_exactly_the_listed_providers() {
        let registry = Arc::new(
            JwksRegistry::init(JwtAuthConfig::default(), Arc::new(EgressPolicy::strict()))
                .await
                .expect("a registry with no static provider"),
        );
        let dropped = registry.register_catalog_provider("dropped", "https://d.example/jwks");
        registry
            .cache
            .update_provider(&dropped.cache_identity, Vec::new());

        registry.sync_catalog_providers(&[stored("kept", "https://k.example/jwks")]);

        assert!(registry.has_catalog_provider("kept"));
        assert!(!registry.has_catalog_provider("dropped"));
        assert!(!registry.cache.has_provider(&dropped.cache_identity));
    }

    #[tokio::test]
    async fn a_moved_or_dropped_provider_loses_its_cached_keys() {
        let registry =
            JwksRegistry::init(JwtAuthConfig::default(), Arc::new(EgressPolicy::strict()))
                .await
                .expect("a registry with no static provider");
        let first = registry.register_catalog_provider("idp", "https://a.example/jwks");
        registry
            .cache
            .update_provider(&first.cache_identity, Vec::new());
        assert!(registry.has_catalog_provider("idp"));

        // ALTER-style re-registration under a new endpoint.
        let moved = registry.register_catalog_provider("idp", "https://b.example/jwks");
        assert_ne!(moved.cache_identity, first.cache_identity);
        assert!(!registry.cache.has_provider(&first.cache_identity));
        registry
            .cache
            .update_provider(&moved.cache_identity, Vec::new());

        // The same endpoint keeps its keys.
        let same = registry.register_catalog_provider("idp", "https://b.example/jwks");
        assert!(registry.cache.has_provider(&same.cache_identity));

        registry.unregister_catalog_provider("idp");
        assert!(!registry.has_catalog_provider("idp"));
        assert!(!registry.cache.has_provider(&moved.cache_identity));
    }
}
