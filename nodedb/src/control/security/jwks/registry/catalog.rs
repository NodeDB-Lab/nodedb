// SPDX-License-Identifier: BUSL-1.1

//! Register and unregister catalog OIDC providers.
//!
//! Every node applies a `PutOidcProvider` / `DeleteOidcProvider` entry, and
//! boot registers every provider the catalog holds. A registered provider
//! joins the periodic refresh, and its keys are fetched once at
//! registration so its first token needs no on-demand fetch. A provider
//! that is dropped, or whose JWKS endpoint changed, loses its cached keys.

use super::cache_identity::catalog_cache_identity;
use super::state::JwksRegistry;
use super::targets::RefreshTarget;
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
