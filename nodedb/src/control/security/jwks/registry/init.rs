// SPDX-License-Identifier: BUSL-1.1

//! Registry construction: fetch JWKS from every configured provider on
//! startup (falling back to the disk cache), then spawn the periodic
//! refresh task.

use std::sync::Arc;

use crate::config::auth::JwtAuthConfig;
use crate::control::security::egress::EgressPolicy;
use crate::control::security::jwks::cache::JwksCache;
use crate::control::security::jwks::fetch;

use super::cache_identity::static_cache_identity;
use super::state::JwksRegistry;
use super::targets::{RefreshTarget, RefreshTargets};

impl JwksRegistry {
    /// Create and initialize the registry.
    ///
    /// Fetches JWKS from all providers on startup, loads disk cache as fallback,
    /// and spawns the periodic refresh task. Every fetch goes through
    /// `policy`, the server's `[egress]` policy.
    pub async fn init(config: JwtAuthConfig, policy: Arc<EgressPolicy>) -> crate::Result<Self> {
        // Registry construction is also a public entry point, so it must not
        // rely on the server-config loader to reject unsafe static providers.
        // Validate before creating cache state, fetching remote keys, or
        // spawning a refresh task.
        config.validate(&policy)?;
        let cache = Arc::new(JwksCache::new(config.jwks_cache_path.clone()));

        // Load disk cache first (offline fallback).
        cache.load_from_disk();

        // Fetch from all providers (best-effort — failures use disk cache).
        for provider in &config.providers {
            let cache_identity = static_cache_identity(&provider.name);
            fetch::fetch_and_cache(
                &cache_identity,
                &provider.name,
                &provider.jwks_url,
                &cache,
                &policy,
            )
            .await;
        }

        // Spawn periodic refresh. It runs with no static provider too: a
        // catalog provider registered later joins its targets.
        let statics = config
            .providers
            .iter()
            .map(|p| RefreshTarget {
                cache_identity: static_cache_identity(&p.name),
                provider_name: p.name.clone(),
                jwks_url: p.jwks_url.clone(),
            })
            .collect();
        let targets = Arc::new(parking_lot::RwLock::new(RefreshTargets::with_statics(
            statics,
        )));
        let refresh_handle = fetch::spawn_refresh_task(
            Arc::clone(&targets),
            cache.clone(),
            config.jwks_refresh_secs,
            policy.clone(),
        );

        Ok(Self {
            providers: config.providers.clone(),
            cache,
            config,
            policy,
            targets,
            _refresh_handle: Some(refresh_handle),
        })
    }
}
