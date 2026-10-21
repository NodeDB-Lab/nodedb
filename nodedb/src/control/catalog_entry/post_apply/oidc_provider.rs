// SPDX-License-Identifier: BUSL-1.1

//! Post-apply for `PutOidcProvider` / `DeleteOidcProvider`: register the
//! provider with this node's JWKS registry, or unregister it.
//!
//! Registration is synchronous, so a token verified after the applied index
//! advances finds the provider registered. The key fetch runs detached: it
//! is network I/O, and the first token with an unknown `kid` fetches the
//! keys itself when the detached fetch has not landed.

use std::sync::Arc;

use crate::control::security::catalog::StoredOidcProvider;
use crate::control::state::SharedState;

/// Post-apply for `PutOidcProvider`.
pub fn put(provider: &StoredOidcProvider, shared: &Arc<SharedState>) {
    let Some(registry) = shared.jwks_registry.as_ref() else {
        return;
    };
    let target = registry.register_catalog_provider(&provider.provider_name, &provider.jwks_uri);
    let registry = Arc::clone(registry);
    tokio::spawn(async move {
        registry.prefetch(&target).await;
    });
}

/// Post-apply for `DeleteOidcProvider`.
pub fn delete(name: &str, shared: &Arc<SharedState>) {
    if let Some(registry) = shared.jwks_registry.as_ref() {
        registry.unregister_catalog_provider(name);
    }
}
