// SPDX-License-Identifier: BUSL-1.1

//! Wire the JWKS registry into `SharedState`.
//!
//! The registry is built on every server: static `[auth.jwt]` providers and
//! catalog OIDC providers both verify through it, and a catalog provider can
//! be created at runtime on a server that has no static provider. Every
//! catalog provider the catalog holds at boot is registered here; one created,
//! altered or dropped later registers through its catalog entry's post-apply.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::control::security::jwks::registry::JwksRegistry;
use crate::control::state::SharedState;

/// Build the registry, install it, and register the catalog's providers.
pub async fn wire_jwks_registry(
    shared: &mut Arc<SharedState>,
    config: &ServerConfig,
) -> crate::Result<()> {
    let jwt_config = config.auth.jwt.clone().unwrap_or_default();
    let static_count = jwt_config.providers.len();
    let state = Arc::get_mut(shared).ok_or_else(|| crate::Error::Internal {
        detail: "JWKS wiring: shared state is already shared before the registry was installed"
            .into(),
    })?;
    let registry =
        Arc::new(JwksRegistry::init(jwt_config, Arc::clone(state.remote_sources.egress())).await?);
    let catalog_providers = state.credentials.catalog().list_oidc_providers()?;
    // The key fetches run detached, so an unreachable provider never holds
    // up boot. A token that arrives first fetches its keys itself.
    for provider in &catalog_providers {
        let target =
            registry.register_catalog_provider(&provider.provider_name, &provider.jwks_uri);
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            registry.prefetch(&target).await;
        });
    }
    state.jwks_registry = Some(registry);
    info!(
        static_providers = static_count,
        catalog_providers = catalog_providers.len(),
        "JWKS registry initialised"
    );
    Ok(())
}
