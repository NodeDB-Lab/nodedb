// SPDX-License-Identifier: BUSL-1.1

//! Collision-free cache identities for static and catalog JWKS providers.

/// Build a static cache identity disjoint from the catalog domain.
pub(super) fn static_cache_identity(provider_name: &str) -> String {
    format!("static:{}:{provider_name}", provider_name.len())
}

/// Build a catalog identity bound to its endpoint and disjoint from static identities.
pub(super) fn catalog_cache_identity(provider_name: &str, jwks_uri: &str) -> String {
    format!(
        "catalog:{}:{provider_name}:{}:{jwks_uri}",
        provider_name.len(),
        jwks_uri.len()
    )
}
