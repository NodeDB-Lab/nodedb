// SPDX-License-Identifier: BUSL-1.1

//! The JWKS endpoints the periodic refresh task fetches: every static
//! provider of `[auth.jwt]`, and every catalog OIDC provider registered on
//! this node.

use std::collections::HashMap;

/// One JWKS endpoint and the cache identity its keys live under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshTarget {
    pub cache_identity: String,
    pub provider_name: String,
    pub jwks_url: String,
}

/// The endpoints the refresh task fetches each tick.
#[derive(Debug, Default)]
pub struct RefreshTargets {
    statics: Vec<RefreshTarget>,
    /// Catalog providers by name.
    catalog: HashMap<String, RefreshTarget>,
}

impl RefreshTargets {
    /// The static providers of `[auth.jwt]`.
    pub fn with_statics(statics: Vec<RefreshTarget>) -> Self {
        Self {
            statics,
            catalog: HashMap::new(),
        }
    }

    /// Register catalog provider `target`. Returns the target it replaced,
    /// whose cached keys the caller drops when its identity differs.
    pub fn put_catalog(&mut self, target: RefreshTarget) -> Option<RefreshTarget> {
        self.catalog.insert(target.provider_name.clone(), target)
    }

    /// Unregister catalog provider `name`. Returns its target.
    pub fn remove_catalog(&mut self, name: &str) -> Option<RefreshTarget> {
        self.catalog.remove(name)
    }

    /// Whether catalog provider `name` is registered.
    pub fn has_catalog(&self, name: &str) -> bool {
        self.catalog.contains_key(name)
    }

    /// Every endpoint to fetch, static providers first.
    pub fn snapshot(&self) -> Vec<RefreshTarget> {
        self.statics
            .iter()
            .chain(self.catalog.values())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(identity: &str, name: &str) -> RefreshTarget {
        RefreshTarget {
            cache_identity: identity.into(),
            provider_name: name.into(),
            jwks_url: format!("https://{name}.example/jwks.json"),
        }
    }

    #[test]
    fn catalog_providers_register_replace_and_unregister() {
        let mut targets = RefreshTargets::with_statics(vec![target("static:a", "a")]);
        assert_eq!(targets.put_catalog(target("catalog:b1", "b")), None);
        let replaced = targets.put_catalog(target("catalog:b2", "b"));
        assert_eq!(
            replaced.map(|t| t.cache_identity),
            Some("catalog:b1".into())
        );
        let names: Vec<String> = targets
            .snapshot()
            .into_iter()
            .map(|t| t.cache_identity)
            .collect();
        assert_eq!(
            names,
            vec!["static:a".to_string(), "catalog:b2".to_string()]
        );
        assert!(targets.remove_catalog("b").is_some());
        assert_eq!(targets.snapshot().len(), 1);
    }
}
