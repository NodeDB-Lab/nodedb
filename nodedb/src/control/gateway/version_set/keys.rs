// SPDX-License-Identifier: BUSL-1.1

//! Synthetic pseudo-collection keys folding tenant permission-tree / RLS
//! policy versions into a [`super::GatewayVersionSet`].

/// Prefix for the synthetic entry carrying a tenant's permission-tree
/// version. `\0` makes it unrepresentable as a real collection name, so it
/// can never collide with one.
const PERMISSION_TREE_VERSION_KEY_PREFIX: &str = "\0__permission_tree_version::";

/// Prefix for the synthetic entry carrying a tenant's RLS policy version.
const RLS_VERSION_KEY_PREFIX: &str = "\0__rls_version::";

/// The synthetic pseudo-collection key a tenant's permission-tree version is
/// folded into the set under, so a revoked grant makes a cached gateway plan
/// unlookupable exactly like a bumped descriptor does.
pub fn permission_tree_version_key(tenant_id: u64) -> String {
    format!("{PERMISSION_TREE_VERSION_KEY_PREFIX}{tenant_id}")
}

/// The synthetic pseudo-collection key a tenant's RLS policy version is
/// folded into the set under.
pub fn rls_version_key(tenant_id: u64) -> String {
    format!("{RLS_VERSION_KEY_PREFIX}{tenant_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pseudo_keys_are_scoped_per_tenant() {
        assert_ne!(
            permission_tree_version_key(1),
            permission_tree_version_key(2)
        );
        assert_ne!(rls_version_key(1), rls_version_key(2));
        assert_ne!(permission_tree_version_key(1), rls_version_key(1));
    }
}
