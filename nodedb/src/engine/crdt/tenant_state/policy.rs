// SPDX-License-Identifier: BUSL-1.1

//! Collection conflict-resolution policy DDL.

use nodedb_crdt::policy::CollectionPolicy;

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Read the current conflict resolution policy for a collection.
    /// Returns a cloned `CollectionPolicy`; falls back to `CollectionPolicy::ephemeral()`
    /// if no explicit policy has been registered for this collection.
    pub fn get_collection_policy(&self, collection: &str) -> nodedb_crdt::policy::CollectionPolicy {
        self.validator.policies().get_owned(collection)
    }

    /// The policy registered for a collection, or `None` when it has none.
    pub fn registered_collection_policy(&self, collection: &str) -> Option<CollectionPolicy> {
        let policies = self.validator.policies();
        policies
            .has(collection)
            .then(|| policies.get_owned(collection))
    }

    /// Put back what [`Self::registered_collection_policy`] returned. `None`
    /// removes the collection's policy.
    pub fn restore_collection_policy(
        &mut self,
        collection: &str,
        previous: Option<CollectionPolicy>,
    ) {
        match previous {
            Some(policy) => self.validator.policies_mut().set(collection, policy),
            None => {
                self.validator.policies_mut().remove(collection);
            }
        }
    }

    /// Set conflict resolution policy for a collection from JSON.
    ///
    /// Called when the Data Plane receives a `SetCollectionPolicy` physical plan
    /// from the `ALTER COLLECTION ... SET ON CONFLICT ...` DDL.
    pub fn set_collection_policy(
        &mut self,
        collection: &str,
        policy_json: &str,
    ) -> crate::Result<()> {
        let policy: CollectionPolicy =
            sonic_rs::from_str(policy_json).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid collection policy JSON: {e}"),
            })?;
        validate_policy(&policy)?;
        self.validator.policies_mut().set(collection, policy);
        Ok(())
    }
}

/// Validate business rules on a collection policy before accepting it.
fn validate_policy(policy: &CollectionPolicy) -> crate::Result<()> {
    validate_conflict_policy(&policy.unique, "unique")?;
    validate_conflict_policy(&policy.foreign_key, "foreign_key")?;
    validate_conflict_policy(&policy.not_null, "not_null")?;
    validate_conflict_policy(&policy.check, "check")?;
    Ok(())
}

fn validate_conflict_policy(
    policy: &nodedb_crdt::policy::ConflictPolicy,
    field_name: &str,
) -> crate::Result<()> {
    use nodedb_crdt::policy::ConflictPolicy;
    match policy {
        ConflictPolicy::CascadeDefer {
            max_retries,
            ttl_secs,
        } => {
            if *max_retries == 0 {
                return Err(crate::Error::BadRequest {
                    detail: format!("{field_name}: max_retries must be > 0"),
                });
            }
            if *ttl_secs == 0 {
                return Err(crate::Error::BadRequest {
                    detail: format!("{field_name}: ttl_secs must be > 0"),
                });
            }
        }
        ConflictPolicy::Custom {
            webhook_url,
            timeout_secs,
        } => {
            if webhook_url.is_empty() {
                return Err(crate::Error::BadRequest {
                    detail: format!("{field_name}: webhook_url must not be empty"),
                });
            }
            if !webhook_url.starts_with("http://") && !webhook_url.starts_with("https://") {
                return Err(crate::Error::BadRequest {
                    detail: format!("{field_name}: webhook_url must be an HTTP(S) URL"),
                });
            }
            if *timeout_secs == 0 {
                return Err(crate::Error::BadRequest {
                    detail: format!("{field_name}: timeout_secs must be > 0"),
                });
            }
        }
        _ => {}
    }
    Ok(())
}
