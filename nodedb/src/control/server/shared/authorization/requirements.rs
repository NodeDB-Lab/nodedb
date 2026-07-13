//! Physical-plan authorization requirements.
//!
//! This deliberately does not use `shared::plan_util::extract_collection`: a
//! single collection cannot represent joins or source/target DML correctly.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::control::security::identity::Permission;

mod collect;
mod order;
mod query;

pub use collect::plan_requirements;

/// A protected resource and the permission needed to use it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AuthorizationRequirement {
    /// A collection-scoped operation. The name is the physical-plan name and
    /// may be database-qualified; the authorization service normalizes it to
    /// the grant-store name before looking up grants.
    Collection {
        collection: String,
        permission: Permission,
    },
    /// An operation with no collection-level resource, such as an array or a
    /// tenant-wide maintenance action. It must still be authorized at tenant
    /// scope rather than silently allowed.
    Tenant { permission: Permission },
}

impl AuthorizationRequirement {
    fn collection(collection: impl Into<String>, permission: Permission) -> Self {
        Self::Collection {
            collection: collection.into(),
            permission,
        }
    }

    fn tenant(permission: Permission) -> Self {
        Self::Tenant { permission }
    }
}

#[cfg(test)]
#[path = "requirements/tests.rs"]
mod tests;
