// SPDX-License-Identifier: Apache-2.0

//! Stable array identifier — tenant- and database-scoped logical name.

use serde::{Deserialize, Serialize};

use nodedb_types::{DatabaseId, TenantId};

/// Logical handle to an array within a tenant and database. The tuple
/// `(tenant_id, database_id, name)` is the canonical storage/catalog key;
/// `name` remains the user-visible identifier.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayId {
    pub tenant_id: TenantId,
    /// Database scope.
    pub database_id: DatabaseId,
    pub name: String,
}

impl ArrayId {
    /// Construct an identifier in the default database.
    pub fn new(tenant_id: TenantId, name: impl Into<String>) -> Self {
        Self::in_database(tenant_id, DatabaseId::DEFAULT, name)
    }

    /// Construct an identifier in an explicit database scope.
    pub fn in_database(
        tenant_id: TenantId,
        database_id: DatabaseId,
        name: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            database_id,
            name: name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_id_round_trip_eq() {
        let a = ArrayId::new(TenantId::new(1), "genome");
        let b = ArrayId::new(TenantId::new(1), "genome");
        assert_eq!(a, b);
        assert_ne!(
            a,
            ArrayId::in_database(TenantId::new(1), DatabaseId::new(7), "genome")
        );
    }
}
