//! Stable array identifier — tenant-scoped logical name.

use serde::{Deserialize, Serialize};

use nodedb_types::TenantId;

/// Logical handle to an array within a tenant. The pair
/// `(tenant_id, name)` is the canonical key used by storage and the
/// catalog; `name` is the user-visible identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrayId {
    pub tenant_id: TenantId,
    pub name: String,
}

impl ArrayId {
    pub fn new(tenant_id: TenantId, name: impl Into<String>) -> Self {
        Self {
            tenant_id,
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
    }
}
