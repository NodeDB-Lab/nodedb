// SPDX-License-Identifier: Apache-2.0

//! Tenant identifier.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a tenant. All data is tenant-scoped by construction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct TenantId(u64);

impl TenantId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for TenantId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_display() {
        let t = TenantId::new(42);
        assert_eq!(t.to_string(), "tenant:42");
        assert_eq!(t.as_u64(), 42);
    }

    #[test]
    fn tenant_id_above_u32_max_roundtrip() {
        let large = u32::MAX as u64 + 1;
        let t = TenantId::new(large);
        assert_eq!(t.as_u64(), large);
        assert_eq!(t.to_string(), format!("tenant:{large}"));
    }

    #[test]
    fn tenant_id_from_u64() {
        let t: TenantId = TenantId::from(4_294_967_296u64);
        assert_eq!(t.as_u64(), 4_294_967_296u64);
    }

    #[test]
    fn serde_roundtrip() {
        let tid = TenantId::new(7);
        let json = sonic_rs::to_string(&tid).unwrap();
        let decoded: TenantId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(tid, decoded);
    }

    #[test]
    fn serde_roundtrip_above_u32_max() {
        let tid = TenantId::new(u32::MAX as u64 + 1);
        let json = sonic_rs::to_string(&tid).unwrap();
        let decoded: TenantId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(tid, decoded);
    }
}
