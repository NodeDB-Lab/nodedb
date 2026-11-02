// SPDX-License-Identifier: Apache-2.0

//! Database identifier.
//!
//! A database is a top-level catalog namespace, one step above tenant.
//! `DatabaseId(0)` is permanently reserved for the built-in `default`
//! database. `DatabaseId(1..=1023)` is reserved for future system
//! databases; none are assigned in v1. User-created databases start
//! at `DatabaseId(1024)` and are allocated by `DatabaseRegistry`.
//!
//! The zero-value reservation means WAL records with `reserved = 0`
//! (written before this field existed) decode cleanly as
//! `DatabaseId::DEFAULT` without any format-version bump.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a database within a NodeDB instance.
///
/// Every collection lives in exactly one database. Databases are catalog
/// namespaces — the same collection name may appear in two different databases
/// without conflict. Resolution order at session bind time:
///
/// 1. Explicit database name from the connection string or pgwire startup
///    message (`database` parameter — set by `psql -d <name>`).
/// 2. Per-user default database (`ALTER USER <name> SET DEFAULT DATABASE <db>`).
/// 3. `DatabaseId::DEFAULT` — the built-in `default` database.
///
/// `DatabaseId(0)` is permanently reserved for the built-in `default` database
/// and cannot be dropped. User-created databases are allocated by
/// `DatabaseRegistry` starting at id `1024`.
///
/// # Access control
///
/// Database-level privileges are granted with:
/// - `GRANT ALL ON DATABASE <name> TO <user>` — full access
/// - `GRANT CREATE COLLECTION ON DATABASE <name> TO <user>` — DDL only
/// - `GRANT SELECT ON DATABASE <name> TO <user>` — read-only
///
/// Session bind rejects connections to databases not in the user's
/// `accessible_databases` set with `ACCESS_DENIED` (SQLSTATE 42501).
/// Superusers bypass this check.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct DatabaseId(u64);

impl DatabaseId {
    /// The built-in `default` database. Permanently reserved; cannot be
    /// dropped.
    pub const DEFAULT: DatabaseId = DatabaseId(0);

    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for DatabaseId {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<u64> for DatabaseId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "db:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_id_display() {
        let d = DatabaseId::new(42);
        assert_eq!(d.to_string(), "db:42");
        assert_eq!(d.as_u64(), 42);
    }

    #[test]
    fn database_id_above_u32_max_roundtrip() {
        let large = u32::MAX as u64 + 1;
        let d = DatabaseId::new(large);
        assert_eq!(d.as_u64(), large);
        assert_eq!(d.to_string(), format!("db:{large}"));
    }

    #[test]
    fn database_id_from_u64() {
        let d: DatabaseId = DatabaseId::from(4_294_967_296u64);
        assert_eq!(d.as_u64(), 4_294_967_296u64);
    }

    #[test]
    fn default_constant_is_zero() {
        assert_eq!(DatabaseId::DEFAULT.as_u64(), 0);
        assert_eq!(DatabaseId::DEFAULT.to_string(), "db:0");
    }

    #[test]
    fn serde_roundtrip() {
        let did = DatabaseId::new(7);
        let json = sonic_rs::to_string(&did).unwrap();
        let decoded: DatabaseId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(did, decoded);
    }

    #[test]
    fn serde_roundtrip_above_u32_max() {
        let did = DatabaseId::new(u32::MAX as u64 + 1);
        let json = sonic_rs::to_string(&did).unwrap();
        let decoded: DatabaseId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(did, decoded);
    }
}
