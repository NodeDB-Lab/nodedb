// SPDX-License-Identifier: BUSL-1.1

//! Durable OID allocator for the `_system.custom_type_oid_hwm` table.
//!
//! Singleton table — one row keyed `"global"` holding the highest custom-type
//! OID ever assigned. A pgwire client reads an OID as the identity of one type,
//! so two distinct types must never share one. The counter therefore spans
//! every database and tenant, and `DROP TYPE` never lowers it.
//!
//! This is the same durable-counter idiom the tenant-id allocator uses; see
//! [`super::tenant_id_hwm`]. Assignment takes the max of this counter and the
//! OIDs present in `_system.custom_types`, so a catalog restored from a backup
//! self-heals on the next assignment instead of colliding with a stored type.
//!
//! [`SystemCatalog::put_custom_type_assigning_oid`] is the sole authority on
//! which OID a type carries. It ignores the OID on the record handed to it and
//! derives the value from stored state alone. In cluster mode it runs from the
//! metadata applier on every node, in identical log order, over identical redb
//! state, so every node computes the same OID for the same entry. Two `CREATE
//! TYPE` statements that race on two nodes reach the log in some order, and
//! the second one to apply is assigned past the first on every node at once.

use redb::ReadableTable;

use super::custom_types::{StoredCustomType, custom_type_key};
use super::types::{CUSTOM_TYPES, SystemCatalog, catalog_err};

/// Redb table: singleton `"global"` -> highest assigned custom-type OID (`u32`).
pub(super) const CUSTOM_TYPE_OID_HWM: redb::TableDefinition<&str, u32> =
    redb::TableDefinition::new("_system.custom_type_oid_hwm");

/// Singleton row key.
const HWM_KEY: &str = "global";

/// Floor for user-defined type OIDs. PostgreSQL built-in OIDs end well below
/// 10000 and extension OIDs start at 16384, so 70000 leaves both ranges clear.
/// The first assigned OID is `USER_TYPE_OID_BASE + 1`.
pub const USER_TYPE_OID_BASE: u32 = 70_000;

impl SystemCatalog {
    /// Write a custom type, assigning its OID.
    ///
    /// The `oid` on `def` is ignored. A proposing node cannot know which OID a
    /// type will carry, because a concurrent statement on another node reaches
    /// the log first as often as not. The value comes from stored state, which
    /// every node holds identically at a given log position.
    ///
    /// A type that already exists keeps the OID it was assigned: `ALTER TYPE
    /// ADD VALUE` ships a full record, and a pgwire client that cached the old
    /// OID must keep resolving the same type. A type that does not exist is
    /// assigned an OID strictly greater than every stored OID and than the
    /// counter, and the counter advances to it.
    ///
    /// Returns the record as written. The caller registers that record, not
    /// the one it passed in — only this method knows the assigned OID.
    pub fn put_custom_type_assigning_oid(
        &self,
        def: &StoredCustomType,
    ) -> crate::Result<StoredCustomType> {
        let key = custom_type_key(def.database_id, def.tenant_id, &def.name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("custom_type_oid_hwm write txn", e))?;
        let effective;
        {
            let mut types = write_txn
                .open_table(CUSTOM_TYPES)
                .map_err(|e| catalog_err("open custom_types", e))?;
            let existing_oid = types
                .get(key.as_str())
                .map_err(|e| catalog_err("get custom type", e))?
                .and_then(|v| zerompk::from_msgpack::<StoredCustomType>(v.value()).ok())
                .map(|t| t.oid);

            let oid = match existing_oid {
                Some(oid) => oid,
                None => {
                    let mut hwm = write_txn
                        .open_table(CUSTOM_TYPE_OID_HWM)
                        .map_err(|e| catalog_err("open custom_type_oid_hwm", e))?;
                    let stored = hwm
                        .get(HWM_KEY)
                        .map_err(|e| catalog_err("get custom_type_oid_hwm", e))?
                        .map(|v| v.value())
                        .unwrap_or(0);
                    let mut floor = stored.max(USER_TYPE_OID_BASE);
                    for entry in types
                        .range(..)
                        .map_err(|e| catalog_err("range custom_types", e))?
                    {
                        let (_, value) = entry.map_err(|e| catalog_err("read custom type", e))?;
                        if let Ok(t) = zerompk::from_msgpack::<StoredCustomType>(value.value()) {
                            floor = floor.max(t.oid);
                        }
                    }
                    let assigned = next_oid(floor)?;
                    hwm.insert(HWM_KEY, assigned)
                        .map_err(|e| catalog_err("insert custom_type_oid_hwm", e))?;
                    assigned
                }
            };

            effective = StoredCustomType { oid, ..def.clone() };
            let bytes = zerompk::to_msgpack_vec(&effective)
                .map_err(|e| catalog_err("serialize custom type", e))?;
            types
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert custom type", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("custom_type_oid_hwm commit", e))?;
        Ok(effective)
    }
}

/// The OID that follows `floor`, or a typed error when the range is spent.
///
/// OIDs are `u32` and a wrap would hand a live type's identity to a new one,
/// which is the exact defect the counter exists to prevent. The range is
/// refused instead.
fn next_oid(floor: u32) -> crate::Result<u32> {
    floor.checked_add(1).ok_or(crate::Error::LimitExceeded {
        limit_name: "custom_type_oid",
        value: floor as u64,
        max: (u32::MAX - 1) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::CustomTypeDef;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn enum_type(database_id: u64, tenant_id: u64, name: &str, oid: u32) -> StoredCustomType {
        StoredCustomType {
            database_id,
            tenant_id,
            name: name.into(),
            def: CustomTypeDef::Enum {
                labels: vec!["a".into()],
            },
            oid,
            created_at: 0,
        }
    }

    /// PostgreSQL built-in and extension OIDs live below this floor. A type
    /// assigned into that range would be decoded as a built-in by a client.
    #[test]
    fn the_first_assigned_oid_clears_the_postgres_ranges() {
        let (_dir, catalog) = open();
        assert_eq!(USER_TYPE_OID_BASE, 70_000);
        let written = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "first", 0))
            .unwrap();
        assert_eq!(written.oid, 70_001);
    }

    #[test]
    fn assignments_are_strictly_increasing_and_distinct() {
        let (_dir, catalog) = open();
        let a = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "a", 0))
            .unwrap()
            .oid;
        let b = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "b", 0))
            .unwrap()
            .oid;
        let c = catalog
            .put_custom_type_assigning_oid(&enum_type(2, 7, "a", 0))
            .unwrap()
            .oid;
        assert!(a < b && b < c, "{a} {b} {c}");
    }

    /// Two nodes handling concurrent `CREATE TYPE` statements build records
    /// that carry the same OID, because neither node can see the other's
    /// statement. The applier runs on both nodes in one log order and derives
    /// each OID from stored state, so the records do not share an identity.
    #[test]
    fn two_entries_carrying_one_oid_receive_distinct_oids() {
        let (_dir, catalog) = open();
        let carried = 70_001;

        let first = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "from_node_a", carried))
            .unwrap();
        let second = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "from_node_b", carried))
            .unwrap();

        assert_eq!(first.oid, 70_001);
        assert_ne!(
            first.oid, second.oid,
            "a pgwire client reads the OID as the identity of one type"
        );
        assert_eq!(second.oid, 70_002);
    }

    /// The record's own OID never reaches the row. A node that guessed high
    /// would otherwise burn the range between the guess and the counter.
    #[test]
    fn the_records_oid_is_ignored_on_the_create_path() {
        let (_dir, catalog) = open();
        let written = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "guessed_high", 900_000))
            .unwrap();
        assert_eq!(written.oid, 70_001);
        let read_back = catalog
            .get_custom_type(1, 7, "guessed_high")
            .unwrap()
            .unwrap();
        assert_eq!(read_back.oid, 70_001);
    }

    /// `ALTER TYPE ADD VALUE` ships a full record. The type keeps its OID, and
    /// the counter does not move, so no OID is burned per label.
    #[test]
    fn an_existing_type_keeps_its_oid() {
        let (_dir, catalog) = open();
        let created = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "state", 0))
            .unwrap();

        let mut altered = created.clone();
        altered.def = CustomTypeDef::Enum {
            labels: vec!["a".into(), "b".into()],
        };
        altered.oid = 0;
        let rewritten = catalog.put_custom_type_assigning_oid(&altered).unwrap();

        assert_eq!(rewritten.oid, created.oid);
        let next = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "after", 0))
            .unwrap();
        assert_eq!(next.oid, created.oid + 1, "an update must not burn an OID");
    }

    /// The counter is the only thing standing between a dropped type's OID and
    /// a new type. A client that cached the old OID must not decode the new
    /// type's values with it.
    #[test]
    fn a_dropped_types_oid_is_never_reassigned() {
        let (_dir, catalog) = open();
        let dropped = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "gone", 0))
            .unwrap();
        assert!(catalog.delete_custom_type(1, 7, "gone").unwrap());

        let fresh = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "new", 0))
            .unwrap();
        assert!(fresh.oid > dropped.oid);
    }

    /// A restart reloads the counter from redb. Reopening the catalog must not
    /// reissue an OID already handed out.
    #[test]
    fn an_oid_survives_a_reopen_and_is_never_reissued() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let before = {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_custom_type_assigning_oid(&enum_type(1, 7, "pre_restart", 0))
                .unwrap()
                .oid
        };

        let catalog = SystemCatalog::open(&path).unwrap();
        let after = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "post_restart", 0))
            .unwrap()
            .oid;
        assert!(after > before, "{after} must be past {before}");
    }

    /// A backup restore writes type rows without the counter. The next
    /// assignment reads the rows and skips past them.
    #[test]
    fn self_heals_against_rows_written_without_the_counter() {
        let (_dir, catalog) = open();
        catalog
            .put_custom_type(&enum_type(1, 7, "restored", 90_000))
            .unwrap();
        let fresh = catalog
            .put_custom_type_assigning_oid(&enum_type(1, 7, "fresh", 0))
            .unwrap();
        assert_eq!(fresh.oid, 90_001);
    }

    /// A wrap would hand a live type's identity to a new one. The allocator
    /// refuses the range instead.
    #[test]
    fn an_exhausted_range_is_an_error_not_a_wrap() {
        assert!(next_oid(u32::MAX - 1).is_ok());
        assert!(matches!(
            next_oid(u32::MAX),
            Err(crate::Error::LimitExceeded {
                limit_name: "custom_type_oid",
                ..
            })
        ));

        let (_dir, catalog) = open();
        catalog
            .put_custom_type(&enum_type(1, 7, "last", u32::MAX))
            .unwrap();
        assert!(
            catalog
                .put_custom_type_assigning_oid(&enum_type(1, 7, "overflow", 0))
                .is_err()
        );
    }
}
