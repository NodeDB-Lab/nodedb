// SPDX-License-Identifier: BUSL-1.1

//! The incarnation of an array a transaction names, read from this node's
//! array mirror.
//!
//! A Calvin transaction and a committed redo name each array they write by
//! its database-qualified name, with the incarnation the proposer planned
//! against. The array's own gate (`write_gate::GateKey::Array`) and its own
//! catalog decide the write. A same-name collection never shares either.

use std::sync::RwLockReadGuard;

use nodedb_types::{CollectionKey, Hlc};

use super::cell_route::{CellRoute, gate_incarnation, route};
use super::registry::{ArrayCatalog, ArrayCatalogHandle};
use crate::control::write_gate::GateKey;
use crate::types::{DatabaseId, TenantId};

/// The `(database, name)` key `named` stands for in `database_id`.
fn key_of(database_id: DatabaseId, named: &str) -> CollectionKey<'_> {
    CollectionKey::from_qualified_str(database_id, named)
        .unwrap_or_else(|_| CollectionKey::from_bare(database_id, named))
}

/// Read `handle`'s mirror. A poisoned lock still holds a whole mirror: every
/// writer replaces entries in one step.
pub(crate) fn read_mirror(handle: &ArrayCatalogHandle) -> RwLockReadGuard<'_, ArrayCatalog> {
    match handle.read() {
        Ok(mirror) => mirror,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// The incarnation `mirror` holds for the array `named` in `database_id`.
/// `Hlc::ZERO` when the mirror holds no such array.
pub(crate) fn array_incarnation_of(
    mirror: &ArrayCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    named: &str,
) -> Hlc {
    let key = key_of(database_id, named);
    mirror
        .lookup_by_name_in_database(tenant_id, key.database_id(), key.name())
        .map_or(Hlc::ZERO, |entry| entry.incarnation)
}

/// The gate a write of the array `named` at `incarnation` holds shared.
/// `None` when an unstamped write names no array.
pub(crate) fn array_gate_key(
    mirror: &ArrayCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    named: &str,
    incarnation: Hlc,
) -> Option<GateKey> {
    let key = key_of(database_id, named);
    gate_incarnation(
        mirror,
        tenant_id,
        key.database_id(),
        key.name(),
        incarnation,
    )
    .map(GateKey::Array)
}

/// Whether the array `named` in `database_id` still holds `incarnation`
/// under its own key. An array that moved or is gone does not: a write
/// planned against it has nothing to mutate there.
pub(crate) fn array_holds_incarnation(
    mirror: &ArrayCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    named: &str,
    incarnation: Hlc,
) -> bool {
    let key = key_of(database_id, named);
    route(
        mirror,
        tenant_id,
        key.database_id(),
        key.name(),
        incarnation,
    ) == CellRoute::Here
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;

    use super::*;
    use crate::control::array_catalog::ArrayCatalogEntry;

    fn mirror_with(db: u64, name: &str, incarnation: Hlc) -> ArrayCatalog {
        let mut mirror = ArrayCatalog::new();
        mirror
            .register(ArrayCatalogEntry {
                array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::new(db), name),
                name: name.to_string(),
                schema_msgpack: vec![0x90],
                schema_hash: 7,
                created_at_ms: 0,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
                modification_hlc: incarnation,
                incarnation,
            })
            .expect("register");
        mirror
    }

    #[test]
    fn a_qualified_name_reads_its_own_database() {
        let incarnation = Hlc::new(70, 0);
        let mirror = mirror_with(5, "grid", incarnation);
        let named = nodedb_types::QualifiedCollection::new(DatabaseId::new(5), "grid");
        assert_eq!(
            array_incarnation_of(
                &mirror,
                TenantId::new(1),
                DatabaseId::new(5),
                named.as_str()
            ),
            incarnation
        );
        assert_eq!(
            array_incarnation_of(&mirror, TenantId::new(1), DatabaseId::DEFAULT, "other"),
            Hlc::ZERO
        );
    }

    /// The gate is the array's incarnation gate, never a collection gate.
    #[test]
    fn the_gate_is_the_arrays_own() {
        let incarnation = Hlc::new(71, 0);
        let mirror = mirror_with(0, "grid", incarnation);
        assert_eq!(
            array_gate_key(
                &mirror,
                TenantId::new(1),
                DatabaseId::DEFAULT,
                "grid",
                incarnation
            ),
            Some(GateKey::Array(incarnation))
        );
    }

    /// A recreated array holds another incarnation: the planned one is gone.
    #[test]
    fn a_recreated_array_no_longer_holds_the_planned_incarnation() {
        let mirror = mirror_with(0, "grid", Hlc::new(73, 0));
        assert!(array_holds_incarnation(
            &mirror,
            TenantId::new(1),
            DatabaseId::DEFAULT,
            "grid",
            Hlc::new(73, 0)
        ));
        assert!(!array_holds_incarnation(
            &mirror,
            TenantId::new(1),
            DatabaseId::DEFAULT,
            "grid",
            Hlc::new(72, 0)
        ));
    }
}
