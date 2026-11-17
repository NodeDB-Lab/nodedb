// SPDX-License-Identifier: BUSL-1.1

//! The key of a core's collection config map, and a borrowed form of it
//! that a lookup builds without allocating.

use std::hash::{Hash, Hasher};

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};

/// The key of `CoreLoop::doc_configs`: `(database, tenant, collection)`.
pub(in crate::data::executor) type DocConfigKey = (DatabaseId, TenantId, String);

/// A borrowed [`DocConfigKey`]. It hashes and compares like the owned key,
/// so a config lookup builds no owned collection name.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct DocConfigRef<'a> {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: &'a str,
}

impl Hash for DocConfigRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // A tuple hashes its elements in order, and a `String` hashes as
        // its `str`. Hashing the same sequence keeps the two keys equal.
        (self.database_id, self.tenant_id, self.collection).hash(state);
    }
}

impl hashbrown::Equivalent<DocConfigKey> for DocConfigRef<'_> {
    fn equivalent(&self, key: &DocConfigKey) -> bool {
        self.database_id == key.0 && self.tenant_id == key.1 && self.collection == key.2
    }
}

impl CoreLoop {
    /// The registered config of `collection`, looked up without
    /// allocating. `None` when this core holds no config for it.
    pub(in crate::data::executor) fn doc_config(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<&CollectionConfig> {
        self.doc_configs.get(&DocConfigRef {
            database_id: DatabaseId::new(database_id),
            tenant_id: TenantId::new(tid),
            collection,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_borrowed_key_finds_the_owned_entry() {
        let mut map: hashbrown::HashMap<DocConfigKey, u8> = hashbrown::HashMap::new();
        map.insert(
            (DatabaseId::new(3), TenantId::new(7), "notes".to_string()),
            1,
        );
        let found = map.get(&DocConfigRef {
            database_id: DatabaseId::new(3),
            tenant_id: TenantId::new(7),
            collection: "notes",
        });
        assert_eq!(found, Some(&1));
        let other = map.get(&DocConfigRef {
            database_id: DatabaseId::new(3),
            tenant_id: TenantId::new(8),
            collection: "notes",
        });
        assert_eq!(other, None);
    }
}
