// SPDX-License-Identifier: Apache-2.0

//! The config content a committed redo's proposer pins for its validate.
//!
//! The proposer pins the document config of each collection the redo writes
//! or folds into, and the user-defined CRS set. The pins travel with the
//! redo, so every replica can judge it against the same config content.

use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId, WireBytes};

/// The content digest a pin carries for `bytes`: their XXH3-64 hash. A
/// config pin hashes its canonical wire bytes, a CRS pin its set's bytes.
pub fn pin_digest(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

/// What a redo writes into one collection, as its pins see it.
///
/// The proposer refuses a commit whose write requires a pin it cannot
/// derive. A core refuses a pinned record whose write requires a pin it
/// does not carry. Both judge by [`PinnedWrite::requires_pin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinnedWrite {
    /// Document rows: a document plan's writes, or a RESTORE batch's rows.
    /// The validate judges them under the collection's config.
    DocumentRows,
    /// A materialized-sum target a document write folds into.
    SumTarget,
    /// Any other write, such as a graph edge. Its collection may have no
    /// catalog row, and it is pinned only when one exists.
    Other,
}

impl PinnedWrite {
    /// Whether a pinned redo must carry a pin of the collection.
    pub fn requires_pin(self) -> bool {
        match self {
            Self::DocumentRows | Self::SumTarget => true,
            Self::Other => false,
        }
    }
}

/// The key of one pinned document config: the key of a core's config map.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ConfigPinKey {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    /// The database-qualified collection name.
    pub collection: String,
}

/// One collection's document config, as the proposer pinned it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ConfigPin {
    pub key: ConfigPinKey,
    /// The content digest of `wire`. It equals the digest a core stores
    /// for the same config.
    pub digest: u64,
    /// The canonical bytes of the config's wire form.
    pub wire: WireBytes,
}

/// The user-defined CRS set, as the proposer pinned it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CrsPin {
    /// The content digest of `bytes`.
    pub digest: u64,
    /// The MessagePack bytes of the set.
    pub bytes: WireBytes,
}

/// Every pin one committed redo carries.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoPins {
    /// One pin per collection, sorted by key, with no key twice. A shared
    /// pin encodes the same bytes as the pin it holds.
    pub configs: Vec<Arc<ConfigPin>>,
    /// The CRS set pin. `None` when the proposer pinned no CRS set.
    pub crs: Option<Arc<CrsPin>>,
}

impl RedoPins {
    /// Whether the redo carries no pin.
    pub fn is_empty(&self) -> bool {
        self.configs.is_empty() && self.crs.is_none()
    }

    /// Whether the proposer pinned the redo. A pinned redo carries a pin
    /// for every document collection it writes. The proposer always pins
    /// the CRS set, so a pinned redo holds it.
    pub fn pinned(&self) -> bool {
        self.crs.is_some()
    }

    /// The pinned config of `key`, if the redo carries one.
    pub fn config(&self, key: &ConfigPinKey) -> Option<&ConfigPin> {
        self.config_named(key.database_id, key.tenant_id, &key.collection)
    }

    /// The pinned config of the database-qualified `collection`, if the
    /// redo carries one. It builds no key.
    pub fn config_named(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<&ConfigPin> {
        self.position_named(database_id, tenant_id, collection)
            .map(|i| &*self.configs[i])
    }

    /// The index in `configs` of the pin of the database-qualified
    /// `collection`, if the redo carries one. It builds no key.
    pub fn position_named(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<usize> {
        // The order `ConfigPinKey` derives: its fields in declaration order.
        self.configs
            .binary_search_by(|pin| {
                pin.key
                    .database_id
                    .cmp(&database_id)
                    .then_with(|| pin.key.tenant_id.cmp(&tenant_id))
                    .then_with(|| pin.key.collection.as_str().cmp(collection))
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(collection: &str) -> ConfigPin {
        ConfigPin {
            key: ConfigPinKey {
                database_id: DatabaseId::DEFAULT,
                tenant_id: TenantId::new(1),
                collection: collection.into(),
            },
            digest: 7,
            wire: vec![1, 2, 3].into(),
        }
    }

    #[test]
    fn pins_round_trip_through_msgpack() {
        let crs = CrsPin {
            digest: 9,
            bytes: vec![0x90].into(),
        };
        let pins = RedoPins {
            configs: vec![Arc::new(pin("accounts")), Arc::new(pin("entries"))],
            crs: Some(Arc::new(crs.clone())),
        };
        let bytes = zerompk::to_msgpack_vec(&pins).expect("encode");
        let decoded: RedoPins = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, pins);
        // A shared pin encodes the bytes of the pin it holds.
        assert_eq!(
            zerompk::to_msgpack_vec(&pins.configs).expect("encode shared"),
            zerompk::to_msgpack_vec(&[pin("accounts"), pin("entries")]).expect("encode owned")
        );
        assert_eq!(
            zerompk::to_msgpack_vec(&pins.crs).expect("encode shared"),
            zerompk::to_msgpack_vec(&Some(crs)).expect("encode owned")
        );
        assert_eq!(decoded.config(&pin("entries").key), Some(&pin("entries")));
        assert_eq!(decoded.config(&pin("orders").key), None);
        assert!(RedoPins::default().is_empty());
        assert!(decoded.pinned());
        assert!(!RedoPins::default().pinned());
    }
}
