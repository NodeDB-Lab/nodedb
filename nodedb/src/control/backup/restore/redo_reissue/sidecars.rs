// SPDX-License-Identifier: BUSL-1.1

//! Backed-up sidecar rows of vector-primary collections.
//!
//! A vector-primary row keeps its vector on an index node and every other
//! column in a sparse-store sidecar. The sidecar is a `zerompk` tagged
//! `HashMap<String, Value>`, the encoding every live write stores. The
//! backup's document section carries each sidecar version in that encoding,
//! keyed by the row's storage key.
//!
//! A sidecar re-issues with its row's nodes, in the row's `vector_restore`
//! sub-record, never as a document row. A document put decodes its body as
//! plain MessagePack, which reads each tagged value as a two-element array.
//! The vector install writes the sidecar bytes as the backup holds them, at
//! each version's system time and valid interval.

use std::collections::BTreeMap;

use nodedb_types::{StorageKey, Surrogate};

use crate::engine::sparse::btree_versioned::{TAG_LIVE, TAG_TOMBSTONE, decode_value};
use crate::wal::VectorSidecarVersion;

use super::super::target::RestoredName;

/// One collection's sidecar rows: every version of each row, ascending by
/// `sys_from`, keyed by the surrogate the backup stored the row under.
pub(super) type CollectionSidecars = BTreeMap<Surrogate, Vec<VectorSidecarVersion>>;

/// Every backed-up sidecar row of one database, keyed by bare collection
/// name, with the collection's names in the destination.
#[derive(Default)]
pub(in crate::control::backup::restore) struct RestoredSidecars {
    by_collection: BTreeMap<String, (RestoredName, CollectionSidecars)>,
}

impl RestoredSidecars {
    /// Add row `key` of collection `name` with its stored `versions`, each
    /// `(sys_from, versioned value)`, in system-time order.
    pub(super) fn insert_row(
        &mut self,
        name: &RestoredName,
        key: StorageKey,
        versions: &[(i64, Vec<u8>)],
    ) -> crate::Result<()> {
        let mut decoded = Vec::with_capacity(versions.len());
        for (sys_from, raw) in versions {
            let version = decode_value(raw)?;
            let body = match version.tag {
                TAG_LIVE => Some(version.body.to_vec()),
                TAG_TOMBSTONE => None,
                tag => {
                    return Err(crate::Error::Serialization {
                        format: "versioned-doc".into(),
                        detail: format!(
                            "restore: version {sys_from} of sidecar row {key} of '{}' carries \
                             tag {tag:#04x}, which no write path records",
                            name.bare
                        ),
                    });
                }
            };
            decoded.push(VectorSidecarVersion {
                sys_from: *sys_from,
                valid_from_ms: version.valid_from_ms,
                valid_until_ms: version.valid_until_ms,
                body,
            });
        }
        self.by_collection
            .entry(name.bare.clone())
            .or_insert_with(|| (name.clone(), CollectionSidecars::new()))
            .1
            .insert(key.surrogate(), decoded);
        Ok(())
    }

    /// Take every sidecar row of the collection named `bare`.
    pub(super) fn take(&mut self, bare: &str) -> CollectionSidecars {
        self.by_collection
            .remove(bare)
            .map(|(_, rows)| rows)
            .unwrap_or_default()
    }

    /// The collections no backed-up index took, with their rows.
    pub(super) fn into_remaining(self) -> impl Iterator<Item = (RestoredName, CollectionSidecars)> {
        self.by_collection.into_values()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::Value;

    use crate::engine::sparse::btree_versioned::encode_value;

    use super::*;

    fn name() -> RestoredName {
        RestoredName {
            bare: "vp".into(),
            stored: nodedb_types::QualifiedCollection::from_stored(
                crate::types::DatabaseId::DEFAULT,
                "vp".to_string(),
            )
            .expect("stored name"),
        }
    }

    /// A sidecar version keeps its tagged bytes, its system time and its
    /// valid interval. A tombstone carries no body.
    #[test]
    fn a_sidecar_row_keeps_its_stored_bytes() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String("v1".into()));
        let tagged = zerompk::to_msgpack_vec(&fields).expect("encode");
        let key = StorageKey::for_surrogate(Surrogate::new(5));
        let versions = vec![
            (100, encode_value(TAG_LIVE, 10, 20, &tagged)),
            (200, encode_value(TAG_TOMBSTONE, 0, 0, &[])),
        ];
        let mut sidecars = RestoredSidecars::default();
        sidecars
            .insert_row(&name(), key, &versions)
            .expect("insert");
        let rows = sidecars.take("vp");
        let row = &rows[&Surrogate::new(5)];
        assert_eq!(
            row[0],
            VectorSidecarVersion {
                sys_from: 100,
                valid_from_ms: 10,
                valid_until_ms: 20,
                body: Some(tagged),
            }
        );
        assert_eq!(row[1].sys_from, 200);
        assert!(row[1].body.is_none());
        assert!(sidecars.into_remaining().next().is_none());
    }
}
