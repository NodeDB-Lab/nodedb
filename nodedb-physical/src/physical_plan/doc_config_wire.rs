// SPDX-License-Identifier: Apache-2.0

//! The wire form of the document config a write validate reads, and its
//! content digest.
//!
//! The digest is stable across nodes and boots: set-like lists are sorted,
//! map entries are written in key order, and the hash is XXH3-64. A core
//! decodes a config pin's wire form with no catalog.

use super::doc_config_canon::{CanonError, canonicalize};
use super::{
    DeclaredColumn, EnforcementOptions, MaterializedSumBinding, RegisteredIndex, StorageMode,
};

/// The document config fields that validate and install read.
///
/// `enforcement` carries the type guards, the generated columns and the
/// materialized-sum sources this collection drives.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DocConfigWire {
    pub bitemporal: bool,
    /// The collection stores CRDT state. A CRDT write injects no type guard.
    pub crdt_enabled: bool,
    pub enforcement: EnforcementOptions,
    pub storage_mode: StorageMode,
    pub index_paths: Vec<RegisteredIndex>,
    pub declared_key: Option<String>,
    pub vector_primary: Option<nodedb_types::VectorPrimaryConfig>,
    /// Declared numeric columns of a schemaless or KV collection. A write
    /// re-types each value stored under one of them.
    pub declared_columns: Vec<DeclaredColumn>,
    /// Declared `VECTOR(n)` columns of a schemaless collection, as
    /// `(column, n)`.
    pub vector_fields: Vec<(String, usize)>,
}

/// The wire form did not reduce to canonical bytes.
#[derive(Debug, thiserror::Error)]
pub enum DocConfigWireError {
    #[error("doc config wire does not encode: {source}")]
    Encode {
        #[source]
        source: zerompk::Error,
    },
    #[error("doc config wire does not canonicalize: {source}")]
    Canonical {
        #[from]
        source: CanonError,
    },
}

fn sum_source_key(binding: &MaterializedSumBinding) -> (&str, &str, &str) {
    (
        binding.target_collection.as_str(),
        binding.target_column.as_str(),
        binding.join_column.as_str(),
    )
}

impl DocConfigWire {
    /// The bytes the digest hashes. One config gives one byte string.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DocConfigWireError> {
        let mut sorted = self.clone();
        sorted
            .index_paths
            .sort_by(|a, b| (&a.path, &a.name).cmp(&(&b.path, &b.name)));
        sorted
            .enforcement
            .materialized_sum_sources
            .sort_by(|a, b| sum_source_key(a).cmp(&sum_source_key(b)));
        sorted.declared_columns.sort_by(|a, b| a.name.cmp(&b.name));
        sorted.vector_fields.sort();
        let encoded = zerompk::to_msgpack_vec(&sorted)
            .map_err(|source| DocConfigWireError::Encode { source })?;
        Ok(canonicalize(&encoded)?)
    }

    /// The XXH3-64 hash of [`Self::canonical_bytes`].
    pub fn digest(&self) -> Result<u64, DocConfigWireError> {
        Ok(super::redo_pins::pin_digest(&self.canonical_bytes()?))
    }

    /// The canonical bytes and their digest, as a config pin carries them.
    /// The digest equals [`Self::digest`].
    pub fn pin_parts(&self) -> Result<(Vec<u8>, u64), DocConfigWireError> {
        let bytes = self.canonical_bytes()?;
        let digest = super::redo_pins::pin_digest(&bytes);
        Ok((bytes, digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::{BalancedDef, PeriodLockConfig, RegisteredIndexState};
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    fn index(name: &str, path: &str) -> RegisteredIndex {
        RegisteredIndex {
            name: name.to_string(),
            path: path.to_string(),
            unique: false,
            case_insensitive: false,
            state: RegisteredIndexState::Ready,
            predicate: None,
        }
    }

    fn base() -> DocConfigWire {
        DocConfigWire {
            bitemporal: false,
            crdt_enabled: false,
            enforcement: EnforcementOptions::default(),
            storage_mode: StorageMode::Schemaless,
            index_paths: vec![index("a", "$.a"), index("b", "$.b")],
            declared_key: None,
            vector_primary: None,
            declared_columns: vec![column("a", "INTEGER"), column("b", "REAL")],
            vector_fields: vec![("e".into(), 3), ("f".into(), 4)],
        }
    }

    fn column(name: &str, declared: &str) -> DeclaredColumn {
        DeclaredColumn::from_declared(name, declared).expect("numeric column")
    }

    fn digest(wire: &DocConfigWire) -> u64 {
        wire.digest().expect("digest")
    }

    #[test]
    fn each_validate_field_changes_the_digest() {
        let base_digest = digest(&base());
        let mut variants: Vec<DocConfigWire> = Vec::new();

        let mut w = base();
        w.bitemporal = true;
        variants.push(w);

        let mut w = base();
        w.enforcement.append_only = true;
        variants.push(w);

        let mut w = base();
        w.enforcement.hash_chain = true;
        variants.push(w);

        let mut w = base();
        w.enforcement.has_legal_hold = true;
        variants.push(w);

        let mut w = base();
        w.enforcement.balanced = Some(BalancedDef {
            group_key_column: "g".into(),
            entry_type_column: "t".into(),
            debit_value: "d".into(),
            credit_value: "c".into(),
            amount_column: "amt".into(),
        });
        variants.push(w);

        let mut w = base();
        w.enforcement
            .type_guards
            .push(nodedb_types::TypeGuardFieldDef {
                field: "f".into(),
                type_expr: "STRING".into(),
                required: true,
                check_expr: None,
                default_expr: None,
                value_expr: None,
            });
        variants.push(w);

        let mut w = base();
        w.index_paths[0].unique = true;
        variants.push(w);

        let mut w = base();
        w.index_paths[0].case_insensitive = true;
        variants.push(w);

        let mut w = base();
        w.index_paths[0].predicate = Some("x > 1".into());
        variants.push(w);

        let mut w = base();
        w.index_paths[0].state = RegisteredIndexState::Building;
        variants.push(w);

        let mut w = base();
        w.enforcement.period_lock = Some(PeriodLockConfig {
            period_column: "p".into(),
            ref_table: "periods".into(),
            ref_pk: "k".into(),
            status_column: "s".into(),
            allowed_statuses: vec!["OPEN".into()],
        });
        variants.push(w);

        let mut w = base();
        w.storage_mode = StorageMode::Strict {
            schema: StrictSchema::new(vec![ColumnDef::required("_rowid", ColumnType::Int64)])
                .expect("schema"),
        };
        variants.push(w);

        let mut w = base();
        w.declared_key = Some("sku".into());
        variants.push(w);

        let mut w = base();
        w.vector_primary = Some(nodedb_types::VectorPrimaryConfig::default());
        variants.push(w);

        let mut w = base();
        w.declared_columns.push(column("c", "BIGINT"));
        variants.push(w);

        let mut w = base();
        w.declared_columns[0] = column("a", "SMALLINT");
        variants.push(w);

        let mut w = base();
        w.vector_fields.push(("g".into(), 8));
        variants.push(w);

        let mut w = base();
        w.vector_fields[0].1 = 5;
        variants.push(w);

        for (i, variant) in variants.iter().enumerate() {
            assert_ne!(digest(variant), base_digest, "variant {i} kept the digest");
        }
    }

    #[test]
    fn set_like_lists_in_another_order_give_one_digest() {
        let mut reordered = base();
        reordered.index_paths.reverse();
        reordered.declared_columns.reverse();
        reordered.vector_fields.reverse();
        assert_eq!(digest(&reordered), digest(&base()));
    }

    /// The canonical bytes decode to the wire form they came from. The base
    /// holds every list sorted already.
    #[test]
    fn canonical_bytes_decode_back_to_the_wire_form() {
        let mut wire = base();
        wire.enforcement.append_only = true;
        wire.declared_key = Some("sku".into());
        let bytes = wire.canonical_bytes().expect("canonical");
        let decoded: DocConfigWire = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, wire);
        assert_eq!(digest(&decoded), digest(&wire));
    }

    #[test]
    fn the_same_config_gives_the_same_digest() {
        assert_eq!(digest(&base()), digest(&base()));
    }

    #[test]
    fn pin_parts_carry_the_digest_of_the_canonical_bytes() {
        let (bytes, pinned) = base().pin_parts().expect("pin parts");
        assert_eq!(pinned, digest(&base()));
        assert_eq!(bytes, base().canonical_bytes().expect("canonical"));
    }
}
