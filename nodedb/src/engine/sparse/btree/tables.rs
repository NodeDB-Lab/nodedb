// SPDX-License-Identifier: BUSL-1.1

//! The shared redb-error mapping helper every sparse module reports through,
//! and the storage-key error of the document and index tables.

/// Map a redb error into our crate error with context.
pub(in crate::engine::sparse) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// The redb tables whose keys embed a [`nodedb_types::StorageKey`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum KeyedTable {
    DocumentsVersioned,
    IndexesVersioned,
}

impl KeyedTable {
    fn name(self) -> &'static str {
        match self {
            Self::DocumentsVersioned => "DOCUMENTS_VERSIONED",
            Self::IndexesVersioned => "INDEXES_VERSIONED",
        }
    }
}

/// Report a row on `table` whose key does not parse as a [`nodedb_types::StorageKey`].
///
/// A non-parsing key is a violated storage invariant, not a row to skip:
/// every stored key is minted by [`StorageKey::for_surrogate`].
pub(crate) fn invalid_storage_key_err(
    table: KeyedTable,
    collection: &str,
    key: &str,
) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!(
            "collection '{collection}' has a {} row whose key is not a valid storage key: '{key}'",
            table.name()
        ),
    }
}
