// SPDX-License-Identifier: Apache-2.0

//! Database-qualified collection name.
//!
//! Storage engines key data on `(tenant_id, collection, document_id)`.
//! Embedding the database ID into the collection token makes isolation
//! between databases automatic, but only if every reader and writer
//! qualifies the same way. This type is the single place that rule lives.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::DatabaseId;
use crate::error::NodeDbError;

/// A collection name qualified by its database.
///
/// The default database yields the bare collection name; any other
/// database prefixes it with `{database_id}/`. Security state (RLS
/// policies, redaction policies, CRDT write gates) must be stored and
/// looked up under the same `QualifiedCollection`, never a bare name,
/// or the policy silently misses outside the default database.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct QualifiedCollection(String);

impl QualifiedCollection {
    /// Qualify `collection` for `database_id`.
    ///
    /// `DatabaseId::DEFAULT` yields the bare name; any other database
    /// yields `{database_id}/{collection}`.
    pub fn new(database_id: DatabaseId, collection: &str) -> Self {
        if database_id == DatabaseId::DEFAULT {
            Self(collection.to_string())
        } else {
            Self(format!("{}/{}", database_id.as_u64(), collection))
        }
    }

    /// Rebuild a `QualifiedCollection` from a string already qualified on
    /// disk or on the wire. The only way to construct one without going
    /// through `new`; never call this with an unqualified name.
    pub fn from_stored(qualified: String) -> Self {
        Self(qualified)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The database a stored collection name belongs to: the `{id}/`
    /// qualifier [`Self::new`] writes outside the default database, else
    /// the default database.
    ///
    /// A collection name never holds a `/`. A `/` after a non-numeric head
    /// marks a malformed stored name and returns a serialization error.
    pub fn database_of_stored(qualified: &str) -> Result<DatabaseId, NodeDbError> {
        match qualified.split_once('/') {
            None => Ok(DatabaseId::DEFAULT),
            Some((head, _)) => head.parse::<u64>().map(DatabaseId::new).map_err(|e| {
                NodeDbError::serialization(
                    "qualified collection name",
                    format!("collection '{qualified}' has a non-numeric database qualifier: {e}"),
                )
            }),
        }
    }

    /// The bare collection name of a stored name: the name without the
    /// `{id}/` qualifier [`Self::new`] writes outside the default database.
    /// A stored name in the default database is its bare name.
    pub fn bare_of_stored(qualified: &str) -> &str {
        match qualified.split_once('/') {
            Some((head, bare)) if head.parse::<u64>().is_ok() => bare,
            _ => qualified,
        }
    }

    /// The collection name [`Self::new`] qualified for `database_id`: the
    /// catalog key a collection lookup in that database takes.
    pub fn collection_name(&self, database_id: DatabaseId) -> &str {
        if database_id == DatabaseId::DEFAULT {
            return &self.0;
        }
        let prefix = format!("{}/", database_id.as_u64());
        self.0.strip_prefix(prefix.as_str()).unwrap_or(&self.0)
    }
}

impl fmt::Display for QualifiedCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_name_names_its_bare_collection() {
        assert_eq!(QualifiedCollection::bare_of_stored("users"), "users");
        assert_eq!(QualifiedCollection::bare_of_stored("7/users"), "users");
        assert_eq!(
            QualifiedCollection::bare_of_stored(
                QualifiedCollection::new(DatabaseId::new(9), "a/b").as_str()
            ),
            "a/b"
        );
    }

    #[test]
    fn a_stored_name_names_its_database() {
        assert_eq!(
            QualifiedCollection::database_of_stored("users").expect("bare name"),
            DatabaseId::DEFAULT
        );
        assert_eq!(
            QualifiedCollection::database_of_stored("7/users").expect("qualified name"),
            DatabaseId::new(7)
        );
        let qualified = QualifiedCollection::new(DatabaseId::new(9), "orders");
        assert_eq!(
            QualifiedCollection::database_of_stored(qualified.as_str()).expect("qualified name"),
            DatabaseId::new(9)
        );
    }

    #[test]
    fn a_non_numeric_qualifier_is_refused() {
        let err = QualifiedCollection::database_of_stored("tenant/users")
            .expect_err("a non-numeric qualifier must fail");
        assert_eq!(err.code(), crate::error::ErrorCode::SERIALIZATION);
    }

    #[test]
    fn default_database_yields_bare_name() {
        let q = QualifiedCollection::new(DatabaseId::DEFAULT, "users");
        assert_eq!(q.as_str(), "users");
        assert_eq!(q.to_string(), "users");
    }

    #[test]
    fn non_default_database_yields_prefixed_name() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        assert_eq!(q.as_str(), "7/users");
    }

    #[test]
    fn collection_name_inverts_new() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        assert_eq!(q.collection_name(DatabaseId::new(7)), "users");
        let bare = QualifiedCollection::new(DatabaseId::DEFAULT, "users");
        assert_eq!(bare.collection_name(DatabaseId::DEFAULT), "users");
    }

    #[test]
    fn serde_roundtrips_as_plain_string() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        let json = sonic_rs::to_string(&q).expect("serialize");
        assert_eq!(json, "\"7/users\"");
        let decoded: QualifiedCollection = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(q, decoded);
    }

    #[test]
    fn zerompk_roundtrips() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        let bytes = zerompk::to_msgpack_vec(&q).expect("msgpack serialization must succeed");
        let decoded: QualifiedCollection =
            zerompk::from_msgpack(&bytes).expect("msgpack deserialization must succeed");
        assert_eq!(q, decoded);
    }

    #[test]
    fn equal_inputs_produce_equal_and_same_hash_values() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = QualifiedCollection::new(DatabaseId::new(7), "users");
        let b = QualifiedCollection::new(DatabaseId::new(7), "users");
        assert_eq!(a, b);

        let mut ha = DefaultHasher::new();
        a.hash(&mut ha);
        let mut hb = DefaultHasher::new();
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn from_stored_does_not_qualify() {
        let q = QualifiedCollection::from_stored("7/users".to_string());
        assert_eq!(q.as_str(), "7/users");
    }
}
