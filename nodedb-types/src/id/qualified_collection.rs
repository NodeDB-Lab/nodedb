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
use super::collection_key::{
    CollectionKey, CollectionKeyError, QUALIFIER_SEPARATOR, parse_qualifier,
};
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
    /// disk, on a plan, or on the wire.
    ///
    /// `stored` must have the form [`Self::new`] writes for `database_id`.
    /// Any other form is [`CollectionKeyError::NotQualified`]. The empty
    /// string is the no-collection sentinel and is accepted.
    pub fn from_stored(
        database_id: DatabaseId,
        stored: String,
    ) -> Result<Self, CollectionKeyError> {
        CollectionKey::from_qualified_str(database_id, &stored)?;
        Ok(Self(stored))
    }

    /// Qualify a collection name a caller supplied for `database_id`.
    ///
    /// A bare name qualifies through [`Self::new`]. A name that carries
    /// exactly this database's qualifier is kept as it is. A name that
    /// carries another database's qualifier is
    /// [`CollectionKeyError::ForeignDatabase`]. An empty name, or a name
    /// with any other `/`, is [`CollectionKeyError::InvalidName`].
    pub fn from_request(
        database_id: DatabaseId,
        caller_form: &str,
    ) -> Result<Self, CollectionKeyError> {
        if CollectionKey::is_bare_name(caller_form) {
            return Ok(Self::new(database_id, caller_form));
        }
        let invalid = || CollectionKeyError::InvalidName {
            database_id: database_id.as_u64(),
            name: caller_form.to_owned(),
        };
        let (head, bare) = caller_form
            .split_once(QUALIFIER_SEPARATOR)
            .ok_or_else(invalid)?;
        if !CollectionKey::is_bare_name(bare) {
            return Err(invalid());
        }
        match parse_qualifier(head) {
            Some(raw) if raw == database_id.as_u64() => Ok(Self(caller_form.to_owned())),
            Some(raw) => Err(CollectionKeyError::ForeignDatabase {
                database_id: database_id.as_u64(),
                foreign_database_id: raw,
                name: caller_form.to_owned(),
            }),
            None => Err(invalid()),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The database a stored collection name belongs to: the `{id}/`
    /// qualifier [`Self::new`] writes outside the default database, else
    /// the default database.
    ///
    /// A collection name never holds a `/`. A stored name with a qualifier
    /// that is not a canonical non-default id, an empty tail, or a second
    /// `/` is malformed and returns a serialization error.
    pub fn database_of_stored(qualified: &str) -> Result<DatabaseId, NodeDbError> {
        let Some((head, bare)) = qualified.split_once(QUALIFIER_SEPARATOR) else {
            return Ok(DatabaseId::DEFAULT);
        };
        match parse_qualifier(head) {
            Some(raw) if CollectionKey::is_bare_name(bare) => Ok(DatabaseId::new(raw)),
            _ => Err(NodeDbError::serialization(
                "qualified collection name",
                format!(
                    "collection '{qualified}' is not a stored collection name: \
                     expected '<name>' or '<database_id>/<name>'"
                ),
            )),
        }
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
        for malformed in ["7/a/b", "07/users", "0/users", "7/", "/users"] {
            assert!(
                QualifiedCollection::database_of_stored(malformed).is_err(),
                "{malformed}"
            );
        }
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
        let q = QualifiedCollection::from_stored(DatabaseId::new(7), "7/users".to_string())
            .expect("qualified for database 7");
        assert_eq!(q.as_str(), "7/users");
    }

    #[test]
    fn from_stored_refuses_a_name_of_another_form() {
        let db = DatabaseId::new(7);
        for stored in ["users", "8/users", "7/a/b", "7/"] {
            let err = QualifiedCollection::from_stored(db, stored.to_string()).expect_err(stored);
            assert!(
                matches!(err, CollectionKeyError::NotQualified { .. }),
                "{stored}: {err}"
            );
        }
        assert!(QualifiedCollection::from_stored(DatabaseId::DEFAULT, "7/users".into()).is_err());
        assert!(QualifiedCollection::from_stored(db, String::new()).is_ok());
    }

    #[test]
    fn from_request_qualifies_a_bare_name() {
        let q = QualifiedCollection::from_request(DatabaseId::new(7), "users").expect("bare");
        assert_eq!(q, QualifiedCollection::new(DatabaseId::new(7), "users"));
        let q = QualifiedCollection::from_request(DatabaseId::DEFAULT, "users").expect("bare");
        assert_eq!(q.as_str(), "users");
    }

    #[test]
    fn from_request_keeps_this_database_qualifier() {
        let q = QualifiedCollection::from_request(DatabaseId::new(7), "7/users").expect("own");
        assert_eq!(q, QualifiedCollection::new(DatabaseId::new(7), "users"));
    }

    #[test]
    fn from_request_refuses_a_foreign_qualifier() {
        let err =
            QualifiedCollection::from_request(DatabaseId::new(7), "8/users").expect_err("foreign");
        assert_eq!(
            err,
            CollectionKeyError::ForeignDatabase {
                database_id: 7,
                foreign_database_id: 8,
                name: "8/users".to_owned(),
            }
        );
        assert!(matches!(
            QualifiedCollection::from_request(DatabaseId::DEFAULT, "7/users"),
            Err(CollectionKeyError::ForeignDatabase { .. })
        ));
    }

    #[test]
    fn from_request_refuses_a_malformed_name() {
        let db = DatabaseId::new(7);
        for name in ["", "/users", "7/", "7/a/b", "users/", "x/users", "07/users"] {
            let err = QualifiedCollection::from_request(db, name).expect_err(name);
            assert!(
                matches!(err, CollectionKeyError::InvalidName { .. }),
                "{name}: {err}"
            );
        }
        assert!(matches!(
            QualifiedCollection::from_request(DatabaseId::DEFAULT, "0/users"),
            Err(CollectionKeyError::InvalidName { .. })
        ));
    }
}
