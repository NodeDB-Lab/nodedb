// SPDX-License-Identifier: Apache-2.0

//! Canonical collection identity for placement and surrogate identity.
//!
//! A collection's vShard and its surrogate bindings are keyed by the pair
//! `(database_id, bare_name)`. The bare name is the name the catalog keys the
//! collection by. The database-qualified form `"{database_id}/{name}"` names
//! the same collection, but it folds the database into the string a second
//! time. Hashing it gives a different vShard than hashing the bare name.
//!
//! [`CollectionKey`] is the only input the vShard hash and the surrogate
//! allocator accept. It has no `From<&str>`. A caller builds it from a bare
//! catalog name with [`CollectionKey::from_bare`], or from a qualified name
//! with [`CollectionKey::from_qualified`], which strips the qualifier.
//!
//! A collection name never holds `/`. Every DDL path that mints a name
//! refuses one, so `"{database_id}/{name}"` parses in exactly one way.

use super::{DatabaseId, QualifiedCollection};

/// The separator between the database qualifier and the bare name.
///
/// No collection name holds it, so a qualified name splits in exactly one way.
pub(crate) const QUALIFIER_SEPARATOR: char = '/';

/// Error returned when a collection name does not have the form its database
/// gives it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CollectionKeyError {
    /// A stored or plan-carried name lacks the form [`QualifiedCollection::new`]
    /// writes for its database. This is an internal invariant break.
    #[error(
        "stored collection name '{name}' is not a name database {database_id} writes; \
         the default database stores '<name>', any other database stores '{database_id}/<name>'"
    )]
    NotQualified {
        /// Raw id of the database the name was resolved in.
        database_id: u64,
        /// The rejected name.
        name: String,
    },
    /// A caller-supplied name carries the qualifier of another database.
    #[error(
        "collection '{name}' names database {foreign_database_id}, \
         not the current database {database_id}"
    )]
    ForeignDatabase {
        /// Raw id of the database the request runs in.
        database_id: u64,
        /// Raw id of the database the qualifier names.
        foreign_database_id: u64,
        /// The rejected name.
        name: String,
    },
    /// A caller-supplied name is empty or holds a stray `/`.
    #[error(
        "collection name '{name}' is not valid in database {database_id}: \
         a collection name is non-empty and holds no '/'"
    )]
    InvalidName {
        /// Raw id of the database the request runs in.
        database_id: u64,
        /// The rejected name.
        name: String,
    },
}

/// The canonical `(database_id, bare_name)` identity of a collection.
///
/// Borrowed: it never allocates. Build it with [`Self::from_bare`] from a
/// catalog name, or with [`Self::from_qualified`] /
/// [`Self::from_qualified_str`] from a database-qualified name.
///
/// No `From<&str>` impl exists, so a raw string never converts implicitly:
///
/// ```compile_fail
/// use nodedb_types::CollectionKey;
/// let key: CollectionKey<'_> = "1024/users".into();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionKey<'a> {
    database_id: DatabaseId,
    name: &'a str,
}

/// The database id a qualifier head names, in the canonical form
/// [`QualifiedCollection::new`] writes: ASCII digits with no leading zero.
///
/// The default database is never written as a qualifier, so `0` yields `None`.
pub(crate) fn parse_qualifier(head: &str) -> Option<u64> {
    if head.is_empty() || head.starts_with('0') || !head.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    head.parse::<u64>().ok()
}

impl<'a> CollectionKey<'a> {
    /// `true` when `name` is a bare collection name: non-empty, with no `/`.
    ///
    /// Every DDL path that mints a collection name refuses a name this rejects.
    pub fn is_bare_name(name: &str) -> bool {
        !name.is_empty() && !name.contains(QUALIFIER_SEPARATOR)
    }

    /// Build a key from the bare name the catalog keys the collection by.
    ///
    /// `name` must be the catalog name, never a `"{database_id}/{name}"`
    /// string. A qualified name goes through [`Self::from_qualified`].
    pub fn from_bare(database_id: DatabaseId, name: &'a str) -> Self {
        Self { database_id, name }
    }

    /// Build a key from a [`QualifiedCollection`], stripping its qualifier.
    pub fn from_qualified(
        database_id: DatabaseId,
        qualified: &'a QualifiedCollection,
    ) -> Result<Self, CollectionKeyError> {
        Self::from_qualified_str(database_id, qualified.as_str())
    }

    /// Build a key from a database-qualified name carried as a string on a
    /// plan, a WAL record, or the wire.
    ///
    /// The default database takes a bare name with no `/`. Any other
    /// database takes exactly `"{database_id}/{name}"`, with a bare `name`.
    /// The empty string marks a plan with no routing collection in every
    /// database, so it maps to the empty key. Every other form is an error.
    pub fn from_qualified_str(
        database_id: DatabaseId,
        qualified: &'a str,
    ) -> Result<Self, CollectionKeyError> {
        if qualified.is_empty() {
            return Ok(Self::from_bare(database_id, qualified));
        }
        let not_qualified = || CollectionKeyError::NotQualified {
            database_id: database_id.as_u64(),
            name: qualified.to_owned(),
        };
        if database_id == DatabaseId::DEFAULT {
            return if Self::is_bare_name(qualified) {
                Ok(Self::from_bare(database_id, qualified))
            } else {
                Err(not_qualified())
            };
        }
        let (head, bare) = qualified
            .split_once(QUALIFIER_SEPARATOR)
            .ok_or_else(not_qualified)?;
        match parse_qualifier(head) {
            Some(raw) if raw == database_id.as_u64() && Self::is_bare_name(bare) => {
                Ok(Self::from_bare(database_id, bare))
            }
            _ => Err(not_qualified()),
        }
    }

    /// Build a key from a relation name a physical plan reports.
    ///
    /// A plan names a collection in the form [`QualifiedCollection::new`]
    /// writes. It names a provider relation (a catalog table or a continuous
    /// aggregate) and an array bare. No relation name holds `/`, so a name
    /// without `/` is bare in every database, and a name with `/` must
    /// carry exactly this database's qualifier. Every other form is an error.
    pub fn from_relation_name(
        database_id: DatabaseId,
        name: &'a str,
    ) -> Result<Self, CollectionKeyError> {
        if Self::is_bare_name(name) {
            return Ok(Self::from_bare(database_id, name));
        }
        Self::from_qualified_str(database_id, name)
    }

    /// The database the collection lives in.
    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// The bare catalog name.
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// The database-qualified name storage engines key data by.
    pub fn qualified(&self) -> QualifiedCollection {
        QualifiedCollection::new(self.database_id, self.name)
    }

    /// The vShard this collection homes to.
    pub fn vshard(&self) -> super::VShardId {
        super::VShardId::from_collection(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::new(1024);

    #[test]
    fn qualified_input_yields_the_bare_key() {
        let qualified = QualifiedCollection::new(DB, "users");
        let from_qualified = CollectionKey::from_qualified(DB, &qualified).expect("qualified");
        assert_eq!(from_qualified.name(), "users");
        assert_eq!(from_qualified, CollectionKey::from_bare(DB, "users"));
        assert_eq!(
            from_qualified.vshard(),
            CollectionKey::from_bare(DB, "users").vshard()
        );
    }

    #[test]
    fn qualified_string_cannot_become_a_key_without_dequalifying() {
        // The only string-accepting constructors are `from_bare`, which the
        // caller names explicitly, and the qualified constructors, which strip
        // the qualifier. The qualified path therefore always lands on the bare
        // name, never on the qualified string.
        let qualified = QualifiedCollection::new(DB, "users");
        let key = CollectionKey::from_qualified_str(DB, qualified.as_str()).expect("qualified");
        assert_ne!(key.name(), qualified.as_str());
        assert_eq!(key.name(), "users");
        assert_eq!(key.qualified(), qualified);
    }

    #[test]
    fn default_database_names_are_already_bare() {
        let key = CollectionKey::from_qualified_str(DatabaseId::DEFAULT, "users").expect("default");
        assert_eq!(key, CollectionKey::from_bare(DatabaseId::DEFAULT, "users"));
    }

    #[test]
    fn empty_name_is_the_no_collection_sentinel_in_every_database() {
        let key = CollectionKey::from_qualified_str(DB, "").expect("empty");
        assert_eq!(key, CollectionKey::from_bare(DB, ""));
    }

    #[test]
    fn a_nested_qualifier_is_rejected() {
        let qualified = QualifiedCollection::new(DB, "1024/nested");
        assert!(CollectionKey::from_qualified(DB, &qualified).is_err());
        assert!(CollectionKey::from_qualified_str(DatabaseId::new(7), "1024/x").is_err());
        assert!(CollectionKey::from_qualified_str(DB, "1024/a/b").is_err());
    }

    #[test]
    fn a_default_database_name_with_a_separator_is_rejected() {
        let err =
            CollectionKey::from_qualified_str(DatabaseId::DEFAULT, "7/x").expect_err("separator");
        assert_eq!(
            err,
            CollectionKeyError::NotQualified {
                database_id: 0,
                name: "7/x".to_owned(),
            }
        );
        assert!(CollectionKey::from_qualified_str(DatabaseId::DEFAULT, "/x").is_err());
        assert!(CollectionKey::from_qualified_str(DatabaseId::DEFAULT, "x/").is_err());
    }

    #[test]
    fn an_empty_tail_is_rejected() {
        assert!(CollectionKey::from_qualified_str(DB, "1024/").is_err());
    }

    #[test]
    fn unqualified_name_in_a_named_database_is_rejected() {
        let err = CollectionKey::from_qualified_str(DB, "users").expect_err("unqualified");
        assert_eq!(
            err,
            CollectionKeyError::NotQualified {
                database_id: 1024,
                name: "users".to_owned(),
            }
        );
    }

    #[test]
    fn foreign_database_qualifier_is_rejected() {
        assert!(CollectionKey::from_qualified_str(DB, "7/users").is_err());
        assert!(CollectionKey::from_qualified_str(DB, "+1024/users").is_err());
        assert!(CollectionKey::from_qualified_str(DB, "01024/users").is_err());
        assert!(CollectionKey::from_qualified_str(DB, "/users").is_err());
        assert!(CollectionKey::from_qualified_str(DB, "0/users").is_err());
    }

    #[test]
    fn a_relation_name_is_bare_or_carries_this_database_qualifier() {
        let db = DatabaseId::new(7);
        let key = CollectionKey::from_relation_name(db, "7/orders").expect("qualified");
        assert_eq!(key, CollectionKey::from_bare(db, "orders"));
        let key = CollectionKey::from_relation_name(db, "_system.audit_log").expect("bare");
        assert_eq!(key.name(), "_system.audit_log");
        let key = CollectionKey::from_relation_name(db, "").expect("sentinel");
        assert_eq!(key.name(), "");
        for name in ["8/orders", "7/a/b", "7/", "x/orders"] {
            assert!(
                CollectionKey::from_relation_name(db, name).is_err(),
                "{name}"
            );
        }
        assert!(CollectionKey::from_relation_name(DatabaseId::DEFAULT, "7/orders").is_err());
    }

    #[test]
    fn bare_names_hold_no_separator() {
        assert!(CollectionKey::is_bare_name("users"));
        assert!(!CollectionKey::is_bare_name(""));
        assert!(!CollectionKey::is_bare_name("7/users"));
        assert!(!CollectionKey::is_bare_name("users/"));
    }

    #[test]
    fn bare_and_qualified_hashes_differ_for_a_named_database() {
        // The two strings name one collection. Only the key keeps them on one
        // vShard, so hashing the qualified string directly is a routing error.
        let mut differs = false;
        for i in 0..64 {
            let name = format!("coll_{i}");
            let qualified = QualifiedCollection::new(DB, &name);
            let key = CollectionKey::from_qualified(DB, &qualified).expect("qualified");
            let raw_qualified = CollectionKey::from_bare(DB, qualified.as_str());
            assert_eq!(key.vshard(), CollectionKey::from_bare(DB, &name).vshard());
            differs |= key.vshard() != raw_qualified.vshard();
        }
        assert!(differs);
    }
}
