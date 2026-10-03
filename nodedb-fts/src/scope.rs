// SPDX-License-Identifier: Apache-2.0

//! The inverted index a read or write addresses: a collection's
//! whole-document text, or one of its fields.

/// One inverted index of a collection. `field` is empty for the
/// whole-document index. A field index always has a non-empty name, so no
/// field can address the whole-document index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexScope<'a> {
    collection: &'a str,
    field: &'a str,
}

impl<'a> IndexScope<'a> {
    /// The whole-document index of `collection`.
    pub const fn document(collection: &'a str) -> Self {
        Self {
            collection,
            field: "",
        }
    }

    /// The index of `field`. `None` for an empty name: it has no index of its own.
    pub fn field(collection: &'a str, field: &'a str) -> Option<Self> {
        (!field.is_empty()).then_some(Self { collection, field })
    }

    /// The collection that owns this index.
    pub const fn collection(&self) -> &'a str {
        self.collection
    }

    /// The field name. `None` for the whole-document index.
    pub fn field_name(&self) -> Option<&'a str> {
        (!self.field.is_empty()).then_some(self.field)
    }

    /// Storage key component: empty for the whole-document index.
    pub const fn field_key(&self) -> &'a str {
        self.field
    }

    /// The scope a stored `(collection, field_key)` pair names.
    pub const fn from_key(collection: &'a str, field_key: &'a str) -> Self {
        Self {
            collection,
            field: field_key,
        }
    }
}

impl<'a> From<&'a str> for IndexScope<'a> {
    fn from(collection: &'a str) -> Self {
        Self::document(collection)
    }
}

impl<'a> From<&'a String> for IndexScope<'a> {
    fn from(collection: &'a String) -> Self {
        Self::document(collection.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_field_name_has_no_scope() {
        assert_eq!(IndexScope::field("docs", ""), None);
    }

    #[test]
    fn document_and_field_scopes_never_alias() {
        let doc = IndexScope::document("docs");
        let field = IndexScope::field("docs", "title").expect("non-empty field");
        assert_ne!(doc, field);
        assert_eq!(doc.field_name(), None);
        assert_eq!(doc.field_key(), "");
        assert_eq!(field.field_name(), Some("title"));
        assert_eq!(field.collection(), "docs");
    }

    #[test]
    fn from_key_round_trips_both_kinds() {
        let doc = IndexScope::document("docs");
        assert_eq!(IndexScope::from_key("docs", doc.field_key()), doc);
        let field = IndexScope::field("docs", "body").expect("non-empty field");
        assert_eq!(IndexScope::from_key("docs", field.field_key()), field);
    }

    #[test]
    fn str_converts_to_document_scope() {
        let scope: IndexScope<'_> = "docs".into();
        assert_eq!(scope, IndexScope::document("docs"));
    }
}
