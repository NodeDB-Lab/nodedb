// SPDX-License-Identifier: Apache-2.0

//! A document's indexable text: its top-level string fields, in field-name order.

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::scope::IndexScope;

/// The `(field, text)` pairs of one document, sorted by field name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentText {
    /// Sorted by field name, names unique.
    fields: Vec<(String, String)>,
}

impl DocumentText {
    /// Collect `(field, text)` pairs. A repeated name keeps its last text.
    pub fn from_fields<I: IntoIterator<Item = (String, String)>>(fields: I) -> Self {
        let sorted: BTreeMap<String, String> = fields.into_iter().collect();
        Self {
            fields: sorted.into_iter().collect(),
        }
    }

    /// The `(field, text)` pairs, sorted by field name.
    pub fn fields(&self) -> &[(String, String)] {
        &self.fields
    }

    /// Consume into the sorted `(field, text)` pairs.
    pub fn into_fields(self) -> Vec<(String, String)> {
        self.fields
    }

    /// Whether no field holds any text.
    pub fn is_empty(&self) -> bool {
        self.fields.iter().all(|(_, t)| t.is_empty())
    }

    /// Every field's text, in field-name order, joined by one space.
    pub fn whole(&self) -> String {
        let mut out = String::new();
        for (i, (_, text)) in self.fields.iter().enumerate() {
            if i != 0 {
                out.push(' ');
            }
            out.push_str(text);
        }
        out
    }

    /// Each field that has an index of its own, with its scope.
    pub fn field_scopes<'s>(
        &'s self,
        collection: &'s str,
    ) -> impl Iterator<Item = (IndexScope<'s>, &'s str)> {
        self.fields
            .iter()
            .filter_map(move |(f, t)| IndexScope::field(collection, f).map(|s| (s, t.as_str())))
    }

    /// The text one index holds for this document: empty when the field is absent.
    pub fn text_of(&self, index: IndexScope<'_>) -> Cow<'_, str> {
        match index.field_name() {
            None => Cow::Owned(self.whole()),
            Some(f) => Cow::Borrowed(
                self.fields
                    .iter()
                    .find(|(n, _)| n == f)
                    .map_or("", |(_, t)| t.as_str()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs() -> Vec<(String, String)> {
        vec![
            ("title".into(), "Rust book".into()),
            ("body".into(), "fearless concurrency".into()),
            ("author".into(), "".into()),
            ("zeta".into(), "last".into()),
        ]
    }

    /// The join every deployment indexed before per-field scopes: string
    /// values sorted by field name, joined by one space.
    fn reference_join(mut texts: Vec<(String, String)>) -> String {
        texts.sort_by(|a, b| a.0.cmp(&b.0));
        texts
            .into_iter()
            .map(|(_, t)| t)
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn whole_matches_the_field_name_ordered_join() {
        let text = DocumentText::from_fields(pairs());
        assert_eq!(text.whole(), reference_join(pairs()));
        assert_eq!(text.whole(), " fearless concurrency Rust book last");
    }

    #[test]
    fn repeated_name_keeps_last_text() {
        let text = DocumentText::from_fields([
            ("a".to_string(), "one".to_string()),
            ("a".to_string(), "two".to_string()),
        ]);
        assert_eq!(text.fields(), &[("a".to_string(), "two".to_string())]);
    }

    #[test]
    fn text_of_resolves_document_and_field_scopes() {
        let text = DocumentText::from_fields(pairs());
        let title = IndexScope::field("c", "title").expect("non-empty field");
        let missing = IndexScope::field("c", "missing").expect("non-empty field");
        assert_eq!(text.text_of(title), "Rust book");
        assert_eq!(text.text_of(missing), "");
        assert_eq!(text.text_of(IndexScope::document("c")), text.whole());
    }

    #[test]
    fn field_scopes_skip_empty_names() {
        let text = DocumentText::from_fields([
            (String::new(), "orphan".to_string()),
            ("title".to_string(), "kept".to_string()),
        ]);
        let scopes: Vec<_> = text.field_scopes("c").collect();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].0.field_name(), Some("title"));
        assert_eq!(scopes[0].1, "kept");
    }

    #[test]
    fn empty_when_no_field_has_text() {
        assert!(DocumentText::default().is_empty());
        assert!(DocumentText::from_fields([("a".to_string(), String::new())]).is_empty());
        assert!(!DocumentText::from_fields(pairs()).is_empty());
    }
}
