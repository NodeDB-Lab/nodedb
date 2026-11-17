// SPDX-License-Identifier: BUSL-1.1

//! Shared full-text extraction: a document's declared top-level string
//! fields, the text the inverted index analyzes per field and as a whole.

use nodedb_fts::DocumentText;

use crate::data::executor::core_loop::CoreLoop;

/// Collect the top-level string-valued fields of a document object that
/// `text_fields` names, as the text the full-text inverted index indexes.
///
/// `text_fields` is the collection's declared field list, sorted and deduped.
/// The whole-document text joins only these fields. Every path that indexes
/// document text calls this, so a put, an update, a rollback, and a staged
/// overlay produce identical text and identical postings. Non-object values,
/// non-string fields, and undeclared fields contribute nothing.
pub(in crate::data::executor) fn extract_fts_fields(
    doc: &serde_json::Value,
    text_fields: &[String],
) -> DocumentText {
    match doc.as_object() {
        Some(obj) => DocumentText::from_fields(obj.iter().filter_map(|(k, v)| {
            if text_fields.binary_search(k).is_err() {
                return None;
            }
            v.as_str().map(|s| (k.clone(), s.to_owned()))
        })),
        None => DocumentText::default(),
    }
}

/// Keep the `(field, text)` pairs whose field `text_fields` names.
///
/// `text_fields` is sorted and deduped. Used where the text arrives already
/// extracted, as on a sync frame.
pub(in crate::data::executor) fn declared_pairs(
    fields: &[(String, String)],
    text_fields: &[String],
) -> DocumentText {
    DocumentText::from_fields(
        fields
            .iter()
            .filter(|(name, _)| text_fields.binary_search(name).is_ok())
            .cloned(),
    )
}

impl CoreLoop {
    /// The full-text fields a write to a collection indexes, sorted and
    /// deduped: the fields of every building or ready index.
    ///
    /// Empty when the collection declares no full-text index or has no
    /// registered config. An empty list means a write touches no
    /// inverted-index state. The lookup borrows `collection`.
    pub(in crate::data::executor) fn declared_text_fields(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> &[String] {
        self.doc_config(database_id, tid, collection)
            .map(|config| config.text_fields.indexed())
            .unwrap_or(&[])
    }

    /// Whether a delete or an undo on `collection` touches inverted-index
    /// state. It does when the collection declares a text field, or while
    /// a rebuild of the collection is open: the rebuild's journal must
    /// note the document, so the cutover derives it again.
    pub(in crate::data::executor) fn removal_touches_text(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        !self
            .declared_text_fields(database_id, tid, collection)
            .is_empty()
            || self.inverted.has_open_rebuild(
                database_id,
                crate::types::TenantId::new(tid),
                collection,
            )
    }

    /// [`Self::declared_text_fields`] for a collection named by its
    /// `doc_configs` key.
    pub(in crate::data::executor) fn declared_text_fields_of(
        &self,
        key: &crate::data::executor::core_loop::doc_config_key::DocConfigKey,
    ) -> &[String] {
        self.doc_configs
            .get(key)
            .map(|config| config.text_fields.indexed())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn only_declared_top_level_strings_are_collected_in_field_order() {
        let doc = serde_json::json!({
            "title": "Rust",
            "body": "fearless",
            "n": 3,
            "nested": {"inner": "skip"},
        });
        let text = extract_fts_fields(&doc, &fields(&["body", "n", "nested", "title"]));
        assert_eq!(
            text.fields(),
            &[
                ("body".to_string(), "fearless".to_string()),
                ("title".to_string(), "Rust".to_string()),
            ]
        );
        assert_eq!(text.whole(), "fearless Rust");
    }

    #[test]
    fn an_undeclared_string_field_is_dropped() {
        let doc = serde_json::json!({"title": "Rust", "body": "fearless"});
        let text = extract_fts_fields(&doc, &fields(&["title"]));
        assert_eq!(text.fields(), &[("title".to_string(), "Rust".to_string())]);
        assert_eq!(text.whole(), "Rust");
    }

    #[test]
    fn an_empty_field_list_yields_empty_text() {
        let doc = serde_json::json!({"title": "Rust", "body": "fearless"});
        let text = extract_fts_fields(&doc, &[]);
        assert!(text.fields().is_empty());
        assert!(text.whole().is_empty());
    }

    #[test]
    fn a_non_object_has_no_text() {
        let declared = fields(&["title"]);
        assert!(extract_fts_fields(&serde_json::json!("plain"), &declared).is_empty());
        assert!(
            extract_fts_fields(&serde_json::json!(null), &declared)
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn declared_pairs_keeps_only_declared_fields() {
        let pairs = vec![
            ("title".to_string(), "Rust".to_string()),
            ("body".to_string(), "fearless".to_string()),
        ];
        let text = declared_pairs(&pairs, &fields(&["body"]));
        assert_eq!(
            text.fields(),
            &[("body".to_string(), "fearless".to_string())]
        );
        assert!(declared_pairs(&pairs, &[]).fields().is_empty());
    }
}
