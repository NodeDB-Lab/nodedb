// SPDX-License-Identifier: BUSL-1.1

//! Shared full-text extraction: a document's top-level string fields, the
//! text the inverted index analyzes per field and as a whole.

use nodedb_fts::DocumentText;

/// Collect every top-level string-valued field of a document object as the
/// text the full-text inverted index indexes.
///
/// Used by the forward PUT indexing path AND by DELETE-rollback re-indexing so
/// both produce identical text (and therefore identical postings, since
/// `nodedb_fts::analyze` is deterministic). Non-object values and non-string
/// fields contribute nothing.
pub(in crate::data::executor) fn extract_fts_fields(doc: &serde_json::Value) -> DocumentText {
    match doc.as_object() {
        Some(obj) => DocumentText::from_fields(
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned()))),
        ),
        None => DocumentText::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_top_level_strings_are_collected_in_field_order() {
        let doc = serde_json::json!({
            "title": "Rust",
            "body": "fearless",
            "n": 3,
            "nested": {"inner": "skip"},
        });
        let text = extract_fts_fields(&doc);
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
    fn a_non_object_has_no_text() {
        assert!(extract_fts_fields(&serde_json::json!("plain")).is_empty());
        assert!(
            extract_fts_fields(&serde_json::json!(null))
                .fields()
                .is_empty()
        );
    }
}
