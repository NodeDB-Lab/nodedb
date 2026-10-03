// SPDX-License-Identifier: BUSL-1.1

//! Fixtures for the inverted index's inline tests.

use nodedb_fts::DocumentText;

/// A document whose only string field is `body`.
pub(crate) fn body(text: &str) -> DocumentText {
    fields(&[("body", text)])
}

/// A document with the given `(field, text)` pairs.
pub(crate) fn fields(pairs: &[(&str, &str)]) -> DocumentText {
    DocumentText::from_fields(
        pairs
            .iter()
            .map(|(f, t)| ((*f).to_string(), (*t).to_string())),
    )
}
