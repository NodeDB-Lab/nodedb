// SPDX-License-Identifier: BUSL-1.1

//! Staged-document text for the full-text reads of an open transaction.
//!
//! A transaction's document writes are not in the inverted index until it
//! commits. A full-text read inside the transaction tokenizes each staged
//! body with the collection's configured analyzer, the same resolution
//! (`InvertedIndex::analyze_for_collection`) the forward indexing path uses,
//! so a staged document is tokenized identically whether it is still staged
//! or already committed.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_fields;
use crate::types::{DatabaseId, TenantId};
use nodedb_fts::IndexScope;

impl CoreLoop {
    /// Decode a staged body via the collection's storage mode and analyze the
    /// text `index` holds (one field, or the whole document) with the
    /// collection's analyzer.
    ///
    /// `Ok(None)` means the document holds no text in `index`: the
    /// collection is unregistered, the index's text is empty (which the
    /// forward indexer never indexes either), or the analyzer produced no
    /// tokens. An undecodable body or an analyzer resolution error is `Err`:
    /// a staged row that silently drops out of the read is the opposite of
    /// read-your-own-writes.
    pub(in crate::data::executor) fn tokenize_staged_body(
        &self,
        database_id: u64,
        config_key: &(DatabaseId, TenantId, String),
        index: IndexScope<'_>,
        body: &[u8],
    ) -> crate::Result<Option<Vec<String>>> {
        let Some(doc) = self.decode_indexed_body(config_key, body)? else {
            return Ok(None);
        };
        let fields = extract_fts_fields(&doc);
        let text = fields.text_of(index);
        if text.is_empty() {
            return Ok(None);
        }
        let (_, tid, collection) = config_key;
        let tokens = self
            .inverted
            .analyze_for_collection(database_id, *tid, collection, &text)?;
        Ok((!tokens.is_empty()).then_some(tokens))
    }
}

/// Return the earliest start index at which `phrase` occurs as a contiguous,
/// in-order subsequence of `tokens`, or `None` if it never does. Adjacency
/// is exact (zero slop), matching the durable phrase search.
pub(in crate::data::executor) fn earliest_contiguous_match(
    tokens: &[String],
    phrase: &[String],
) -> Option<u32> {
    if phrase.is_empty() || phrase.len() > tokens.len() {
        return None;
    }
    let last_start = tokens.len() - phrase.len();
    (0..=last_start)
        .find(|start| {
            tokens[*start..*start + phrase.len()]
                .iter()
                .zip(phrase)
                .all(|(a, b)| a == b)
        })
        .map(|start| u32::try_from(start).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::earliest_contiguous_match;

    fn words(text: &str) -> Vec<String> {
        text.split(' ').map(str::to_string).collect()
    }

    #[test]
    fn finds_the_earliest_contiguous_run() {
        let tokens = words("a b c b c");
        assert_eq!(earliest_contiguous_match(&tokens, &words("b c")), Some(1));
        assert_eq!(earliest_contiguous_match(&tokens, &words("c a")), None);
        assert_eq!(earliest_contiguous_match(&tokens, &[]), None);
    }
}
