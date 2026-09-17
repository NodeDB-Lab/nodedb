// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::SurrogateBitmap;

/// Whether `doc_id`'s surrogate is a member of `prefilter`.
///
/// `doc_id` is the hex-encoded surrogate for a document-collection row, or
/// a columnar-family user `id` that never parses as a storage key — the
/// latter is never admitted, matching a sparse miss on a parsed key.
pub(super) fn prefilter_admits(prefilter: &SurrogateBitmap, doc_id: &str) -> bool {
    match nodedb_types::StorageKey::parse(doc_id) {
        Some(key) => prefilter.contains(key.surrogate()),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::prefilter_admits;
    use nodedb_types::{Surrogate, SurrogateBitmap};

    fn doc_id(surrogate: u32) -> String {
        nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)).to_string()
    }

    #[test]
    fn prefilter_skips_non_member_doc_ids() {
        // Direct unit on the production prefilter check (`prefilter_admits`),
        // not a re-implementation of it.
        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(2));

        let candidate_doc_ids = [doc_id(1), doc_id(2), doc_id(3)];

        let kept: Vec<_> = candidate_doc_ids
            .iter()
            .filter(|doc_id| prefilter_admits(&bitmap, doc_id))
            .cloned()
            .collect();

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], doc_id(2));
    }
}
