// SPDX-License-Identifier: Apache-2.0

//! Native memory-backend copies for isolated statement preparation.

use super::MemoryBackend;
use crate::retained_size::{hash, strings, sum, tree, vector};

impl MemoryBackend {
    pub(crate) fn clone_for_preparation(&self) -> Self {
        Self {
            postings: self.postings.clone(),
            doc_lengths: self.doc_lengths.clone(),
            stats: self.stats.clone(),
            meta: self.meta.clone(),
            segments: self.segments.clone(),
            history: self.history.clone(),
        }
    }

    pub(crate) fn estimated_clone_bytes(&self) -> usize {
        let postings = self.postings.borrow();
        let lengths = self.doc_lengths.borrow();
        let stats = self.stats.borrow();
        let meta = self.meta.borrow();
        let segments = self.segments.borrow();
        sum([
            hash(&postings),
            hash(&lengths),
            hash(&stats),
            hash(&meta),
            hash(&segments),
            sum(postings.iter().map(|((key, term), list)| {
                sum([
                    strings([&key.2, &key.3, term]),
                    tree::<(nodedb_types::Surrogate, crate::posting::Posting)>(list.len()),
                    sum(list.values().map(|posting| vector(&posting.positions))),
                ])
            })),
            sum(lengths.keys().map(|(key, _)| strings([&key.2, &key.3]))),
            sum(stats.keys().map(|key| strings([&key.2, &key.3]))),
            sum(meta
                .iter()
                .chain(segments.iter())
                .map(|((key, sub), value)| sum([strings([&key.2, &key.3, sub]), vector(value)]))),
            self.history.borrow().estimated_clone_bytes(),
        ])
    }
}
