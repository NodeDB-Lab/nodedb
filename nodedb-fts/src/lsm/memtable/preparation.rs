// SPDX-License-Identifier: Apache-2.0

//! Native memtable copies and retained allocation accounting.

use super::Memtable;
use crate::retained_size::{hash, strings, sum, vector};

impl Memtable {
    pub(crate) fn clone_for_preparation(&self) -> Self {
        Self {
            scopes: self.scopes.clone(),
            total_postings: self.total_postings.clone(),
            total_terms: self.total_terms.clone(),
            config: self.config,
        }
    }

    pub(crate) fn estimated_clone_bytes(&self) -> usize {
        let scopes = self.scopes.borrow();
        sum([
            hash(&scopes),
            sum(scopes.values().map(|collections| {
                sum([
                    hash(collections),
                    strings(collections.keys()),
                    sum(collections.values().map(|fields| {
                        sum([
                            hash(fields),
                            strings(fields.keys()),
                            sum(fields.values().map(|state| {
                                sum([
                                    hash(&state.postings),
                                    strings(state.postings.keys()),
                                    vector(&state.fieldnorms),
                                    vector(&state.fieldnorms_exact),
                                    sum(state.postings.values().map(|postings| {
                                        sum([
                                            vector(postings),
                                            sum(postings
                                                .iter()
                                                .map(|posting| vector(&posting.positions))),
                                        ])
                                    })),
                                ])
                            })),
                        ])
                    })),
                ])
            })),
        ])
    }
}
