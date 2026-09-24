// SPDX-License-Identifier: BUSL-1.1

//! The reply a staged Calvin transaction answers with when its flush
//! installs it.

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::{RowIdentity, Surrogate};

use super::target::RowEngine;

/// The reply a Calvin transaction's flush answers with.
///
/// The transaction answers with its last `RETURNING` plan's rows, or with
/// its last plan's affected count when no plan carries `RETURNING`. A plan
/// derived from the statement, such as a graph edge or a balance move,
/// therefore never replaces the rows the statement asked for.
#[derive(Debug)]
pub(in crate::data::executor) enum CalvinReply {
    /// An affected count, decided when the plan staged.
    Count(Vec<u8>),
    /// `RETURNING` rows, decided when the plan staged.
    Rows(Vec<u8>),
    /// `RETURNING` rows the flush reads from base after the install, so each
    /// row is exactly what a `SELECT` reads.
    PostImages(PostImages),
}

impl Default for CalvinReply {
    fn default() -> Self {
        Self::Count(Vec::new())
    }
}

impl CalvinReply {
    /// A reply the flush cannot render: post-images of a columnar row, which
    /// base keys by no surrogate.
    #[cfg(test)]
    pub(in crate::data::executor) fn unrenderable_for_test(collection: &str) -> Self {
        Self::PostImages(PostImages {
            spec: ReturningSpec {
                columns: nodedb_physical::physical_plan::ReturningColumns::Star,
            },
            rls_filters: Vec::new(),
            collection: collection.to_string(),
            engine: RowEngine::Columnar,
            rows: vec![(RowIdentity::from_user_key("r1"), Surrogate::new(1))],
        })
    }

    /// Whether this reply carries `RETURNING` rows.
    pub(super) fn has_rows(&self) -> bool {
        !matches!(self, Self::Count(_))
    }
}

/// The rows of a `RETURNING` plan the flush reads after the install.
#[derive(Debug)]
pub(in crate::data::executor) struct PostImages {
    pub(super) spec: ReturningSpec,
    pub(super) rls_filters: Vec<u8>,
    pub(super) collection: String,
    pub(super) engine: RowEngine,
    /// Each row the plan wrote: its client identity and its surrogate, in
    /// the order the plan wrote them.
    pub(super) rows: Vec<(RowIdentity, Surrogate)>,
}
