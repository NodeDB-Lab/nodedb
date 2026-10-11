// SPDX-License-Identifier: BUSL-1.1

//! The reply a staged Calvin transaction answers with when its redo record
//! installs.

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::{RowIdentity, Surrogate};

use super::target::RowEngine;

/// The reply a Calvin transaction's install answers with.
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
    /// `RETURNING` rows the install reads from base after it writes, so each
    /// row is exactly what a `SELECT` reads.
    PostImages(PostImages),
    /// `RETURNING` rows of a resolved timeseries ingest: the rows its install
    /// stored, as a scan reads them. Rows the install rejected are reported
    /// beside them.
    InstalledTimeseries(InstalledTimeseries),
}

impl Default for CalvinReply {
    fn default() -> Self {
        Self::Count(Vec::new())
    }
}

impl CalvinReply {
    /// A reply the install cannot render: post-images of a columnar row, which
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

/// The `RETURNING` projection of a resolved timeseries ingest, rendered from
/// what its own install stored into `collection`.
#[derive(Debug)]
pub(in crate::data::executor) struct InstalledTimeseries {
    pub(super) spec: ReturningSpec,
    pub(super) rls_filters: Vec<u8>,
    pub(super) collection: String,
    /// The ingest's position among the transaction's timeseries ingests on
    /// this vShard. Each ingest becomes one redo sub-record in plan order,
    /// and its install is the install at this position.
    pub(super) ordinal: usize,
}

/// What staging a Calvin transaction's plans carries from one plan to the
/// next.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct CalvinStaging {
    /// The reply the plans staged so far answer with.
    pub(in crate::data::executor) reply: CalvinReply,
    /// The timeseries ingests staged so far.
    pub(super) ts_ingests: usize,
    /// Whether a plan the statement names, not a derived one, decided the
    /// count in `reply`.
    pub(super) user_count: bool,
    /// The edges this home owns among the edge writes staged so far.
    pub(super) owned_edges: u64,
}

/// The rows of a `RETURNING` plan the install reads after it writes.
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
