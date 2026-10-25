// SPDX-License-Identifier: Apache-2.0

//! The relation a Control-Plane-orchestrated `INSERT ... SELECT`, `MERGE`, or
//! `UPDATE ... FROM` reads its source rows from.

use nodedb_types::QualifiedCollection;

use crate::physical_plan::PhysicalPlan;

/// Source relation of an orchestrated copy or join write.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RowSource {
    /// A plain document scan, read page by page through
    /// `DocumentOp::MaterializeScan`. The B-tree key cursor is stable under
    /// concurrent writes, so the read has no row-count bound. The Control
    /// Plane applies the WHERE filter, the offset, the limit, and the column
    /// map to each page.
    DocumentPages(DocumentPageSource),
    /// Any other SELECT, lowered by the normal plan converter and read once
    /// through the Data Plane dispatch funnel. It has exactly SELECT
    /// semantics: engine, filters, ordering, distinct, windows, joins, `AS
    /// OF`, and read policies all come from the normal pipeline, under the
    /// normal result-byte budget.
    Query(QuerySource),
}

/// A plain document scan source, read page by page.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DocumentPageSource {
    pub collection: QualifiedCollection,
    /// Serialized `Vec<ScanFilter>`: the WHERE clause, ANDed with the
    /// collection's read policy by the RLS pass.
    pub filters: Vec<u8>,
    /// `Current`, or the `AS OF SYSTEM TIME` cut every page reads at. Never
    /// `AllVersions`.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Matching rows skipped before the first row is read.
    pub offset: usize,
    /// Matching rows read after the offset. `None` reads every matching row.
    pub limit: Option<usize>,
    /// zerompk `Vec<ComputedColumn>`: one entry per target column, its alias
    /// the target column and its expression the source-row expression.
    /// Empty passes each source row through unchanged.
    pub column_map: Vec<u8>,
}

/// A source SELECT read once as a whole relation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct QuerySource {
    /// The lowered SELECT.
    pub plan: Box<PhysicalPlan>,
    /// zerompk `Vec<ComputedColumn>` that renames each output column to the
    /// target column it writes. Empty passes each output row through
    /// unchanged.
    pub column_map: Vec<u8>,
}

impl RowSource {
    /// A page source that reads every current row of `collection` unchanged.
    pub fn whole_collection(collection: QualifiedCollection) -> Self {
        RowSource::DocumentPages(DocumentPageSource {
            collection,
            filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            offset: 0,
            limit: None,
            column_map: Vec::new(),
        })
    }

    /// The collection a page source scans. `None` for a query source, whose
    /// collections are the ones its plan reads.
    pub fn paged_collection(&self) -> Option<&QualifiedCollection> {
        match self {
            RowSource::DocumentPages(page) => Some(&page.collection),
            RowSource::Query(_) => None,
        }
    }

    /// The plan a query source runs. `None` for a page source.
    pub fn query_plan(&self) -> Option<&PhysicalPlan> {
        match self {
            RowSource::DocumentPages(_) => None,
            RowSource::Query(query) => Some(&query.plan),
        }
    }

    /// The plan a query source runs, for an in-place rewrite. `None` for a
    /// page source.
    pub fn query_plan_mut(&mut self) -> Option<&mut PhysicalPlan> {
        match self {
            RowSource::DocumentPages(_) => None,
            RowSource::Query(query) => Some(&mut query.plan),
        }
    }

    /// A short label for logs and error messages: the paged collection, or
    /// the primary collection of the query plan.
    pub fn label(&self) -> &str {
        match self {
            RowSource::DocumentPages(page) => page.collection.as_str(),
            RowSource::Query(query) => query.plan.collection().unwrap_or("<query>"),
        }
    }
}
