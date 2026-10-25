// SPDX-License-Identifier: BUSL-1.1

//! Paged read of a plain document scan source.
//!
//! Each page is one `DocumentOp::MaterializeScan` round trip on the source's
//! owner. The scan returns standard msgpack bodies carrying the row's `id`,
//! so a strict source reads like a schemaless one. The reader applies the
//! WHERE filter, then the offset, then the limit, then the column map, so
//! OFFSET and LIMIT count matching rows, as a SELECT does.

use nodedb_physical::physical_plan::DocumentPageSource;

use crate::bridge::scan_filter::ScanFilter;
use crate::control::maintenance::clone_materializer::scan_source_page;

use super::context::SourceReadContext;
use super::shape::RowShaper;

/// A page-by-page reader over a [`DocumentPageSource`].
pub(crate) struct PageReader {
    collection: String,
    system_time: nodedb_types::SystemTimeScope,
    filters: Vec<ScanFilter>,
    shaper: RowShaper,
    cursor: Vec<u8>,
    /// Matching rows still to skip.
    skip: usize,
    /// Matching rows still to read. `None` reads every matching row.
    remaining: Option<usize>,
    done: bool,
}

impl PageReader {
    pub(crate) fn new(page: &DocumentPageSource) -> crate::Result<Self> {
        let filters: Vec<ScanFilter> = if page.filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(&page.filters).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("source filters: {e}"),
            })?
        };
        Ok(Self {
            collection: page.collection.as_str().to_owned(),
            system_time: page.system_time,
            filters,
            shaper: RowShaper::decode(&page.column_map)?,
            cursor: Vec::new(),
            skip: page.offset,
            remaining: page.limit,
            done: page.limit == Some(0),
        })
    }

    /// Read the next page and return its shaped rows. `None` once the source
    /// is exhausted or the limit is reached. A page can hold no matching row
    /// while more pages follow.
    pub(crate) async fn next_page(
        &mut self,
        ctx: &SourceReadContext<'_>,
    ) -> crate::Result<Option<Vec<Vec<u8>>>> {
        if self.done {
            return Ok(None);
        }
        let (entries, next_cursor) = scan_source_page(
            ctx.state,
            ctx.tenant_id,
            ctx.database_id,
            &self.collection,
            &self.cursor,
            self.system_time,
            ctx.txn_id,
        )
        .await?;
        let crs = crate::control::crs_context::crs_context_for(ctx.state);
        let mut rows = Vec::with_capacity(entries.len());
        for (_doc_id, _surrogate, body) in entries {
            if self.remaining == Some(0) {
                break;
            }
            if !self.filters.is_empty()
                && !ScanFilter::all_match_binary(&self.filters, &body, &crs)?
            {
                continue;
            }
            if self.skip > 0 {
                self.skip -= 1;
                continue;
            }
            rows.push(self.shaper.shape(body, &crs)?);
            if let Some(remaining) = self.remaining.as_mut() {
                *remaining -= 1;
            }
        }
        if next_cursor.is_empty() || self.remaining == Some(0) {
            self.done = true;
        }
        self.cursor = next_cursor;
        Ok(Some(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn page(offset: usize, limit: Option<usize>) -> DocumentPageSource {
        DocumentPageSource {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            offset,
            limit,
            column_map: Vec::new(),
        }
    }

    #[test]
    fn a_zero_limit_reads_nothing() {
        let reader = PageReader::new(&page(0, Some(0))).expect("reader");
        assert!(reader.done);
    }

    #[test]
    fn offset_and_limit_start_from_the_source() {
        let reader = PageReader::new(&page(3, Some(5))).expect("reader");
        assert_eq!(reader.skip, 3);
        assert_eq!(reader.remaining, Some(5));
        assert!(!reader.done);
    }

    #[test]
    fn malformed_filters_are_an_error() {
        let mut source = page(0, None);
        source.filters = vec![0xc1];
        assert!(PageReader::new(&source).is_err());
    }
}
