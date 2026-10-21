// SPDX-License-Identifier: BUSL-1.1

//! `SourceSet`: the sources of one load, one transport per object. Source
//! `i` of the statement is object `i` of every range the readers name.

use nodedb_raster::ByteRange;

use super::error::FetchError;
use super::http::HttpRange;
use super::local::LocalFile;
use super::store::ObjectStoreRange;
use super::traits::{RangeFetch, fetch_bounded};

/// The transport of one source.
pub enum SourceFetch {
    Local(LocalFile),
    Http(HttpRange),
    Store(ObjectStoreRange),
}

impl SourceFetch {
    async fn fetch_one(&self, range: ByteRange) -> Result<Option<Vec<u8>>, FetchError> {
        match self {
            Self::Local(local) => local.fetch_one(range).await,
            Self::Http(http) => http.fetch_one(range).await,
            Self::Store(store) => store.fetch_one(range).await,
        }
    }
}

/// Every source of one load.
pub struct SourceSet {
    sources: Vec<SourceFetch>,
}

impl SourceSet {
    /// `sources`, in statement order.
    pub fn new(sources: Vec<SourceFetch>) -> Self {
        Self { sources }
    }

    async fn route(&self, range: ByteRange) -> Result<Option<Vec<u8>>, FetchError> {
        // A range of an object no source holds names a missing object.
        match self.sources.get(range.object.0 as usize) {
            Some(source) => source.fetch_one(range).await,
            None => Ok(None),
        }
    }
}

impl RangeFetch for SourceSet {
    async fn fetch(&self, ranges: &[ByteRange]) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
        fetch_bounded(ranges, |range| self.route(range)).await
    }
}
