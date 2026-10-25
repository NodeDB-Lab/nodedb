// SPDX-License-Identifier: BUSL-1.1

//! `RangeFetch`: one transport's byte-range reads, and the bound on how
//! many run at once.

use std::future::Future;

use futures::{StreamExt as _, TryStreamExt as _};
use nodedb_raster::ByteRange;

use super::error::FetchError;

/// Most range fetches one load runs at once.
pub const MAX_IN_FLIGHT: usize = 8;

/// Reads byte ranges of raster source objects. Each answer is the range's
/// bytes, fewer where the object ends, or `None` when the object does not
/// exist. The raster readers bound every range they name.
pub trait RangeFetch: Send + Sync {
    /// Fetch `ranges`, answering in order.
    fn fetch(
        &self,
        ranges: &[ByteRange],
    ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, FetchError>> + Send;
}

/// Run `fetch_one` over `ranges` with at most [`MAX_IN_FLIGHT`] in flight,
/// answering in order. The first error ends the run.
pub async fn fetch_bounded<F, Fut>(
    ranges: &[ByteRange],
    fetch_one: F,
) -> Result<Vec<Option<Vec<u8>>>, FetchError>
where
    F: Fn(ByteRange) -> Fut,
    Fut: Future<Output = Result<Option<Vec<u8>>, FetchError>>,
{
    futures::stream::iter(ranges.iter().copied().map(fetch_one))
        .buffered(MAX_IN_FLIGHT)
        .try_collect()
        .await
}
