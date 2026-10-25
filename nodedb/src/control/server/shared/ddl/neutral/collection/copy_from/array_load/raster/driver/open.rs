// SPDX-License-Identifier: BUSL-1.1

//! Open every source of a load in shared rounds.
//!
//! Each round asks every source still opening for the ranges it needs,
//! merges the ranges that lie close together, and fetches them in one
//! batch. A cloud-optimised GeoTIFF names every IFD in its first 64 KiB, so
//! a load of COGs opens in one round: one request per source.

use nodedb_raster::{
    COALESCE_GAP, Lookup, Opened, RangeCache, RasterError, RasterLimits, RasterOpener, TiffOpener,
    TiffReader, coalesce,
};

use super::super::fetch::RangeFetch;
use super::error::DriveError;

/// Rounds before an open gives up.
const MAX_OPEN_ROUNDS: usize = 64;

/// Open the sources `openers` name, one per object, under `limits`.
pub async fn open_sources<F: RangeFetch>(
    fetch: &F,
    openers: &[TiffOpener],
    limits: RasterLimits,
) -> Result<Vec<TiffReader>, DriveError> {
    let mut caches: Vec<RangeCache> = openers
        .iter()
        .map(|_| RangeCache::new(limits.header_bytes))
        .collect();
    let mut readers: Vec<Option<TiffReader>> = vec![None; openers.len()];
    for _ in 0..MAX_OPEN_ROUNDS {
        let mut need = Vec::new();
        let slots = openers.iter().zip(&caches).zip(readers.iter_mut());
        for (index, ((opener, cache), slot)) in slots.enumerate() {
            if slot.is_some() {
                continue;
            }
            let opened = opener
                .open(cache, limits)
                .map_err(|error| DriveError::Raster { index, error })?;
            match opened {
                Opened::Ready(reader) => *slot = Some(reader),
                Opened::Need(ranges) => need.extend(
                    ranges
                        .into_iter()
                        .filter(|range| matches!(cache.get(*range), Lookup::Missing)),
                ),
            }
        }
        if readers.iter().all(Option::is_some) {
            return Ok(readers.into_iter().flatten().collect());
        }
        if need.is_empty() {
            let index = readers.iter().position(Option::is_none).unwrap_or(0);
            return Err(stalled(index));
        }
        let merged = coalesce(need, COALESCE_GAP);
        let answers = fetch.fetch(&merged).await?;
        for (range, bytes) in merged.into_iter().zip(answers) {
            let index = range.object.0 as usize;
            let Some(cache) = caches.get_mut(index) else {
                continue;
            };
            cache
                .insert(range, bytes)
                .map_err(|error| DriveError::Raster { index, error })?;
        }
    }
    let index = readers.iter().position(Option::is_none).unwrap_or(0);
    Err(stalled(index))
}

fn stalled(index: usize) -> DriveError {
    DriveError::Raster {
        index,
        error: RasterError::OpenStalled {
            rounds: MAX_OPEN_ROUNDS,
        },
    }
}
