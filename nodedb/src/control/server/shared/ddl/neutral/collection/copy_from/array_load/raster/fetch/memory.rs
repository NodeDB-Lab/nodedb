// SPDX-License-Identifier: BUSL-1.1

//! An in-memory `RangeFetch` that counts what a driver asks for.

use std::sync::Mutex;

use nodedb_raster::ByteRange;

use super::error::FetchError;
use super::traits::RangeFetch;

/// Objects held in memory. Every `fetch` call and every range it asked for
/// is recorded.
pub struct MemoryFetch {
    objects: Vec<Vec<u8>>,
    calls: Mutex<Vec<Vec<ByteRange>>>,
}

impl MemoryFetch {
    /// `objects[i]` is object `i`.
    pub fn new(objects: Vec<Vec<u8>>) -> Self {
        Self {
            objects,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// The ranges of each `fetch` call so far, in call order.
    pub fn calls(&self) -> Vec<Vec<ByteRange>> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl RangeFetch for MemoryFetch {
    async fn fetch(&self, ranges: &[ByteRange]) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(ranges.to_vec());
        Ok(ranges
            .iter()
            .map(|range| {
                self.objects.get(range.object.0 as usize).map(|bytes| {
                    let len = bytes.len() as u64;
                    let (start, end) = (range.start.min(len), range.end.min(len));
                    bytes
                        .get(start as usize..end.max(start) as usize)
                        .unwrap_or_default()
                        .to_vec()
                })
            })
            .collect())
    }
}
