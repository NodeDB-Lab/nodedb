// SPDX-License-Identifier: BUSL-1.1

//! `DriveError`: why reading the sources of a raster load failed.

use nodedb_raster::RasterError;

use super::super::fetch::FetchError;

/// A failed open or block read of a raster load.
#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    /// Source `index` (0-based, statement order) is malformed or uses a
    /// feature the reader lacks.
    #[error("source {}: {error}", index + 1)]
    Raster { index: usize, error: RasterError },
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error("raster decode task failed: {0}")]
    Task(String),
}
