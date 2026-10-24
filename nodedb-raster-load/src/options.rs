// SPDX-License-Identifier: Apache-2.0

//! The options a raster load plans from. The server converts its statement
//! options into this form, so the crate holds no SQL types for them.

use crate::window::PixelWindow;

/// Where one band of the load goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BandTarget {
    /// The band loads into this array attr.
    Attr(String),
    /// The band does not load.
    Skip,
}

/// The planning options of one load.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LoadOptions {
    /// Entry `i` names where band `i` of the sources' bands in order loads.
    /// `None` loads band `i` into attr `i`.
    pub bands: Option<Vec<BandTarget>>,
    /// The nodata of every band, in place of the file's nodata.
    pub nodata: Option<f64>,
    /// The scene instant every loaded cell carries, in milliseconds since
    /// the Unix epoch.
    pub time_ms: Option<i64>,
    /// The pixel window to load. `None` loads the whole image.
    pub window: Option<PixelWindow>,
}
