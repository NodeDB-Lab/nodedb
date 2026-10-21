// SPDX-License-Identifier: Apache-2.0

use super::copy_source::CopyWindow;

/// Source for `COPY ... TO`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    /// `COPY <collection> TO '<path>'` — export from a named collection.
    Collection(String),
    /// `COPY (SELECT ...) TO '<path>'` — export from an arbitrary query.
    Query(String),
}

/// Row format of `COPY ... FROM STDIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStdinFormat {
    /// PostgreSQL text format: tab-delimited by default, `\N` is NULL, and a
    /// backslash escapes the next character.
    Text,
    /// CSV: comma-delimited by default, `"` quotes a field, and an unquoted
    /// empty field is NULL.
    Csv,
}

/// Format for `COPY ... FROM` bulk import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFormat {
    /// One JSON object per line (`.ndjson` / `.jsonl`).
    Ndjson,
    /// A JSON array of objects (`.json`).
    JsonArray,
    /// CSV with an optional header row (`.csv`).
    Csv,
    /// A GeoTIFF raster (`.tif` / `.tiff`), read from a server path, an
    /// HTTP(S) URL or an S3 object into an array.
    GeoTiff,
}

/// One `BANDS (...)` entry of a GeoTIFF `COPY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyBand {
    /// The band loads into this array attribute.
    Attr(String),
    /// `SKIP`: the band is not loaded.
    Skip,
}

/// The GeoTIFF options of `COPY <array> FROM <sources> WITH (FORMAT geotiff, ...)`.
/// Every field is unset for the other formats.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CopyRasterOptions {
    /// `BANDS (a, SKIP, b)`: entry `i` names where band `i` of the sources'
    /// bands in order loads. `None` loads band `i` into attribute `i`.
    pub bands: Option<Vec<CopyBand>>,
    /// `NODATA <number>`: the nodata value, in place of the file's
    /// `GDAL_NODATA`.
    pub nodata: Option<f64>,
    /// `TIME '<iso>'`: the scene instant every loaded cell carries, in
    /// milliseconds since the Unix epoch.
    pub time_ms: Option<i64>,
    /// `CREDENTIAL <name>`: the credential remote sources authenticate with.
    pub credential: Option<String>,
    /// `ADOPT_METADATA true`: an attr with no metadata takes the band's
    /// nodata, scale, offset and units from the file.
    pub adopt_metadata: bool,
    /// `OVERVIEW <n>`: read overview `n` (1 is the first) in place of the
    /// full-resolution image.
    pub overview: Option<u32>,
    /// `WINDOW (x0, y0, x1, y1)`: load this pixel window only.
    pub window: Option<CopyWindow>,
    /// `REQUESTER_PAYS true`: an `s3://` read bills the requester.
    pub requester_pays: bool,
}

impl CopyRasterOptions {
    /// True when no GeoTIFF option is given.
    pub fn is_empty(&self) -> bool {
        self.bands.is_none()
            && self.nodata.is_none()
            && self.time_ms.is_none()
            && self.credential.is_none()
            && !self.adopt_metadata
            && self.overview.is_none()
            && self.window.is_none()
            && !self.requester_pays
    }
}
