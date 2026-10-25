// SPDX-License-Identifier: Apache-2.0

//! The TIFF reader over GeoTIFFs the tiff encoder builds in-test, and over
//! the generated files under `tests/fixtures`.

mod cog;
mod files;
mod fixtures;
mod georef;
mod image;
mod malformed;
mod robustness;
mod strips;
mod tiles;
