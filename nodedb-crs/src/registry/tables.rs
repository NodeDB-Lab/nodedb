// SPDX-License-Identifier: Apache-2.0

//! The EPSG tables compiled into the binary.
//!
//! The core table always ships. It holds WGS 84 (4326), Web Mercator
//! (3857) and the WGS 84 UTM and UPS grids (32601 to 32661, 32701 to
//! 32761). Its rows are copies of rows in the full table.
//!
//! The `epsg` feature adds the full table and its supplement. The full table
//! ships zstd-compressed and decompresses once, on its first read. A code
//! lookup searches the core table first, so a core code never reads it. A
//! build without the feature searches the core table only.

/// WGS 84, Web Mercator, and the WGS 84 UTM and UPS grids.
pub(crate) const CORE_TSV: &str = include_str!("../../data/epsg_core.tsv");

/// The full EPSG table from the pinned EPSG dataset, zstd level 19.
#[cfg(feature = "epsg")]
const EPSG_TSV_ZST: &[u8] = include_bytes!("../../data/epsg.tsv.zst");

/// EPSG CRSs newer than the pinned dataset, such as GDM2020.
#[cfg(feature = "epsg")]
pub(crate) const EPSG_SUPPLEMENT_TSV: &str = include_str!("../../data/epsg_supplement.tsv");

/// The EPSG tables that together hold every built-in EPSG row once.
#[cfg(feature = "epsg")]
pub(crate) fn epsg_tables() -> [&'static str; 2] {
    [epsg_main(), EPSG_SUPPLEMENT_TSV]
}

/// The EPSG tables that together hold every built-in EPSG row once: the
/// core table only.
#[cfg(not(feature = "epsg"))]
pub(crate) fn epsg_tables() -> [&'static str; 1] {
    [CORE_TSV]
}

/// The tables a code lookup searches after the core table. A core code
/// therefore resolves without decompressing the full table.
#[cfg(feature = "epsg")]
pub(crate) fn tables_after_core() -> [&'static str; 2] {
    epsg_tables()
}

/// The tables a code lookup searches after the core table: none.
#[cfg(not(feature = "epsg"))]
pub(crate) fn tables_after_core() -> [&'static str; 0] {
    []
}

/// The full EPSG table, decompressed on first use and kept for the process.
///
/// The frame is a constant, and a unit test decodes it with this decoder.
/// A decode error therefore cannot occur at run time. If one does, the core
/// table answers, as in a build without the feature.
#[cfg(feature = "epsg")]
pub(crate) fn epsg_main() -> &'static str {
    static TEXT: std::sync::OnceLock<Option<Box<str>>> = std::sync::OnceLock::new();
    TEXT.get_or_init(|| decompress(EPSG_TSV_ZST))
        .as_deref()
        .unwrap_or(CORE_TSV)
}

/// The UTF-8 text of one zstd frame, or `None` when it does not decode.
#[cfg(feature = "epsg")]
fn decompress(frame: &[u8]) -> Option<Box<str>> {
    use std::io::Read;
    let mut decoder = ruzstd::decoding::StreamingDecoder::new(frame).ok()?;
    let mut text = String::new();
    decoder.read_to_string(&mut text).ok()?;
    Some(text.into_boxed_str())
}

#[cfg(all(test, feature = "epsg"))]
mod tests {
    use super::*;

    /// The committed source of the compressed frame.
    const EPSG_TSV: &str = include_str!("../../data/epsg.tsv");

    #[test]
    fn full_table_decodes_to_its_source() {
        let decoded = decompress(EPSG_TSV_ZST);
        assert!(
            decoded.as_deref() == Some(EPSG_TSV),
            "data/epsg.tsv.zst does not decode to data/epsg.tsv: run scripts/pack_epsg.sh"
        );
        assert_eq!(epsg_main(), EPSG_TSV);
    }

    #[test]
    fn a_corrupt_frame_decodes_to_none() {
        assert_eq!(decompress(b"not a zstd frame"), None);
        assert_eq!(decompress(&EPSG_TSV_ZST[..EPSG_TSV_ZST.len() / 2]), None);
    }
}
