// SPDX-License-Identifier: BUSL-1.1

//! The committed fixtures `fixtures/gen_fixtures.py` writes.

/// The fixture directory.
pub const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/wire/cases/array_geotiff/fixtures"
);

/// The absolute path of fixture `name`.
pub fn fixture(name: &str) -> String {
    format!("{FIXTURES}/{name}")
}

/// The bytes of fixture `name`.
pub fn fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(fixture(name)).unwrap_or_else(|e| panic!("read fixture {name}: {e}"))
}

/// A value of `fixtures/expected.tsv`.
pub fn expected(key: &str) -> u64 {
    include_str!("fixtures/expected.tsv")
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or_else(|| panic!("expected.tsv has no {key}"))
}

/// Band `k` of the band fixtures at `(r, c)`: `1000 k + (7 r + 13 c) mod
/// 997`, 0 in the nodata block rows 100..110, cols 120..130.
pub fn band(k: u32, r: u32, c: u32) -> u16 {
    super::fixture::sample(k, r, c)
}

/// The QA fixture at `(r, c)`: `1 + (r + c) mod 251`.
pub fn qa(r: u32, c: u32) -> u16 {
    u16::try_from(1 + (r + c) % 251).expect("fits u16")
}
