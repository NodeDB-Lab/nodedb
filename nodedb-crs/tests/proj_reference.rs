// SPDX-License-Identifier: Apache-2.0

//! PROJ 9.8.1 reference points, written by `scripts/gen_epsg.py`.
//!
//! `proj_reference.tsv` comes from the PROJ strings of the built-in table.
//! `proj_reference_malaysia.tsv` comes from the official EPSG definitions.
//! A Malaysian row with `tol_m` is one where PROJ picks another Helmert than
//! the built-in table. It is held to that Helmert's accuracy, in metres.

use nodedb_crs::{CrsContext, CrsKind, Srid};

const GENERATED: &str = include_str!("fixtures/proj_reference.tsv");
const MALAYSIA: &str = include_str!("fixtures/proj_reference_malaysia.tsv");

/// Projected tolerance, metres.
const PROJECTED_TOL_M: f64 = 1e-3;

/// Geographic tolerance, degrees.
const GEOGRAPHIC_TOL_DEG: f64 = 1e-9;

/// Metres per degree on the equator. The generator converts a degree error
/// to metres with the same factor before it compares it with `tol_m`.
const METRES_PER_DEGREE: f64 = 111_320.0;

/// Degrees in a full turn of longitude.
const FULL_TURN_DEG: f64 = 360.0;

/// Number of failing rows the assertion message lists.
const SHOWN: usize = 20;

/// One fixture row.
struct Row {
    line: usize,
    src: u32,
    dst: u32,
    input: [f64; 2],
    expected: [f64; 2],
    tol_m: Option<f64>,
}

/// The data rows of a fixture. A data row starts with a digit.
fn rows(name: &str, text: &str) -> Vec<Row> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| l.as_bytes().first().is_some_and(u8::is_ascii_digit))
        .map(|(i, l)| {
            let f: Vec<&str> = l.split('\t').collect();
            let num = |k: usize| -> f64 {
                f.get(k)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("{name}:{}: bad column {k}: {l}", i + 1))
            };
            let code = |k: usize| -> u32 {
                f.get(k)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("{name}:{}: bad code column {k}: {l}", i + 1))
            };
            Row {
                line: i + 1,
                src: code(0),
                dst: code(1),
                input: [num(2), num(3)],
                expected: [num(4), num(5)],
                tol_m: f.get(6).filter(|v| !v.is_empty()).map(|_| num(6)),
            }
        })
        .collect()
}

/// Transform one row and compare it with the PROJ output.
fn check_row(ctx: &CrsContext<'_>, row: &Row) -> Result<(), String> {
    let srid = |code: u32| Srid::new(code).map_err(|e| e.to_string());
    let (src, dst) = (srid(row.src)?, srid(row.dst)?);
    let transformer = ctx.transformer(src, dst).map_err(|e| e.to_string())?;
    let target = ctx.resolve(dst).map_err(|e| e.to_string())?;
    let [x, y] = row.input;
    let got = transformer.apply([x, y, 0.0]).map_err(|e| e.to_string())?;
    let dy = (got[1] - row.expected[1]).abs();
    let (error_m, tol_m) = match target.kind {
        CrsKind::Geographic => {
            // ±180° name one meridian.
            let dx = (got[0] - row.expected[0] + FULL_TURN_DEG / 2.0).rem_euclid(FULL_TURN_DEG)
                - FULL_TURN_DEG / 2.0;
            let error_deg = dx.abs().max(dy);
            match row.tol_m {
                Some(tol) => (error_deg * METRES_PER_DEGREE, tol),
                None if error_deg <= GEOGRAPHIC_TOL_DEG => return Ok(()),
                None => (
                    error_deg * METRES_PER_DEGREE,
                    GEOGRAPHIC_TOL_DEG * METRES_PER_DEGREE,
                ),
            }
        }
        CrsKind::Projected => {
            let dx = (got[0] - row.expected[0]).abs();
            let error_m = dx.max(dy) * target.unit.metres_per_unit;
            (error_m, row.tol_m.unwrap_or(PROJECTED_TOL_M))
        }
    };
    if error_m <= tol_m {
        Ok(())
    } else {
        Err(format!(
            "got ({}, {}), off by {error_m:e} m, tolerance {tol_m:e} m",
            got[0], got[1]
        ))
    }
}

/// Check every row of a fixture and report all failures at once.
fn check_fixture(name: &str, text: &str) {
    let ctx = CrsContext::builtin_only();
    let rows = rows(name, text);
    assert!(!rows.is_empty(), "{name} has no data rows");
    let failures: Vec<String> = rows
        .iter()
        .filter_map(|row| {
            check_row(&ctx, row).err().map(|why| {
                format!(
                    "{name}:{} {} -> {} in ({}, {}) expected ({}, {}): {why}",
                    row.line,
                    row.src,
                    row.dst,
                    row.input[0],
                    row.input[1],
                    row.expected[0],
                    row.expected[1]
                )
            })
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} rows fail, first {SHOWN}:\n{}",
        failures.len(),
        rows.len(),
        failures
            .iter()
            .take(SHOWN)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn built_in_table_matches_proj() {
    check_fixture("proj_reference.tsv", GENERATED);
}

#[test]
fn malaysia_matches_official_epsg() {
    check_fixture("proj_reference_malaysia.tsv", MALAYSIA);
}
