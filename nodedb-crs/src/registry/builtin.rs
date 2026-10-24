// SPDX-License-Identifier: Apache-2.0

//! The built-in CRS table: NodeDB's own codes, then every EPSG CRS.
//!
//! The EPSG table is two tab-separated files compiled into the binary:
//! `epsg.tsv` from the pinned EPSG dataset, and `epsg_supplement.tsv` with
//! EPSG CRSs published after it. No code is in both. One CRS per line,
//! sorted ascending by code, `#` starts a comment line:
//!
//! ```text
//! code<TAB>kind<TAB>name<TAB>area<TAB>accuracy_m<TAB>datum<TAB>proj
//! ```
//!
//! `datum` is the EPSG code of the CRS's datum, or empty. CRSs with one
//! datum code share a datum, whatever their `+towgs84`.
//!
//! A line that does not start with a digit is not data, so a column-header
//! line is skipped like a comment.
//!
//! Lookup bisects the text by byte offset. It allocates nothing and keeps
//! no state.

use crate::def::{AreaOfUse, CrsDef, CrsKind, DatumShift};
use crate::error::CrsParseError;
use crate::parse::parse_proj_string;
use crate::srid::Srid;

/// The EPSG table from the pinned EPSG dataset.
const EPSG_TSV: &str = include_str!("../../data/epsg.tsv");

/// EPSG CRSs newer than the pinned dataset, such as GDM2020.
const EPSG_SUPPLEMENT_TSV: &str = include_str!("../../data/epsg_supplement.tsv");

/// Number of tab-separated fields per data line.
const FIELDS: usize = 7;

/// One row of the built-in table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BuiltinEntry {
    pub code: u32,
    pub kind: CrsKind,
    pub name: &'static str,
    /// `west,south,east,north` degrees, or empty.
    pub area: &'static str,
    /// Transformation accuracy in metres, or empty.
    pub accuracy_m: &'static str,
    /// EPSG datum code, or empty.
    pub datum: &'static str,
    pub proj: &'static str,
}

impl BuiltinEntry {
    /// The parsed area of use.
    pub fn area_of_use(&self) -> Result<Option<AreaOfUse>, CrsParseError> {
        AreaOfUse::parse(self.area)
    }

    /// The parsed transformation accuracy, metres.
    pub fn accuracy(&self) -> Result<Option<f64>, CrsParseError> {
        if self.accuracy_m.is_empty() {
            return Ok(None);
        }
        self.accuracy_m
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite() && *v >= 0.0)
            .map(Some)
            .ok_or_else(|| CrsParseError::invalid("accuracy_m", self.accuracy_m))
    }

    /// The parsed EPSG datum code.
    pub fn datum_code(&self) -> Result<Option<u32>, CrsParseError> {
        if self.datum.is_empty() {
            return Ok(None);
        }
        self.datum
            .parse::<u32>()
            .map(Some)
            .map_err(|_| CrsParseError::invalid("datum", self.datum))
    }

    /// The definition this row describes, under `srid`: its PROJ string
    /// with its area, transformation accuracy and EPSG datum code.
    pub fn definition(&self, srid: Srid) -> Result<CrsDef, CrsParseError> {
        let mut def = parse_proj_string(srid, self.name, self.proj)?;
        if def.kind != self.kind {
            return Err(CrsParseError::invalid("kind", self.kind.as_str()));
        }
        def.area_of_use = self.area_of_use()?;
        def.datum.epsg_code = self.datum_code()?;
        let accuracy = self.accuracy()?;
        match &mut def.datum.to_wgs84 {
            DatumShift::Helmert(h) => h.accuracy_m = accuracy,
            DatumShift::Identity => {}
        }
        Ok(def)
    }
}

/// NodeDB's own CRSs, checked before the EPSG table.
const NODEDB_BUILTINS: &[BuiltinEntry] = &[
    BuiltinEntry {
        code: 990_001,
        kind: CrsKind::Projected,
        name: "MODIS Sinusoidal (SR-ORG:6842)",
        area: "-180,-90,180,90",
        accuracy_m: "",
        datum: "",
        proj: "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs",
    },
    BuiltinEntry {
        code: 990_002,
        kind: CrsKind::Projected,
        name: "Himawari geostationary, 140.7 E",
        area: "",
        accuracy_m: "",
        datum: "",
        proj: "+proj=geos +h=35785863 +lon_0=140.7 +ellps=WGS84 +units=m +no_defs",
    },
];

/// The built-in CRS with `code`: NodeDB's own first, then EPSG.
pub fn builtin_lookup(code: u32) -> Option<BuiltinEntry> {
    NODEDB_BUILTINS
        .iter()
        .find(|e| e.code == code)
        .copied()
        .or_else(|| lookup_in(EPSG_TSV, code))
        .or_else(|| lookup_in(EPSG_SUPPLEMENT_TSV, code))
}

/// Bisect `table` for `code`.
///
/// Invariant: `lo` and `hi` sit on line starts (or the end), and the line
/// with `code`, if present, lies in `lo..hi`.
pub fn lookup_in(table: &'static str, code: u32) -> Option<BuiltinEntry> {
    let bytes = table.as_bytes();
    let (mut lo, mut hi) = (0, bytes.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let start = line_start(bytes, lo, mid);
        match first_data_line(table, start, hi) {
            None => hi = start,
            Some((line_at, line, next)) => {
                let row_code: u32 = line.split('\t').next()?.parse().ok()?;
                match row_code.cmp(&code) {
                    core::cmp::Ordering::Equal => return parse_row(line),
                    core::cmp::Ordering::Less => lo = next,
                    core::cmp::Ordering::Greater => hi = line_at,
                }
            }
        }
    }
    None
}

/// Start of the line holding byte `at`, never before `floor`.
fn line_start(bytes: &[u8], floor: usize, at: usize) -> usize {
    bytes[floor..at]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(floor, |i| floor + i + 1)
}

/// The first data line starting at or after `from` and
/// before `hi`: its start, its text without the line break, and the start
/// of the line after it.
fn first_data_line(
    table: &'static str,
    from: usize,
    hi: usize,
) -> Option<(usize, &'static str, usize)> {
    let mut at = from;
    while at < hi {
        let end = table[at..].find('\n').map_or(table.len(), |i| at + i);
        let next = (end + 1).min(table.len());
        let line = table[at..end].trim_end_matches('\r');
        if is_data_line(line) {
            return Some((at, line, next));
        }
        at = next;
    }
    None
}

/// Whether `line` is a data row: it starts with a code digit. Comments
/// (`#`), blank lines and a column-header line are not.
fn is_data_line(line: &str) -> bool {
    line.as_bytes().first().is_some_and(u8::is_ascii_digit)
}

/// Split one data line into a [`BuiltinEntry`].
fn parse_row(line: &'static str) -> Option<BuiltinEntry> {
    let mut f = line.splitn(FIELDS, '\t');
    let code = f.next()?.parse().ok()?;
    let kind = CrsKind::from_name(f.next()?)?;
    Some(BuiltinEntry {
        code,
        kind,
        name: f.next()?,
        area: f.next()?,
        accuracy_m: f.next()?,
        datum: f.next()?,
        proj: f.next()?,
    })
}

/// Every data line of `table`, in file order.
fn table_rows(table: &'static str) -> impl Iterator<Item = &'static str> {
    table
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| is_data_line(l))
}

/// Every EPSG row of both files, split into its fields, ascending by code.
fn epsg_entries() -> impl Iterator<Item = BuiltinEntry> {
    let mut main = table_rows(EPSG_TSV).filter_map(parse_row).peekable();
    let mut newer = table_rows(EPSG_SUPPLEMENT_TSV)
        .filter_map(parse_row)
        .peekable();
    core::iter::from_fn(
        move || match (main.peek().map(|e| e.code), newer.peek().map(|e| e.code)) {
            (Some(a), Some(b)) if b < a => newer.next(),
            (Some(_), Some(_) | None) => main.next(),
            (None, Some(_) | None) => newer.next(),
        },
    )
}

/// Every built-in CRS row: NodeDB's own first, then every EPSG CRS
/// ascending.
pub fn builtin_entries() -> impl Iterator<Item = BuiltinEntry> {
    NODEDB_BUILTINS.iter().copied().chain(epsg_entries())
}

/// The code of every built-in CRS of `kind`: NodeDB's own first, then
/// every EPSG CRS ascending.
pub fn builtin_codes(kind: CrsKind) -> impl Iterator<Item = Srid> {
    builtin_entries()
        .filter(move |e| e.kind == kind)
        .filter_map(|e| Srid::new(e.code).ok())
}

/// The lowest-coded EPSG geographic CRS on the EPSG datum `epsg_datum`.
pub fn geographic_crs_on_datum(epsg_datum: u32) -> Option<Srid> {
    epsg_entries()
        .filter(|e| e.kind == CrsKind::Geographic)
        .find(|e| e.datum_code().ok().flatten() == Some(epsg_datum))
        .and_then(|e| Srid::new(e.code).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_proj_string;

    const TABLE: &str = "# EPSG 12.0 PROJ 9.8.1\n\
        code\tkind\tname\tarea\taccuracy_m\tdatum\tproj\n\
        # second comment\n\
        3375\tprojected\tGDM2000 / Peninsula RSO\t99.59,1.21,104.6,7.81\t\t6742\t+proj=omerc +no_uoff +lat_0=4 +lonc=102.25 +alpha=323.025796466667 +gamma=323.130102361111 +k=0.99984 +x_0=804671 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs\n\
        4326\tgeographic\tWGS 84\t-180,-90,180,90\t\t6326\t+proj=longlat +datum=WGS84 +no_defs\n\
        # a comment between rows\n\
        \n\
        4742\tgeographic\tGDM2000\t98.02,0.85,119.61,7.81\t\t6742\t+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs\n\
        32647\tprojected\tWGS 84 / UTM zone 47N\t96,0,102,84\t\t6326\t+proj=utm +zone=47 +datum=WGS84 +units=m +no_defs\n\
        32648\tprojected\tWGS 84 / UTM zone 48N\t102,0,108,84\t\t6326\t+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs\n";

    const CRLF_TABLE: &str = "# h\r\n10\tgeographic\tA\t\t1.5\t6326\t+proj=longlat\r\n20\tgeographic\tB\t\t\t\t+proj=longlat\r\n";

    #[test]
    fn finds_first_last_and_middle() {
        for code in [3375, 4326, 4742, 32647, 32648] {
            let entry = lookup_in(TABLE, code).unwrap();
            assert_eq!(entry.code, code);
        }
        let last = lookup_in(TABLE, 32648).unwrap();
        assert_eq!(last.kind, CrsKind::Projected);
        assert_eq!(last.name, "WGS 84 / UTM zone 48N");
        assert_eq!(
            last.proj,
            "+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs"
        );
        assert_eq!(last.area_of_use().unwrap().unwrap().west, 102.0);
    }

    #[test]
    fn missing_codes_return_none() {
        for code in [1, 3374, 3376, 4000, 4743, 32649, 999_999] {
            assert!(lookup_in(TABLE, code).is_none(), "{code}");
        }
    }

    #[test]
    fn comment_only_and_empty_tables() {
        assert!(lookup_in("", 4326).is_none());
        assert!(lookup_in("# only\n# comments\n", 4326).is_none());
    }

    #[test]
    fn crlf_lines_and_accuracy() {
        let a = lookup_in(CRLF_TABLE, 10).unwrap();
        assert_eq!(a.accuracy(), Ok(Some(1.5)));
        assert_eq!(a.datum_code(), Ok(Some(6326)));
        let b = lookup_in(CRLF_TABLE, 20).unwrap();
        assert_eq!(b.proj, "+proj=longlat");
        assert_eq!(b.accuracy(), Ok(None));
        assert_eq!(b.datum_code(), Ok(None));
    }

    #[test]
    fn nodedb_builtins_come_first() {
        let modis = builtin_lookup(990_001).unwrap();
        assert!(modis.proj.contains("+R=6371007.181"));
        assert_eq!(
            builtin_lookup(990_002).map(|e| e.kind),
            Some(CrsKind::Projected)
        );
    }

    #[test]
    fn builtin_codes_list_nodedb_first_then_epsg_by_kind() {
        let projected: Vec<u32> = builtin_codes(CrsKind::Projected)
            .take(3)
            .map(Srid::code)
            .collect();
        assert_eq!(&projected[..2], &[990_001, 990_002]);
        assert!(projected[2] < 900_000);
        assert!(builtin_codes(CrsKind::Geographic).any(|s| s == Srid::WGS84));
        assert!(!builtin_codes(CrsKind::Geographic).any(|s| s.code() == 32649));
        assert!(!builtin_codes(CrsKind::Projected).any(|s| s == Srid::WGS84));
    }

    #[test]
    fn geographic_crs_on_a_datum() {
        assert_eq!(geographic_crs_on_datum(6326), Some(Srid::WGS84));
        assert_eq!(geographic_crs_on_datum(6742).map(Srid::code), Some(4742));
        assert_eq!(geographic_crs_on_datum(1), None);
    }

    /// Every row of both EPSG files is well formed, the codes ascend
    /// strictly, and no code is in both files.
    #[test]
    fn epsg_tables_are_well_formed_and_sorted() {
        for table in [EPSG_TSV, EPSG_SUPPLEMENT_TSV] {
            let mut previous = 0u32;
            let mut rows = 0usize;
            for line in table_rows(table) {
                let entry = parse_row(line).unwrap_or_else(|| panic!("malformed row: {line}"));
                assert!(entry.code > previous, "unsorted at {}", entry.code);
                previous = entry.code;
                entry.area_of_use().unwrap();
                entry.accuracy().unwrap();
                entry.datum_code().unwrap();
                assert_eq!(lookup_in(table, entry.code), Some(entry));
                assert_eq!(builtin_lookup(entry.code), Some(entry));
                rows += 1;
            }
            assert!(rows > 0);
            assert!(table.starts_with("# EPSG "));
        }
        for line in table_rows(EPSG_SUPPLEMENT_TSV) {
            let code = parse_row(line).map(|e| e.code).unwrap_or_default();
            assert!(
                lookup_in(EPSG_TSV, code).is_none(),
                "{code} is in both files"
            );
        }
        let codes: Vec<u32> = epsg_entries().map(|e| e.code).collect();
        assert!(codes.windows(2).all(|w| w[0] < w[1]), "merged codes ascend");
        assert_eq!(
            codes.len(),
            table_rows(EPSG_TSV).count() + table_rows(EPSG_SUPPLEMENT_TSV).count()
        );
    }

    /// GDM2020 resolves from the supplement with its EPSG datum, 1471.
    #[test]
    fn gdm2020_is_built_in() {
        let gdm = builtin_lookup(12287).unwrap();
        assert_eq!(gdm.kind, CrsKind::Geographic);
        assert_eq!(gdm.datum_code(), Ok(Some(1471)));
        assert_eq!(geographic_crs_on_datum(1471).map(Srid::code), Some(12287));
        for code in 25314..=25322 {
            let grid = builtin_lookup(code).unwrap();
            assert_eq!(grid.kind, CrsKind::Projected, "{code}");
            assert!(grid.name.starts_with("GDM2020 / "), "{code}");
            assert!(builtin_codes(CrsKind::Projected).any(|s| s.code() == code));
        }
    }

    /// Every EPSG row parses to a definition of its listed kind.
    #[test]
    fn every_epsg_row_parses() {
        for line in table_rows(EPSG_TSV).chain(table_rows(EPSG_SUPPLEMENT_TSV)) {
            let entry = parse_row(line).unwrap_or_else(|| panic!("malformed row: {line}"));
            let Ok(srid) = Srid::new(entry.code) else {
                panic!("code out of range: {}", entry.code);
            };
            match parse_proj_string(srid, entry.name, entry.proj) {
                Ok(def) => assert_eq!(def.kind, entry.kind, "{line}"),
                Err(e) => panic!("{}: {e}", entry.code),
            }
        }
    }
}
