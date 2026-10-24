// SPDX-License-Identifier: Apache-2.0

//! The `WITH (...)` options of a server-path `COPY`, and format detection
//! from the file extension.
//!
//! Text formats take `FORMAT`, `DELIMITER` and `HEADER`. GeoTIFF takes
//! `FORMAT geotiff`, `BANDS (name | SKIP, ...)`, `NODATA <number>`,
//! `TIME '<iso>'`, `CREDENTIAL <name>`, `ADOPT_METADATA <bool>`,
//! `OVERVIEW <n>`, `WINDOW (x0, y0, x1, y1)` and `REQUESTER_PAYS <bool>`.
//! An option of the other family is refused once the format is known.

use nodedb_types::NdbDateTime;

use super::clause::{ClauseCursor, Tok, Token, parse_err};
use crate::ddl_ast::statement::{CopyBand, CopyFormat, CopyRasterOptions, CopyWindow};
use crate::error::SqlError;

/// The options of one `COPY ... WITH (...)` clause, before the format is
/// resolved.
#[derive(Debug, Default)]
pub(super) struct CopyOptions {
    pub format: Option<CopyFormat>,
    pub delimiter: Option<char>,
    /// `None` when `HEADER` is absent: each format picks its own default.
    pub header: Option<bool>,
    pub raster: CopyRasterOptions,
}

impl CopyOptions {
    /// Refuse options that do not apply to `format`.
    pub(super) fn check_for(&self, format: Option<&CopyFormat>) -> Result<(), SqlError> {
        let geotiff = format == Some(&CopyFormat::GeoTiff);
        if geotiff && (self.delimiter.is_some() || self.header.is_some()) {
            return Err(parse_err(
                "COPY: DELIMITER and HEADER apply to text formats, not FORMAT geotiff",
            ));
        }
        if !geotiff && !self.raster.is_empty() {
            return Err(parse_err(
                "COPY: BANDS, NODATA, TIME, CREDENTIAL, ADOPT_METADATA, OVERVIEW, WINDOW and \
                 REQUESTER_PAYS need FORMAT geotiff (or a .tif / .tiff path)",
            ));
        }
        Ok(())
    }
}

/// One option value, typed by its key.
enum OptionValue {
    Text(String),
    Bands(Vec<CopyBand>),
    Nodata(f64),
    TimeMs(i64),
    Credential(String),
    AdoptMetadata(bool),
    Overview(u32),
    Window(CopyWindow),
    RequesterPays(bool),
}

/// Read an optional `WITH (key [=] value, ...)` and require the end of the
/// statement.
pub(super) fn parse_copy_options(c: &mut ClauseCursor<'_, '_>) -> Result<CopyOptions, SqlError> {
    let mut options = CopyOptions::default();
    if c.eat_keyword("WITH") {
        let entries = c.paren_list("WITH options", |c| {
            let key = c.word("option name")?.to_ascii_uppercase();
            c.eat_punct('=');
            let value = match key.as_str() {
                "BANDS" => OptionValue::Bands(bands_value(c)?),
                "NODATA" => OptionValue::Nodata(nodata_value(c)?),
                "TIME" => OptionValue::TimeMs(time_value(c)?),
                "CREDENTIAL" => OptionValue::Credential(c.name("CREDENTIAL")?),
                "ADOPT_METADATA" => OptionValue::AdoptMetadata(c.bool_literal("ADOPT_METADATA")?),
                "OVERVIEW" => OptionValue::Overview(overview_value(c)?),
                "WINDOW" => OptionValue::Window(window_value(c)?),
                "REQUESTER_PAYS" => OptionValue::RequesterPays(c.bool_literal("REQUESTER_PAYS")?),
                _ => OptionValue::Text(match c.string(&key) {
                    Ok(value) => value,
                    Err(_) => c.word(&key)?.to_string(),
                }),
            };
            Ok((key, value))
        })?;
        let mut seen: Vec<String> = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if seen.contains(&key) {
                return Err(c.error(format!("WITH: option '{key}' given twice")));
            }
            apply(c, &mut options, &key, value)?;
            seen.push(key);
        }
    }
    if !c.at_end() {
        return Err(c.error(format!("unexpected trailing content {}", c.describe_next())));
    }
    Ok(options)
}

fn apply(
    c: &ClauseCursor<'_, '_>,
    options: &mut CopyOptions,
    key: &str,
    value: OptionValue,
) -> Result<(), SqlError> {
    match (key, value) {
        ("FORMAT", OptionValue::Text(text)) => options.format = Some(parse_format_value(&text)?),
        ("DELIMITER", OptionValue::Text(text)) => {
            options.delimiter = Some(parse_delimiter_value(&text)?);
        }
        ("HEADER", OptionValue::Text(text)) => {
            let header = parse_bool_value(&text)
                .ok_or_else(|| c.error(format!("HEADER must be true or false — found '{text}'")))?;
            options.header = Some(header);
        }
        (_, OptionValue::Bands(bands)) => options.raster.bands = Some(bands),
        (_, OptionValue::Nodata(nodata)) => options.raster.nodata = Some(nodata),
        (_, OptionValue::TimeMs(time_ms)) => options.raster.time_ms = Some(time_ms),
        (_, OptionValue::Credential(name)) => options.raster.credential = Some(name),
        (_, OptionValue::AdoptMetadata(adopt)) => options.raster.adopt_metadata = adopt,
        (_, OptionValue::Overview(level)) => options.raster.overview = Some(level),
        (_, OptionValue::Window(window)) => options.raster.window = Some(window),
        (_, OptionValue::RequesterPays(pays)) => options.raster.requester_pays = pays,
        (other, OptionValue::Text(_)) => {
            return Err(c.error(format!("WITH: unknown option '{other}'")));
        }
    }
    Ok(())
}

/// `( name | SKIP , ... )` with at least one name and no name twice.
fn bands_value(c: &mut ClauseCursor<'_, '_>) -> Result<Vec<CopyBand>, SqlError> {
    let bands = c.paren_list("BANDS", |c| {
        if c.eat_keyword("SKIP") {
            Ok(CopyBand::Skip)
        } else {
            c.name("BANDS entry").map(CopyBand::Attr)
        }
    })?;
    let mut names: Vec<&str> = Vec::with_capacity(bands.len());
    for band in &bands {
        if let CopyBand::Attr(name) = band {
            if names.contains(&name.as_str()) {
                return Err(c.error(format!("BANDS names attribute '{name}' twice")));
            }
            names.push(name);
        }
    }
    if names.is_empty() {
        return Err(c.error("BANDS must load at least one band; every entry is SKIP"));
    }
    Ok(bands)
}

/// A number with an optional leading `-`. The tokenizer splits an exponent
/// sign, so `1e-5` arrives as `1e`, `-`, `5` and is joined back here.
fn nodata_value(c: &mut ClauseCursor<'_, '_>) -> Result<f64, SqlError> {
    let toks = c.remaining_tokens();
    let mut text = String::new();
    let mut used = 0;
    if matches!(
        toks.first(),
        Some(Token {
            tok: Tok::Punct('-'),
            ..
        })
    ) {
        text.push('-');
        used = 1;
    }
    let Some(Token {
        tok: Tok::Word(mantissa),
        span,
    }) = toks.get(used)
    else {
        return Err(c.error(format!(
            "NODATA needs a number — found {}",
            c.describe_next()
        )));
    };
    text.push_str(mantissa);
    used += 1;
    if mantissa.ends_with(['e', 'E'])
        && let (
            Some(Token {
                tok: Tok::Punct(sign @ ('+' | '-')),
                span: sign_span,
            }),
            Some(Token {
                tok: Tok::Word(exponent),
                span: exponent_span,
            }),
        ) = (toks.get(used), toks.get(used + 1))
        && sign_span.start == span.end
        && exponent_span.start == sign_span.end
    {
        text.push(*sign);
        text.push_str(exponent);
        used += 2;
    }
    let value = text
        .parse::<f64>()
        .map_err(|_| c.error(format!("NODATA must be a number — found '{text}'")))?;
    c.skip(used);
    Ok(value)
}

/// An image index: 0 is full resolution, `n` the n-th overview.
fn overview_value(c: &mut ClauseCursor<'_, '_>) -> Result<u32, SqlError> {
    let level = c.u64("OVERVIEW")?;
    u32::try_from(level).map_err(|_| c.error(format!("OVERVIEW {level} is out of range")))
}

/// `(x0, y0, x1, y1)`: a non-empty half-open pixel window.
fn window_value(c: &mut ClauseCursor<'_, '_>) -> Result<CopyWindow, SqlError> {
    let bounds = c.paren_list("WINDOW", |c| {
        let value = c.u64("WINDOW bound")?;
        u32::try_from(value).map_err(|_| c.error(format!("WINDOW bound {value} is out of range")))
    })?;
    let &[x0, y0, x1, y1] = bounds.as_slice() else {
        return Err(c.error(format!(
            "WINDOW takes four bounds (x0, y0, x1, y1) — found {}",
            bounds.len()
        )));
    };
    if x1 <= x0 || y1 <= y0 {
        return Err(c.error(format!(
            "WINDOW ({x0}, {y0}, {x1}, {y1}) is empty; x1 must exceed x0 and y1 must exceed y0"
        )));
    }
    Ok(CopyWindow { x0, y0, x1, y1 })
}

/// A quoted ISO 8601 instant, in milliseconds since the Unix epoch.
fn time_value(c: &mut ClauseCursor<'_, '_>) -> Result<i64, SqlError> {
    let text = c.string("TIME")?;
    NdbDateTime::parse(&text)
        .map(|instant| instant.unix_millis())
        .ok_or_else(|| {
            c.error(format!(
                "TIME must be an ISO 8601 timestamp such as '2024-03-15T10:30:00Z' — found '{text}'"
            ))
        })
}

pub(super) fn parse_format_value(s: &str) -> Result<CopyFormat, SqlError> {
    match s.trim().to_lowercase().as_str() {
        "ndjson" | "jsonl" => Ok(CopyFormat::Ndjson),
        "json" => Ok(CopyFormat::JsonArray),
        "csv" => Ok(CopyFormat::Csv),
        "geotiff" => Ok(CopyFormat::GeoTiff),
        other => Err(SqlError::Parse {
            detail: format!(
                "COPY: unknown FORMAT '{other}'; expected ndjson, json, csv, or geotiff"
            ),
        }),
    }
}

pub(super) fn parse_delimiter_value(s: &str) -> Result<char, SqlError> {
    let mut chars = s.chars();
    let ch = chars.next().ok_or_else(|| SqlError::Parse {
        detail: "COPY: DELIMITER must be a single character".to_string(),
    })?;
    if chars.next().is_some() {
        return Err(SqlError::Parse {
            detail: "COPY: DELIMITER must be a single character".to_string(),
        });
    }
    Ok(ch)
}

pub(super) fn parse_bool_value(s: &str) -> Option<bool> {
    match s.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Detect the copy format from the file path extension.
///
/// Returns `Err` if the extension is unrecognised or absent (suggests an
/// explicit WITH clause).
pub(super) fn detect_format_from_path(path: &str) -> Result<Option<CopyFormat>, SqlError> {
    let lower = path.to_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return Ok(Some(CopyFormat::Ndjson));
    }
    if lower.ends_with(".json") {
        return Ok(Some(CopyFormat::JsonArray));
    }
    if lower.ends_with(".csv") {
        return Ok(Some(CopyFormat::Csv));
    }
    if lower.ends_with(".tif") || lower.ends_with(".tiff") {
        return Ok(Some(CopyFormat::GeoTiff));
    }
    Err(SqlError::Parse {
        detail: format!(
            "COPY: cannot infer format from path '{path}'; \
             add WITH (FORMAT ndjson|json|csv|geotiff)"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;
    use crate::ddl_ast::statement::{MiscStmt, NodedbStatement};

    fn copy_from(sql: &str) -> (Option<CopyFormat>, CopyRasterOptions) {
        match parse(sql).expect("expected Some").expect("expected Ok") {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, raster, .. }) => {
                (format, raster)
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    fn refusal(sql: &str) -> String {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
            .to_string()
    }

    #[test]
    fn geotiff_format_parses_explicitly() {
        let (format, raster) = copy_from("COPY scene FROM '/data/scene.bin' WITH (FORMAT geotiff)");
        assert_eq!(format, Some(CopyFormat::GeoTiff));
        assert!(raster.is_empty());
    }

    #[test]
    fn tif_and_tiff_paths_detect_geotiff() {
        for path in ["/data/a.tif", "/data/b.TIFF", "/data/c.Tif"] {
            let (format, _) = copy_from(&format!("COPY scene FROM '{path}'"));
            assert_eq!(format, Some(CopyFormat::GeoTiff), "{path}");
        }
    }

    #[test]
    fn bands_take_names_and_skip() {
        let (_, raster) = copy_from(
            "COPY scene FROM '/d/s.tif' WITH (FORMAT geotiff, BANDS (red, SKIP, \"NIR\"))",
        );
        assert_eq!(
            raster.bands,
            Some(vec![
                CopyBand::Attr("red".into()),
                CopyBand::Skip,
                CopyBand::Attr("NIR".into()),
            ])
        );
    }

    #[test]
    fn bands_refuse_duplicates_and_all_skip() {
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (BANDS (a, b, a))").contains("twice"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (BANDS (SKIP, skip))").contains("SKIP"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (BANDS ())").contains("empty"));
    }

    #[test]
    fn nodata_takes_signed_and_exponent_numbers() {
        let cases = [
            ("NODATA 0", 0.0),
            ("NODATA -9999", -9999.0),
            ("NODATA = 255", 255.0),
            ("NODATA -3.4028235e+38", -3.402_823_5e38),
            ("NODATA 1e-5", 1e-5),
            ("NODATA 2.5E3", 2500.0),
        ];
        for (option, want) in cases {
            let (_, raster) = copy_from(&format!("COPY s FROM '/d/s.tif' WITH ({option})"));
            assert_eq!(raster.nodata, Some(want), "{option}");
        }
        let (_, raster) = copy_from("COPY s FROM '/d/s.tif' WITH (NODATA NaN)");
        assert!(raster.nodata.is_some_and(f64::is_nan));
    }

    #[test]
    fn nodata_refuses_non_numbers() {
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (NODATA abc)").contains("NODATA"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (NODATA '5')").contains("NODATA"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (NODATA)").contains("NODATA"));
    }

    #[test]
    fn time_parses_iso_8601_to_millis() {
        let (_, raster) =
            copy_from("COPY s FROM '/d/s.tif' WITH (TIME '2024-03-15T10:30:00.250Z')");
        assert_eq!(raster.time_ms, Some(1_710_498_600_250));
        let (_, raster) = copy_from("COPY s FROM '/d/s.tif' WITH (TIME '2024-03-15')");
        assert_eq!(raster.time_ms, Some(1_710_460_800_000));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (TIME 'yesterday')").contains("ISO 8601"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (TIME 2024)").contains("TIME"));
    }

    #[test]
    fn every_raster_option_together() {
        let (format, raster) = copy_from(
            "COPY s FROM '/d/s.tiff' WITH (FORMAT geotiff, BANDS (b1, b2), NODATA -1, \
             TIME '2024-01-01T00:00:00Z')",
        );
        assert_eq!(format, Some(CopyFormat::GeoTiff));
        assert_eq!(raster.bands.as_ref().map(Vec::len), Some(2));
        assert_eq!(raster.nodata, Some(-1.0));
        assert_eq!(raster.time_ms, Some(1_704_067_200_000));
    }

    #[test]
    fn raster_options_need_geotiff() {
        let message = refusal("COPY s FROM '/d/s.csv' WITH (NODATA 0)");
        assert!(message.contains("FORMAT geotiff"), "{message}");
        let message = refusal("COPY s FROM '/d/s.tif' WITH (FORMAT csv, BANDS (a))");
        assert!(message.contains("FORMAT geotiff"), "{message}");
    }

    #[test]
    fn text_options_are_refused_for_geotiff() {
        let message = refusal("COPY s FROM '/d/s.tif' WITH (HEADER true)");
        assert!(message.contains("text formats"), "{message}");
        let message = refusal("COPY s FROM '/d/s.bin' WITH (FORMAT geotiff, DELIMITER ';')");
        assert!(message.contains("text formats"), "{message}");
    }

    #[test]
    fn an_option_given_twice_is_refused() {
        let message = refusal("COPY s FROM '/d/s.tif' WITH (NODATA 1, NODATA 2)");
        assert!(message.contains("twice"), "{message}");
        let message = refusal("COPY s FROM '/d/s.csv' WITH (FORMAT csv, FORMAT json)");
        assert!(message.contains("twice"), "{message}");
    }

    #[test]
    fn remote_load_options_parse() {
        let (_, raster) = copy_from(
            "COPY s FROM 's3://b/k.tif' WITH (CREDENTIAL earthdata, ADOPT_METADATA true, \
             OVERVIEW 2, WINDOW (10, 20, 266, 276), REQUESTER_PAYS true)",
        );
        assert_eq!(raster.credential.as_deref(), Some("earthdata"));
        assert!(raster.adopt_metadata);
        assert!(raster.requester_pays);
        assert_eq!(raster.overview, Some(2));
        assert_eq!(
            raster.window,
            Some(CopyWindow {
                x0: 10,
                y0: 20,
                x1: 266,
                y1: 276
            })
        );
    }

    #[test]
    fn a_window_needs_four_ordered_bounds() {
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (WINDOW (1, 2, 3))").contains("four"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (WINDOW (5, 0, 5, 9))").contains("empty"));
        assert!(refusal("COPY s FROM '/d/s.tif' WITH (OVERVIEW -1)").contains("OVERVIEW"));
        assert!(
            refusal("COPY s FROM '/d/s.csv' WITH (ADOPT_METADATA true)").contains("FORMAT geotiff")
        );
    }

    #[test]
    fn unknown_format_lists_geotiff() {
        let message = refusal("COPY s FROM '/d/s.csv' WITH (FORMAT parquet)");
        assert!(message.contains("geotiff"), "{message}");
    }
}
