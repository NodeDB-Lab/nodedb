// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <collection> FROM '<source>' [WITH (...)]` and the raster
//! list form `COPY <array> FROM ('<source>', ...) WITH (FORMAT geotiff, ...)`.
//!
//! A source is a server path, an `http(s)://` URL or an `s3://bucket/key`
//! object. Text formats read one server file. A raster reads one source per
//! band group, and every remote source needs `FORMAT geotiff` or a `.tif`
//! path. The file-path form parses here. Its options parse in
//! `copy_options`. The STDIN streaming shape
//! (`COPY <name> FROM STDIN`) parses in `copy_from_stdin`. The restore shape
//! `COPY tenant_restore(<id>) FROM STDIN` returns `None`, so dispatch falls
//! through to the backup/restore handler. `COPY ... TO` and
//! `COPY (SELECT ...) TO` belong to the `copy_to` family.
//!
//! The direction is the keyword right after the copied relation, never a
//! `FROM` or `TO` inside a path, a name, or a query.

use super::clause::{ClauseCursor, Tok, Token, tokenize};
use super::copy_options::{detect_format_from_path, parse_copy_options};
use crate::ddl_ast::statement::{CopyFormat, CopySource, MiscStmt, NodedbStatement};
use crate::error::SqlError;

/// Which way a `COPY` statement moves rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyDirection {
    /// `COPY <relation> FROM '<path>'`.
    FromFile,
    /// `COPY <relation> FROM STDIN`.
    FromStdin,
    /// `COPY <relation> TO ...`.
    To,
}

/// The direction of a `COPY` statement, read from the keyword that follows
/// the copied relation: a name (`users`, `"Users"`, `tenant_restore(1)`) or a
/// parenthesized query. `None` when the statement is not a `COPY` or names
/// no direction there.
pub(super) fn copy_direction(sql: &str) -> Option<CopyDirection> {
    let toks = tokenize(sql);
    let end = relation_end(&toks)?;
    let mut c = ClauseCursor::new(sql, &toks[end..], "COPY");
    if c.eat_keyword("TO") {
        return Some(CopyDirection::To);
    }
    if c.eat_keyword("FROM") {
        return Some(if c.peek_keyword("STDIN") {
            CopyDirection::FromStdin
        } else {
            CopyDirection::FromFile
        });
    }
    None
}

/// Index of the first token after the copied relation, or `None` when the
/// statement does not start with `COPY`.
fn relation_end(toks: &[Token<'_>]) -> Option<usize> {
    if !matches!(toks.first().map(|t| &t.tok), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("COPY"))
    {
        return None;
    }
    let mut i = 1;
    let mut depth = 0i32;
    while let Some(tok) = toks.get(i) {
        match &tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => {
                depth -= 1;
                if depth == 0 && i > 1 && toks[1].tok == Tok::Punct('(') {
                    // A parenthesized query ends at its matching `)`.
                    return Some(i + 1);
                }
            }
            Tok::Word(w)
                if depth == 0
                    && i > 1
                    && (w.eq_ignore_ascii_case("FROM") || w.eq_ignore_ascii_case("TO")) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Try to parse a COPY statement.
///
/// Returns `None` unless the statement is `COPY <collection> FROM '<path>'`
/// or `COPY <name> FROM STDIN`. Returns `Some(Err)` for a malformed
/// statement of either shape.
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !upper.starts_with("COPY ") {
        return None;
    }
    match copy_direction(trimmed)? {
        CopyDirection::FromFile => Some(parse_copy_from(trimmed)),
        CopyDirection::FromStdin => super::copy_from_stdin::try_parse(trimmed),
        CopyDirection::To => None,
    }
}

fn parse_copy_from(sql: &str) -> Result<NodedbStatement, SqlError> {
    // Grammar: COPY <name> FROM '<source>' | ('<source>', ...) [WITH (...)]
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "COPY");
    c.expect_keyword("COPY")?;
    let collection = copy_collection_name(&mut c)?;
    c.expect_keyword("FROM")?;
    let listed = c.peek_punct('(');
    let sources = if listed {
        c.paren_list("FROM sources", copy_source)?
    } else {
        vec![copy_source(&mut c)?]
    };
    let options = parse_copy_options(&mut c)?;
    let format = match options.format.clone() {
        Some(f) => Some(f),
        None => match sources.first() {
            Some(first) => detect_format_from_path(first.format_hint())?,
            None => None,
        },
    };
    options.check_for(format.as_ref())?;
    check_sources(&c, &sources, listed, format.as_ref())?;
    Ok(NodedbStatement::Misc(MiscStmt::CopyFromFile {
        collection,
        sources,
        format,
        delimiter: options.delimiter,
        header: options.header.unwrap_or(true),
        raster: options.raster,
    }))
}

/// One quoted source.
fn copy_source(c: &mut ClauseCursor<'_, '_>) -> Result<CopySource, SqlError> {
    let text = c
        .string("FROM")
        .map_err(|_| c.error("expected a single-quoted path or URL after FROM"))?;
    CopySource::parse(&text).map_err(|e| c.error(format!("FROM: {e}")))
}

/// A text format reads one server file. A list of sources, and every
/// remote source, need `FORMAT geotiff`.
fn check_sources(
    c: &ClauseCursor<'_, '_>,
    sources: &[CopySource],
    listed: bool,
    format: Option<&CopyFormat>,
) -> Result<(), SqlError> {
    if format == Some(&CopyFormat::GeoTiff) {
        return Ok(());
    }
    if listed {
        return Err(
            c.error("a list of sources loads one raster per band group and needs FORMAT geotiff")
        );
    }
    match sources {
        [CopySource::File(_)] => Ok(()),
        _ => Err(
            c.error("text formats read one server file; HTTP and S3 sources need FORMAT geotiff")
        ),
    }
}

/// The copied collection: one bare or quoted name, quotes removed and
/// lower-cased.
pub(super) fn copy_collection_name(c: &mut ClauseCursor<'_, '_>) -> Result<String, SqlError> {
    let raw = c.raw_run("collection name")?;
    let unquoted = raw
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(raw);
    Ok(unquoted.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn unicode_collection_before_from_preserves_original_offsets() {
        let stmt = ok("COPY usersßﬀİ FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection,
                sources,
                ..
            }) => {
                assert_eq!(collection, "usersßﬀi\u{307}");
                assert_eq!(sources, vec![CopySource::File("/tmp/users.ndjson".into())]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn basic_ndjson_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection,
                sources,
                format,
                delimiter,
                header,
                raster,
            }) => {
                assert_eq!(collection, "users");
                assert_eq!(sources, vec![CopySource::File("/tmp/users.ndjson".into())]);
                assert_eq!(format, Some(CopyFormat::Ndjson));
                assert_eq!(delimiter, None);
                assert!(header);
                assert!(raster.is_empty());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_array_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.json'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.csv'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn explicit_format_overrides_extension() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT json)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_with_delimiter() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, DELIMITER ';')");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                format, delimiter, ..
            }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
                assert_eq!(delimiter, Some(';'));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn header_false() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER false)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { header, .. }) => {
                assert!(!header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn non_boolean_header_is_refused() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (HEADER maybe)");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn trailing_content_after_with_is_refused() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv) EXTRA");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn to_inside_the_path_does_not_change_the_direction() {
        let stmt = ok("COPY users FROM '/tmp/copy to mars.csv'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyFromFile { .. })
        ));
    }

    #[test]
    fn unknown_extension_is_err() {
        let e = err("COPY users FROM '/tmp/data.parquet'");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("cannot infer format"));
    }

    #[test]
    fn copy_to_handled_by_copy_to_parser() {
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY users TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(try_parse(&upper, &parts, sql).is_none());
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_query_form_handled_by_copy_to_parser() {
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY (SELECT * FROM users) TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(try_parse(&upper, &parts, sql).is_none());
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_from_stdin_returns_none() {
        // Should return None so backup/restore handler catches it.
        assert!(parse("COPY tenant_restore(1) FROM STDIN").is_none());
    }

    #[test]
    fn a_source_list_loads_one_raster_per_band_group() {
        let stmt = ok("COPY scene FROM ('/B4.TIF', 'https://h.example/B5.TIF', \
             's3://bucket/QA_PIXEL.TIF') WITH (FORMAT geotiff, BANDS (red, nir, qa))");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                sources, format, ..
            }) => {
                assert_eq!(format, Some(CopyFormat::GeoTiff));
                assert_eq!(
                    sources,
                    vec![
                        CopySource::File("/B4.TIF".into()),
                        CopySource::Http("https://h.example/B5.TIF".into()),
                        CopySource::S3 {
                            bucket: "bucket".into(),
                            key: "QA_PIXEL.TIF".into()
                        },
                    ]
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn remote_and_listed_sources_need_geotiff() {
        let e = err("COPY users FROM 'https://h.example/u.csv'");
        assert!(e.to_string().contains("FORMAT geotiff"), "{e}");
        let e = err("COPY users FROM ('/a.csv', '/b.csv') WITH (FORMAT csv)");
        assert!(e.to_string().contains("FORMAT geotiff"), "{e}");
        let e = err("COPY scene FROM 'ftp://h/a.tif'");
        assert!(e.to_string().contains("scheme"), "{e}");
        let e = err("COPY scene FROM ()");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn an_s3_url_detects_geotiff_from_its_key() {
        let stmt = ok("COPY scene FROM 's3://landsat/a/B4.TIF'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                format: Some(CopyFormat::GeoTiff),
                ..
            })
        ));
    }

    #[test]
    fn unknown_with_option_is_err() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, BOGUS opt)");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("BOGUS"));
    }
}
