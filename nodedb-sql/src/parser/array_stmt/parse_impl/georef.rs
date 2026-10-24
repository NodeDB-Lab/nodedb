// SPDX-License-Identifier: Apache-2.0

//! `GEOREF (SRID n, X dim, Y dim [, TRANSFORM (gt0, .., gt5)])` clause parsing.
//!
//! The parser checks the SRID code range and the coefficient count. The
//! planner checks the SRID is defined, the dims and the coefficient values.

use nodedb_crs::UnknownSrid;
use nodedb_types::geometry::Srid;

use crate::error::{Result, SqlError};
use crate::parser::array_stmt::lexer::Tok;
use crate::types_array::ArrayGeorefAst;

use super::core::Parser;

/// Number of coefficients in a GDAL geotransform.
const TRANSFORM_LEN: usize = 6;

impl<'a> Parser<'a> {
    /// Parse the parenthesised body after the `GEOREF` keyword.
    pub(super) fn parse_georef(&mut self) -> Result<ArrayGeorefAst> {
        self.expect(&Tok::LParen)?;
        self.expect_kw("SRID")?;
        let code = self.expect_int()?;
        let srid = u32::try_from(code)
            .ok()
            .and_then(|c| Srid::new(c).ok())
            .ok_or_else(|| SqlError::DataException {
                detail: format!("GEOREF: {}", UnknownSrid::OutOfRange { code }),
            })?
            .code();
        self.expect(&Tok::Comma)?;
        self.expect_kw("X")?;
        let x_dim = self.expect_ident()?;
        self.expect(&Tok::Comma)?;
        self.expect_kw("Y")?;
        let y_dim = self.expect_ident()?;
        let transform = if self.match_token(&Tok::Comma) {
            self.expect_kw("TRANSFORM")?;
            Some(self.parse_transform()?)
        } else {
            None
        };
        self.expect(&Tok::RParen)?;
        Ok(ArrayGeorefAst {
            srid,
            x_dim,
            y_dim,
            transform,
        })
    }

    fn parse_transform(&mut self) -> Result<[f64; TRANSFORM_LEN]> {
        self.expect(&Tok::LParen)?;
        let mut coefficients = Vec::with_capacity(TRANSFORM_LEN);
        loop {
            coefficients.push(self.expect_float_or_int_as_f64()?);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        let count = coefficients.len();
        <[f64; TRANSFORM_LEN]>::try_from(coefficients).map_err(|_| SqlError::DataException {
            detail: format!(
                "GEOREF TRANSFORM takes {TRANSFORM_LEN} coefficients in GDAL order, got {count}"
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SqlError;
    use crate::parser::array_stmt::ast::{AlterArrayAction, ArrayStatement};
    use crate::parser::array_stmt::try_parse_array_statement;
    use crate::types_array::{ArrayGeorefAlter, ArrayGeorefAst};

    const BASE: &str = "CREATE ARRAY scene DIMS (row INT64 [0..1023], col INT64 [0..1023]) \
                        ATTRS (b1 UINT16) TILE_EXTENTS (256, 256)";

    fn create_georef(tail: &str) -> Result<Option<ArrayGeorefAst>, SqlError> {
        let sql = format!("{BASE} {tail}");
        match try_parse_array_statement(&sql)? {
            Some(ArrayStatement::Create(c)) => Ok(c.georef),
            other => panic!("expected CREATE, got {other:?}"),
        }
    }

    #[test]
    fn create_without_georef() {
        assert_eq!(create_georef("").unwrap(), None);
    }

    #[test]
    fn create_world_georef() {
        let g = create_georef("GEOREF (SRID 32647, X col, Y row)")
            .unwrap()
            .unwrap();
        assert_eq!(g.srid, 32647);
        assert_eq!(g.x_dim, "col");
        assert_eq!(g.y_dim, "row");
        assert_eq!(g.transform, None);
    }

    #[test]
    fn create_pixel_georef_with_exponent_floats() {
        let g = create_georef(
            "TILE_ORDER ROW_MAJOR \
             GEOREF (SRID 4326, X col, Y row, \
             TRANSFORM (100.5, 8.983152841195214e-05, 0, 6.5, 0.0, -8.983152841195214E-05)) \
             WITH (prefix_bits = 4)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(g.srid, 4326);
        assert_eq!(
            g.transform,
            Some([
                100.5,
                8.983152841195214e-05,
                0.0,
                6.5,
                0.0,
                -8.983152841195214e-05
            ])
        );
    }

    #[test]
    fn create_georef_keywords_are_case_insensitive() {
        let g = create_georef(
            "georef (srid 3375, x col, y row, transform (1e3, 1, 0, -2.5e+2, 0, -1))",
        )
        .unwrap()
        .unwrap();
        assert_eq!(g.srid, 3375);
        assert_eq!(g.transform, Some([1000.0, 1.0, 0.0, -250.0, 0.0, -1.0]));
    }

    #[test]
    fn out_of_range_srid_is_data_exception() {
        let err = create_georef("GEOREF (SRID 0, X col, Y row)").unwrap_err();
        let SqlError::DataException { detail } = err else {
            panic!("expected DataException, got {err:?}");
        };
        assert!(detail.contains("SRID 0"), "{detail}");
    }

    /// The parser checks syntax only: an undefined code in range parses and
    /// the planner refuses it.
    #[test]
    fn undefined_srid_in_range_parses() {
        let g = create_georef("GEOREF (SRID 999, X col, Y row)")
            .unwrap()
            .unwrap();
        assert_eq!(g.srid, 999);
    }

    #[test]
    fn negative_srid_is_data_exception() {
        let err = create_georef("GEOREF (SRID -4326, X col, Y row)").unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
    }

    #[test]
    fn wrong_coefficient_count_is_data_exception() {
        for gt in ["(1, 2, 3, 4, 5)", "(1, 2, 3, 4, 5, 6, 7)"] {
            let err = create_georef(&format!("GEOREF (SRID 4326, X col, Y row, TRANSFORM {gt})"))
                .unwrap_err();
            assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        }
    }

    #[test]
    fn malformed_georef_is_parse_error() {
        for tail in [
            "GEOREF (X col, Y row)",
            "GEOREF (SRID 4326, Y row, X col)",
            "GEOREF (SRID 4326, X col)",
            "GEOREF (SRID 4326, X col, Y row",
            "GEOREF (SRID 4326, X col, Y row, TRANSFORM (1, 'a', 0, 0, 0, 1))",
            "GEOREF (SRID 4326, X col, Y row, ROTATE (1))",
            "GEOREF (SRID 4.5, X col, Y row)",
        ] {
            let err = create_georef(tail).unwrap_err();
            assert!(matches!(err, SqlError::Parse { .. }), "{tail}: {err:?}");
        }
    }

    #[test]
    fn georef_after_with_is_refused() {
        let err =
            create_georef("WITH (prefix_bits = 4) GEOREF (SRID 4326, X col, Y row)").unwrap_err();
        assert!(matches!(err, SqlError::Parse { .. }), "{err:?}");
    }

    fn alter(sql: &str) -> Result<AlterArrayAction, SqlError> {
        match try_parse_array_statement(sql)? {
            Some(ArrayStatement::Alter(a)) => Ok(a.action),
            other => panic!("expected ALTER, got {other:?}"),
        }
    }

    #[test]
    fn alter_set_georef() {
        let action = alter(
            "ALTER ARRAY scene SET GEOREF (SRID 32648, X col, Y row, \
             TRANSFORM (600000, 10, 0, 700000, 0, -10))",
        )
        .unwrap();
        let AlterArrayAction::Georef(ArrayGeorefAlter::Set(g)) = action else {
            panic!("expected SET GEOREF, got {action:?}");
        };
        assert_eq!(g.srid, 32648);
        assert_eq!(
            g.transform,
            Some([600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0])
        );
    }

    #[test]
    fn alter_drop_georef() {
        let action = alter("ALTER ARRAY scene DROP GEOREF").unwrap();
        assert_eq!(action, AlterArrayAction::Georef(ArrayGeorefAlter::Drop));
    }

    #[test]
    fn alter_georef_errors() {
        let err = alter("ALTER ARRAY scene SET GEOREF (SRID 0, X col, Y row)").unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        for sql in [
            "ALTER ARRAY scene DROP GEOREF extra",
            "ALTER ARRAY scene DROP",
            "ALTER ARRAY scene DROP COLUMN x",
            "ALTER ARRAY scene SET GEOREF (SRID 4326, X col, Y row) extra",
        ] {
            let err = alter(sql).unwrap_err();
            assert!(matches!(err, SqlError::Parse { .. }), "{sql}: {err:?}");
        }
    }
}
