// SPDX-License-Identifier: BUSL-1.1

//! The decode, check and re-encode every array schema change shares.
//!
//! A georef or attr metadata change leaves `content_msgpack` as it is, so
//! `schema_hash` and every segment stay valid. Each change re-encodes the
//! schema and checks that the content bytes did not move.

use nodedb_array::ArrayError;
use nodedb_array::schema::ArraySchema;

/// The stored schema `msgpack` holds.
pub(crate) fn decode_schema(statement: &str, msgpack: &[u8]) -> crate::Result<ArraySchema> {
    zerompk::from_msgpack(msgpack).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{statement}: array schema decode: {e}"),
    })
}

/// The error a schema build, georef change or attr metadata change reports.
///
/// A georef or attr metadata refusal is `22000`. Every other schema
/// refusal is a plan error.
pub(crate) fn schema_error(statement: &str, error: ArrayError) -> crate::Error {
    match error {
        ArrayError::InvalidGeoref { .. } | ArrayError::InvalidAttrMeta { .. } => {
            crate::Error::DataException {
                detail: format!("{statement}: {error}"),
            }
        }
        other => crate::Error::PlanError {
            detail: format!("{statement}: {other}"),
        },
    }
}

/// Check that `altered_msgpack` keeps the content of `stored_msgpack`: every
/// field `schema_hash` covers. Only the georef and attr metadata can differ.
pub(crate) fn check_same_content(
    statement: &str,
    stored_msgpack: &[u8],
    altered_msgpack: &[u8],
) -> crate::Result<()> {
    let stored = decode_schema(statement, stored_msgpack)?;
    let altered = decode_schema(statement, altered_msgpack)?;
    let content = |schema: &ArraySchema| {
        schema
            .content_msgpack()
            .map_err(|e| schema_error(statement, e))
    };
    if stored.name != altered.name || content(&stored)? != content(&altered)? {
        return Err(crate::Error::Internal {
            detail: format!(
                "{statement}: the altered schema changes more than the georef and attr \
                 metadata, so schema_hash no longer matches"
            ),
        });
    }
    Ok(())
}

/// `altered` encoded, checked to keep the content of `stored_msgpack`.
pub(crate) fn reencode_checked(
    statement: &str,
    stored_msgpack: &[u8],
    altered: &ArraySchema,
) -> crate::Result<Vec<u8>> {
    let altered_msgpack =
        zerompk::to_msgpack_vec(altered).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("{statement}: schema encode: {e}"),
        })?;
    check_same_content(statement, stored_msgpack, &altered_msgpack)?;
    Ok(altered_msgpack)
}
