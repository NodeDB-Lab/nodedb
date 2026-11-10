// SPDX-License-Identifier: BUSL-1.1

//! Decoders for the parameter types a value keeps exactly: `uuid`, `bytea`,
//! `timestamp` and `timestamptz`, in either wire format.
//!
//! Each decoder returns the typed value or a SQLSTATE-tagged error naming
//! the parameter. Text that spells no value of the declared type is refused
//! rather than passed on as text: a string bound for a `uuid` parameter
//! would reach a schemaless field as a string.

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Build a typed pgwire `ErrorInfo::new("ERROR", sqlstate, message)` error.
/// Shared by every SQLSTATE-tagged error parameter conversion raises.
pub(super) fn pg_error(sqlstate: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        sqlstate.to_owned(),
        message,
    )))
}

/// The SQLSTATE of text that spells no value of its type
/// (`invalid_text_representation`).
const INVALID_TEXT: &str = "22P02";
/// The SQLSTATE of text that spells no instant (`invalid_datetime_format`).
const INVALID_DATETIME: &str = "22007";

/// Microseconds between the Unix epoch and the PostgreSQL binary timestamp
/// epoch (2000-01-01T00:00:00Z): 10_957 days x 86_400 s x 1e6. PostgreSQL
/// transmits binary TIMESTAMP/TIMESTAMPTZ as microseconds from that epoch.
const POSTGRES_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Decode a binary TIMESTAMP/TIMESTAMPTZ payload: an 8-byte big-endian
/// microsecond offset from the PostgreSQL epoch (2000-01-01T00:00:00Z),
/// converted to `NdbDateTime` (microseconds since the Unix epoch).
///
/// Rejects payloads that are not exactly 8 bytes (SQLSTATE 22P02).
///
/// Also rejects PostgreSQL's +/-infinity sentinels, which are transmitted
/// as the raw `i64::MAX` / `i64::MIN` values. `i64::MIN` rebases to a valid
/// (if absurdly distant) Unix-epoch offset under plain addition — it does
/// not overflow `checked_add` — so it is checked explicitly rather than
/// relying on overflow detection alone. `i64::MAX` does overflow once
/// rebased and is caught by `checked_add`. `NdbDateTime` has no infinity
/// concept, so both sentinels must error rather than decode to a finite
/// (and wrong) timestamp.
pub(super) fn binary_timestamp(
    bytes: &[u8],
    index: usize,
) -> PgWireResult<nodedb_types::datetime::NdbDateTime> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
        pg_error(
            INVALID_TEXT,
            format!(
                "invalid binary representation for parameter ${}: expected 8 bytes, got {}",
                index + 1,
                bytes.len()
            ),
        )
    })?;
    let pg_micros = i64::from_be_bytes(raw);
    let out_of_range = || {
        pg_error(
            INVALID_TEXT,
            format!(
                "invalid binary representation for parameter ${}: timestamp out of range",
                index + 1
            ),
        )
    };
    if pg_micros == i64::MAX || pg_micros == i64::MIN {
        return Err(out_of_range());
    }
    let unix_micros = pg_micros
        .checked_add(POSTGRES_EPOCH_OFFSET_MICROS)
        .ok_or_else(out_of_range)?;
    Ok(nodedb_types::datetime::NdbDateTime::from_micros(
        unix_micros,
    ))
}

/// Decode a binary UUID payload: its 16 bytes, as the canonical UUID text.
/// Any other length is refused (SQLSTATE 22P02).
pub(super) fn binary_uuid(bytes: &[u8], index: usize) -> PgWireResult<String> {
    nodedb_types::id_gen::uuid_from_bytes(bytes).ok_or_else(|| {
        pg_error(
            INVALID_TEXT,
            format!(
                "invalid binary representation for parameter ${}: expected 16 bytes, got {}",
                index + 1,
                bytes.len()
            ),
        )
    })
}

/// Decode a text UUID parameter to its canonical text. Text that spells no
/// UUID is refused (SQLSTATE 22P02).
pub(super) fn text_uuid(text: &str, index: usize) -> PgWireResult<String> {
    nodedb_types::id_gen::canonical_uuid(text).ok_or_else(|| invalid_text("uuid", text, index))
}

/// Decode a text BYTEA parameter, hex or escape form. Text that is neither
/// is refused (SQLSTATE 22P02).
pub(super) fn text_bytea(text: &str, index: usize) -> PgWireResult<Vec<u8>> {
    nodedb_types::bytea_text::parse_bytea_text(text)
        .ok_or_else(|| invalid_text("bytea", text, index))
}

/// Decode a text TIMESTAMP / TIMESTAMPTZ parameter. Text that spells no
/// instant is refused (SQLSTATE 22007), never passed on as text.
pub(super) fn text_timestamp(
    text: &str,
    type_name: &str,
    index: usize,
) -> PgWireResult<nodedb_types::datetime::NdbDateTime> {
    nodedb_types::datetime::NdbDateTime::parse(text).ok_or_else(|| {
        pg_error(
            INVALID_DATETIME,
            format!(
                "invalid input syntax for type {type_name}: \"{text}\" (parameter ${})",
                index + 1
            ),
        )
    })
}

fn invalid_text(type_name: &str, text: &str, index: usize) -> PgWireError {
    pg_error(
        INVALID_TEXT,
        format!(
            "invalid input syntax for type {type_name}: \"{text}\" (parameter ${})",
            index + 1
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn sqlstate(error: PgWireError) -> String {
        match error {
            PgWireError::UserError(info) => info.code,
            other => panic!("a user error, got {other:?}"),
        }
    }

    #[test]
    fn uuid_decodes_from_either_format() {
        let bytes = nodedb_types::id_gen::uuid_to_bytes(UUID).expect("valid uuid");
        assert_eq!(binary_uuid(&bytes, 0).expect("16 bytes"), UUID);
        assert_eq!(
            text_uuid(&UUID.to_ascii_uppercase(), 0).expect("uuid text"),
            UUID
        );
    }

    #[test]
    fn invalid_uuid_and_bytea_are_22p02() {
        assert_eq!(sqlstate(binary_uuid(&[0; 15], 0).unwrap_err()), "22P02");
        assert_eq!(sqlstate(text_uuid("not-a-uuid", 0).unwrap_err()), "22P02");
        assert_eq!(sqlstate(text_bytea("\\x0", 0).unwrap_err()), "22P02");
    }

    #[test]
    fn bytea_text_decodes_hex_and_escape_forms() {
        assert_eq!(text_bytea("\\x0102ff", 0).expect("hex"), vec![1, 2, 0xff]);
        assert_eq!(text_bytea("a\\001", 0).expect("escape"), vec![b'a', 1]);
    }

    #[test]
    fn invalid_timestamp_text_is_22007() {
        let error = text_timestamp("not a time", "timestamptz", 2).unwrap_err();
        assert!(error.to_string().contains("$3"), "{error}");
        assert_eq!(sqlstate(error), "22007");
    }
}
