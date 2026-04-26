//! Document storage keys are derived from the row's stable surrogate identity.
//!
//! The substrate redb tables (`DOCUMENTS`, `INDEXES`) use string keys; the
//! Document engine encodes each surrogate as a fixed-width 8-character
//! lowercase hexadecimal string (e.g. `Surrogate(42)` → `"0000002a"`).
//!
//! The format is intentionally fixed width: lexicographic ordering of the
//! hex string matches numeric ordering of the underlying surrogate, so
//! redb range scans can iterate rows in surrogate order without any
//! additional index. The user-visible primary key is bound to the
//! surrogate via the `_system.surrogate_pk{,_rev}` catalog tables.
use nodedb_types::Surrogate;

/// Format a surrogate as the 8-character zero-padded lowercase hex string
/// used as the document's redb key.
pub fn surrogate_to_doc_id(surrogate: Surrogate) -> String {
    format!("{:08x}", surrogate.as_u32())
}

/// Parse a hex-encoded document storage key back to a `Surrogate`.
///
/// Returns `None` if the key is not exactly 8 lowercase hex characters —
/// this handles legacy non-surrogate document IDs gracefully.
pub fn doc_id_to_surrogate(doc_id: &str) -> Option<Surrogate> {
    if doc_id.len() != 8 {
        return None;
    }
    u32::from_str_radix(doc_id, 16).ok().map(Surrogate::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_zero_padded_lowercase() {
        assert_eq!(surrogate_to_doc_id(Surrogate::new(0)), "00000000");
        assert_eq!(surrogate_to_doc_id(Surrogate::new(42)), "0000002a");
        assert_eq!(surrogate_to_doc_id(Surrogate::new(0xDEAD_BEEF)), "deadbeef");
    }

    #[test]
    fn lex_order_matches_numeric() {
        let a = surrogate_to_doc_id(Surrogate::new(0x10));
        let b = surrogate_to_doc_id(Surrogate::new(0x100));
        assert!(a < b);
    }
}
