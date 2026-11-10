// SPDX-License-Identifier: Apache-2.0

//! First-class ID type generation and validation.
//!
//! Supports: UUID v4, UUID v7, ULID, CUID2, NanoID.
//!
//! All IDs are represented as strings for JSON compatibility.
//! UUID v7 and ULID are time-sortable (lexicographic order = chronological order).

use rand::RngExt;

use crate::id::{IdError, IdType};

// ── UUID ──

/// Generate a random UUID v4 (128-bit, not time-sortable).
pub fn uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generate a time-sorted UUID v7 (128-bit, time-sortable).
///
/// Embeds a Unix millisecond timestamp in the high bits, so lexicographic
/// sort order matches insertion order. Recommended for primary keys.
pub fn uuid_v7() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Validate whether a string is a valid UUID (any version).
pub fn is_uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

/// The canonical text of a UUID: lowercase, hyphenated.
///
/// Accepts the spellings PostgreSQL's `uuid` input accepts: hyphenated,
/// 32 plain hex digits, braced, upper or lower case. `None` when `s` is no
/// UUID.
pub fn canonical_uuid(s: &str) -> Option<String> {
    uuid::Uuid::parse_str(s.trim())
        .ok()
        .map(|u| u.hyphenated().to_string())
}

/// The canonical text of the UUID in a 16-byte payload, the PostgreSQL
/// binary `uuid` form. `None` when `bytes` is not 16 bytes long.
pub fn uuid_from_bytes(bytes: &[u8]) -> Option<String> {
    uuid::Uuid::from_slice(bytes)
        .ok()
        .map(|u| u.hyphenated().to_string())
}

/// The 16 bytes of a UUID's text, the PostgreSQL binary `uuid` form.
/// `None` when `s` is no UUID.
pub fn uuid_to_bytes(s: &str) -> Option<[u8; 16]> {
    uuid::Uuid::parse_str(s.trim()).ok().map(|u| *u.as_bytes())
}

/// Extract the version from a UUID string (1-7, or 0 if invalid).
pub fn uuid_version(s: &str) -> u8 {
    uuid::Uuid::parse_str(s)
        .map(|u| u.get_version_num())
        .unwrap_or(0) as u8
}

// ── ULID ──

/// Generate a ULID (Universally Unique Lexicographically Sortable Identifier).
///
/// 128-bit: 48-bit timestamp (ms) + 80-bit random. Crockford Base32 encoded.
/// Time-sortable: lexicographic order = chronological order.
pub fn ulid() -> String {
    ulid::Ulid::generate().to_string()
}

/// Validate whether a string is a valid ULID.
pub fn is_ulid(s: &str) -> bool {
    ulid::Ulid::from_string(s).is_ok()
}

/// Extract the millisecond timestamp from a ULID.
pub fn ulid_timestamp_ms(s: &str) -> Option<u64> {
    ulid::Ulid::from_string(s).ok().map(|u| u.timestamp_ms())
}

// ── CUID2 ──

/// CUID2 minimum allowed length (per spec).
const CUID2_MIN_LEN: usize = 4;
/// CUID2 maximum allowed length (per spec).
const CUID2_MAX_LEN: usize = 32;

/// Generate a CUID2 (Collision-resistant Unique Identifier v2) with default length 24.
///
/// Variable length (24 chars), cryptographically random, starts with a letter
/// (safe for HTML IDs, CSS selectors, database keys).
pub fn cuid2() -> String {
    cuid2_with_length(24).expect("24 is within [4, 32]")
}

/// Generate a CUID2 with a custom length.
///
/// # Errors
///
/// Returns [`IdError::LengthOutOfRange`] if `length` is outside `[4, 32]`.
pub fn cuid2_with_length(length: usize) -> Result<String, IdError> {
    if !(CUID2_MIN_LEN..=CUID2_MAX_LEN).contains(&length) {
        return Err(IdError::LengthOutOfRange {
            requested: length,
            min: CUID2_MIN_LEN,
            max: CUID2_MAX_LEN,
        });
    }

    let mut rng = rand::rng();

    // CUID2 always starts with a lowercase letter.
    let first = (b'a' + rng.random_range(0..26)) as char;

    // Remaining characters are from a base36-like alphabet.
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let rest: String = (1..length)
        .map(|_| {
            let idx = rng.random_range(0..ALPHABET.len());
            ALPHABET[idx] as char
        })
        .collect();

    Ok(format!("{first}{rest}"))
}

/// Validate whether a string looks like a CUID2.
///
/// A valid CUID2:
/// - Has length in `[4, 32]`
/// - Starts with a lowercase ASCII letter
/// - Contains only lowercase ASCII letters and ASCII digits
pub fn is_cuid2(s: &str) -> bool {
    let len = s.len();
    if !(CUID2_MIN_LEN..=CUID2_MAX_LEN).contains(&len) {
        return false;
    }
    let bytes = s.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

// ── NanoID ──

/// NanoID minimum detection length.
const NANOID_MIN_LEN: usize = 10;
/// NanoID maximum detection length.
const NANOID_MAX_LEN: usize = 64;

/// Generate a NanoID (URL-friendly unique string identifier).
///
/// Default 21 characters using `A-Za-z0-9_-` alphabet.
/// ~149 bits of entropy — comparable to UUID v4.
pub fn nanoid() -> String {
    nanoid::nanoid!()
}

/// Generate a NanoID with custom length.
pub fn nanoid_with_length(length: usize) -> String {
    nanoid::nanoid!(length)
}

/// Validate whether a string looks like a NanoID.
///
/// A valid NanoID for detection purposes:
/// - Has length in `[10, 64]`
/// - Contains only `A-Za-z0-9`, `_`, or `-`
pub fn is_nanoid(s: &str) -> bool {
    let len = s.len();
    if !(NANOID_MIN_LEN..=NANOID_MAX_LEN).contains(&len) {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

// ── Generic ID helpers ──

/// Detect the type of a string ID.
///
/// Returns an [`IdType`] variant. Checks in order of specificity:
/// UUID and ULID have strict structural formats; CUID2 and NanoID overlap
/// in charset so CUID2 (stricter bounds) is tested first.
/// Anything that doesn't match → [`IdType::Custom`].
pub fn detect_id_type(s: &str) -> IdType {
    if is_uuid(s) {
        IdType::Uuid
    } else if is_ulid(s) {
        IdType::Ulid
    } else if is_cuid2(s) {
        IdType::Cuid2
    } else if is_nanoid(s) {
        IdType::NanoId
    } else {
        IdType::Custom
    }
}

/// Generate an ID by type name.
///
/// Supported types: `"uuidv7"`, `"uuidv4"`, `"ulid"`, `"cuid2"`, `"nanoid"`.
/// Returns `None` for unknown types.
pub fn generate_by_type(id_type: &str) -> Option<String> {
    match id_type {
        "uuidv7" => Some(uuid_v7()),
        "uuidv4" => Some(uuid_v4()),
        "ulid" => Some(ulid()),
        "cuid2" => Some(cuid2()),
        "nanoid" => Some(nanoid()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_text_and_bytes_are_canonical() {
        let canonical = "550e8400-e29b-41d4-a716-446655440000";
        for spelling in [
            canonical,
            "550E8400-E29B-41D4-A716-446655440000",
            "550e8400e29b41d4a716446655440000",
            "{550e8400-e29b-41d4-a716-446655440000}",
        ] {
            assert_eq!(canonical_uuid(spelling).as_deref(), Some(canonical));
        }
        assert_eq!(canonical_uuid("not-a-uuid"), None);
        let bytes = uuid_to_bytes(canonical).expect("valid uuid");
        assert_eq!(uuid_from_bytes(&bytes).as_deref(), Some(canonical));
        assert_eq!(uuid_from_bytes(&bytes[..15]), None);
    }

    #[test]
    fn uuid_v4_valid() {
        let id = uuid_v4();
        assert!(is_uuid(&id));
        assert_eq!(uuid_version(&id), 4);
        assert_eq!(id.len(), 36); // 8-4-4-4-12 with hyphens
    }

    #[test]
    fn uuid_v7_valid_and_sortable() {
        let id1 = uuid_v7();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = uuid_v7();
        assert!(is_uuid(&id1));
        assert!(is_uuid(&id2));
        assert_eq!(uuid_version(&id1), 7);
        // v7 UUIDs are time-sortable: id1 < id2 lexicographically.
        assert!(id1 < id2, "v7 should be time-sortable: {id1} < {id2}");
    }

    #[test]
    fn ulid_valid_and_sortable() {
        let id1 = ulid();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = ulid();
        assert!(is_ulid(&id1));
        assert!(is_ulid(&id2));
        assert_eq!(id1.len(), 26); // Crockford Base32
        assert!(id1 < id2, "ULID should be time-sortable: {id1} < {id2}");
    }

    #[test]
    fn ulid_timestamp() {
        let id = ulid();
        let ts = ulid_timestamp_ms(&id).unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(ts <= now_ms);
        assert!(now_ms - ts < 1000); // within 1 second
    }

    #[test]
    fn cuid2_valid() {
        let id = cuid2();
        assert!(is_cuid2(&id));
        assert_eq!(id.len(), 24);
        assert!(id.as_bytes()[0].is_ascii_lowercase()); // starts with letter
    }

    #[test]
    fn cuid2_with_length_valid_range() {
        for len in [4usize, 8, 16, 24, 32] {
            let id = cuid2_with_length(len).unwrap_or_else(|e| panic!("len {len} failed: {e}"));
            assert_eq!(id.len(), len, "length mismatch for requested {len}");
            assert!(is_cuid2(&id), "is_cuid2 rejected id of len {len}");
        }
    }

    #[test]
    fn cuid2_with_length_out_of_range_errors() {
        let too_small = cuid2_with_length(3);
        assert!(
            matches!(
                too_small,
                Err(IdError::LengthOutOfRange {
                    requested: 3,
                    min: 4,
                    max: 32
                })
            ),
            "expected LengthOutOfRange for len 3, got {too_small:?}"
        );

        let too_large = cuid2_with_length(33);
        assert!(
            matches!(
                too_large,
                Err(IdError::LengthOutOfRange {
                    requested: 33,
                    min: 4,
                    max: 32
                })
            ),
            "expected LengthOutOfRange for len 33, got {too_large:?}"
        );
    }

    #[test]
    fn cuid2_uniqueness() {
        let mut ids: Vec<String> = (0..1000).map(|_| cuid2()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 1000); // all unique
    }

    #[test]
    fn is_cuid2_bounds() {
        // Too short (3 chars)
        assert!(!is_cuid2("abc"));
        // Too long (33 chars) — exceeds CUID2_MAX_LEN
        assert!(!is_cuid2("abcdefghijklmnopqrstuvwxyz1234567"));
        // Uppercase start — rejected
        assert!(!is_cuid2("Abcdefghijklmnopqrstuvwx"));
        // Contains uppercase in body — rejected
        assert!(!is_cuid2("abcDef"));
        // Valid minimal (4 chars)
        assert!(is_cuid2("abcd"));
        // Valid maximal (32 chars)
        assert!(is_cuid2("abcdefghijklmnopqrstuvwxyz123456"));
    }

    #[test]
    fn nanoid_valid() {
        let id = nanoid();
        assert!(is_nanoid(&id));
        assert_eq!(id.len(), 21);
    }

    #[test]
    fn nanoid_custom_length() {
        let id = nanoid_with_length(32);
        assert!(is_nanoid(&id));
        assert_eq!(id.len(), 32);
    }

    #[test]
    fn nanoid_detection_bounds() {
        // Below minimum (9 chars) — not detected as nanoid
        let short = nanoid_with_length(9);
        assert!(!is_nanoid(&short), "9-char nanoid should not be detected");
        // Above maximum (65 chars) — not detected
        let long = nanoid_with_length(65);
        assert!(!is_nanoid(&long), "65-char nanoid should not be detected");
        // At bounds
        assert!(is_nanoid(&nanoid_with_length(10)));
        assert!(is_nanoid(&nanoid_with_length(64)));
    }

    #[test]
    fn detect_types() {
        assert_eq!(detect_id_type(&uuid_v4()), IdType::Uuid);
        assert_eq!(detect_id_type(&uuid_v7()), IdType::Uuid);
        assert_eq!(detect_id_type(&ulid()), IdType::Ulid);
        assert_eq!(detect_id_type(&cuid2()), IdType::Cuid2);
        assert_eq!(detect_id_type("not-a-valid-id!@#"), IdType::Custom);
    }

    #[test]
    fn detect_id_type_exhaustive_match() {
        // Compile-time check: exhaustive match with no `_` arm forces updating
        // this test whenever a new IdType variant is added.
        let id_type = detect_id_type(&cuid2());
        match id_type {
            IdType::Uuid => {}
            IdType::Ulid => {}
            IdType::Cuid2 => {}
            IdType::NanoId => {}
            IdType::Custom => {}
        }
    }

    #[test]
    fn id_type_as_str() {
        assert_eq!(IdType::Uuid.as_str(), "uuid");
        assert_eq!(IdType::Ulid.as_str(), "ulid");
        assert_eq!(IdType::Cuid2.as_str(), "cuid2");
        assert_eq!(IdType::NanoId.as_str(), "nanoid");
        assert_eq!(IdType::Custom.as_str(), "custom");
    }

    #[test]
    fn is_uuid_rejects_invalid() {
        assert!(!is_uuid("not-a-uuid"));
        assert!(!is_uuid(""));
        assert!(!is_uuid("12345"));
    }

    #[test]
    fn is_ulid_rejects_invalid() {
        assert!(!is_ulid("not-a-ulid"));
        assert!(!is_ulid(""));
        assert!(!is_ulid(&uuid_v4())); // UUID is not ULID
    }
}
