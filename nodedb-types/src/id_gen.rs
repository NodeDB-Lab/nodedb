//! First-class ID type generation and validation.
//!
//! Supports: UUID v4, UUID v7, ULID, CUID2, NanoID.
//!
//! All IDs are represented as strings for JSON compatibility.
//! UUID v7 and ULID are time-sortable (lexicographic order = chronological order).

use rand::Rng;

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
    ulid::Ulid::new().to_string()
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

/// Generate a CUID2 (Collision-resistant Unique Identifier v2).
///
/// Variable length (default 24 chars), cryptographically random, starts with
/// a letter (safe for HTML IDs, CSS selectors, database keys).
///
/// Implemented inline — no external crate dependency.
pub fn cuid2() -> String {
    cuid2_with_length(24)
}

/// Generate a CUID2 with custom length (min 4, max 64).
pub fn cuid2_with_length(length: usize) -> String {
    let length = length.clamp(4, 64);
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

    format!("{first}{rest}")
}

/// Validate whether a string looks like a CUID2 (starts with letter, alphanumeric).
pub fn is_cuid2(s: &str) -> bool {
    if s.len() < 4 {
        return false;
    }
    let first = s.as_bytes()[0];
    if !first.is_ascii_lowercase() {
        return false;
    }
    s.bytes().all(|b| b.is_ascii_alphanumeric())
}

// ── NanoID ──

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

/// Validate whether a string looks like a NanoID (URL-safe characters, reasonable length).
pub fn is_nanoid(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

// ── Generic ID helpers ──

/// Detect the type of a string ID.
///
/// Returns one of: "uuid", "ulid", "cuid2", "nanoid", "unknown".
/// Checks in order of specificity (UUID and ULID have strict formats).
pub fn detect_id_type(s: &str) -> &'static str {
    if is_uuid(s) {
        "uuid"
    } else if is_ulid(s) {
        "ulid"
    } else if is_cuid2(s) && s.len() >= 20 {
        "cuid2"
    } else if is_nanoid(s) {
        "nanoid"
    } else {
        "unknown"
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
    fn cuid2_custom_length() {
        let short = cuid2_with_length(8);
        assert_eq!(short.len(), 8);
        assert!(is_cuid2(&short));

        let long = cuid2_with_length(48);
        assert_eq!(long.len(), 48);
        assert!(is_cuid2(&long));
    }

    #[test]
    fn cuid2_uniqueness() {
        let mut ids: Vec<String> = (0..1000).map(|_| cuid2()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 1000); // all unique
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
    fn detect_types() {
        assert_eq!(detect_id_type(&uuid_v4()), "uuid");
        assert_eq!(detect_id_type(&uuid_v7()), "uuid");
        assert_eq!(detect_id_type(&ulid()), "ulid");
        assert_eq!(detect_id_type("not-a-valid-id!@#"), "unknown");
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
