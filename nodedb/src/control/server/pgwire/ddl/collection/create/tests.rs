//! Collection name validation tests.

/// Collection name validation: allowed chars are `[a-zA-Z0-9_-]`.
fn validate_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[test]
fn valid_collection_names() {
    assert!(validate_name("docs"));
    assert!(validate_name("my_collection"));
    assert!(validate_name("my-collection"));
    assert!(validate_name("Collection123"));
    assert!(validate_name("a"));
}

#[test]
fn invalid_collection_names_rejected() {
    // Semicolons are sent by psql in multi-statement queries —
    // must be rejected with a clear error, not stored silently.
    assert!(!validate_name("docs;"));
    assert!(!validate_name("bad;name"));
    assert!(!validate_name("bad name"));
    assert!(!validate_name("bad.name"));
    assert!(!validate_name("bad/name"));
    assert!(!validate_name(""));
    assert!(!validate_name("events;"));
    assert!(!validate_name("orders;"));
}
