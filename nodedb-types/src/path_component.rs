// SPDX-License-Identifier: Apache-2.0

//! Validation for names that become a single filesystem path component.
//!
//! Collection names, partition directory names, and segment file names all
//! arrive from outside the process — SQL DDL, a Raft `InstallSnapshot` blob, a
//! RESTORE payload — and are then joined onto a data directory. A name that is
//! not a single plain component escapes that directory: `..` walks out of it,
//! an embedded separator writes into a subtree the caller never named, and a
//! leading `/` discards the base entirely (`Path::join` replaces the whole path
//! on an absolute argument). Since the same names are also passed to
//! `remove_dir_all`, an unchecked one deletes outside the data directory too.
//!
//! Every path builder that joins an externally supplied name calls
//! [`is_plain_path_component`] first and raises its own domain error, so the
//! rejection names the payload that carried the bad name.

/// Whether `name` is safe to join onto a directory as exactly one component.
///
/// Accepts a non-empty name that is not `.` or `..`, carries no path separator
/// (`/` on every platform, and `\` so a Unix-written name cannot escape when
/// the same data directory is opened on Windows), no interior NUL, no drive
/// prefix (`C:`), and no leading or trailing whitespace or dot that would make
/// two distinct records collide in one directory entry.
pub fn is_plain_path_component(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    if name.contains(['/', '\\', ':', '\0']) {
        return false;
    }
    if name.starts_with(['.', ' ']) || name.ends_with(['.', ' ']) {
        return false;
    }
    // A control character is never part of a legitimate name and turns log
    // lines and error messages into terminal escape sequences.
    !name.chars().any(char::is_control)
}

/// A fixed-length file name stem for `key`: 32 hex digits, the first 128 bits
/// of SHA-256 over the bytes. The result is always a plain path component.
pub fn hashed_file_stem(key: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key);
    hex::encode(&digest[..16])
}

#[cfg(test)]
mod tests {
    use super::is_plain_path_component;

    #[test]
    fn accepts_ordinary_names() {
        for name in [
            "ts-20240101-000000_20240102-000000",
            "partition.meta",
            "users",
            "col_1",
            "a-b.c-d",
        ] {
            assert!(is_plain_path_component(name), "should accept {name:?}");
        }
    }

    #[test]
    fn rejects_traversal_and_separators() {
        for name in [
            "",
            ".",
            "..",
            "../etc",
            "..\\etc",
            "a/b",
            "a\\b",
            "/abs",
            "C:name",
            ".hidden",
            "trailing.",
            " leading",
            "trailing ",
            "nul\0byte",
            "esc\u{1b}[2J",
            "new\nline",
        ] {
            assert!(!is_plain_path_component(name), "should reject {name:?}");
        }
    }
}
