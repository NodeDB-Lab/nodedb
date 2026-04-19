//! CSPRNG-backed identifier generation for security-bearing tokens.
//!
//! Used for session handles (`nds_…`), session IDs (`s_…`), and any other
//! opaque identifier whose confidentiality matters. The output carries
//! 128 bits of OS-random entropy, hex-encoded, with a caller-chosen tag
//! prefix for log grepability.
//!
//! **Never** roll a new identifier generator elsewhere — every
//! security-sensitive ID in this crate MUST call `generate_tagged_random_hex`.
//! Timestamp + counter schemes are forbidden here: they leak server state
//! and enable enumeration.

use argon2::password_hash::rand_core::{OsRng, RngCore};
use std::fmt::Write;

/// Size in bytes of the random payload (128 bits).
const RANDOM_BYTES: usize = 16;

/// Generate a tagged, 128-bit CSPRNG-random hex identifier.
///
/// Output format: `<prefix><32 hex chars>`. The prefix is preserved
/// verbatim — include any trailing underscore the caller wants.
///
/// Example: `generate_tagged_random_hex("nds_")` →
/// `"nds_3f2a81c9e4d5b6a70f8e1d2c3b4a5968"`.
pub fn generate_tagged_random_hex(prefix: &str) -> String {
    let mut bytes = [0u8; RANDOM_BYTES];
    OsRng.fill_bytes(&mut bytes);
    let mut s = String::with_capacity(prefix.len() + RANDOM_BYTES * 2);
    s.push_str(prefix);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The identifier must not embed the wall-clock second. An attacker that
    /// knows roughly when it was issued (from HTTP `Date:` headers, TLS
    /// handshake timestamps, or response timing) would otherwise recover a
    /// timestamp component directly.
    #[test]
    fn does_not_leak_wall_clock_second() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let id = generate_tagged_random_hex("tag_");
        for ts in (ts_now.saturating_sub(2))..=(ts_now + 2) {
            let hex = format!("{ts:x}");
            assert!(
                !id.contains(&hex),
                "identifier {id} embeds wall-clock second {ts} ({hex})"
            );
        }
    }

    /// The identifier must not embed the wall-clock millisecond either —
    /// covers the higher-resolution version of the same leak.
    #[test]
    fn does_not_leak_wall_clock_millisecond() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let id = generate_tagged_random_hex("tag_");
        for ts in (ts_ms.saturating_sub(50))..=(ts_ms + 50) {
            let hex = format!("{ts:x}");
            assert!(
                !id.contains(&hex),
                "identifier {id} embeds wall-clock ms {ts} ({hex})"
            );
        }
    }

    /// An attacker with one identifier must not be able to guess any other.
    /// Two consecutive calls must differ in nearly every byte.
    #[test]
    fn consecutive_ids_are_not_enumerable() {
        let a = generate_tagged_random_hex("tag_");
        let b = generate_tagged_random_hex("tag_");
        assert_ne!(a, b);
        assert_eq!(a.len(), b.len(), "identifier length should be stable");

        let diffs = a.bytes().zip(b.bytes()).filter(|(x, y)| x != y).count();
        assert!(
            diffs >= 16,
            "consecutive identifiers {a} and {b} differ in only {diffs} byte \
             positions — attacker who sees one can enumerate the other"
        );
    }

    /// The identifier must carry ≥128 bits of randomness past the tag —
    /// the minimum to defeat brute-force guessing.
    #[test]
    fn has_at_least_128_bits_of_entropy() {
        let prefix = "tag_";
        let id = generate_tagged_random_hex(prefix);
        let rest = id.strip_prefix(prefix).expect("prefix preserved");
        let random_chars = rest.chars().filter(|c| *c != '_').count();
        assert!(
            random_chars >= 32,
            "identifier {id} has {random_chars} non-delimiter chars after \
             prefix — insufficient entropy"
        );
    }

    /// A batch of identifiers must all be distinct AND must not share any
    /// common prefix beyond the caller-chosen tag — a shared runtime prefix
    /// would indicate a deterministic (timestamp/counter) component.
    #[test]
    fn batch_ids_have_no_shared_deterministic_prefix() {
        let prefix = "tag_";
        let ids: Vec<String> = (0..64)
            .map(|_| generate_tagged_random_hex(prefix))
            .collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate identifiers issued");

        let first = &ids[0];
        let mut max_shared = 0usize;
        for other in &ids[1..] {
            let shared = first
                .bytes()
                .zip(other.bytes())
                .take_while(|(a, b)| a == b)
                .count();
            max_shared = max_shared.max(shared);
        }
        // Threshold chosen to catch deterministic components (e.g. an 8-hex-char
        // wall-clock second or a shared counter) while tolerating the chance
        // that two random hex strings happen to share a few leading chars.
        // False-positive rate at +8 with 63 pairs: 63 * 16^-8 ≈ 4e-9.
        assert!(
            max_shared <= prefix.len() + 8,
            "identifiers share a {max_shared}-byte common prefix — indicates \
             a deterministic component leaking server state"
        );
    }

    #[test]
    fn preserves_caller_prefix() {
        let id = generate_tagged_random_hex("nds_");
        assert!(id.starts_with("nds_"));
        let id = generate_tagged_random_hex("s_");
        assert!(id.starts_with("s_"));
        let id = generate_tagged_random_hex("");
        assert_eq!(id.chars().filter(|c| *c != '_').count(), 32);
    }
}
