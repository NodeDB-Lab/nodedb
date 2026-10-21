// SPDX-License-Identifier: BUSL-1.1

//! IP/CIDR blacklist matching.
//!
//! Checks if a client IP address falls within any blacklisted CIDR range.
//! Used at connection accept time, before authentication.

use std::net::{IpAddr, SocketAddr};

use crate::control::security::net::Cidr;

/// Check if an IP address matches any CIDR entry in a blacklist.
///
/// `entries` is a list of CIDR strings (e.g., `["10.0.0.0/8", "192.168.1.100"]`).
/// Returns the first matching CIDR string, or `None` if no match.
pub fn check_ip_against_cidrs<'a>(ip_str: &str, entries: &'a [String]) -> Option<&'a str> {
    let ip: IpAddr = ip_str.parse().ok()?;
    for entry in entries {
        if let Some(cidr) = Cidr::parse(entry)
            && cidr.contains(&ip)
        {
            return Some(entry);
        }
    }
    None
}

/// Normalize a peer address string to a bare [`IpAddr`].
///
/// Callers on the connection path sometimes pass a plain address
/// (`"10.0.0.5"`) and sometimes a socket address with a port
/// (`"10.0.0.5:54321"`, `"[::1]:54321"`). A port left attached would
/// silently defeat every lookup (exact-match and CIDR alike), so this is
/// the single normalization point both are run through.
///
/// Returns `None` if `s` is neither a valid `IpAddr` nor a valid
/// `SocketAddr`.
pub fn normalize_peer_ip(s: &str) -> Option<IpAddr> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    s.parse::<SocketAddr>().ok().map(|sa| sa.ip())
}

/// A set of CIDR-range entries, each keyed by a string and carrying an
/// opaque value.
///
/// Kept separate from any exact-match map so the common case (an exact
/// address lookup) stays an O(1) hash lookup — only entries that are
/// genuinely ranges pay for iteration.
#[derive(Debug)]
pub struct CidrSet<T> {
    entries: Vec<(String, Cidr, T)>,
}

impl<T: Clone> CidrSet<T> {
    /// Create an empty set.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Insert a new entry, or replace the existing one with the same key.
    pub fn upsert(&mut self, key: String, range: Cidr, value: T) {
        if let Some(slot) = self.entries.iter_mut().find(|(k, _, _)| *k == key) {
            slot.1 = range;
            slot.2 = value;
        } else {
            self.entries.push((key, range, value));
        }
    }

    /// Remove an entry by key. Returns `true` if an entry was removed.
    pub fn remove(&mut self, key: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|(k, _, _)| k != key);
        self.entries.len() != before
    }

    /// Find the first non-"expired" entry whose range contains `ip`.
    ///
    /// `is_expired` is applied to each value as it is scanned; entries for
    /// which it returns `true` are skipped and their keys are returned
    /// alongside the match (if any) so the caller can lazily evict them.
    ///
    /// Every expired entry the scan encounters is reported, not just ones
    /// whose range contains `ip`. The scan visits the whole set regardless,
    /// so sweeping as it goes costs nothing and stops dead ranges — which no
    /// query may ever happen to match — from accumulating indefinitely.
    pub fn find(&self, ip: &IpAddr, is_expired: impl Fn(&T) -> bool) -> (Option<T>, Vec<String>) {
        let mut expired = Vec::new();
        let mut found = None;
        for (key, range, value) in &self.entries {
            if is_expired(value) {
                expired.push(key.clone());
                continue;
            }
            if found.is_none() && range.contains(ip) {
                found = Some(value.clone());
            }
        }
        (found, expired)
    }

    /// Iterate over all values (including any not-yet-evicted expired ones).
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().map(|(_, _, v)| v)
    }

    /// Number of entries in the set.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl<T: Clone> Default for CidrSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_against_list() {
        let entries = vec!["10.0.0.0/8".into(), "192.168.1.100".into()];
        assert_eq!(
            check_ip_against_cidrs("10.0.0.5", &entries),
            Some("10.0.0.0/8")
        );
        assert_eq!(
            check_ip_against_cidrs("192.168.1.100", &entries),
            Some("192.168.1.100")
        );
        assert_eq!(check_ip_against_cidrs("172.16.0.1", &entries), None);
    }

    #[test]
    fn normalize_peer_ip_strips_port() {
        assert_eq!(
            normalize_peer_ip("10.0.0.5:54321"),
            Some("10.0.0.5".parse().unwrap())
        );
        assert_eq!(
            normalize_peer_ip("[::1]:54321"),
            Some("::1".parse().unwrap())
        );
    }

    #[test]
    fn normalize_peer_ip_accepts_bare_addr() {
        assert_eq!(
            normalize_peer_ip("10.0.0.5"),
            Some("10.0.0.5".parse().unwrap())
        );
        assert_eq!(normalize_peer_ip("::1"), Some("::1".parse().unwrap()));
    }

    #[test]
    fn normalize_peer_ip_rejects_garbage() {
        assert_eq!(normalize_peer_ip("not-an-ip"), None);
    }

    #[test]
    fn cidr_set_find_matches_and_skips_expired() {
        let mut set: CidrSet<bool> = CidrSet::new();
        set.upsert(
            "ip:10.0.0.0/8".into(),
            Cidr::parse("10.0.0.0/8").unwrap(),
            false, // not expired
        );
        set.upsert(
            "ip:192.168.0.0/16".into(),
            Cidr::parse("192.168.0.0/16").unwrap(),
            true, // expired
        );

        // A match is returned for the live range, and the expired range is
        // reported for eviction even though it did not match this address:
        // the scan visits every entry anyway, so sweeping expired ones as it
        // goes is free and keeps the set from accumulating dead ranges that
        // no query ever happens to match.
        let (found, expired) = set.find(&"10.0.0.5".parse().unwrap(), |v| *v);
        assert_eq!(found, Some(false));
        assert_eq!(expired, vec!["ip:192.168.0.0/16".to_string()]);

        // An address inside an expired range does not match, and that range
        // is still reported for eviction.
        let (found, expired) = set.find(&"192.168.1.1".parse().unwrap(), |v| *v);
        assert_eq!(found, None);
        assert_eq!(expired, vec!["ip:192.168.0.0/16".to_string()]);
    }

    #[test]
    fn cidr_set_remove_and_upsert_replace() {
        let mut set: CidrSet<u32> = CidrSet::new();
        set.upsert(
            "ip:10.0.0.0/8".into(),
            Cidr::parse("10.0.0.0/8").unwrap(),
            1,
        );
        set.upsert(
            "ip:10.0.0.0/8".into(),
            Cidr::parse("10.0.0.0/8").unwrap(),
            2,
        );
        assert_eq!(set.len(), 1);
        let (found, _) = set.find(&"10.0.0.5".parse().unwrap(), |_| false);
        assert_eq!(found, Some(2));

        assert!(set.remove("ip:10.0.0.0/8"));
        assert!(set.is_empty());
        assert!(!set.remove("ip:10.0.0.0/8"));
    }
}
