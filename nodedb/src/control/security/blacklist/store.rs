// SPDX-License-Identifier: BUSL-1.1

//! User blacklist store: in-memory cache + redb persistence.
//!
//! Provides O(1) lookup for blacklisted user IDs with optional TTL
//! for temporary bans. Checked after JWT signature verification,
//! before authorization (RLS, scopes).
//!
//! Storage: `_system.blacklist` table in redb SystemCatalog.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use super::ip::{CidrSet, normalize_peer_ip};
use crate::control::security::catalog::{StoredBlacklistEntry, SystemCatalog};
use crate::control::security::net::Cidr;

/// A cached blacklist entry with expiry tracking.
#[derive(Debug, Clone)]
pub struct BlacklistEntry {
    /// Entry key: `"user:{id}"` or `"ip:{addr}"`.
    pub key: String,
    /// Entry kind: "user" or "ip".
    pub kind: String,
    /// Reason for blacklisting.
    pub reason: String,
    /// Who created this entry.
    pub created_by: String,
    /// When blacklisted (epoch seconds).
    pub created_at: u64,
    /// When this entry expires (epoch seconds). 0 = permanent.
    pub expires_at: u64,
}

impl BlacklistEntry {
    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        if self.expires_at == 0 {
            return false; // Permanent.
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at
    }

    fn from_stored(s: &StoredBlacklistEntry) -> Self {
        Self {
            key: s.key.clone(),
            kind: s.kind.clone(),
            reason: s.reason.clone(),
            created_by: s.created_by.clone(),
            created_at: s.created_at,
            expires_at: s.expires_at,
        }
    }

    fn to_stored(&self) -> StoredBlacklistEntry {
        StoredBlacklistEntry {
            key: self.key.clone(),
            kind: self.kind.clone(),
            reason: self.reason.clone(),
            created_by: self.created_by.clone(),
            created_at: self.created_at,
            expires_at: self.expires_at,
        }
    }
}

/// Thread-safe blacklist store with O(1) exact-match lookup and CIDR-range
/// matching for IP entries.
pub struct BlacklistStore {
    /// key → BlacklistEntry for exact-match entries. Keys are `"user:{id}"`
    /// or `"ip:{addr}"` (single host, no prefix).
    entries: RwLock<HashMap<String, BlacklistEntry>>,
    /// CIDR-range IP entries (e.g. `"ip:10.0.0.0/8"`), kept separate so the
    /// common exact-match case never pays for a scan.
    cidr_entries: RwLock<CidrSet<BlacklistEntry>>,
    /// Optional catalog for persistence.
    catalog: Option<SystemCatalog>,
}

impl BlacklistStore {
    /// Create an in-memory-only store (for tests).
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            cidr_entries: RwLock::new(CidrSet::new()),
            catalog: None,
        }
    }

    /// Load blacklist entries from a catalog (borrows, does not own).
    ///
    /// Called during `SharedState::open()` after the credential store's
    /// catalog is available. Populates the in-memory cache from redb.
    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored = catalog.load_all_blacklist_entries()?;
        let mut expired_keys = Vec::new();
        let mut loaded_exact = Vec::new();
        let mut loaded_cidr: Vec<(String, Cidr, BlacklistEntry)> = Vec::new();

        for s in &stored {
            let entry = BlacklistEntry::from_stored(s);
            if entry.is_expired() {
                expired_keys.push(s.key.clone());
                continue;
            }
            match cidr_addr_part(&entry) {
                Some(addr) => match Cidr::parse(addr) {
                    Some(range) => loaded_cidr.push((entry.key.clone(), range, entry)),
                    None => {
                        warn!(key = %entry.key, "skipping unparsable CIDR blacklist entry on load");
                    }
                },
                None => loaded_exact.push(entry),
            }
        }

        // Clean up expired entries from redb.
        for key in &expired_keys {
            let _ = catalog.delete_blacklist_entry(key);
        }

        let loaded_count = loaded_exact.len() + loaded_cidr.len();
        if !loaded_exact.is_empty() {
            let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
            for entry in &loaded_exact {
                entries.insert(entry.key.clone(), entry.clone());
            }
        }
        if !loaded_cidr.is_empty() {
            let mut cidrs = self.cidr_entries.write().unwrap_or_else(|p| p.into_inner());
            for (key, range, entry) in loaded_cidr {
                cidrs.upsert(key, range, entry);
            }
        }
        if loaded_count > 0 {
            info!(
                active = loaded_count,
                expired_cleaned = expired_keys.len(),
                "blacklist loaded from catalog"
            );
        }

        Ok(())
    }

    /// Check if a user ID is blacklisted. Returns the entry if blocked.
    pub fn check_user(&self, user_id: &str) -> Option<BlacklistEntry> {
        let key = format!("user:{user_id}");
        self.check(&key)
    }

    /// Check if an IP address is blacklisted. Returns the entry if blocked.
    ///
    /// Checks exact-match entries first (O(1) hash lookup, the common
    /// case), then falls back to scanning CIDR-range entries. `ip` may
    /// carry a port (e.g. `"10.0.0.5:54321"`) — it is normalized before
    /// either check so a port suffix never defeats a match.
    pub fn check_ip(&self, ip: &str) -> Option<BlacklistEntry> {
        let parsed = normalize_peer_ip(ip);
        let key = match &parsed {
            Some(addr) => format!("ip:{addr}"),
            None => format!("ip:{ip}"),
        };
        if let Some(entry) = self.check(&key) {
            return Some(entry);
        }
        self.check_ip_cidrs(parsed.as_ref()?)
    }

    /// Scan CIDR-range entries for a match, lazily evicting any found
    /// expired along the way.
    fn check_ip_cidrs(&self, ip: &IpAddr) -> Option<BlacklistEntry> {
        let (found, expired) = {
            let cidrs = self.cidr_entries.read().unwrap_or_else(|p| p.into_inner());
            cidrs.find(ip, BlacklistEntry::is_expired)
        };
        for key in expired {
            self.evict_expired(&key);
        }
        found
    }

    /// Generic check by key. Handles TTL expiry.
    fn check(&self, key: &str) -> Option<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let entry = entries.get(key)?;
        if entry.is_expired() {
            drop(entries);
            self.evict_expired(key);
            None
        } else {
            Some(entry.clone())
        }
    }

    /// Add a user to the blacklist.
    pub fn blacklist_user(
        &self,
        user_id: &str,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<()> {
        let key = format!("user:{user_id}");
        self.add_entry(key, "user", reason, created_by, expires_at)
    }

    /// Add an IP address or CIDR range to the blacklist.
    ///
    /// A `/`-suffixed entry (e.g. `"10.0.0.0/8"`) is parsed and stored as a
    /// CIDR range matched by [`check_ip`](Self::check_ip) against every
    /// address in the range. A bare address is validated and stored under
    /// its canonical `IpAddr` string form so lookups stay exact-match O(1).
    /// Both IPv4 and IPv6 are supported by [`Cidr`]. Returns a
    /// [`crate::Error::BadRequest`] if `addr` is neither a valid IP address
    /// nor a valid CIDR range — an entry that can never match must not be
    /// stored silently.
    pub fn blacklist_ip(
        &self,
        addr: &str,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<()> {
        if addr.contains('/') {
            let range = Cidr::parse(addr).ok_or_else(|| crate::Error::BadRequest {
                detail: format!(
                    "invalid CIDR range '{addr}': expected e.g. '10.0.0.0/8' or a valid IPv6 prefix"
                ),
            })?;
            self.add_cidr_entry(format!("ip:{addr}"), range, reason, created_by, expires_at)
        } else {
            let parsed: IpAddr = addr.parse().map_err(|_| crate::Error::BadRequest {
                detail: format!("invalid IP address '{addr}'"),
            })?;
            self.add_entry(format!("ip:{parsed}"), "ip", reason, created_by, expires_at)
        }
    }

    /// Build a `BlacklistEntry` and persist it to the catalog (if any).
    /// Shared by [`add_entry`](Self::add_entry) and
    /// [`add_cidr_entry`](Self::add_cidr_entry); persistence happens before
    /// either caller updates its in-memory collection.
    fn build_and_persist_entry(
        &self,
        key: &str,
        kind: &str,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<BlacklistEntry> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = BlacklistEntry {
            key: key.into(),
            kind: kind.into(),
            reason: reason.into(),
            created_by: created_by.into(),
            created_at: now,
            expires_at,
        };

        // Persist first.
        if let Some(ref catalog) = self.catalog {
            catalog.put_blacklist_entry(&entry.to_stored())?;
        }

        Ok(entry)
    }

    /// Add a CIDR-range entry to the blacklist.
    fn add_cidr_entry(
        &self,
        key: String,
        range: Cidr,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<()> {
        let entry = self.build_and_persist_entry(&key, "ip", reason, created_by, expires_at)?;

        let mut cidrs = self.cidr_entries.write().unwrap_or_else(|p| p.into_inner());
        info!(key = %key, reason = %reason, expires_at, "blacklist CIDR entry added");
        cidrs.upsert(key, range, entry);
        Ok(())
    }

    /// Add an entry to the blacklist.
    fn add_entry(
        &self,
        key: String,
        kind: &str,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<()> {
        let entry = self.build_and_persist_entry(&key, kind, reason, created_by, expires_at)?;

        // Update cache.
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        info!(key = %key, reason = %reason, expires_at, "blacklist entry added");
        entries.insert(key, entry);
        Ok(())
    }

    /// Lazily drop an entry whose TTL has passed.
    ///
    /// Unlike an operator-issued removal this cannot fail the caller: an
    /// expired entry is already unenforced by every read path, so a catalog
    /// delete that does not land only means the eviction is retried on the
    /// next lookup. The failure is still surfaced, because a delete that keeps
    /// failing is a storage problem the retry will never resolve.
    fn evict_expired(&self, key: &str) {
        if let Err(e) = self.remove_entry(key) {
            warn!(key = %key, error = %e, "failed to evict expired blacklist entry");
        }
    }

    /// Remove a blacklist entry (exact-match or CIDR-range), reporting whether
    /// one was present.
    ///
    /// The catalog delete happens first and its failure is fatal to the whole
    /// removal: dropping the entry from memory anyway would report a lifted
    /// ban to the operator while leaving it on disk to be reloaded, verbatim,
    /// by the next restart. Persistence and cache therefore either both change
    /// or neither does.
    pub fn remove_entry(&self, key: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_blacklist_entry(key)?;
        }

        let removed_exact = {
            let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
            entries.remove(key).is_some()
        };
        if removed_exact {
            return Ok(true);
        }

        let mut cidrs = self.cidr_entries.write().unwrap_or_else(|p| p.into_inner());
        Ok(cidrs.remove(key))
    }

    /// Remove a user from the blacklist.
    pub fn unblacklist_user(&self, user_id: &str) -> crate::Result<bool> {
        self.remove_entry(&format!("user:{user_id}"))
    }

    /// Remove an IP or CIDR range from the blacklist.
    ///
    /// The key is derived exactly as [`Self::blacklist_ip`] derives it, or the
    /// removal silently fails to find the entry: a CIDR range is keyed
    /// verbatim, while a single address is keyed by its *canonical* form. An
    /// IPv6 address admits several spellings of the same address
    /// (`2001:0db8::1` and `2001:db8::1`), so removing by the raw caller
    /// string would no-op whenever the spelling differed from the one used to
    /// add it — leaving a ban in place that an operator believes they lifted.
    pub fn unblacklist_ip(&self, addr: &str) -> crate::Result<bool> {
        if addr.contains('/') {
            return self.remove_entry(&format!("ip:{addr}"));
        }
        match addr.parse::<IpAddr>() {
            Ok(parsed) => self.remove_entry(&format!("ip:{parsed}")),
            // Not a valid address, so no canonical form exists and nothing
            // could have been stored under one. Try the raw string anyway so a
            // legacy entry predating add-time validation can still be removed.
            Err(_) => self.remove_entry(&format!("ip:{addr}")),
        }
    }

    /// List all active (non-expired) entries, optionally filtered by kind.
    pub fn list(&self, kind_filter: Option<&str>) -> Vec<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let cidrs = self.cidr_entries.read().unwrap_or_else(|p| p.into_inner());
        entries
            .values()
            .cloned()
            .chain(cidrs.iter().cloned())
            .filter(|e| !e.is_expired() && kind_filter.map(|k| e.kind == k).unwrap_or(true))
            .collect()
    }

    /// All in-memory entries (including potentially expired ones that
    /// haven't been lazily evicted yet). Used by the recovery verifier
    /// for exact redb↔memory comparison.
    pub fn list_all_entries(&self) -> Vec<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let cidrs = self.cidr_entries.read().unwrap_or_else(|p| p.into_inner());
        entries
            .values()
            .cloned()
            .chain(cidrs.iter().cloned())
            .collect()
    }

    /// Clear all in-memory entries and reload from catalog.
    /// Used by the recovery verifier repair path.
    pub fn clear_and_reload(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        // Reload by clearing first then re-applying — load_from only appends.
        let stored = catalog.load_all_blacklist_entries()?;
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        let mut cidrs = self.cidr_entries.write().unwrap_or_else(|p| p.into_inner());
        entries.clear();
        cidrs.clear();
        for s in stored {
            let entry = BlacklistEntry::from_stored(&s);
            if entry.is_expired() {
                continue;
            }
            match cidr_addr_part(&entry) {
                Some(addr) => {
                    if let Some(range) = Cidr::parse(addr) {
                        cidrs.upsert(entry.key.clone(), range, entry);
                    } else {
                        warn!(key = %entry.key, "skipping unparsable CIDR blacklist entry on reload");
                    }
                }
                None => {
                    entries.insert(entry.key.clone(), entry);
                }
            }
        }
        Ok(())
    }

    /// Total active entries.
    pub fn count(&self) -> usize {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let cidrs = self.cidr_entries.read().unwrap_or_else(|p| p.into_inner());
        entries.values().filter(|e| !e.is_expired()).count()
            + cidrs.iter().filter(|e| !e.is_expired()).count()
    }

    /// Access the catalog (for shared use with other stores).
    pub fn catalog(&self) -> Option<&SystemCatalog> {
        self.catalog.as_ref()
    }
}

impl Default for BlacklistStore {
    fn default() -> Self {
        Self::new()
    }
}

/// If `entry` is a CIDR-range IP entry (key `"ip:<addr>/<prefix>"`), return
/// the `<addr>/<prefix>` part to be parsed by [`Cidr::parse`].
/// Returns `None` for exact-match entries (user or single-host IP).
fn cidr_addr_part(entry: &BlacklistEntry) -> Option<&str> {
    if entry.kind != "ip" {
        return None;
    }
    let addr = entry.key.strip_prefix("ip:")?;
    if addr.contains('/') { Some(addr) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blacklist_user_and_check() {
        let store = BlacklistStore::new();
        store.blacklist_user("user_42", "spam", "admin", 0).unwrap();

        assert!(store.check_user("user_42").is_some());
        assert!(store.check_user("user_99").is_none());
    }

    #[test]
    fn blacklist_ip_and_check() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("192.168.1.100", "abuse", "admin", 0)
            .unwrap();

        assert!(store.check_ip("192.168.1.100").is_some());
        assert!(store.check_ip("10.0.0.1").is_none());
    }

    #[test]
    fn expired_entry_not_returned() {
        let store = BlacklistStore::new();
        // Expires 1 second in the past.
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1;
        store
            .blacklist_user("user_expired", "test", "admin", past)
            .unwrap();

        assert!(store.check_user("user_expired").is_none());
    }

    #[test]
    fn unblacklist_removes_entry() {
        let store = BlacklistStore::new();
        store.blacklist_user("user_42", "spam", "admin", 0).unwrap();
        assert!(store.check_user("user_42").is_some());

        store.unblacklist_user("user_42").expect("lift the ban");
        assert!(store.check_user("user_42").is_none());
    }

    #[test]
    fn list_filters_by_kind() {
        let store = BlacklistStore::new();
        store.blacklist_user("u1", "spam", "admin", 0).unwrap();
        store.blacklist_ip("1.2.3.4", "abuse", "admin", 0).unwrap();

        assert_eq!(store.list(Some("user")).len(), 1);
        assert_eq!(store.list(Some("ip")).len(), 1);
        assert_eq!(store.list(None).len(), 2);
    }

    #[test]
    fn exact_ip_entry_still_matches() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("192.168.1.100", "abuse", "admin", 0)
            .unwrap();

        let entry = store.check_ip("192.168.1.100").expect("exact match");
        assert_eq!(entry.reason, "abuse");
        assert!(store.check_ip("192.168.1.101").is_none());
    }

    #[test]
    fn cidr_entry_matches_address_in_range() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", 0)
            .unwrap();

        let entry = store.check_ip("10.0.0.5").expect("in-range address");
        assert_eq!(entry.reason, "blocked network");
        let entry = store.check_ip("10.255.255.255").expect("in-range address");
        assert_eq!(entry.reason, "blocked network");
    }

    #[test]
    fn cidr_entry_does_not_match_address_outside_range() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", 0)
            .unwrap();

        assert!(store.check_ip("11.0.0.1").is_none());
        assert!(store.check_ip("192.168.1.1").is_none());
    }

    #[test]
    fn expired_cidr_entry_does_not_match() {
        let store = BlacklistStore::new();
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1;
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", past)
            .unwrap();

        assert!(store.check_ip("10.0.0.5").is_none());
    }

    #[test]
    fn ipv6_cidr_entry_matches_address_in_range() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("2001:db8::/32", "blocked network", "admin", 0)
            .unwrap();

        assert!(store.check_ip("2001:db8::1").is_some());
        assert!(store.check_ip("2001:db9::1").is_none());
    }

    #[test]
    fn check_ip_matches_addr_with_port_suffix() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", 0)
            .unwrap();
        store
            .blacklist_ip("192.168.1.100", "abuse", "admin", 0)
            .unwrap();
        store
            .blacklist_ip("2001:db8::/32", "blocked network", "admin", 0)
            .unwrap();

        // Peer address strings may arrive with a port attached — it must
        // not defeat either the exact-match or CIDR-range lookup, for
        // IPv4 or IPv6.
        assert!(store.check_ip("10.0.0.5:54321").is_some());
        assert!(store.check_ip("192.168.1.100:54321").is_some());
        assert!(store.check_ip("[2001:db8::1]:443").is_some());
        assert!(store.check_ip("[2001:db9::1]:443").is_none());
    }

    #[test]
    fn invalid_cidr_range_is_rejected() {
        let store = BlacklistStore::new();
        let err = store
            .blacklist_ip("10.0.0.0/33", "bad", "admin", 0)
            .unwrap_err();
        assert!(err.to_string().contains("invalid CIDR range"));
    }

    #[test]
    fn invalid_ip_address_is_rejected() {
        let store = BlacklistStore::new();
        let err = store
            .blacklist_ip("not-an-ip", "bad", "admin", 0)
            .unwrap_err();
        assert!(err.to_string().contains("invalid IP address"));
    }

    #[test]
    fn unblacklist_removes_cidr_entry() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", 0)
            .unwrap();
        assert!(store.check_ip("10.0.0.5").is_some());

        assert!(store.unblacklist_ip("10.0.0.0/8").expect("lift the ban"));
        assert!(store.check_ip("10.0.0.5").is_none());
    }

    /// An IPv6 address has several valid spellings. Removal must canonicalize
    /// the same way the add path does, or an operator who lifts a ban using a
    /// different (equally valid) spelling gets a silent no-op and the ban
    /// stays in force.
    #[test]
    fn unblacklist_ipv6_matches_non_canonical_spelling() {
        let store = BlacklistStore::new();
        store
            .blacklist_ip("2001:0db8::1", "abuse", "admin", 0)
            .expect("blacklist a valid IPv6 address");
        assert!(store.check_ip("2001:db8::1").is_some());

        assert!(store.unblacklist_ip("2001:0db8::1").expect("lift the ban"));
        assert!(store.check_ip("2001:db8::1").is_none());
    }

    #[test]
    fn list_and_count_include_cidr_entries() {
        let store = BlacklistStore::new();
        store.blacklist_user("u1", "spam", "admin", 0).unwrap();
        store
            .blacklist_ip("10.0.0.0/8", "blocked network", "admin", 0)
            .unwrap();
        store.blacklist_ip("1.2.3.4", "abuse", "admin", 0).unwrap();

        assert_eq!(store.count(), 3);
        assert_eq!(store.list(Some("ip")).len(), 2);
        assert_eq!(store.list(None).len(), 3);
    }
}
