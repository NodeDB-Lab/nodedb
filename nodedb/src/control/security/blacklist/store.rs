//! User blacklist store: in-memory cache + redb persistence.
//!
//! Provides O(1) lookup for blacklisted user IDs with optional TTL
//! for temporary bans. Checked after JWT signature verification,
//! before authorization (RLS, scopes).
//!
//! Storage: `_system.blacklist` table in redb SystemCatalog.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use crate::control::security::catalog::{StoredBlacklistEntry, SystemCatalog};

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

/// Thread-safe blacklist store with O(1) lookup.
pub struct BlacklistStore {
    /// key → BlacklistEntry. Keys are `"user:{id}"` or `"ip:{addr}"`.
    entries: RwLock<HashMap<String, BlacklistEntry>>,
    /// Optional catalog for persistence.
    catalog: Option<SystemCatalog>,
    /// JWT claim-based blocking: status values that deny access.
    blocked_statuses: Vec<String>,
    /// JWT claim name for status (e.g., "account_status").
    status_claim: Option<String>,
}

impl BlacklistStore {
    /// Create an in-memory-only store (for tests).
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            catalog: None,
            blocked_statuses: Vec::new(),
            status_claim: None,
        }
    }

    /// Configure JWT claim-based blocking.
    pub fn set_claim_blocking(
        &mut self,
        status_claim: Option<String>,
        blocked_statuses: Vec<String>,
    ) {
        self.status_claim = status_claim;
        self.blocked_statuses = blocked_statuses;
    }

    /// Check if a JWT status claim value is blocked.
    /// Returns `Some(status_value)` if blocked, `None` if allowed.
    pub fn check_jwt_status(
        &self,
        auth_ctx: &super::super::auth_context::AuthContext,
    ) -> Option<String> {
        let claim = self.status_claim.as_deref()?;
        if self.blocked_statuses.is_empty() {
            return None;
        }
        // Check metadata first (custom claims land there).
        let val = auth_ctx.metadata.get(claim).cloned().or_else(|| {
            if claim == "status" {
                Some(auth_ctx.status.to_string())
            } else {
                None
            }
        })?;
        if self.blocked_statuses.contains(&val) {
            Some(val)
        } else {
            None
        }
    }

    /// Load blacklist entries from a catalog (borrows, does not own).
    ///
    /// Called during `SharedState::open()` after the credential store's
    /// catalog is available. Populates the in-memory cache from redb.
    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored = catalog.load_all_blacklist_entries()?;
        let mut expired_keys = Vec::new();
        let mut loaded = Vec::new();

        for s in &stored {
            let entry = BlacklistEntry::from_stored(s);
            if entry.is_expired() {
                expired_keys.push(s.key.clone());
            } else {
                loaded.push(entry);
            }
        }

        // Clean up expired entries from redb.
        for key in &expired_keys {
            let _ = catalog.delete_blacklist_entry(key);
        }

        if !loaded.is_empty() {
            let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
            for entry in &loaded {
                entries.insert(entry.key.clone(), entry.clone());
            }
            info!(
                active = loaded.len(),
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
    /// Also checks CIDR ranges via the IP blacklist module.
    pub fn check_ip(&self, ip: &str) -> Option<BlacklistEntry> {
        let key = format!("ip:{ip}");
        self.check(&key)
    }

    /// Generic check by key. Handles TTL expiry.
    fn check(&self, key: &str) -> Option<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let entry = entries.get(key)?;
        if entry.is_expired() {
            drop(entries);
            // Lazy cleanup: remove expired entry.
            self.remove_entry(key);
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

    /// Add an IP address or CIDR to the blacklist.
    pub fn blacklist_ip(
        &self,
        addr: &str,
        reason: &str,
        created_by: &str,
        expires_at: u64,
    ) -> crate::Result<()> {
        let key = format!("ip:{addr}");
        self.add_entry(key, "ip", reason, created_by, expires_at)
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = BlacklistEntry {
            key: key.clone(),
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

        // Update cache.
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        info!(key = %key, reason = %reason, expires_at, "blacklist entry added");
        entries.insert(key, entry);
        Ok(())
    }

    /// Remove a blacklist entry.
    pub fn remove_entry(&self, key: &str) -> bool {
        if let Some(ref catalog) = self.catalog
            && let Err(e) = catalog.delete_blacklist_entry(key)
        {
            warn!(key = %key, error = %e, "failed to delete blacklist entry from catalog");
        }

        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        entries.remove(key).is_some()
    }

    /// Remove a user from the blacklist.
    pub fn unblacklist_user(&self, user_id: &str) -> bool {
        self.remove_entry(&format!("user:{user_id}"))
    }

    /// Remove an IP from the blacklist.
    pub fn unblacklist_ip(&self, addr: &str) -> bool {
        self.remove_entry(&format!("ip:{addr}"))
    }

    /// List all active (non-expired) entries, optionally filtered by kind.
    pub fn list(&self, kind_filter: Option<&str>) -> Vec<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        entries
            .values()
            .filter(|e| !e.is_expired() && kind_filter.map(|k| e.kind == k).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// All in-memory entries (including potentially expired ones that
    /// haven't been lazily evicted yet). Used by the recovery verifier
    /// for exact redb↔memory comparison.
    pub fn list_all_entries(&self) -> Vec<BlacklistEntry> {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        entries.values().cloned().collect()
    }

    /// Clear all in-memory entries and reload from catalog.
    /// Used by the recovery verifier repair path.
    pub fn clear_and_reload(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        // Reload by clearing first then re-applying — load_from only appends.
        let stored = catalog.load_all_blacklist_entries()?;
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        entries.clear();
        for s in stored {
            let entry = BlacklistEntry::from_stored(&s);
            if !entry.is_expired() {
                entries.insert(entry.key.clone(), entry);
            }
        }
        Ok(())
    }

    /// Total active entries.
    pub fn count(&self) -> usize {
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        entries.values().filter(|e| !e.is_expired()).count()
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

        store.unblacklist_user("user_42");
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
}
