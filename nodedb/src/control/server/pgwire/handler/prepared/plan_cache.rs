//! Per-session plan cache for prepared statements.
//!
//! Caches compiled `Vec<PhysicalTask>` per SQL string, keyed by `(sql_hash, schema_version)`.
//! When the schema version changes (CREATE/DROP/ALTER), cached entries are invalidated.
//! This avoids re-parsing and re-planning identical SQL on every Execute.

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::control::planner::physical::PhysicalTask;

/// Global schema version counter, bumped on any DDL that changes the schema.
///
/// Stored on `SharedState`. Plan caches compare their snapshot against this
/// to detect invalidation.
pub struct SchemaVersion {
    version: AtomicU64,
}

impl SchemaVersion {
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(1),
        }
    }

    /// Get current schema version.
    pub fn current(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Bump the schema version. Called on CREATE/DROP/ALTER DDL.
    pub fn bump(&self) -> u64 {
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// Cached entry: compiled physical tasks and the schema version at compile time.
struct CachedEntry {
    tasks: Vec<PhysicalTask>,
    schema_version: u64,
}

/// Per-session LRU plan cache.
///
/// Keyed by SQL hash. Each entry records the schema version at plan time.
/// On lookup, if the schema version has changed, the entry is evicted.
pub struct PlanCache {
    entries: HashMap<u64, CachedEntry>,
    max_entries: usize,
    /// Insertion order for LRU eviction (oldest at front). VecDeque for O(1) pop_front.
    order: VecDeque<u64>,
}

impl PlanCache {
    /// Create a new plan cache.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            order: VecDeque::new(),
        }
    }

    /// Look up cached physical tasks for the given SQL.
    ///
    /// Returns `None` if not cached or if the schema version has changed
    /// (stale entries are evicted automatically).
    pub fn get(&mut self, sql: &str, current_schema_version: u64) -> Option<Vec<PhysicalTask>> {
        let key = hash_sql(sql);

        let version_matches = self
            .entries
            .get(&key)
            .map(|entry| entry.schema_version == current_schema_version);

        match version_matches {
            Some(true) => {
                // Clone the cached tasks (PhysicalTask derives Clone).
                Some(self.entries.get(&key).expect("just checked").tasks.clone())
            }
            Some(false) => {
                // Schema changed — evict the stale entry.
                self.entries.remove(&key);
                self.order.retain(|k| *k != key);
                None
            }
            None => None,
        }
    }

    /// Store compiled physical tasks in the cache.
    pub fn put(&mut self, sql: &str, tasks: Vec<PhysicalTask>, schema_version: u64) {
        let key = hash_sql(sql);

        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key) {
            e.insert(CachedEntry {
                tasks,
                schema_version,
            });
            return;
        }

        // Evict oldest if at capacity (O(1) via VecDeque::pop_front).
        while self.entries.len() >= self.max_entries {
            if let Some(oldest_key) = self.order.pop_front() {
                self.entries.remove(&oldest_key);
            } else {
                break;
            }
        }

        self.entries.insert(
            key,
            CachedEntry {
                tasks,
                schema_version,
            },
        );
        self.order.push_back(key);
    }

    /// Invalidate all entries (called on DISCARD ALL, session reset, etc.).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

fn hash_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::MetaOp;
    use crate::control::planner::physical::PostSetOp;
    use crate::types::{TenantId, VShardId};

    fn dummy_tasks() -> Vec<PhysicalTask> {
        vec![PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Checkpoint),
            post_set_op: PostSetOp::None,
        }]
    }

    #[test]
    fn cache_hit_same_version() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), 1);
        assert!(cache.get("SELECT 1", 1).is_some());
    }

    #[test]
    fn cache_miss_version_change() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), 1);
        assert!(cache.get("SELECT 1", 2).is_none());
        assert!(cache.get("SELECT 1", 1).is_none());
    }

    #[test]
    fn lru_eviction() {
        let mut cache = PlanCache::new(2);
        cache.put("SELECT 1", dummy_tasks(), 1);
        cache.put("SELECT 2", dummy_tasks(), 1);
        cache.put("SELECT 3", dummy_tasks(), 1);
        assert!(cache.get("SELECT 1", 1).is_none());
        assert!(cache.get("SELECT 2", 1).is_some());
        assert!(cache.get("SELECT 3", 1).is_some());
    }

    #[test]
    fn clear_empties_cache() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), 1);
        cache.clear();
        assert!(cache.get("SELECT 1", 1).is_none());
    }

    #[test]
    fn schema_version_bump() {
        let sv = SchemaVersion::new();
        assert_eq!(sv.current(), 1);
        assert_eq!(sv.bump(), 2);
        assert_eq!(sv.current(), 2);
    }
}
