//! Order-statistic tree: augmented AVL tree with subtree counts.
//!
//! Provides O(log N) rank, insert, update, remove, and O(log N + K) top_k / range.
//! Arena-allocated (Vec-backed) for cache friendliness. Each node stores:
//! - `sort_key`: composite sort key bytes (encoded via `SortKeyEncoder`)
//! - `primary_key`: the original key (e.g., player_id)
//! - `count`: number of entries in this node's subtree (including itself)
//! - `height`: AVL balance factor
//! - `left`, `right`: child indices (0 = null)
//!
//! The tree is keyed by `(sort_key, primary_key)` to handle duplicate scores
//! (two players with the same score get distinct tree entries).

const NULL: u32 = 0;

/// A single node in the order-statistic AVL tree.
#[derive(Debug, Clone)]
struct Node {
    sort_key: Vec<u8>,
    primary_key: Vec<u8>,
    left: u32,
    right: u32,
    height: i8,
    /// Number of entries in this subtree (self + left + right).
    count: u32,
}

/// Arena-allocated AVL tree with order statistics.
///
/// Nodes are stored in a `Vec<Node>` with index 0 reserved as null.
/// Deleted nodes are pushed onto a free list for reuse.
#[derive(Debug)]
pub struct OrderStatTree {
    nodes: Vec<Node>,
    root: u32,
    free_list: Vec<u32>,
    /// Reverse lookup: primary_key → sort_key (for update/remove by primary key).
    key_to_sort: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

impl OrderStatTree {
    pub fn new() -> Self {
        // Index 0 is the null sentinel.
        let null_node = Node {
            sort_key: Vec::new(),
            primary_key: Vec::new(),
            left: NULL,
            right: NULL,
            height: 0,
            count: 0,
        };
        Self {
            nodes: vec![null_node],
            root: NULL,
            free_list: Vec::new(),
            key_to_sort: std::collections::HashMap::new(),
        }
    }

    /// Total number of entries in the tree.
    pub fn count(&self) -> u32 {
        self.nodes[self.root as usize].count
    }

    /// Insert a new entry. If the primary key already exists, updates its sort key.
    ///
    /// Returns `true` if this was a new insert, `false` if it was an update.
    pub fn insert(&mut self, sort_key: Vec<u8>, primary_key: Vec<u8>) -> bool {
        // If primary key already exists with a different sort key, remove first.
        if let Some(old_sort) = self.key_to_sort.get(&primary_key) {
            if *old_sort == sort_key {
                return false; // Same sort key, nothing to do.
            }
            let old_sort = old_sort.clone();
            self.remove_by_composite(&old_sort, &primary_key);
        }

        self.key_to_sort
            .insert(primary_key.clone(), sort_key.clone());
        let new_root = self.avl_insert(self.root, sort_key, primary_key);
        self.root = new_root;
        true
    }

    /// Remove an entry by primary key. Returns `true` if found and removed.
    pub fn remove(&mut self, primary_key: &[u8]) -> bool {
        let Some(sort_key) = self.key_to_sort.remove(primary_key) else {
            return false;
        };
        self.remove_by_composite(&sort_key, primary_key);
        true
    }

    /// Get the 1-based rank of a primary key (position in sorted order).
    ///
    /// Rank 1 = the entry with the "lowest" sort key (which, for DESC columns,
    /// means the highest score due to complement encoding).
    /// Returns `None` if the primary key is not in the tree.
    pub fn rank(&self, primary_key: &[u8]) -> Option<u32> {
        let sort_key = self.key_to_sort.get(primary_key)?;
        Some(self.rank_of(self.root, sort_key, primary_key))
    }

    /// Get the top K entries (by sort order). Returns `(sort_key, primary_key)` pairs.
    ///
    /// "Top" means the first K entries in the tree's natural order. For a
    /// DESC-encoded score column, this returns the highest scores.
    pub fn top_k(&self, k: u32) -> Vec<(&[u8], &[u8])> {
        let mut result = Vec::with_capacity(k as usize);
        self.in_order_collect(self.root, k, &mut result);
        result
    }

    /// Get entries in a sort key range [min, max] (inclusive).
    ///
    /// Both `min` and `max` are encoded sort keys. Returns all entries whose
    /// sort key is >= min and <= max, in sort order.
    pub fn range(&self, min: Option<&[u8]>, max: Option<&[u8]>) -> Vec<(&[u8], &[u8])> {
        let mut result = Vec::new();
        self.range_collect(self.root, min, max, &mut result);
        result
    }

    /// Iterate entries in sort order, calling `f` for each.
    ///
    /// `f` receives `(sort_key, primary_key)` and returns `true` to continue,
    /// `false` to stop. This avoids allocating all entries into a Vec.
    pub fn for_each_in_order<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        self.in_order_visit(self.root, &mut f);
    }

    /// Check if a primary key exists in the tree.
    pub fn contains(&self, primary_key: &[u8]) -> bool {
        self.key_to_sort.contains_key(primary_key)
    }

    /// Get the sort key for a primary key, if it exists.
    pub fn get_sort_key(&self, primary_key: &[u8]) -> Option<&[u8]> {
        self.key_to_sort.get(primary_key).map(|v| v.as_slice())
    }

    // ── AVL internal methods ───────────────────────────────────────────

    fn alloc_node(&mut self, sort_key: Vec<u8>, primary_key: Vec<u8>) -> u32 {
        let node = Node {
            sort_key,
            primary_key,
            left: NULL,
            right: NULL,
            height: 1,
            count: 1,
        };
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx as usize] = node;
            idx
        } else {
            let idx = self.nodes.len() as u32;
            self.nodes.push(node);
            idx
        }
    }

    fn free_node(&mut self, idx: u32) {
        self.free_list.push(idx);
        let n = &mut self.nodes[idx as usize];
        n.sort_key.clear();
        n.primary_key.clear();
        n.count = 0;
        n.left = NULL;
        n.right = NULL;
        n.height = 0;
    }

    fn height(&self, idx: u32) -> i8 {
        if idx == NULL {
            0
        } else {
            self.nodes[idx as usize].height
        }
    }

    fn cnt(&self, idx: u32) -> u32 {
        if idx == NULL {
            0
        } else {
            self.nodes[idx as usize].count
        }
    }

    fn update_meta(&mut self, idx: u32) {
        if idx == NULL {
            return;
        }
        let n = &self.nodes[idx as usize];
        let l = n.left;
        let r = n.right;
        let new_height = 1 + std::cmp::max(self.height(l), self.height(r));
        let new_count = 1 + self.cnt(l) + self.cnt(r);
        let n = &mut self.nodes[idx as usize];
        n.height = new_height;
        n.count = new_count;
    }

    fn balance_factor(&self, idx: u32) -> i8 {
        if idx == NULL {
            0
        } else {
            let n = &self.nodes[idx as usize];
            self.height(n.left) - self.height(n.right)
        }
    }

    fn rotate_right(&mut self, y: u32) -> u32 {
        let x = self.nodes[y as usize].left;
        let t2 = self.nodes[x as usize].right;
        self.nodes[x as usize].right = y;
        self.nodes[y as usize].left = t2;
        self.update_meta(y);
        self.update_meta(x);
        x
    }

    fn rotate_left(&mut self, x: u32) -> u32 {
        let y = self.nodes[x as usize].right;
        let t2 = self.nodes[y as usize].left;
        self.nodes[y as usize].left = x;
        self.nodes[x as usize].right = t2;
        self.update_meta(x);
        self.update_meta(y);
        y
    }

    fn rebalance(&mut self, idx: u32) -> u32 {
        self.update_meta(idx);
        let bf = self.balance_factor(idx);

        if bf > 1 {
            let left = self.nodes[idx as usize].left;
            if self.balance_factor(left) < 0 {
                let new_left = self.rotate_left(left);
                self.nodes[idx as usize].left = new_left;
            }
            return self.rotate_right(idx);
        }

        if bf < -1 {
            let right = self.nodes[idx as usize].right;
            if self.balance_factor(right) > 0 {
                let new_right = self.rotate_right(right);
                self.nodes[idx as usize].right = new_right;
            }
            return self.rotate_left(idx);
        }

        idx
    }

    /// Compare two entries by (sort_key, primary_key).
    fn cmp_keys(sort_a: &[u8], pk_a: &[u8], sort_b: &[u8], pk_b: &[u8]) -> std::cmp::Ordering {
        sort_a.cmp(sort_b).then_with(|| pk_a.cmp(pk_b))
    }

    fn avl_insert(&mut self, idx: u32, sort_key: Vec<u8>, primary_key: Vec<u8>) -> u32 {
        if idx == NULL {
            return self.alloc_node(sort_key, primary_key);
        }

        let n = &self.nodes[idx as usize];
        let cmp = Self::cmp_keys(&sort_key, &primary_key, &n.sort_key, &n.primary_key);

        match cmp {
            std::cmp::Ordering::Less => {
                let left = self.nodes[idx as usize].left;
                let new_left = self.avl_insert(left, sort_key, primary_key);
                self.nodes[idx as usize].left = new_left;
            }
            std::cmp::Ordering::Greater => {
                let right = self.nodes[idx as usize].right;
                let new_right = self.avl_insert(right, sort_key, primary_key);
                self.nodes[idx as usize].right = new_right;
            }
            std::cmp::Ordering::Equal => {
                // Exact duplicate (same sort_key + primary_key) — shouldn't happen
                // since we check key_to_sort before inserting. No-op.
                return idx;
            }
        }

        self.rebalance(idx)
    }

    fn avl_remove(&mut self, idx: u32, sort_key: &[u8], primary_key: &[u8]) -> u32 {
        if idx == NULL {
            return NULL;
        }

        let n = &self.nodes[idx as usize];
        let cmp = Self::cmp_keys(sort_key, primary_key, &n.sort_key, &n.primary_key);

        match cmp {
            std::cmp::Ordering::Less => {
                let left = self.nodes[idx as usize].left;
                let new_left = self.avl_remove(left, sort_key, primary_key);
                self.nodes[idx as usize].left = new_left;
            }
            std::cmp::Ordering::Greater => {
                let right = self.nodes[idx as usize].right;
                let new_right = self.avl_remove(right, sort_key, primary_key);
                self.nodes[idx as usize].right = new_right;
            }
            std::cmp::Ordering::Equal => {
                let left = self.nodes[idx as usize].left;
                let right = self.nodes[idx as usize].right;

                if left == NULL || right == NULL {
                    // One or no children.
                    let child = if left != NULL { left } else { right };
                    self.free_node(idx);
                    return child;
                }

                // Two children: replace with in-order successor (smallest in right subtree).
                let succ = self.find_min(right);
                let succ_sort = self.nodes[succ as usize].sort_key.clone();
                let succ_pk = self.nodes[succ as usize].primary_key.clone();

                // Remove successor from right subtree.
                let new_right = self.avl_remove(right, &succ_sort, &succ_pk);
                self.nodes[idx as usize].right = new_right;

                // Copy successor data into current node.
                self.nodes[idx as usize].sort_key = succ_sort;
                self.nodes[idx as usize].primary_key = succ_pk;
            }
        }

        self.rebalance(idx)
    }

    fn find_min(&self, mut idx: u32) -> u32 {
        while self.nodes[idx as usize].left != NULL {
            idx = self.nodes[idx as usize].left;
        }
        idx
    }

    fn remove_by_composite(&mut self, sort_key: &[u8], primary_key: &[u8]) {
        let new_root = self.avl_remove(self.root, sort_key, primary_key);
        self.root = new_root;
    }

    /// Compute the 1-based rank of (sort_key, primary_key) in the subtree rooted at `idx`.
    fn rank_of(&self, idx: u32, sort_key: &[u8], primary_key: &[u8]) -> u32 {
        if idx == NULL {
            return 0; // Should not happen if called correctly.
        }

        let n = &self.nodes[idx as usize];
        let cmp = Self::cmp_keys(sort_key, primary_key, &n.sort_key, &n.primary_key);

        match cmp {
            std::cmp::Ordering::Less => {
                // Target is in left subtree.
                self.rank_of(n.left, sort_key, primary_key)
            }
            std::cmp::Ordering::Equal => {
                // Found: rank = left subtree count + 1.
                self.cnt(n.left) + 1
            }
            std::cmp::Ordering::Greater => {
                // Target is in right subtree. Rank = left count + 1 (this node) + rank in right.
                self.cnt(n.left) + 1 + self.rank_of(n.right, sort_key, primary_key)
            }
        }
    }

    /// Collect up to `k` entries in in-order traversal (ascending sort key order).
    fn in_order_collect<'a>(&'a self, idx: u32, k: u32, result: &mut Vec<(&'a [u8], &'a [u8])>) {
        if idx == NULL || result.len() as u32 >= k {
            return;
        }

        let n = &self.nodes[idx as usize];

        // Visit left subtree first.
        self.in_order_collect(n.left, k, result);

        // Visit this node.
        if result.len() as u32 >= k {
            return;
        }
        result.push((&n.sort_key, &n.primary_key));

        // Visit right subtree.
        self.in_order_collect(n.right, k, result);
    }

    /// In-order traversal with callback. Returns false from callback to stop.
    fn in_order_visit<F>(&self, idx: u32, f: &mut F) -> bool
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        if idx == NULL {
            return true;
        }
        let n = &self.nodes[idx as usize];
        if !self.in_order_visit(n.left, f) {
            return false;
        }
        if !f(&n.sort_key, &n.primary_key) {
            return false;
        }
        self.in_order_visit(n.right, f)
    }

    /// Collect all entries in sort key range [min, max] (inclusive bounds).
    fn range_collect<'a>(
        &'a self,
        idx: u32,
        min: Option<&[u8]>,
        max: Option<&[u8]>,
        result: &mut Vec<(&'a [u8], &'a [u8])>,
    ) {
        if idx == NULL {
            return;
        }

        let n = &self.nodes[idx as usize];
        let key = n.sort_key.as_slice();

        // Pruning: left subtree is all < current node.
        // Visit left if current > min (left might contain values in [min, current)).
        let go_left = min.is_none() || key > min.unwrap();
        // Pruning: right subtree is all > current node.
        // Visit right if current < max (right might contain values in (current, max]).
        let go_right = max.is_none() || key < max.unwrap();

        if go_left {
            self.range_collect(n.left, min, max, result);
        }

        let in_range =
            (min.is_none() || key >= min.unwrap()) && (max.is_none() || key <= max.unwrap());
        if in_range {
            result.push((&n.sort_key, &n.primary_key));
        }

        if go_right {
            self.range_collect(n.right, min, max, result);
        }
    }
}

impl Default for OrderStatTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree() {
        let tree = OrderStatTree::new();
        assert_eq!(tree.count(), 0);
        assert!(tree.rank(b"nonexistent").is_none());
        assert!(tree.top_k(10).is_empty());
    }

    #[test]
    fn single_insert_and_rank() {
        let mut tree = OrderStatTree::new();
        assert!(tree.insert(vec![0, 0, 0, 100], b"alice".to_vec()));
        assert_eq!(tree.count(), 1);
        assert_eq!(tree.rank(b"alice"), Some(1));
    }

    #[test]
    fn multiple_inserts_ordered() {
        let mut tree = OrderStatTree::new();
        // Insert scores: 300, 100, 200 (sort keys are just the score bytes).
        tree.insert(vec![3], b"charlie".to_vec());
        tree.insert(vec![1], b"alice".to_vec());
        tree.insert(vec![2], b"bob".to_vec());

        assert_eq!(tree.count(), 3);

        // Sorted order: alice(1) < bob(2) < charlie(3).
        assert_eq!(tree.rank(b"alice"), Some(1));
        assert_eq!(tree.rank(b"bob"), Some(2));
        assert_eq!(tree.rank(b"charlie"), Some(3));
    }

    #[test]
    fn top_k() {
        let mut tree = OrderStatTree::new();
        tree.insert(vec![3], b"c".to_vec());
        tree.insert(vec![1], b"a".to_vec());
        tree.insert(vec![2], b"b".to_vec());
        tree.insert(vec![4], b"d".to_vec());

        let top2 = tree.top_k(2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].1, b"a"); // sort_key=1
        assert_eq!(top2[1].1, b"b"); // sort_key=2

        let top10 = tree.top_k(10);
        assert_eq!(top10.len(), 4); // Only 4 entries exist.
    }

    #[test]
    fn update_existing_key() {
        let mut tree = OrderStatTree::new();
        tree.insert(vec![1], b"alice".to_vec());
        tree.insert(vec![3], b"bob".to_vec());

        assert_eq!(tree.rank(b"alice"), Some(1));
        assert_eq!(tree.rank(b"bob"), Some(2));

        // Update alice's score to 5 (now she's after bob).
        assert!(!tree.insert(vec![1], b"alice".to_vec())); // Same score = no-op, returns false.
        tree.insert(vec![5], b"alice".to_vec()); // New score.

        assert_eq!(tree.count(), 2);
        assert_eq!(tree.rank(b"bob"), Some(1)); // bob(3) is now first.
        assert_eq!(tree.rank(b"alice"), Some(2)); // alice(5) is now second.
    }

    #[test]
    fn remove() {
        let mut tree = OrderStatTree::new();
        tree.insert(vec![1], b"a".to_vec());
        tree.insert(vec![2], b"b".to_vec());
        tree.insert(vec![3], b"c".to_vec());

        assert!(tree.remove(b"b"));
        assert_eq!(tree.count(), 2);
        assert!(tree.rank(b"b").is_none());
        assert_eq!(tree.rank(b"a"), Some(1));
        assert_eq!(tree.rank(b"c"), Some(2));

        // Remove non-existent.
        assert!(!tree.remove(b"z"));
    }

    #[test]
    fn range_query() {
        let mut tree = OrderStatTree::new();
        for i in 0u8..10 {
            tree.insert(vec![i], vec![b'a' + i]);
        }

        // Range [3, 6] (inclusive).
        let range = tree.range(Some(&[3u8]), Some(&[6u8]));
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, &[3u8]);
        assert_eq!(range[3].0, &[6u8]);

        // Unbounded start.
        let range = tree.range(None, Some(&[2u8]));
        assert_eq!(range.len(), 3); // 0, 1, 2

        // Unbounded end.
        let range = tree.range(Some(&[8u8]), None);
        assert_eq!(range.len(), 2); // 8, 9
    }

    #[test]
    fn duplicate_scores_different_keys() {
        let mut tree = OrderStatTree::new();
        tree.insert(vec![5], b"alice".to_vec());
        tree.insert(vec![5], b"bob".to_vec());
        tree.insert(vec![5], b"charlie".to_vec());

        assert_eq!(tree.count(), 3);

        // Tiebreak by primary key (lexicographic).
        assert_eq!(tree.rank(b"alice"), Some(1));
        assert_eq!(tree.rank(b"bob"), Some(2));
        assert_eq!(tree.rank(b"charlie"), Some(3));
    }

    #[test]
    fn stress_insert_remove_rank() {
        let mut tree = OrderStatTree::new();
        let n = 1000u32;

        // Insert n entries.
        for i in 0..n {
            let key = i.to_be_bytes().to_vec();
            let pk = format!("player_{i}").into_bytes();
            tree.insert(key, pk);
        }
        assert_eq!(tree.count(), n);

        // Check ranks.
        for i in 0..n {
            let pk = format!("player_{i}").into_bytes();
            assert_eq!(tree.rank(&pk), Some(i + 1));
        }

        // Remove even entries.
        for i in (0..n).step_by(2) {
            let pk = format!("player_{i}").into_bytes();
            assert!(tree.remove(&pk));
        }
        assert_eq!(tree.count(), n / 2);

        // Top-k should return the first n/2 odd entries.
        let top5 = tree.top_k(5);
        assert_eq!(top5.len(), 5);
    }

    #[test]
    fn avl_balance_maintained() {
        // Insert in ascending order (worst case for naive BST).
        let mut tree = OrderStatTree::new();
        for i in 0u32..100 {
            tree.insert(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }
        assert_eq!(tree.count(), 100);

        // With AVL balance, height should be O(log N) ≈ 7 for N=100.
        let root_height = tree.nodes[tree.root as usize].height;
        assert!(
            root_height <= 10,
            "height {root_height} too large for 100 nodes"
        );

        // Verify all ranks are correct.
        for i in 0u32..100 {
            let pk = i.to_be_bytes().to_vec();
            assert_eq!(tree.rank(&pk), Some(i + 1));
        }
    }

    #[test]
    fn contains_and_get_sort_key() {
        let mut tree = OrderStatTree::new();
        tree.insert(vec![42], b"alice".to_vec());

        assert!(tree.contains(b"alice"));
        assert!(!tree.contains(b"bob"));
        assert_eq!(tree.get_sort_key(b"alice"), Some([42u8].as_slice()));
        assert_eq!(tree.get_sort_key(b"bob"), None);
    }
}
