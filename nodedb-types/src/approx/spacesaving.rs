//! SpaceSaving — approximate top-K heavy hitters (bounded memory).

use std::collections::HashMap;

/// Space-saving algorithm for approximate top-K heavy hitters.
///
/// Tracks the K most frequent items with bounded memory. Items not in
/// the top K are approximated — their counts may be over-estimated by
/// at most the minimum count in the structure.
#[derive(Debug)]
pub struct SpaceSaving {
    items: HashMap<u64, (u64, u64)>,
    max_items: usize,
}

impl SpaceSaving {
    pub fn new(k: usize) -> Self {
        Self {
            items: HashMap::with_capacity(k + 1),
            max_items: k.max(1),
        }
    }

    pub fn add(&mut self, item: u64) {
        if let Some(entry) = self.items.get_mut(&item) {
            entry.0 += 1;
            return;
        }

        if self.items.len() < self.max_items {
            self.items.insert(item, (1, 0));
        } else {
            let Some((&min_key, &(min_count, _))) =
                self.items.iter().min_by_key(|(_, (count, _))| *count)
            else {
                return;
            };
            self.items.remove(&min_key);
            self.items.insert(item, (min_count + 1, min_count));
        }
    }

    pub fn add_batch(&mut self, items: &[u64]) {
        for &item in items {
            self.add(item);
        }
    }

    /// Get the top-K items sorted by count (descending).
    ///
    /// Returns `(item, count, error_bound)` tuples.
    pub fn top_k(&self) -> Vec<(u64, u64, u64)> {
        let mut result: Vec<(u64, u64, u64)> = self
            .items
            .iter()
            .map(|(&item, &(count, error))| (item, count, error))
            .collect();
        result.sort_by_key(|item| std::cmp::Reverse(item.1));
        result
    }

    pub fn merge(&mut self, other: &SpaceSaving) {
        for (&item, &(count, error)) in &other.items {
            let entry = self.items.entry(item).or_insert((0, 0));
            entry.0 += count;
            entry.1 += error;
        }

        while self.items.len() > self.max_items {
            let Some((&min_key, _)) = self.items.iter().min_by_key(|(_, (count, _))| *count) else {
                break;
            };
            self.items.remove(&min_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topk_basic() {
        let mut ss = SpaceSaving::new(3);
        for _ in 0..100 {
            ss.add(1);
        }
        for _ in 0..50 {
            ss.add(2);
        }
        for _ in 0..30 {
            ss.add(3);
        }
        for _ in 0..10 {
            ss.add(4);
        }
        let top = ss.top_k();
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1, 100);
    }

    #[test]
    fn topk_merge() {
        let mut a = SpaceSaving::new(5);
        let mut b = SpaceSaving::new(5);
        for _ in 0..100 {
            a.add(1);
        }
        for _ in 0..80 {
            b.add(1);
        }
        for _ in 0..50 {
            b.add(2);
        }
        a.merge(&b);
        let top = a.top_k();
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1, 180);
    }

    #[test]
    fn topk_eviction() {
        let mut ss = SpaceSaving::new(3);
        for i in 0..10u64 {
            for _ in 0..(10 - i) {
                ss.add(i);
            }
        }
        let top = ss.top_k();
        assert_eq!(top.len(), 3);
        assert!(top[0].1 >= top[1].1);
        assert!(top[1].1 >= top[2].1);
    }
}
