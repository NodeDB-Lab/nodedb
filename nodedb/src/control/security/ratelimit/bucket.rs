//! Token bucket rate limiter: burst + sustained rate control.
//!
//! Each bucket has a capacity (burst) and a refill rate (sustained QPS).
//! Tokens are consumed per-request. When empty, requests are rejected
//! until tokens refill.
//!
//! Thread-safe: uses atomic operations for hot-path token consumption.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A single token bucket with burst capacity and sustained refill rate.
pub struct TokenBucket {
    /// Maximum tokens (burst capacity).
    capacity: u64,
    /// Tokens added per second (sustained rate).
    refill_rate: f64,
    /// Current tokens (scaled by 1000 for sub-token precision).
    tokens_millis: AtomicU64,
    /// Last refill timestamp in milliseconds.
    last_refill_ms: AtomicU64,
}

impl TokenBucket {
    /// Create a new token bucket.
    ///
    /// `capacity` = maximum burst size (e.g., 100 requests).
    /// `rate_per_sec` = sustained rate (e.g., 50.0 = 50 QPS).
    pub fn new(capacity: u64, rate_per_sec: f64) -> Self {
        let now_ms = current_ms();
        Self {
            capacity,
            refill_rate: rate_per_sec,
            tokens_millis: AtomicU64::new(capacity * 1000),
            last_refill_ms: AtomicU64::new(now_ms),
        }
    }

    /// Try to consume `cost` tokens. Returns `true` if allowed, `false` if rate-limited.
    pub fn try_acquire(&self, cost: u64) -> bool {
        self.refill();
        let cost_millis = cost * 1000;
        loop {
            let current = self.tokens_millis.load(Ordering::Relaxed);
            if current < cost_millis {
                return false; // Not enough tokens.
            }
            if self
                .tokens_millis
                .compare_exchange_weak(
                    current,
                    current - cost_millis,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
            // CAS failed — retry (another thread consumed tokens concurrently).
        }
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&self) {
        let now_ms = current_ms();
        let last = self.last_refill_ms.load(Ordering::Relaxed);
        let elapsed_ms = now_ms.saturating_sub(last);

        if elapsed_ms < 10 {
            return; // Don't refill more often than every 10ms.
        }

        // Calculate tokens to add.
        let added = (self.refill_rate * elapsed_ms as f64 / 1000.0) as u64 * 1000;
        if added == 0 {
            return;
        }

        // Try to update last_refill timestamp (CAS to avoid double-refill).
        if self
            .last_refill_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread already refilled.
        }

        let cap_millis = self.capacity * 1000;
        loop {
            let current = self.tokens_millis.load(Ordering::Relaxed);
            let new_val = (current + added).min(cap_millis);
            if self
                .tokens_millis
                .compare_exchange_weak(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Current available tokens.
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens_millis.load(Ordering::Relaxed) / 1000
    }

    /// Maximum capacity (burst).
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Sustained rate (tokens per second).
    pub fn rate(&self) -> f64 {
        self.refill_rate
    }

    /// Time until next token is available (milliseconds). 0 if tokens available.
    pub fn retry_after_ms(&self) -> u64 {
        self.refill();
        let current = self.tokens_millis.load(Ordering::Relaxed);
        if current >= 1000 {
            return 0;
        }
        let deficit = 1000 - current;
        if self.refill_rate > 0.0 {
            (deficit as f64 / self.refill_rate).ceil() as u64
        } else {
            u64::MAX
        }
    }
}

fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_bucket_full() {
        let bucket = TokenBucket::new(100, 50.0);
        assert_eq!(bucket.available(), 100);
        assert_eq!(bucket.capacity(), 100);
    }

    #[test]
    fn consume_tokens() {
        let bucket = TokenBucket::new(10, 10.0);
        assert!(bucket.try_acquire(5));
        assert_eq!(bucket.available(), 5);
        assert!(bucket.try_acquire(5));
        assert_eq!(bucket.available(), 0);
        assert!(!bucket.try_acquire(1)); // Empty.
    }

    #[test]
    fn overconsume_rejected() {
        let bucket = TokenBucket::new(5, 10.0);
        assert!(!bucket.try_acquire(6)); // Over capacity.
        assert_eq!(bucket.available(), 5); // Unchanged.
    }

    #[test]
    fn cost_multiplier() {
        let bucket = TokenBucket::new(100, 50.0);
        assert!(bucket.try_acquire(20)); // vector_search cost = 20
        assert_eq!(bucket.available(), 80);
    }

    #[test]
    fn retry_after_when_empty() {
        let bucket = TokenBucket::new(1, 10.0);
        bucket.try_acquire(1);
        assert!(bucket.retry_after_ms() > 0);
    }
}
