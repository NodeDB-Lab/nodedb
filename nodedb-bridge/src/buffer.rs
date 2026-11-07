// SPDX-License-Identifier: BUSL-1.1

//! Lock-free SPSC ring buffer.
//!
//! The core data structure: a fixed-capacity circular buffer with cache-line-padded
//! head (consumer) and tail (producer) counters. Only two atomic operations per
//! enqueue/dequeue — one `Relaxed` load of the remote counter, one `Release`/`Acquire`
//! store of the local counter.
//!
//! ## Memory layout
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  [CacheLine] tail (written by producer only)        │
//! │  [CacheLine] head (written by consumer only)        │
//! │  [capacity]  slot array (T values)                  │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! The `tail` and `head` live on separate cache lines to prevent false sharing.
//! Capacity must be a power of two so we can use bitwise AND instead of modulo.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{BridgeError, Result};
use crate::metrics::BridgeMetrics;

/// Cache line size on x86_64 and most aarch64 implementations.
const CACHE_LINE: usize = 64;

/// Shared state between producer and consumer.
///
/// The producer writes `tail`, reads `head`.
/// The consumer writes `head`, reads `tail`.
///
/// Both counters are monotonically increasing u64 values. The actual slot index
/// is `counter & (capacity - 1)` (bitwise AND because capacity is power-of-two).
struct Shared<T> {
    /// Producer's write cursor. Padded to its own cache line.
    tail: CacheAligned<AtomicU64>,

    /// Consumer's read cursor. Padded to its own cache line.
    head: CacheAligned<AtomicU64>,

    /// Fixed-size slot array.
    slots: Box<[UnsafeCell<Option<T>>]>,

    /// Capacity (always power of two).
    capacity: usize,

    /// Bitmask: `capacity - 1` for fast modulo.
    mask: usize,

    /// Set to true when either side is dropped.
    disconnected: AtomicBool,

    /// Shared metrics counters.
    metrics: BridgeMetrics,

    /// Called by the producer after each push. A consumer that sleeps between
    /// polls wakes from it, so a pushed value waits no longer than the wake.
    push_hook: Option<PushHook>,
}

/// A wake the producer runs after each push.
pub type PushHook = Arc<dyn Fn() + Send + Sync>;

// SAFETY: The SPSC protocol ensures that producer and consumer never access
// the same slot concurrently. The producer writes slots[tail & mask] and the
// consumer reads slots[head & mask]. As long as the ring buffer is not full,
// these indices never collide. The atomic head/tail counters enforce this.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

/// Cache-line-aligned wrapper to prevent false sharing between head and tail.
#[repr(align(64))]
struct CacheAligned<T> {
    value: T,
    _pad: [u8; CACHE_LINE - std::mem::size_of::<AtomicU64>()],
}

impl<T: Default> CacheAligned<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            _pad: [0u8; CACHE_LINE - std::mem::size_of::<AtomicU64>()],
        }
    }
}

/// The producer half of the ring buffer.
///
/// Lives on the **Control Plane** (Tokio). It is `Send` so it can be moved
/// between Tokio worker threads.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,

    /// Cached copy of `head` to avoid atomic loads on every enqueue.
    /// Only refreshed when the ring appears full.
    cached_head: u64,
}

/// The consumer half of the ring buffer.
///
/// Lives on the **Data Plane** (TPC). It is `Send` so it can be sent to
/// the TPC core during setup, but once pinned to a core it stays there.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,

    /// Cached copy of `tail` to avoid atomic loads on every dequeue.
    /// Only refreshed when the ring appears empty.
    cached_tail: u64,
}

/// Create a new SPSC ring buffer with the given capacity.
///
/// Capacity is rounded up to the next power of two.
///
/// Returns `(Producer, Consumer)` — send the producer to the Control Plane
/// and the consumer to the Data Plane.
pub struct RingBuffer;

impl RingBuffer {
    pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
        Self::build(capacity, None)
    }

    /// Like [`Self::channel`], with `hook` run by the producer after every
    /// successful push.
    pub fn channel_with_push_hook<T>(
        capacity: usize,
        hook: PushHook,
    ) -> (Producer<T>, Consumer<T>) {
        Self::build(capacity, Some(hook))
    }

    fn build<T>(capacity: usize, push_hook: Option<PushHook>) -> (Producer<T>, Consumer<T>) {
        assert!(capacity > 0, "ring buffer capacity must be > 0");

        let capacity = capacity.next_power_of_two();
        let mask = capacity - 1;

        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(None));
        }

        let shared = Arc::new(Shared {
            tail: CacheAligned::new(AtomicU64::new(0)),
            head: CacheAligned::new(AtomicU64::new(0)),
            slots: slots.into_boxed_slice(),
            capacity,
            mask,
            disconnected: AtomicBool::new(false),
            metrics: BridgeMetrics::new(),
            push_hook,
        });

        let producer = Producer {
            shared: Arc::clone(&shared),
            cached_head: 0,
        };

        let consumer = Consumer {
            shared,
            cached_tail: 0,
        };

        (producer, consumer)
    }
}

impl<T> Producer<T> {
    /// Try to enqueue a value. Returns `Err(BridgeError::Full)` if the ring is full,
    /// or `Err(BridgeError::Disconnected)` if the consumer was dropped.
    pub fn try_push(&mut self, value: T) -> Result<()> {
        if self.shared.disconnected.load(Ordering::Relaxed) {
            return Err(BridgeError::Disconnected { side: "consumer" });
        }

        let tail = self.shared.tail.value.load(Ordering::Relaxed);

        // Fast path: check cached head first (avoids atomic load).
        if tail.wrapping_sub(self.cached_head) >= self.shared.capacity as u64 {
            // Slow path: refresh cached head from the atomic.
            self.cached_head = self.shared.head.value.load(Ordering::Acquire);

            if tail.wrapping_sub(self.cached_head) >= self.shared.capacity as u64 {
                self.shared.metrics.record_full();
                return Err(BridgeError::Full {
                    capacity: self.shared.capacity,
                    pending: (tail.wrapping_sub(self.cached_head)) as usize,
                });
            }
        }

        let idx = (tail as usize) & self.shared.mask;

        // SAFETY: We have exclusive write access to this slot because:
        // 1. We are the only producer (SPSC).
        // 2. The consumer's head hasn't reached this slot yet (checked above).
        unsafe {
            (*self.shared.slots[idx].get()) = Some(value);
        }

        // Make the value visible to the consumer.
        self.shared
            .tail
            .value
            .store(tail.wrapping_add(1), Ordering::Release);

        self.shared.metrics.record_push();
        if let Some(hook) = &self.shared.push_hook {
            hook();
        }
        Ok(())
    }

    /// Returns the current queue utilization as a percentage (0-100).
    pub fn utilization(&self) -> u8 {
        let tail = self.shared.tail.value.load(Ordering::Relaxed);
        let head = self.shared.head.value.load(Ordering::Relaxed);
        let pending = tail.wrapping_sub(head) as usize;
        ((pending * 100) / self.shared.capacity) as u8
    }

    /// Returns the number of items currently in the queue.
    pub fn len(&self) -> usize {
        let tail = self.shared.tail.value.load(Ordering::Relaxed);
        let head = self.shared.head.value.load(Ordering::Relaxed);
        tail.wrapping_sub(head) as usize
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the fixed capacity of the ring buffer.
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// Returns `true` once either half of the channel has been dropped.
    ///
    /// On the producer this means the consumer — a Data Plane core — is gone,
    /// so every subsequent `try_push` will fail and anything still staged for
    /// that core must be failed rather than queued.
    pub fn is_disconnected(&self) -> bool {
        self.shared.disconnected.load(Ordering::Acquire)
    }

    /// Returns a reference to the shared metrics.
    pub fn metrics(&self) -> &BridgeMetrics {
        &self.shared.metrics
    }
}

impl<T> Consumer<T> {
    /// Try to dequeue a value. Returns `Err(BridgeError::Empty)` if the ring is empty,
    /// or `Err(BridgeError::Disconnected)` if the producer was dropped.
    pub fn try_pop(&mut self) -> Result<T> {
        let head = self.shared.head.value.load(Ordering::Relaxed);

        // Fast path: check cached tail first.
        if head == self.cached_tail {
            // Slow path: refresh cached tail from the atomic.
            self.cached_tail = self.shared.tail.value.load(Ordering::Acquire);

            if head == self.cached_tail {
                if self.shared.disconnected.load(Ordering::Relaxed) {
                    return Err(BridgeError::Disconnected { side: "producer" });
                }
                return Err(BridgeError::Empty);
            }
        }

        let idx = (head as usize) & self.shared.mask;

        // SAFETY: We have exclusive read access to this slot because:
        // 1. We are the only consumer (SPSC).
        // 2. The producer has already written to this slot (tail > head, checked above).
        let value = unsafe { (*self.shared.slots[idx].get()).take() };

        // Advance head to free the slot for the producer.
        self.shared
            .head
            .value
            .store(head.wrapping_add(1), Ordering::Release);

        self.shared.metrics.record_pop();

        // SAFETY: The producer wrote `Some(value)` before advancing tail.
        // We only reach here when tail > head, so the slot is guaranteed occupied.
        Ok(value.expect("BUG: slot was None despite tail > head"))
    }

    /// Drain up to `max` items into the provided vector.
    ///
    /// Returns `(count, disconnected)`: the number of items moved into `buf`,
    /// and whether the far end of the channel is gone. The disconnect is
    /// reported *alongside* the count, never instead of it — items the
    /// producer published before it died are still delivered, and only then
    /// does the caller learn the channel is dead. Without this flag a caller
    /// draining responses cannot tell an idle core from a dead one, and every
    /// request outstanding on a dead core is stranded.
    ///
    /// More efficient than calling `try_pop` in a loop because it batches the
    /// atomic tail load.
    pub fn drain_into(&mut self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        let head = self.shared.head.value.load(Ordering::Relaxed);
        self.cached_tail = self.shared.tail.value.load(Ordering::Acquire);

        let available = self.cached_tail.wrapping_sub(head) as usize;
        let count = available.min(max);

        for i in 0..count {
            let idx = ((head.wrapping_add(i as u64)) as usize) & self.shared.mask;
            // SAFETY: same as try_pop — we've verified these slots are occupied.
            let value = unsafe { (*self.shared.slots[idx].get()).take() };
            buf.push(value.expect("BUG: slot was None during drain"));
        }

        if count > 0 {
            self.shared
                .head
                .value
                .store(head.wrapping_add(count as u64), Ordering::Release);
            self.shared.metrics.record_pops(count as u64);
        }

        (count, self.shared.disconnected.load(Ordering::Acquire))
    }

    /// Returns `true` once either half of the channel has been dropped.
    ///
    /// On the consumer this means the producer is gone and no further items
    /// will ever arrive beyond what is already buffered.
    pub fn is_disconnected(&self) -> bool {
        self.shared.disconnected.load(Ordering::Acquire)
    }

    /// Returns the number of items currently in the queue.
    pub fn len(&self) -> usize {
        let tail = self.shared.tail.value.load(Ordering::Relaxed);
        let head = self.shared.head.value.load(Ordering::Relaxed);
        tail.wrapping_sub(head) as usize
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the shared metrics.
    pub fn metrics(&self) -> &BridgeMetrics {
        &self.shared.metrics
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_push_pop() {
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(4);

        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        tx.try_push(3).unwrap();
        tx.try_push(4).unwrap();

        // Buffer should be full (capacity rounds to 4).
        assert!(matches!(tx.try_push(5), Err(BridgeError::Full { .. })));

        assert_eq!(rx.try_pop().unwrap(), 1);
        assert_eq!(rx.try_pop().unwrap(), 2);

        // Now there's room for more.
        tx.try_push(5).unwrap();
        tx.try_push(6).unwrap();

        assert_eq!(rx.try_pop().unwrap(), 3);
        assert_eq!(rx.try_pop().unwrap(), 4);
        assert_eq!(rx.try_pop().unwrap(), 5);
        assert_eq!(rx.try_pop().unwrap(), 6);

        assert!(matches!(rx.try_pop(), Err(BridgeError::Empty)));
    }

    #[test]
    fn power_of_two_rounding() {
        let (tx, _rx) = RingBuffer::channel::<u64>(3);
        assert_eq!(tx.capacity(), 4);

        let (tx, _rx) = RingBuffer::channel::<u64>(5);
        assert_eq!(tx.capacity(), 8);

        let (tx, _rx) = RingBuffer::channel::<u64>(8);
        assert_eq!(tx.capacity(), 8);
    }

    #[test]
    fn utilization_tracking() {
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(8);

        assert_eq!(tx.utilization(), 0);

        for i in 0..6 {
            tx.try_push(i).unwrap();
        }
        assert_eq!(tx.utilization(), 75);

        rx.try_pop().unwrap();
        rx.try_pop().unwrap();
        assert_eq!(tx.utilization(), 50);
    }

    #[test]
    fn drain_into_batch() {
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(16);

        for i in 0..10 {
            tx.try_push(i).unwrap();
        }

        let mut buf = Vec::new();
        let (drained, disconnected) = rx.drain_into(&mut buf, 5);
        assert_eq!(drained, 5);
        assert!(!disconnected);
        assert_eq!(buf, vec![0, 1, 2, 3, 4]);

        let (drained, disconnected) = rx.drain_into(&mut buf, 100);
        assert_eq!(drained, 5);
        assert!(!disconnected);
        assert_eq!(buf, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn disconnect_detection_producer_drops() {
        let (tx, mut rx) = RingBuffer::channel::<u64>(4);
        drop(tx);

        assert!(matches!(
            rx.try_pop(),
            Err(BridgeError::Disconnected { side: "producer" })
        ));
    }

    #[test]
    fn disconnect_detection_consumer_drops() {
        let (mut tx, rx) = RingBuffer::channel::<u64>(4);
        drop(rx);

        assert!(matches!(
            tx.try_push(1),
            Err(BridgeError::Disconnected { side: "consumer" })
        ));
    }

    #[test]
    fn wrapping_behavior() {
        // Verify the ring buffer works correctly when counters wrap past capacity.
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(4);

        for round in 0..1000u64 {
            for i in 0..4 {
                tx.try_push(round * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(rx.try_pop().unwrap(), round * 4 + i);
            }
        }
    }

    #[test]
    fn is_disconnected_false_while_both_ends_alive() {
        let (tx, rx) = RingBuffer::channel::<u64>(4);
        assert!(!tx.is_disconnected());
        assert!(!rx.is_disconnected());
    }

    #[test]
    fn is_disconnected_true_on_producer_after_consumer_drop() {
        let (tx, rx) = RingBuffer::channel::<u64>(4);
        drop(rx);
        assert!(tx.is_disconnected());
    }

    #[test]
    fn is_disconnected_true_on_consumer_after_producer_drop() {
        let (tx, rx) = RingBuffer::channel::<u64>(4);
        drop(tx);
        assert!(rx.is_disconnected());
    }

    #[test]
    fn drain_into_reports_disconnect_on_dead_producer() {
        let (tx, mut rx) = RingBuffer::channel::<u64>(4);
        drop(tx);

        let mut buf = Vec::new();
        let (drained, disconnected) = rx.drain_into(&mut buf, 10);
        assert_eq!(drained, 0);
        assert!(buf.is_empty());
        assert!(disconnected);
    }

    #[test]
    fn drain_into_drains_pending_items_then_reports_disconnect() {
        // Consequence #3 of the dead-core defect: items already pushed before
        // the producer died must still be drained, not lost — the disconnect
        // flag is reported *alongside* the count, never instead of it.
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(4);
        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        drop(tx);

        let mut buf = Vec::new();
        let (drained, disconnected) = rx.drain_into(&mut buf, 10);
        assert_eq!(drained, 2);
        assert_eq!(buf, vec![1, 2]);
        assert!(disconnected);
    }

    #[test]
    fn push_hook_runs_once_per_successful_push() {
        use std::sync::atomic::AtomicUsize;

        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = Arc::clone(&calls);
        let hook: PushHook = Arc::new(move || {
            hook_calls.fetch_add(1, Ordering::SeqCst);
        });
        let (mut tx, mut rx) = RingBuffer::channel_with_push_hook::<u64>(2, hook);

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        tx.try_push(1).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tx.try_push(2).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // A push that fails with Full must not run the hook.
        assert!(matches!(tx.try_push(3), Err(BridgeError::Full { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Popping never runs the hook.
        assert_eq!(rx.try_pop().unwrap(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // A push after room opens runs it again.
        tx.try_push(3).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn push_hook_not_run_after_consumer_dropped() {
        use std::sync::atomic::AtomicUsize;

        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = Arc::clone(&calls);
        let hook: PushHook = Arc::new(move || {
            hook_calls.fetch_add(1, Ordering::SeqCst);
        });
        let (mut tx, rx) = RingBuffer::channel_with_push_hook::<u64>(4, hook);
        drop(rx);

        assert!(matches!(
            tx.try_push(1),
            Err(BridgeError::Disconnected { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn metrics_counting() {
        let (mut tx, mut rx) = RingBuffer::channel::<u64>(8);

        for i in 0..5 {
            tx.try_push(i).unwrap();
        }
        assert_eq!(tx.metrics().pushes(), 5);

        for _ in 0..3 {
            rx.try_pop().unwrap();
        }
        assert_eq!(rx.metrics().pops(), 3);
    }
}
