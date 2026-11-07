// SPDX-License-Identifier: BUSL-1.1

//! The bytes of the record a COPY stream's last slice left incomplete.
//!
//! The memory governor holds a reservation for every byte of the buffer.
//! The tails of all open loads therefore share one bound. A tail reserves
//! before it grows. A record the governor refuses to hold stops the load.

use nodedb_mem::{MemError, ReservationToken, ScopedMemory};

/// The smallest reservation of a tail. A tail at most this large keeps its
/// buffer between records, since most slices end inside a record.
const MIN_TAIL_RESERVATION: usize = 64 * 1024;

/// One COPY stream's incomplete record and its reservations.
pub(super) struct RecordTail {
    bytes: Vec<u8>,
    memory: ScopedMemory,
    /// One token per growth. Each growth at least doubles `reserved`, so this
    /// list stays short.
    tokens: Vec<ReservationToken>,
    /// The bytes `tokens` hold.
    reserved: usize,
}

impl RecordTail {
    pub(super) fn new(memory: ScopedMemory) -> Self {
        Self {
            bytes: Vec::new(),
            memory,
            tokens: Vec::new(),
            reserved: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Append `more`, reserving first. A refusal leaves the tail unchanged.
    pub(super) fn extend(&mut self, more: &[u8]) -> Result<(), MemError> {
        let needed = self.bytes.len().saturating_add(more.len());
        if needed > self.reserved {
            self.grow(needed)?;
        }
        self.bytes.extend_from_slice(more);
        Ok(())
    }

    /// Reserve at least double, so a record that arrives a byte at a time
    /// takes few tokens.
    fn grow(&mut self, needed: usize) -> Result<(), MemError> {
        let target = needed
            .max(self.reserved.saturating_mul(2))
            .max(MIN_TAIL_RESERVATION);
        let token = self.memory.reserve(target - self.reserved)?;
        self.reserved = target;
        self.tokens.push(token);
        self.bytes.reserve_exact(target - self.bytes.len());
        Ok(())
    }

    /// The carried bytes. The reservations stay until [`Self::reuse`] or the
    /// tail drops, since the caller still holds the buffer.
    pub(super) fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }

    /// Hand back the buffer of [`Self::take`] once its records are loaded. A
    /// large buffer frees here and its reservations release.
    pub(super) fn reuse(&mut self, mut buffer: Vec<u8>) {
        if self.reserved > MIN_TAIL_RESERVATION {
            self.tokens.clear();
            self.reserved = 0;
            return;
        }
        buffer.clear();
        self.bytes = buffer;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_mem::{EngineId, MemoryGovernor};
    use nodedb_types::{DatabaseId, TenantId};

    use crate::control::server::shared::ddl::neutral::collection::copy_from::row_memory::test_support::governor_of;
    use super::*;

    const BUDGET: usize = 1024 * 1024;

    fn governor() -> Arc<MemoryGovernor> {
        governor_of(BUDGET)
    }

    fn tail(governor: &Arc<MemoryGovernor>) -> RecordTail {
        RecordTail::new(ScopedMemory::new(
            Arc::clone(governor),
            DatabaseId::DEFAULT,
            TenantId::new(1),
            EngineId::Array,
        ))
    }

    fn held(governor: &MemoryGovernor) -> usize {
        governor.budget(EngineId::Array).allocated()
    }

    #[test]
    fn many_connections_share_one_bound() {
        let governor = governor();
        let chunk = vec![b'x'; BUDGET / 3];
        let mut tails: Vec<RecordTail> = (0..8).map(|_| tail(&governor)).collect();
        let mut admitted = 0;
        for tail in &mut tails {
            if tail.extend(&chunk).is_ok() {
                admitted += 1;
            }
            assert!(held(&governor) <= BUDGET, "the engine budget is the bound");
        }
        assert_eq!(admitted, 3, "the fourth tail is over the shared budget");
    }

    #[test]
    fn a_refusal_names_the_budget_and_leaves_the_tail_unchanged() {
        let governor = governor();
        let mut one = tail(&governor);
        one.extend(b"abc").expect("under the budget");
        let error = one.extend(&vec![b'x'; BUDGET * 2]).expect_err("over");
        assert!(matches!(
            error,
            MemError::BudgetExhausted {
                engine: EngineId::Array,
                limit: BUDGET,
                ..
            }
        ));
        assert_eq!(one.len(), 3);
    }

    #[test]
    fn every_reserved_byte_returns_when_the_load_ends() {
        let governor = governor();
        let mut one = tail(&governor);
        one.extend(&vec![b'x'; BUDGET / 2])
            .expect("under the budget");
        assert_eq!(held(&governor), BUDGET / 2);
        drop(one);
        assert_eq!(held(&governor), 0);
    }

    #[test]
    fn a_large_record_releases_its_reservation_once_loaded() {
        let governor = governor();
        let mut one = tail(&governor);
        one.extend(&vec![b'x'; BUDGET / 2])
            .expect("under the budget");
        let carried = one.take();
        assert_eq!(held(&governor), BUDGET / 2, "the buffer is still in use");
        one.reuse(carried);
        assert_eq!(held(&governor), 0);
        assert!(one.is_empty());
        one.extend(b"next").expect("the tail grows again");
    }

    #[test]
    fn a_small_tail_keeps_its_buffer_between_records() {
        let governor = governor();
        let mut one = tail(&governor);
        one.extend(b"partial").expect("under the budget");
        let carried = one.take();
        one.reuse(carried);
        assert!(one.is_empty());
        assert_eq!(held(&governor), MIN_TAIL_RESERVATION);
    }

    #[test]
    fn a_record_arriving_a_byte_at_a_time_takes_few_tokens() {
        let governor = governor();
        let mut one = tail(&governor);
        for _ in 0..200_000 {
            one.extend(b"x").expect("under the budget");
        }
        assert_eq!(one.len(), 200_000);
        assert!(one.tokens.len() <= 3, "{} tokens", one.tokens.len());
    }
}
