// SPDX-License-Identifier: BUSL-1.1

//! What a raster load reserves from the memory governor, so the header caches
//! and block sets of all open loads share one bound.
//!
//! A load reserves the header bytes of each open round and keeps them, since
//! its readers hold what the rounds fetched. It reserves each block set before
//! fetching it and holds that until the rows of the set are fed.
//!
//! A load that cannot reserve a set first waits for its own sets to drain. The
//! refusal comes only when the set does not fit with none of them in flight.

use std::sync::Arc;

use nodedb_mem::{ReservationToken, ScopedMemory};
use nodedb_raster::{COALESCE_GAP, RasterReader, coalesce};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::blocks::BLOCK_QUEUE;
use super::error::DriveError;

/// Most block sets of one load alive at once: the queue, the set the producer
/// holds waiting for a queue slot, and the set the load feeds.
const SETS_IN_FLIGHT: u32 = BLOCK_QUEUE as u32 + 2;

/// A load's reservations for the header bytes of its open rounds.
pub struct OpenReservation {
    memory: ScopedMemory,
    /// One token per open round. The round limit bounds this list.
    tokens: Vec<ReservationToken>,
}

impl OpenReservation {
    pub fn new(memory: ScopedMemory) -> Self {
        Self {
            memory,
            tokens: Vec::new(),
        }
    }

    /// Reserve `bytes` of header ranges before the fetch.
    pub fn reserve(&mut self, bytes: u64) -> Result<(), DriveError> {
        let token = reserve(&self.memory, bytes)?;
        self.tokens.push(token);
        Ok(())
    }
}

/// Admits the block sets of one load.
#[derive(Clone)]
pub struct SetGate {
    memory: ScopedMemory,
    in_flight: Arc<Semaphore>,
}

/// What one block set holds until it drops.
pub struct SetReservation {
    _token: ReservationToken,
    _permit: OwnedSemaphorePermit,
}

impl SetGate {
    pub fn new(memory: ScopedMemory) -> Self {
        Self {
            memory,
            in_flight: Arc::new(Semaphore::new(SETS_IN_FLIGHT as usize)),
        }
    }

    /// Reserve `bytes` for one block set. The load's reader of the queue drains
    /// its own sets, so the wait here always ends.
    pub async fn admit(&self, bytes: u64) -> Result<SetReservation, DriveError> {
        let token = match reserve(&self.memory, bytes) {
            Ok(token) => token,
            Err(_) => {
                let drained = self
                    .in_flight
                    .acquire_many(SETS_IN_FLIGHT)
                    .await
                    .map_err(|e| DriveError::Task(e.to_string()))?;
                drop(drained);
                reserve(&self.memory, bytes)?
            }
        };
        let permit = Arc::clone(&self.in_flight)
            .acquire_owned()
            .await
            .map_err(|e| DriveError::Task(e.to_string()))?;
        Ok(SetReservation {
            _token: token,
            _permit: permit,
        })
    }
}

fn reserve(memory: &ScopedMemory, bytes: u64) -> Result<ReservationToken, DriveError> {
    let size = usize::try_from(bytes).unwrap_or(usize::MAX);
    memory.reserve(size).map_err(|refusal| DriveError::Memory {
        needed: bytes,
        refusal,
    })
}

/// The most bytes block `index` of every reader takes: the merged fetch and
/// its per-range copies, the decoded chunk and the split bands, and one mask
/// byte per pixel for each band and for the internal mask.
pub fn set_bytes<R: RasterReader>(readers: &[R], index: u32) -> Result<u64, DriveError> {
    let mut total = 0u64;
    for (source, reader) in readers.iter().enumerate() {
        let raster = |error| DriveError::Raster {
            index: source,
            error,
        };
        let ranges = reader.block_ranges(index).map_err(raster)?;
        let window = reader.block_window(index).map_err(raster)?;
        let copies: u64 = ranges.iter().map(|range| range.len()).sum();
        let merged: u64 = coalesce(ranges, COALESCE_GAP)
            .iter()
            .map(|range| range.len())
            .sum();
        let masks =
            (window.pixels() as u64).saturating_mul(reader.header().band_count() as u64 + 1);
        total = total
            .saturating_add(copies)
            .saturating_add(merged)
            .saturating_add(reader.block_bytes().saturating_mul(2))
            .saturating_add(masks);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_mem::{EngineId, MemError};

    use crate::control::server::shared::ddl::neutral::collection::copy_from::row_memory::test_support::memory_of;
    use super::*;

    const BUDGET: usize = 1 << 16;

    fn held(memory: &ScopedMemory) -> usize {
        memory.governor().budget(EngineId::Query).allocated()
    }

    #[tokio::test]
    async fn a_set_too_large_for_the_budget_names_its_size_and_the_limit() {
        let gate = SetGate::new(memory_of(BUDGET));
        let needed = BUDGET as u64 + 1;
        let Err(DriveError::Memory {
            needed: found,
            refusal,
        }) = gate.admit(needed).await
        else {
            panic!("a set over the budget is refused");
        };
        assert_eq!(found, needed);
        assert!(matches!(
            refusal,
            MemError::BudgetExhausted { limit: BUDGET, .. }
        ));
    }

    #[tokio::test]
    async fn every_reserved_byte_returns_when_the_set_drops() {
        let memory = memory_of(BUDGET);
        let gate = SetGate::new(memory.clone());
        let set = gate.admit(1000).await.expect("under the budget");
        assert_eq!(held(&memory), 1000);
        drop(set);
        assert_eq!(held(&memory), 0);
    }

    /// Two loads on one governor share its bound, so the second cannot take
    /// what the first holds.
    #[tokio::test]
    async fn two_loads_share_one_bound() {
        let memory = memory_of(BUDGET);
        let first = SetGate::new(memory.clone());
        let second = SetGate::new(memory.clone());
        let held_set = first.admit(BUDGET as u64).await.expect("fits");
        assert!(matches!(
            second.admit(1).await,
            Err(DriveError::Memory { .. })
        ));
        drop(held_set);
        assert!(second.admit(1).await.is_ok());
    }

    /// The load's own earlier set holds the budget. The next set waits for it
    /// instead of refusing the load.
    #[tokio::test]
    async fn a_set_waits_for_the_loads_own_sets_to_drain() {
        let memory = memory_of(BUDGET);
        let gate = SetGate::new(memory.clone());
        let earlier = gate.admit(BUDGET as u64).await.expect("fits");
        let waiting = gate.clone();
        let next = tokio::spawn(async move { waiting.admit(BUDGET as u64).await.is_ok() });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!next.is_finished(), "the budget is still held");
        drop(earlier);
        assert!(next.await.expect("the task ends"));
    }

    #[test]
    fn open_rounds_keep_their_reservations_until_the_load_ends() {
        let memory = memory_of(BUDGET);
        let mut open = OpenReservation::new(memory.clone());
        open.reserve(1000).expect("first round");
        open.reserve(2000).expect("second round");
        assert_eq!(held(&memory), 3000);
        assert!(matches!(
            open.reserve(BUDGET as u64),
            Err(DriveError::Memory { .. })
        ));
        drop(open);
        assert_eq!(held(&memory), 0);
    }
}
