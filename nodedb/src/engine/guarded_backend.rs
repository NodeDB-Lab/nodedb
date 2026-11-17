// SPDX-License-Identifier: BUSL-1.1

use std::io;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use redb::{BackendError, StorageBackend};

/// Shared across the database wrapper and surviving redb transactions.
#[derive(Clone, Debug, Default)]
pub(super) struct WriteFence(Arc<Mutex<bool>>);

impl WriteFence {
    fn state(&self) -> MutexGuard<'_, bool> {
        match self.0.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = true;
                state
            }
        }
    }

    pub(super) fn seal(&self) {
        *self.state() = true;
    }

    pub(super) fn check(&self) -> io::Result<()> {
        Self::check_state(*self.state())
    }

    fn check_state(sealed: bool) -> io::Result<()> {
        if sealed {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "database storage is closed for writes until reopen",
            ))
        } else {
            Ok(())
        }
    }

    fn mutate(&self, action: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let state = self.state();
        Self::check_state(*state)?;
        // Keep the gate locked until the delegated mutation completes.
        let result = action();
        drop(state);
        result
    }
}

#[derive(Debug)]
pub(super) struct GuardedBackend<B> {
    backend: B,
    fence: WriteFence,
}

impl<B> GuardedBackend<B> {
    pub(super) fn new(backend: B, fence: WriteFence) -> Self {
        Self { backend, fence }
    }
}

impl<B: StorageBackend> StorageBackend for GuardedBackend<B> {
    fn len(&self) -> io::Result<u64> {
        self.backend.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.backend.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fence.mutate(|| self.backend.set_len(len))
    }

    fn sync_data(&self) -> io::Result<()> {
        self.fence.mutate(|| self.backend.sync_data())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.fence.mutate(|| self.backend.write(offset, data))
    }

    fn close(&self) -> io::Result<()> {
        self.backend.close()
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.backend.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.query_lock_range(start, end)
    }
}

#[cfg(test)]
#[path = "guarded_backend_tests.rs"]
mod tests;
