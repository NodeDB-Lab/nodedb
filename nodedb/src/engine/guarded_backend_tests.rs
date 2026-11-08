// SPDX-License-Identifier: BUSL-1.1

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
use std::time::Duration;

use super::*;

#[derive(Debug, Default)]
struct Probe {
    mutations: AtomicUsize,
    calls: AtomicUsize,
    entered: Option<Arc<Barrier>>,
    release: Option<Arc<Barrier>>,
}

#[derive(Debug)]
struct MockBackend(Arc<Probe>);

impl StorageBackend for MockBackend {
    fn len(&self) -> io::Result<u64> {
        self.0.calls.fetch_or(1, Ordering::SeqCst);
        Ok(12)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        assert_eq!(offset, 3);
        out.fill(4);
        self.0.calls.fetch_or(2, Ordering::SeqCst);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        assert_eq!(len, 12);
        self.0.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        assert_eq!((offset, data), (3, [4].as_slice()));
        if let Some(entered) = &self.0.entered {
            entered.wait();
        }
        if let Some(release) = &self.0.release {
            release.wait();
        }
        self.0.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        self.0.calls.fetch_or(4, Ordering::SeqCst);
        Ok(())
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(8, Ordering::SeqCst);
        Ok(false)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(16, Ordering::SeqCst);
        Ok(true)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(32, Ordering::SeqCst);
        Ok(())
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(64, Ordering::SeqCst);
        Ok(())
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(128, Ordering::SeqCst);
        Ok(())
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        assert_eq!((start, end), (Bound::Included(2), Bound::Excluded(5)));
        self.0.calls.fetch_or(256, Ordering::SeqCst);
        Ok(true)
    }
}

#[test]
fn sealed_backend_forwards_reads_locks_and_close() {
    let probe = Arc::new(Probe::default());
    let fence = WriteFence::default();
    let backend = GuardedBackend::new(MockBackend(probe.clone()), fence.clone());
    backend.write(3, &[4]).unwrap();
    backend.set_len(12).unwrap();
    backend.sync_data().unwrap();
    fence.seal();
    assert_eq!(backend.len().unwrap(), 12);
    let mut out = [0];
    backend.read(3, &mut out).unwrap();
    assert_eq!(out, [4]);
    let (start, end) = (Bound::Included(2), Bound::Excluded(5));
    assert!(!backend.try_lock_range(start, end).unwrap());
    assert!(backend.try_lock_shared_range(start, end).unwrap());
    backend.lock_range(start, end).unwrap();
    backend.lock_shared_range(start, end).unwrap();
    backend.unlock_range(start, end).unwrap();
    assert!(backend.query_lock_range(start, end).unwrap());
    backend.close().unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 511);
    for error in [
        backend.write(3, &[4]).unwrap_err(),
        backend.set_len(12).unwrap_err(),
        backend.sync_data().unwrap_err(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            error.to_string(),
            "database storage is closed for writes until reopen"
        );
    }
    assert_eq!(probe.mutations.load(Ordering::SeqCst), 3);
}

#[test]
fn seal_waits_for_inflight_mutation_and_blocks_later_delegation() {
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let probe = Arc::new(Probe {
        entered: Some(entered.clone()),
        release: Some(release.clone()),
        ..Probe::default()
    });
    let fence = WriteFence::default();
    let backend = Arc::new(GuardedBackend::new(
        MockBackend(probe.clone()),
        fence.clone(),
    ));
    let writer = {
        let backend = backend.clone();
        std::thread::spawn(move || backend.write(3, &[4]))
    };
    entered.wait();
    let (started_tx, started_rx) = mpsc::channel();
    let (sealed_tx, sealed_rx) = mpsc::channel();
    let sealer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        fence.seal();
        sealed_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        sealed_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.wait();
    writer.join().unwrap().unwrap();
    sealed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    sealer.join().unwrap();
    assert!(backend.write(3, &[4]).is_err());
    assert!(backend.set_len(12).is_err());
    assert!(backend.sync_data().is_err());
    assert_eq!(probe.mutations.load(Ordering::SeqCst), 1);
}

#[test]
fn poisoned_gate_seals_before_any_mutation_delegates() {
    let fence = WriteFence::default();
    let poison = fence.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.state();
            panic!("poison mutation gate");
        })
        .join()
        .is_err()
    );
    let probe = Arc::new(Probe::default());
    let backend = GuardedBackend::new(MockBackend(probe.clone()), fence.clone());
    assert!(backend.write(3, &[4]).is_err());
    assert!(backend.set_len(12).is_err());
    assert!(backend.sync_data().is_err());
    assert!(fence.check().is_err());
    fence.seal();
    backend.close().unwrap();
    assert_eq!(probe.mutations.load(Ordering::SeqCst), 0);
}
