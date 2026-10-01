// SPDX-License-Identifier: BUSL-1.1

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tracing::info;

use nodedb_types::config::tuning::WalTuning;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};
use nodedb_wal::writer::WalWriterConfig;

/// WAL manager: owns the segmented WAL and coordinates appends + sync.
///
/// The WAL is the single source of truth for durability. Every mutation
/// goes through here before being applied to any engine's in-memory state.
///
/// Thread-safety: the segmented WAL is behind a `Mutex` because multiple
/// Control Plane tasks may submit WAL appends concurrently. The mutex
/// serializes writes, which is correct — WAL appends must be ordered anyway.
/// The `sync()` call (fsync) is the expensive part and is batched via group commit.
pub struct WalManager {
    /// The segmented WAL. `Arc` so the group-commit fsync
    /// ([`WalManager::wait_durable`]) can move a handle into a `spawn_blocking`
    /// thread — the O_DIRECT `sync()` must never run inline on a Tokio worker.
    pub(super) wal: Arc<Mutex<SegmentedWal>>,
    /// The WAL directory path (for replay without holding the lock).
    pub(super) wal_dir: PathBuf,
    /// Encryption key ring (if configured). Supports dual-key reads during rotation.
    pub(super) encryption_ring: Option<nodedb_wal::crypto::KeyRing>,
    /// Stable CRDT signing root, persisted only as WAL-key-wrapped ciphertext.
    pub(super) crdt_signing_root: Option<[u8; 32]>,
    /// Dedicated audit WAL segment. When present, audit entries are written
    /// atomically alongside data writes. Append-only, never compacted.
    pub(super) audit_wal: Option<crate::wal::AuditWalSegment>,
    /// Highest LSN known to be fsync-durable. Advanced only by the
    /// group-commit leader in [`WalManager::wait_durable`] after a successful
    /// `sync()`, never past what that fsync actually made durable.
    pub(super) durable_lsn: AtomicU64,
    /// Async group-commit leader election: the first `wait_durable` caller to
    /// acquire this becomes the leader and performs the single coalesced fsync
    /// covering every concurrently-buffered write. Async so waiting for
    /// leadership never blocks a Tokio worker.
    pub(super) commit_lock: tokio::sync::Mutex<()>,
    /// Wakes `wait_durable` followers when `durable_lsn` advances (or a leader's
    /// fsync fails, so they re-attempt and observe the same error).
    pub(super) durable_notify: tokio::sync::Notify,
    /// System metrics registry handle for recording group-commit fsync latency.
    pub(super) metrics: std::sync::RwLock<Option<Arc<crate::control::metrics::SystemMetrics>>>,
}

impl WalManager {
    /// Whether every appended payload is protected by an AEAD key ring.
    pub fn payloads_authenticated(&self) -> bool {
        self.encryption_ring.is_some()
    }

    /// Highest LSN the WAL is known to be fsync-durable through.
    ///
    /// An append only buffers its record, so this is what separates a write
    /// that survives a `kill -9` from one that does not: a caller holding
    /// `lsn` is durable exactly when this is `>= lsn.as_u64()`. Advanced only
    /// by a successful group-commit fsync in [`WalManager::wait_durable`].
    pub fn durable_through(&self) -> u64 {
        self.durable_lsn.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Wire or replace the system metrics handle used to record group-commit fsync latency.
    pub fn set_metrics(&self, metrics: Arc<crate::control::metrics::SystemMetrics>) {
        let mut w = self.metrics.write().unwrap_or_else(|p| p.into_inner());
        *w = Some(metrics);
    }

    /// Read the optional system metrics handle.
    pub fn metrics(&self) -> Option<Arc<crate::control::metrics::SystemMetrics>> {
        self.metrics
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Return the stable in-memory root for per-user CRDT signing keys.
    /// The root is persisted only as WAL-key-wrapped ciphertext and is
    /// rewrapped on rotation, so offline signatures survive chained key
    /// rotations and restarts. No root exists when WAL encryption is disabled,
    /// which prevents `SIGNED_DELTAS` from being enabled.
    pub fn crdt_signing_root(&self) -> crate::Result<Option<[u8; 32]>> {
        Ok(self.crdt_signing_root)
    }

    /// Open or create a segmented WAL at the given path.
    ///
    /// The `path` argument is the WAL directory for the segmented format.
    pub fn open(path: &Path, use_direct_io: bool) -> crate::Result<Self> {
        Self::open_with_segment_size(path, use_direct_io, 0)
    }

    /// Open with explicit segment target size (bytes). 0 = default (64 MiB).
    pub fn open_with_segment_size(
        path: &Path,
        use_direct_io: bool,
        segment_target_size: u64,
    ) -> crate::Result<Self> {
        Self::open_internal(
            path,
            segment_target_size,
            WalWriterConfig {
                use_direct_io,
                ..Default::default()
            },
        )
    }

    /// Open with explicit segment target size and WAL tuning from `TuningConfig`.
    ///
    /// Every writer setting — buffer size, alignment, and whether the segments
    /// are opened with `O_DIRECT` — comes from [`WalTuning`], so there is no
    /// second place a deployment's direct-I/O decision could be contradicted.
    /// `segment_target_size` of 0 uses the default (64 MiB).
    pub fn open_with_tuning(
        path: &Path,
        segment_target_size: u64,
        tuning: &WalTuning,
    ) -> crate::Result<Self> {
        Self::open_internal(
            path,
            segment_target_size,
            WalWriterConfig {
                write_buffer_size: tuning.write_buffer_size,
                alignment: tuning.alignment,
                use_direct_io: tuning.direct_io,
                dwb_mode: None,
            },
        )
    }

    /// Shared WAL open logic: resolve segment size, open.
    pub(super) fn open_internal(
        path: &Path,
        segment_target_size: u64,
        writer_config: WalWriterConfig,
    ) -> crate::Result<Self> {
        let wal_dir = path.to_path_buf();

        let effective_target = if segment_target_size > 0 {
            segment_target_size
        } else {
            nodedb_wal::segment::DEFAULT_SEGMENT_TARGET_SIZE
        };

        let use_direct_io = writer_config.use_direct_io;
        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: effective_target,
            writer_config,
        };

        let wal = SegmentedWal::open(config).map_err(crate::Error::Wal)?;

        // `direct_io` is logged because a WAL running buffered is a weaker
        // durability posture than the default, and the log is the only place
        // an operator can confirm which one this process actually got.
        info!(
            wal_dir = %wal_dir.display(),
            next_lsn = wal.next_lsn(),
            direct_io = use_direct_io,
            "WAL opened"
        );

        let audit_dir = wal_dir.join("audit.wal");
        let audit_wal = match crate::wal::AuditWalSegment::open(&audit_dir, use_direct_io) {
            Ok(aw) => {
                info!(audit_dir = %audit_dir.display(), "audit WAL opened");
                Some(aw)
            }
            Err(e) => {
                tracing::warn!(error = %e, "audit WAL failed to open (audit entries not durable)");
                None
            }
        };

        Ok(Self {
            wal: Arc::new(Mutex::new(wal)),
            wal_dir: wal_dir.to_path_buf(),
            encryption_ring: None,
            crdt_signing_root: None,
            audit_wal,
            durable_lsn: AtomicU64::new(0),
            commit_lock: tokio::sync::Mutex::new(()),
            durable_notify: tokio::sync::Notify::new(),
            metrics: std::sync::RwLock::new(None),
        })
    }

    /// Open without `O_DIRECT`.
    ///
    /// The in-process test harnesses put their data directories in tempdirs,
    /// whose filesystem is whatever `TMPDIR` happens to point at and is not
    /// guaranteed to accept `O_DIRECT`. Production opens through
    /// [`WalManager::open_with_tuning`], where direct I/O is the default and a
    /// filesystem that cannot provide it fails startup instead of downgrading.
    pub fn open_for_testing(path: &Path) -> crate::Result<Self> {
        Self::open(path, false)
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }
}
