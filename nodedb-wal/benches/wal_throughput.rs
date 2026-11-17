// SPDX-License-Identifier: Apache-2.0

//! WAL throughput benchmarks.
//!
//! Risk 4: Validate 100K writes/sec WAL target.
//!
//! Run with: cargo bench -p nodedb-wal --bench wal_throughput

use fluxbench::prelude::*;
use fluxbench::{bench, synthetic, verify};
use std::hint::black_box;
use std::sync::{Arc, Mutex};

use nodedb_wal::record::RecordType;
use nodedb_wal::writer::WalWriter;

/// Single-threaded WAL append + fsync — 1000 x 128-byte records per iteration.
#[bench(id = "wal_append_fsync_1k_128b", group = "wal_write", tags = "core")]
fn wal_append_fsync_1k(b: &mut Bencher) {
    let dir = tempfile::tempdir().expect("create the temp directory");
    let path = dir.path().join("bench.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let mut writer = WalWriter::open_without_direct_io(&path).expect("open the WAL writer");
        for _ in 0..1000 {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &payload)
                .expect("append the record");
        }
        writer.sync().expect("sync the WAL");
        black_box(writer.next_lsn())
    });
}

/// WAL append-only (no fsync) — measures raw serialization + buffer speed.
#[bench(id = "wal_append_only_10k_128b", group = "wal_write")]
fn wal_append_only_10k(b: &mut Bencher) {
    let dir = tempfile::tempdir().expect("create the temp directory");
    let path = dir.path().join("bench_nofs.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let mut writer = WalWriter::open_without_direct_io(&path).expect("open the WAL writer");
        for _ in 0..10_000 {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &payload)
                .expect("append the record");
        }
        black_box(writer.next_lsn())
    });
}

/// Group commit — 10 threads each appending 1000 records through one shared
/// `WalWriter`, with a batched fsync amortized over the whole batch.
///
/// This is the shape of the live durability barrier: concurrent appenders
/// serialize on the writer lock, and one `sync()` covers every record they
/// buffered, so N appends cost one fsync rather than N.
#[bench(
    id = "wal_group_commit_10t_1k",
    group = "wal_group_commit",
    tags = "core"
)]
fn wal_group_commit(b: &mut Bencher) {
    const THREADS: usize = 10;
    const RECORDS_PER_THREAD: usize = 1000;
    /// Records buffered per thread before it asks for a durability barrier.
    /// Smaller batches mean more fsyncs sharing fewer records, which is what
    /// makes this measure batching rather than raw buffer speed.
    const SYNC_EVERY: usize = 100;

    let dir = tempfile::tempdir().expect("create the temp directory");
    let path = dir.path().join("bench_gc.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let writer = Arc::new(Mutex::new(
            WalWriter::open_without_direct_io(&path).expect("open the WAL writer"),
        ));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let w = Arc::clone(&writer);
                let p = payload.clone();
                std::thread::spawn(move || {
                    for i in 0..RECORDS_PER_THREAD {
                        let mut guard = w.lock().expect("lock the WAL writer");
                        guard
                            .append(RecordType::Put as u32, t as u64, 0, 0, &p)
                            .expect("append the record");
                        // One fsync covers everything every appender buffered
                        // since the last barrier, including other threads'
                        // records — that batching is what is being measured.
                        if i % SYNC_EVERY == SYNC_EVERY - 1 {
                            guard.sync().expect("sync the WAL");
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("join the writer thread");
        }

        let mut w = writer.lock().expect("lock the WAL writer");
        w.sync().expect("sync the WAL");
        black_box(w.next_lsn())
    });
}

// 1000 records per iteration of wal_append_fsync_1k_128b.
#[synthetic(
    id = "wal_writes_per_sec",
    formula = "1000 / wal_append_fsync_1k_128b * 1000000000",
    unit = "writes/s"
)]
#[allow(dead_code)]
struct WalWritesPerSec;

// 10 threads * 1000 records = 10K records per group commit iteration.
#[synthetic(
    id = "wal_group_commit_writes_per_sec",
    formula = "10000 / wal_group_commit_10t_1k * 1000000000",
    unit = "writes/s"
)]
#[allow(dead_code)]
struct WalGroupCommitWritesPerSec;

// Target: 100K writes/sec with group commit.
#[verify(
    expr = "wal_group_commit_writes_per_sec > 100000",
    severity = "critical"
)]
#[allow(dead_code)]
struct WalGroupCommitTarget;

fn main() {
    if let Err(e) = fluxbench::run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
