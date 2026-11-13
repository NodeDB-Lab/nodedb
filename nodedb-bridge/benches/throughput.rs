// SPDX-License-Identifier: BUSL-1.1

//! Bridge throughput benchmarks.
//!
//! Risk 3: Validate 50GB / 5GB/s bridge throughput target.
//!
//! Run with: cargo bench -p nodedb-bridge --bench throughput

use fluxbench::prelude::*;
use fluxbench::{bench, synthetic, verify};
use std::hint::black_box;

use nodedb_bridge::buffer::RingBuffer;

const MSG_SIZE: usize = 256;

/// Single-threaded SPSC push/pop cycle — 1024 x 256-byte messages per iteration.
#[bench(id = "spsc_push_pop_256b_x1024", group = "throughput", tags = "core")]
fn spsc_push_pop(b: &mut Bencher) {
    let (mut prod, mut cons) = RingBuffer::channel::<Vec<u8>>(4096);
    let payload = vec![0xABu8; MSG_SIZE];

    b.iter(|| {
        for _ in 0..1024 {
            while prod.try_push(payload.clone()).is_err() {
                while cons.try_pop().is_ok() {}
            }
        }
        let mut count = 0usize;
        while cons.try_pop().is_ok() {
            count += 1;
        }
        black_box(count)
    });
}

/// Push-only throughput — measures raw producer speed into empty ring.
#[bench(id = "spsc_push_only_256b_x4096", group = "throughput")]
fn spsc_push_only(b: &mut Bencher) {
    let (mut prod, mut cons) = RingBuffer::channel::<Vec<u8>>(8192);
    let payload = vec![0xABu8; MSG_SIZE];

    b.iter(|| {
        while cons.try_pop().is_ok() {}

        let mut pushed = 0usize;
        for _ in 0..4096 {
            if prod.try_push(payload.clone()).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }
        black_box(pushed)
    });
}

/// Pop-only throughput — measures raw consumer speed from full ring.
#[bench(id = "spsc_pop_only_4096", group = "throughput")]
fn spsc_pop_only(b: &mut Bencher) {
    let (mut prod, mut cons) = RingBuffer::channel::<Vec<u8>>(8192);
    let payload = vec![0xABu8; MSG_SIZE];

    b.iter(|| {
        while prod.try_push(payload.clone()).is_ok() {}

        let mut popped = 0usize;
        while cons.try_pop().is_ok() {
            popped += 1;
        }
        black_box(popped)
    });
}

/// Cross-thread SPSC throughput — producer and consumer on separate threads.
/// This is the realistic bridge scenario: 100K x 256-byte messages.
#[bench(
    id = "spsc_cross_thread_100k_256b",
    group = "cross_thread",
    tags = "core"
)]
fn spsc_cross_thread(b: &mut Bencher) {
    const COUNT: usize = 100_000;

    b.iter(|| {
        let (mut prod, mut cons) = RingBuffer::channel::<Vec<u8>>(8192);
        let payload = vec![0xABu8; MSG_SIZE];

        let consumer = std::thread::spawn(move || {
            let mut received = 0usize;
            while received < COUNT {
                if cons.try_pop().is_ok() {
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        let mut sent = 0usize;
        while sent < COUNT {
            if prod.try_push(payload.clone()).is_ok() {
                sent += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        let received = consumer.join().expect("join the consumer thread");
        black_box(received)
    });
}

// 100K messages * 256 bytes = 25.6 MB per iteration.
#[synthetic(
    id = "bridge_throughput_mb_per_sec",
    formula = "25600000 / spsc_cross_thread_100k_256b * 1000000000 / 1048576",
    unit = "MB/s"
)]
#[allow(dead_code)]
struct BridgeThroughput;

#[synthetic(
    id = "bridge_msgs_per_sec",
    formula = "100000 / spsc_cross_thread_100k_256b * 1000000000",
    unit = "msgs/s"
)]
#[allow(dead_code)]
struct BridgeMsgsPerSec;

// Baseline: >= 200 MB/s with Vec<u8> clone per message.
// Production uses Arc<[u8]> / slab IDs (zero-copy) which eliminates the clone overhead.
// The 5 GB/s aggregate target uses multiple bridge pairs + zero-copy payloads.
#[verify(expr = "bridge_throughput_mb_per_sec > 200", severity = "critical")]
#[allow(dead_code)]
struct BridgeThroughputGuard;

fn main() {
    if let Err(e) = fluxbench::run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
