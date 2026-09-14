// SPDX-License-Identifier: Apache-2.0

//! BBQ kernel benches: the zero-copy fused path against the
//! reconstruct-and-measure path it replaces.
//!
//! The unfused bench mirrors the removed path shape: decode the prepared
//! payload into a `Vec<f32>` per candidate, then reconstruct each dimension.
//! (Its residual scale is an approximation: the bench measures the allocation
//! and pass shape, not the codec's exact corrective factor.)
//!
//! Run with: cargo bench -p nodedb-vector --bench bbq_kernel

use fluxbench::bench;
use fluxbench::prelude::*;
use std::hint::black_box;

use nodedb_vector::rerank::codec::{PreparedQuery, RerankCodec};
use nodedb_vector::rerank::codecs::BbqRerank;

/// Installs fluxbench's tracking allocator so the harness reports heap bytes
/// and allocation counts per benchmark — the fused path must show zero
/// allocations, the replaced path must show one `Vec<f32>` per candidate.
#[global_allocator]
static ALLOC: fluxbench::TrackingAllocator = fluxbench::TrackingAllocator;

const OVERSAMPLE: u8 = 4;
const CANDIDATES: usize = 256;

fn det_vec(i: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| (((i * 31 + j) % 100) as f32 / 100.0) - 0.5)
        .collect()
}

fn setup(dim: usize) -> (BbqRerank, PreparedQuery, Vec<Vec<u8>>) {
    let vecs: Vec<Vec<f32>> = (0..CANDIDATES).map(|i| det_vec(i, dim)).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    let mut codec = BbqRerank::new(dim, OVERSAMPLE);
    codec.train(&refs).expect("train");
    let prepared = codec.prepare_query(&vecs[0]).expect("prepare_query");
    let encoded: Vec<Vec<u8>> = vecs
        .iter()
        .map(|v| codec.encode(v).expect("encode"))
        .collect();
    (codec, prepared, encoded)
}

/// The replaced path: decode the prepared payload to `Vec<f32>` per candidate,
/// then reconstruct each dimension. `encoded` carries a 32-byte quant header
/// before the sign bits.
fn unfused_l2(payload: &[u8], encoded: &[u8], dim: usize) -> f32 {
    let centered: Vec<f32> = payload[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect();
    let scale = 1.0f32 / (dim as f32).sqrt();
    let mut acc = 0.0f32;
    for i in 0..dim {
        let bit = (encoded[32 + i / 8] >> (7 - (i % 8))) & 1;
        let recon = if bit != 0 { scale } else { -scale };
        let d = centered[i] - recon;
        acc += d * d;
    }
    acc.sqrt()
}

#[bench(id = "bbq_fused_128", group = "bbq_kernel")]
fn bbq_fused_128(b: &mut Bencher) {
    let (codec, prepared, encoded) = setup(128);
    b.iter(|| {
        let mut acc = 0.0f32;
        for e in &encoded {
            acc += codec.distance_prepared(&prepared, e).expect("distance");
        }
        black_box(acc)
    });
}

#[bench(id = "bbq_unfused_128", group = "bbq_kernel")]
fn bbq_unfused_128(b: &mut Bencher) {
    let (_codec, prepared, encoded) = setup(128);
    let payload = match &prepared {
        PreparedQuery::Bytes(b) => b.as_slice(),
        _ => panic!("bbq prepared form is Bytes"),
    };
    b.iter(|| {
        let mut acc = 0.0f32;
        for e in &encoded {
            acc += unfused_l2(payload, e, 128);
        }
        black_box(acc)
    });
}

#[bench(id = "bbq_fused_768", group = "bbq_kernel")]
fn bbq_fused_768(b: &mut Bencher) {
    let (codec, prepared, encoded) = setup(768);
    b.iter(|| {
        let mut acc = 0.0f32;
        for e in &encoded {
            acc += codec.distance_prepared(&prepared, e).expect("distance");
        }
        black_box(acc)
    });
}

#[bench(id = "bbq_unfused_768", group = "bbq_kernel")]
fn bbq_unfused_768(b: &mut Bencher) {
    let (_codec, prepared, encoded) = setup(768);
    let payload = match &prepared {
        PreparedQuery::Bytes(b) => b.as_slice(),
        _ => panic!("bbq prepared form is Bytes"),
    };
    b.iter(|| {
        let mut acc = 0.0f32;
        for e in &encoded {
            acc += unfused_l2(payload, e, 768);
        }
        black_box(acc)
    });
}

fn main() {
    if let Err(e) = fluxbench::run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
