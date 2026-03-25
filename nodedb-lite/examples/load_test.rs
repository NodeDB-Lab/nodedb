//! Load test: 100 concurrent Lite clients syncing to Origin.
//!
//! Run: `cargo run -p nodedb-lite --example load_test`
//! Requires: Origin with sync endpoint on ws://127.0.0.1:9090

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::wire::*;

const ORIGIN_WS: &str = "ws://127.0.0.1:9090";
const NUM_CLIENTS: u32 = 100;

#[tokio::main]
async fn main() {
    println!("=== Load Test: {NUM_CLIENTS} concurrent sync clients ===\n");

    if tokio_tungstenite::connect_async(ORIGIN_WS).await.is_err() {
        println!("SKIP: Origin not available at {ORIGIN_WS}");
        return;
    }

    let connected = Arc::new(AtomicU32::new(0));
    let handshook = Arc::new(AtomicU32::new(0));
    let deltas_sent = Arc::new(AtomicU32::new(0));
    let deltas_acked = Arc::new(AtomicU32::new(0));
    let deltas_rejected = Arc::new(AtomicU32::new(0));
    let errors = Arc::new(AtomicU32::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(NUM_CLIENTS as usize);

    for i in 0..NUM_CLIENTS {
        let connected = Arc::clone(&connected);
        let handshook = Arc::clone(&handshook);
        let deltas_sent = Arc::clone(&deltas_sent);
        let deltas_acked = Arc::clone(&deltas_acked);
        let deltas_rejected = Arc::clone(&deltas_rejected);
        let errors = Arc::clone(&errors);

        handles.push(tokio::spawn(async move {
            run_client(
                i,
                connected,
                handshook,
                deltas_sent,
                deltas_acked,
                deltas_rejected,
                errors,
            )
            .await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();

    println!("connected:      {}", connected.load(Ordering::Relaxed));
    println!("handshook:      {}", handshook.load(Ordering::Relaxed));
    println!("deltas sent:    {}", deltas_sent.load(Ordering::Relaxed));
    println!("deltas acked:   {}", deltas_acked.load(Ordering::Relaxed));
    println!(
        "deltas rejected:{}",
        deltas_rejected.load(Ordering::Relaxed)
    );
    println!("errors:         {}", errors.load(Ordering::Relaxed));
    println!("total time:     {:.2}s", elapsed.as_secs_f64());
    println!(
        "throughput:     {:.0} handshakes/sec",
        handshook.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64()
    );

    let h = handshook.load(Ordering::Relaxed);
    if h >= NUM_CLIENTS {
        println!("\nresult: PASS — all {NUM_CLIENTS} clients connected and synced");
    } else {
        println!("\nresult: PARTIAL — {h}/{NUM_CLIENTS} connected");
        std::process::exit(1);
    }
}

async fn run_client(
    id: u32,
    connected: Arc<AtomicU32>,
    handshook: Arc<AtomicU32>,
    deltas_sent: Arc<AtomicU32>,
    deltas_acked: Arc<AtomicU32>,
    deltas_rejected: Arc<AtomicU32>,
    errors: Arc<AtomicU32>,
) {
    // Connect.
    let (mut ws, _) = match tokio_tungstenite::connect_async(ORIGIN_WS).await {
        Ok(ws) => ws,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    connected.fetch_add(1, Ordering::Relaxed);

    // Handshake.
    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: format!("load-test-{id}"),
        lite_id: String::new(),
        epoch: 0,
    };
    if ws
        .send(Message::Binary(
            SyncFrame::encode_or_empty(SyncMessageType::Handshake, &hs)
                .to_bytes()
                .into(),
        ))
        .await
        .is_err()
    {
        errors.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let resp = match tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await {
        Ok(Some(Ok(msg))) => msg,
        _ => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    if let Some(frame) = SyncFrame::from_bytes(resp.into_data().as_ref())
        && let Some(ack) = frame.decode_body::<HandshakeAckMsg>()
    {
        if ack.success {
            handshook.fetch_add(1, Ordering::Relaxed);
        } else {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }

    // Send a real Loro delta.
    let mut engine = match CrdtEngine::new(1000 + id as u64) {
        Ok(e) => e,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let _ = engine.upsert(
        "load_test",
        &format!("doc-{id}"),
        &[("client_id", loro::LoroValue::I64(id as i64))],
    );

    let deltas = engine.pending_deltas();
    if let Some(delta) = deltas.first() {
        let msg = DeltaPushMsg {
            collection: "load_test".into(),
            document_id: format!("doc-{id}"),
            delta: delta.delta_bytes.clone(),
            peer_id: 1000 + id as u64,
            mutation_id: 1,
            checksum: 0,
        };
        if ws
            .send(Message::Binary(
                SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &msg)
                    .to_bytes()
                    .into(),
            ))
            .await
            .is_ok()
        {
            deltas_sent.fetch_add(1, Ordering::Relaxed);

            if let Ok(Some(Ok(resp))) =
                tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await
                && let Some(frame) = SyncFrame::from_bytes(resp.into_data().as_ref())
            {
                match frame.msg_type {
                    SyncMessageType::DeltaAck => {
                        deltas_acked.fetch_add(1, Ordering::Relaxed);
                    }
                    SyncMessageType::DeltaReject => {
                        deltas_rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = ws.close(None).await;
}
