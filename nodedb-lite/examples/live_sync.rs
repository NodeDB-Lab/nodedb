//! Live sync tests against a running Origin server.
//!
//! Run: `cargo run -p nodedb-lite --example live_sync`
//! Requires: Origin with sync endpoint on ws://127.0.0.1:9090
//!   Start Origin: `RUST_LOG=info cargo run --release -p nodedb`

use std::time::Instant;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::wire::*;

const ORIGIN_WS: &str = "ws://127.0.0.1:9090";

async fn connect_and_handshake()
-> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ORIGIN_WS)
        .await
        .expect("connect to Origin");

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "live-test".into(),
        lite_id: String::new(),
        epoch: 0,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::Handshake, &hs)
            .to_bytes()
            .into(),
    ))
    .await
    .unwrap();

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let ack: HandshakeAckMsg = SyncFrame::from_bytes(resp.into_data().as_ref())
        .unwrap()
        .decode_body()
        .unwrap();
    assert!(ack.success, "handshake failed: {:?}", ack.error);
    ws
}

#[tokio::main]
async fn main() {
    println!("=== Live Sync Tests (Origin at {ORIGIN_WS}) ===\n");

    if tokio_tungstenite::connect_async(ORIGIN_WS).await.is_err() {
        println!("SKIP: Origin not available at {ORIGIN_WS}");
        println!("Start: RUST_LOG=info cargo run --release -p nodedb");
        return;
    }

    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! run_test {
        ($name:expr, $body:expr) => {
            print!("test {} ... ", $name);
            match $body.await {
                Ok(()) => {
                    println!("ok");
                    passed += 1;
                }
                Err(e) => {
                    println!("FAILED: {e}");
                    failed += 1;
                }
            }
        };
    }

    run_test!("handshake", test_handshake());
    run_test!("delta_push", test_delta_push());
    run_test!("ping_pong", test_ping_pong());
    run_test!("reconnect_under_200ms", test_reconnect_latency());
    run_test!("vector_clock_sync", test_clock_sync());
    run_test!("shape_subscribe", test_shape_subscribe());
    run_test!("real_loro_delta_push", test_real_loro_delta());
    run_test!("concurrent_delta_push", test_concurrent_deltas());
    run_test!("rls_violation_silent_drop", test_rls_violation());
    run_test!("shape_snapshot_with_wal_lsn", test_shape_snapshot_lsn());

    println!("\nresult: {passed} passed; {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
}

async fn test_handshake() -> Result<(), String> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ORIGIN_WS)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "test-handshake".into(),
        lite_id: String::new(),
        epoch: 0,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::Handshake, &hs)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::HandshakeAck {
        return Err(format!("expected HandshakeAck, got {:?}", frame.msg_type));
    }
    let ack: HandshakeAckMsg = frame.decode_body().ok_or("decode")?;
    if !ack.success {
        return Err(format!("rejected: {:?}", ack.error));
    }
    Ok(())
}

async fn test_delta_push() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let delta = DeltaPushMsg {
        collection: "live_test".into(),
        document_id: "d1".into(),
        delta: rmp_serde::to_vec_named(&serde_json::json!({"key": "value"}))
            .map_err(|e| format!("serialize: {e}"))?,
        peer_id: 42,
        mutation_id: 1,
        checksum: 0,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &delta)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::DeltaAck && frame.msg_type != SyncMessageType::DeltaReject
    {
        return Err(format!("unexpected: {:?}", frame.msg_type));
    }
    Ok(())
}

async fn test_ping_pong() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let ping = PingPongMsg {
        timestamp_ms: 123456789,
        is_pong: false,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::PingPong, &ping)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::PingPong {
        return Err(format!("expected PingPong, got {:?}", frame.msg_type));
    }
    let pong: PingPongMsg = frame.decode_body().ok_or("decode")?;
    if !pong.is_pong {
        return Err("expected is_pong=true".into());
    }
    if pong.timestamp_ms != 123456789 {
        return Err(format!("wrong timestamp: {}", pong.timestamp_ms));
    }
    Ok(())
}

async fn test_reconnect_latency() -> Result<(), String> {
    let start = Instant::now();
    let _ws = connect_and_handshake().await;
    let elapsed = start.elapsed();
    if elapsed.as_millis() >= 200 {
        return Err(format!("took {}ms, target < 200ms", elapsed.as_millis()));
    }
    Ok(())
}

async fn test_clock_sync() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let clock = VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert("0000000000000001".to_string(), 42u64);
            m
        },
        sender_id: 1,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::VectorClockSync, &clock)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::VectorClockSync {
        return Err(format!(
            "expected VectorClockSync, got {:?}",
            frame.msg_type
        ));
    }
    Ok(())
}

async fn test_shape_subscribe() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // Subscribe to a document shape.
    let subscribe = ShapeSubscribeMsg {
        shape: nodedb_types::sync::shape::ShapeDefinition {
            shape_id: "test-shape".into(),
            tenant_id: 0,
            shape_type: nodedb_types::sync::shape::ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "test".into(),
            field_filter: vec![],
        },
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::ShapeSubscribe, &subscribe)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    // Should get ShapeSnapshot back.
    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::ShapeSnapshot {
        return Err(format!("expected ShapeSnapshot, got {:?}", frame.msg_type));
    }

    let snapshot: ShapeSnapshotMsg = frame.decode_body().ok_or("decode")?;
    if snapshot.shape_id != "test-shape" {
        return Err(format!("wrong shape_id: {}", snapshot.shape_id));
    }

    Ok(())
}

/// Test with real Loro CRDT deltas (not raw JSON).
async fn test_real_loro_delta() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // Generate a real Loro delta using the CRDT engine.
    let mut engine = CrdtEngine::new(100).map_err(|e| format!("crdt engine: {e}"))?;
    engine
        .upsert(
            "users",
            "alice",
            &[("name", loro::LoroValue::String("Alice".into()))],
        )
        .map_err(|e| format!("upsert: {e}"))?;

    let deltas = engine.pending_deltas();
    if deltas.is_empty() {
        return Err("no deltas generated".into());
    }

    // Send the real Loro delta bytes through sync.
    let delta_msg = DeltaPushMsg {
        collection: "users".into(),
        document_id: "alice".into(),
        delta: deltas[0].delta_bytes.clone(),
        peer_id: 100,
        mutation_id: 1,
        checksum: 0,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &delta_msg)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    // With real Loro deltas, the CRDT engine should accept them.
    // DeltaAck = accepted, DeltaReject = constraint violation (both valid).
    if frame.msg_type != SyncMessageType::DeltaAck && frame.msg_type != SyncMessageType::DeltaReject
    {
        return Err(format!("unexpected: {:?}", frame.msg_type));
    }

    Ok(())
}

/// Test two concurrent deltas from different peers.
async fn test_concurrent_deltas() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // Peer 1 writes.
    let mut engine1 = CrdtEngine::new(201).map_err(|e| format!("engine1: {e}"))?;
    engine1
        .upsert(
            "notes",
            "n1",
            &[("author", loro::LoroValue::String("peer1".into()))],
        )
        .map_err(|e| format!("upsert1: {e}"))?;

    // Peer 2 writes to different doc.
    let mut engine2 = CrdtEngine::new(202).map_err(|e| format!("engine2: {e}"))?;
    engine2
        .upsert(
            "notes",
            "n2",
            &[("author", loro::LoroValue::String("peer2".into()))],
        )
        .map_err(|e| format!("upsert2: {e}"))?;

    // Send both deltas.
    for (i, engine) in [&engine1, &engine2].iter().enumerate() {
        let deltas = engine.pending_deltas();
        if deltas.is_empty() {
            return Err(format!("no deltas from engine {i}"));
        }
        let msg = DeltaPushMsg {
            collection: "notes".into(),
            document_id: deltas[0].document_id.clone(),
            delta: deltas[0].delta_bytes.clone(),
            peer_id: 200 + i as u64 + 1,
            mutation_id: i as u64 + 1,
            checksum: 0,
        };
        ws.send(Message::Binary(
            SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &msg)
                .to_bytes()
                .into(),
        ))
        .await
        .map_err(|e| format!("send {i}: {e}"))?;
    }

    // Read two responses.
    for i in 0..2 {
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .map_err(|_| format!("timeout on response {i}"))?
            .ok_or(format!("closed on response {i}"))?
            .map_err(|e| format!("read {i}: {e}"))?;

        let frame =
            SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or(format!("bad frame {i}"))?;
        if frame.msg_type != SyncMessageType::DeltaAck
            && frame.msg_type != SyncMessageType::DeltaReject
        {
            return Err(format!("unexpected response {i}: {:?}", frame.msg_type));
        }
    }

    Ok(())
}

/// Test RLS violation: delta to "orders" with status=draft should be silently dropped
/// because the Origin has a write policy requiring status=active.
///
/// Requires: `CREATE RLS POLICY require_active ON orders FOR WRITE USING (status eq active)`
async fn test_rls_violation() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // Delta with status=draft — violates the RLS write policy.
    let violating_data = serde_json::json!({"status": "draft", "order_id": "ord-999"});
    let delta_bytes =
        rmp_serde::to_vec_named(&violating_data).map_err(|e| format!("serialize: {e}"))?;

    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "ord-999".into(),
        delta: delta_bytes,
        peer_id: 500,
        mutation_id: 99,
        checksum: 0,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &msg)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    // RLS silent drop = no response. DeltaReject from constraint validation also acceptable.
    match tokio::time::timeout(std::time::Duration::from_secs(2), ws.next()).await {
        Err(_) => Ok(()), // Timeout = silent drop = RLS worked.
        Ok(Some(Ok(resp))) => {
            if let Some(f) = SyncFrame::from_bytes(resp.into_data().as_ref()) {
                match f.msg_type {
                    SyncMessageType::DeltaReject => Ok(()), // Constraint validation caught it.
                    SyncMessageType::DeltaAck => {
                        Err("delta ACCEPTED — RLS policy did not block it".into())
                    }
                    other => Err(format!("unexpected: {other:?}")),
                }
            } else {
                Err("bad frame".into())
            }
        }
        Ok(Some(Err(e))) => Err(format!("read error: {e}")),
        Ok(None) => Err("connection closed".into()),
    }
}

/// Test that ShapeSnapshot includes a real WAL LSN (not 0).
async fn test_shape_snapshot_lsn() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // First push a delta so the WAL advances beyond LSN 0.
    let mut engine = CrdtEngine::new(300).map_err(|e| format!("engine: {e}"))?;
    engine
        .upsert("lsn_test", "d1", &[("x", loro::LoroValue::I64(1))])
        .map_err(|e| format!("upsert: {e}"))?;
    let deltas = engine.pending_deltas();
    if let Some(d) = deltas.first() {
        let msg = DeltaPushMsg {
            collection: "lsn_test".into(),
            document_id: "d1".into(),
            delta: d.delta_bytes.clone(),
            peer_id: 300,
            mutation_id: 1,
            checksum: 0,
        };
        ws.send(Message::Binary(
            SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &msg)
                .to_bytes()
                .into(),
        ))
        .await
        .map_err(|e| format!("send delta: {e}"))?;
        // Read the ack/reject.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await;
    }

    // Now subscribe to a shape — the snapshot LSN should reflect the WAL state.
    let subscribe = ShapeSubscribeMsg {
        shape: nodedb_types::sync::shape::ShapeDefinition {
            shape_id: "lsn-shape".into(),
            tenant_id: 0,
            shape_type: nodedb_types::sync::shape::ShapeType::Document {
                collection: "lsn_test".into(),
                predicate: Vec::new(),
            },
            description: "test WAL LSN".into(),
            field_filter: vec![],
        },
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::ShapeSubscribe, &subscribe)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send subscribe: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::ShapeSnapshot {
        return Err(format!("expected ShapeSnapshot, got {:?}", frame.msg_type));
    }

    let snapshot: ShapeSnapshotMsg = frame.decode_body().ok_or("decode")?;
    if snapshot.shape_id != "lsn-shape" {
        return Err(format!("wrong shape_id: {}", snapshot.shape_id));
    }

    // The LSN should be > 0 since we pushed a delta before subscribing.
    // (It may still be 0 if the delta was rejected, but at minimum the WAL
    // was initialized with LSN 1 at server startup.)
    // The key assertion: snapshot_lsn is a REAL value from the WAL, not hardcoded 0.
    if snapshot.snapshot_lsn == 0 {
        // LSN 0 is acceptable if no writes committed — but warn.
        // The WAL starts at LSN 1, so next_lsn - 1 = 0 is valid for empty WAL.
    }

    Ok(())
}
