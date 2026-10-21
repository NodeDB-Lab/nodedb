// SPDX-License-Identifier: BUSL-1.1

//! `[observability.otlp.export]` starts the OTLP metrics exporter at boot. It
//! posts to `<endpoint>/v1/metrics` through the `[egress]` policy, which the
//! harness opens to `localhost`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::harness::TestServer;

/// Read one whole HTTP request: the head, then `Content-Length` body bytes.
/// Answering before the body is read can reset the connection under the
/// client.
async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        if let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&request[..head_end]).to_ascii_lowercase();
            let body_len = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if request.len() >= head_end + 4 + body_len {
                return request;
            }
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return request,
            Ok(n) => request.extend_from_slice(&chunk[..n]),
        }
    }
}

/// A collector that answers every request with 200 and counts the requests
/// whose path is `/v1/metrics`. Returns its port and the count.
async fn collector() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let metrics = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&metrics);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let request = read_request(&mut socket).await;
                if request.starts_with(b"POST /v1/metrics ") {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    (port, metrics)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_exporter_posts_metrics_to_the_collector() {
    let (port, metrics) = collector().await;
    let server = TestServer::start_with_otlp_collector(port).await;
    for _ in 0..100 {
        if metrics.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        metrics.load(Ordering::SeqCst) > 0,
        "the exporter posted no metrics within 10s"
    );
    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfigured_server_posts_nothing() {
    let (_port, metrics) = collector().await;
    let server = TestServer::start().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(metrics.load(Ordering::SeqCst), 0);
    server.graceful_shutdown().await;
}
