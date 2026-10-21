// SPDX-License-Identifier: BUSL-1.1

//! `[encryption.key_derivation] type = "vault"` takes the WAL key from a
//! Vault transit engine. The server boots with the Vault-supplied key, and a
//! restart unwraps the same key and reads the data it wrote.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
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

/// A Vault stub: `POST /v1/transit/decrypt/nodedb-wal` with the test token
/// answers a fixed 32-byte plaintext key. Every other request is a 403.
/// Returns its port and the count of answered decrypts.
async fn vault_stub() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let decrypts = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&decrypts);
    let plaintext = base64::engine::general_purpose::STANDARD.encode([0x5cu8; 32]);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            let plaintext = plaintext.clone();
            tokio::spawn(async move {
                let request = read_request(&mut socket).await;
                let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
                let answer = if text.starts_with("post /v1/transit/decrypt/nodedb-wal ")
                    && text.contains("x-vault-token: test-vault-token")
                {
                    seen.fetch_add(1, Ordering::SeqCst);
                    let body = format!(r#"{{"data":{{"plaintext":"{plaintext}"}}}}"#);
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                } else {
                    "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = socket.write_all(answer.as_bytes()).await;
            });
        }
    });
    (port, decrypts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vault_supplied_wal_key_boots_and_reopens_the_data() {
    let (port, decrypts) = vault_stub().await;
    let server = TestServer::start_with_vault(port).await;
    assert_eq!(
        decrypts.load(Ordering::SeqCst),
        1,
        "boot unwraps the key once"
    );
    server
        .exec("CREATE COLLECTION vault_wal")
        .await
        .expect("CREATE COLLECTION");
    for i in 0..20 {
        server
            .exec(&format!(
                "INSERT INTO vault_wal (id, note) VALUES ('row-{i}', 'sealed-{i}')"
            ))
            .await
            .expect("INSERT");
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path_with_vault(dir, port).await;
    assert_eq!(
        decrypts.load(Ordering::SeqCst),
        2,
        "the restart unwraps the key again"
    );
    let rows = server
        .query_text("SELECT note FROM vault_wal")
        .await
        .expect("read after restart");
    assert_eq!(rows.len(), 20, "{rows:?}");
}
