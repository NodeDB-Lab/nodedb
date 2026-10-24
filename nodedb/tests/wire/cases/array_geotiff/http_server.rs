// SPDX-License-Identifier: BUSL-1.1

//! A loopback HTTP server that serves fixture files with byte ranges and a
//! strong ETag, optionally behind a bearer token.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

/// The served files and what the server saw.
pub struct Served {
    files: HashMap<String, Vec<u8>>,
    token: Option<String>,
    pub requests: AtomicUsize,
}

/// Serve `files` (name to bytes) at `/<name>` on a free loopback port. With
/// `token`, every request needs `Authorization: Bearer <token>`.
pub async fn serve(files: Vec<(&str, Vec<u8>)>, token: Option<&str>) -> (SocketAddr, Arc<Served>) {
    let served = Arc::new(Served {
        files: files
            .into_iter()
            .map(|(name, bytes)| (name.to_string(), bytes))
            .collect(),
        token: token.map(str::to_string),
        requests: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/{name}", get(answer))
        .with_state(Arc::clone(&served));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, served)
}

async fn answer(
    State(served): State<Arc<Served>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    served.requests.fetch_add(1, Ordering::SeqCst);
    if let Some(token) = &served.token {
        let wanted = format!("Bearer {token}");
        let given = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        if given != Some(wanted.as_str()) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let Some(body) = served.files.get(&name) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let etag = format!("\"{}-{}\"", name, body.len());
    let total = body.len() as u64;
    let Some((first, last)) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
    else {
        return (StatusCode::OK, body.clone()).into_response();
    };
    if first >= total {
        return (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{total}"))],
        )
            .into_response();
    }
    let last = last.min(total - 1);
    (
        StatusCode::PARTIAL_CONTENT,
        [
            (
                header::CONTENT_RANGE,
                format!("bytes {first}-{last}/{total}"),
            ),
            (header::ETAG, etag),
        ],
        body[first as usize..=last as usize].to_vec(),
    )
        .into_response()
}
