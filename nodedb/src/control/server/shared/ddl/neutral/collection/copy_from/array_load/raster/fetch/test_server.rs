// SPDX-License-Identifier: BUSL-1.1

//! A local HTTP server for range-fetch tests: it serves one object with
//! byte ranges and a strong ETag, or ignores ranges, or redirects.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

/// How the server answers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `206` with `Content-Range` and `ETag`, honouring `If-Match`.
    Ranges,
    /// `200` with the whole object, whatever the `Range`.
    IgnoreRange,
    /// `302` to `Location`.
    Redirect,
}

/// The served object and what the server saw.
pub struct Served {
    pub body: Mutex<Vec<u8>>,
    pub etag: Mutex<String>,
    pub requests: AtomicUsize,
    pub if_match_seen: AtomicUsize,
    pub mode: Mode,
    pub location: Mutex<String>,
}

impl Served {
    /// Replace the object and its ETag, as an upload mid-load does.
    pub fn replace(&self, body: Vec<u8>, etag: &str) {
        *self.body.lock().unwrap_or_else(|p| p.into_inner()) = body;
        *self.etag.lock().unwrap_or_else(|p| p.into_inner()) = etag.to_string();
    }
}

/// Serve `body` at `/scene.tif`, and at `/bucket/scene.tif` for a path-style
/// S3 read, on a free loopback port.
pub async fn serve(body: Vec<u8>, mode: Mode) -> (SocketAddr, Arc<Served>) {
    let served = Arc::new(Served {
        body: Mutex::new(body),
        etag: Mutex::new("\"v1\"".to_string()),
        requests: AtomicUsize::new(0),
        if_match_seen: AtomicUsize::new(0),
        mode,
        location: Mutex::new(String::new()),
    });
    let app = Router::new()
        .route("/scene.tif", get(answer))
        .route("/bucket/scene.tif", get(answer))
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

async fn answer(State(served): State<Arc<Served>>, headers: HeaderMap) -> Response {
    served.requests.fetch_add(1, Ordering::SeqCst);
    let body = served
        .body
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let etag = served
        .etag
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    match served.mode {
        Mode::IgnoreRange => return (StatusCode::OK, body).into_response(),
        Mode::Redirect => {
            let location = served
                .location
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            return (StatusCode::FOUND, [(header::LOCATION, location)]).into_response();
        }
        Mode::Ranges => {}
    }
    if let Some(wanted) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        served.if_match_seen.fetch_add(1, Ordering::SeqCst);
        if wanted != etag {
            return StatusCode::PRECONDITION_FAILED.into_response();
        }
    }
    let total = body.len() as u64;
    let Some((first, last)) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
    else {
        return (StatusCode::OK, body).into_response();
    };
    if first >= total {
        return (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{total}"))],
        )
            .into_response();
    }
    let last = last.min(total - 1);
    let slice = body[first as usize..=last as usize].to_vec();
    (
        StatusCode::PARTIAL_CONTENT,
        [
            (
                header::CONTENT_RANGE,
                format!("bytes {first}-{last}/{total}"),
            ),
            (header::ETAG, etag),
        ],
        slice,
    )
        .into_response()
}
