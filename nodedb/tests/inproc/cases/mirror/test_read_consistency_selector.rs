// SPDX-License-Identifier: BUSL-1.1

//! The `read_consistency` request field on HTTP and WebSocket RPC.
//!
//! The default stays `strong`, which a mirror refuses with a message naming the
//! field. `eventual` and a satisfied `bounded_staleness` are served. An unknown
//! level is refused on any database.

use futures::{SinkExt, StreamExt};
use nodedb_test_support::pgwire_harness::TestServer;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use super::helpers::inject_lag_record;
use super::test_read_only_protocols::{DATABASE_HEADER, SELECT, fixture};

const NAMES_THE_FIELD: &str = "read_consistency";
const UNKNOWN_LEVEL: &str = "invalid read_consistency 'sometimes'";
const HTTP_ROUTES: [&str; 2] = ["/v1/query", "/v1/query/stream"];

/// POST `SELECT` at `level` to `route` and return the response body.
async fn http_read(
    server: &TestServer,
    route: &str,
    database: &str,
    level: Option<&str>,
) -> String {
    let mut body = serde_json::json!({ "sql": SELECT });
    if let Some(level) = level {
        body["read_consistency"] = serde_json::json!(level);
    }
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}{route}", server.http_port))
        .header(DATABASE_HEADER, database)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {route}: {e}"))
        .text()
        .await
        .unwrap_or_else(|e| panic!("read {route} body: {e}"))
}

/// Send `SELECT` at `level` over WebSocket RPC and return the answer frame.
async fn ws_read(server: &TestServer, database: &str, level: Option<&str>) -> serde_json::Value {
    let mut request = format!("ws://127.0.0.1:{}/v1/ws", server.http_port)
        .into_client_request()
        .expect("WebSocket request");
    request
        .headers_mut()
        .insert(DATABASE_HEADER, database.parse().expect("header value"));
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("WebSocket connect");
    let mut params = serde_json::json!({ "sql": SELECT });
    if let Some(level) = level {
        params["read_consistency"] = serde_json::json!(level);
    }
    let frame = serde_json::json!({ "id": 1, "method": "query", "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("WebSocket send");
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), ws.next())
            .await
            .expect("an answer in time")
            .expect("the socket stays open")
            .expect("read a frame");
        let Message::Text(text) = message else {
            continue;
        };
        let answer: serde_json::Value = serde_json::from_str(&text).expect("a JSON frame");
        if answer["id"] == 1 {
            return answer;
        }
    }
}

/// A served read holds the fixture's row and no refusal.
fn assert_http_served(body: &str) {
    assert!(!body.contains(NAMES_THE_FIELD), "{body}");
    assert!(!body.contains("\"error\""), "{body}");
    assert!(body.contains("\"v\""), "{body}");
}

fn assert_ws_served(frame: &serde_json::Value) {
    assert!(frame.get("error").is_none(), "{frame}");
    assert!(frame["result"].to_string().contains("\"v\""), "{frame}");
}

fn assert_ws_refused(frame: &serde_json::Value, expected: &str) {
    assert!(frame["error"].to_string().contains(expected), "{frame}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_strong_reads_of_a_mirror_name_the_selector() {
    let (server, name) = fixture().await;
    for route in HTTP_ROUTES {
        for level in [None, Some("strong")] {
            let body = http_read(&server, route, &name, level).await;
            assert!(body.contains(NAMES_THE_FIELD), "{route} {level:?}: {body}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_eventual_reads_of_a_mirror_are_served() {
    let (server, name) = fixture().await;
    for route in HTTP_ROUTES {
        assert_http_served(&http_read(&server, route, &name, Some("eventual")).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_bounded_reads_follow_the_mirror_lag() {
    let (server, name) = fixture().await;
    for route in HTTP_ROUTES {
        let unapplied = http_read(&server, route, &name, Some("bounded_staleness:60")).await;
        assert!(unapplied.contains(NAMES_THE_FIELD), "{route}: {unapplied}");
    }
    inject_lag_record(&server, &name, 0, 1);
    for route in HTTP_ROUTES {
        assert_http_served(&http_read(&server, route, &name, Some("bounded_staleness:60")).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_refuses_an_unknown_level_on_any_database() {
    let (server, name) = fixture().await;
    for route in HTTP_ROUTES {
        for database in [name.as_str(), "default"] {
            let body = http_read(&server, route, database, Some("sometimes")).await;
            assert!(body.contains(UNKNOWN_LEVEL), "{route} {database}: {body}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_reads_follow_the_request_field() {
    let (server, name) = fixture().await;
    for level in [None, Some("strong")] {
        assert_ws_refused(&ws_read(&server, &name, level).await, NAMES_THE_FIELD);
    }
    assert_ws_served(&ws_read(&server, &name, Some("eventual")).await);
    assert_ws_refused(
        &ws_read(&server, &name, Some("bounded_staleness:60")).await,
        NAMES_THE_FIELD,
    );
    inject_lag_record(&server, &name, 0, 1);
    assert_ws_served(&ws_read(&server, &name, Some("bounded_staleness:60")).await);
    assert_ws_refused(
        &ws_read(&server, &name, Some("sometimes")).await,
        UNKNOWN_LEVEL,
    );
}
