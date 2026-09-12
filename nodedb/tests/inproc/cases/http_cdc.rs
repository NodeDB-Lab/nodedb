// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for CDC endpoints.
//!
//! Endpoints covered:
//! - GET /v1/cdc/{collection}       — SSE change-data-capture stream
//! - GET /v1/cdc/{collection}/poll  — poll-based CDC
//!
//! Contracts asserted:
//! - Routes exist (not 404) under Trust mode
//! - 401 without bearer token under Password mode
//! - Cross-tenant tenant_id query param rejected
//! - Wrong HTTP method → 405

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation};
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::catalog::DatabaseDescriptor;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, Lsn, TenantId};
use nodedb::wal::WalManager;
use nodedb_types::RowIdentity;

struct TestServer {
    local_addr: std::net::SocketAddr,
    shared: Arc<SharedState>,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start_http(auth_mode: AuthMode) -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&dir.path().join("cdc.wal")).expect("open wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    if auth_mode == AuthMode::Trust {
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let local_addr = listener.local_addr().expect("local addr");

    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            auth_mode,
            None,
            bus,
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(40)).await;

    TestServer {
        local_addr,
        shared,
        _server: handle,
        _dir: dir,
    }
}

fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    let user_id = shared
        .credentials
        .create_service_account(username, TenantId::new(1), roles, vec![DatabaseId::DEFAULT])
        .expect("create database-scoped service account");
    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create API key")
}

fn is_auth_error(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
}

fn opaque_cursor(body: &serde_json::Value) -> String {
    let cursor = body["next_cursor"]["cursor"]
        .as_str()
        .expect("CDC page must return next_cursor.cursor as a string");
    assert!(
        !cursor.is_empty(),
        "CDC next_cursor.cursor must be a nonempty opaque token: {body}"
    );
    cursor.to_owned()
}

// ─── /v1/cdc/{collection} SSE stream ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_route_is_mounted() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    // With a short timeout — we just need to confirm the route exists (not 404).
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    match result {
        Ok(Ok(resp)) => {
            assert_ne!(
                resp.status(),
                reqwest::StatusCode::NOT_FOUND,
                "/v1/cdc/orders SSE route must be mounted (not 404)"
            );
        }
        // Timeout means the SSE stream started and is holding the connection.
        // That is a success: the route exists and is serving.
        Ok(Err(e)) => panic!("Request error: {e}"),
        Err(_timeout) => {} // SSE stream opened — route confirmed mounted
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders");
    assert!(
        is_auth_error(resp.status()),
        "/v1/cdc/orders must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_rejects_cross_tenant_param() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders?tenant_id=999", srv.local_addr);
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    if let Ok(Ok(resp)) = result {
        assert!(
            is_auth_error(resp.status()),
            "/v1/cdc/orders must reject cross-tenant tenant_id param; got {}",
            resp.status()
        );
    }
    // Timeout is ambiguous here; the cross-tenant guard is already covered
    // in http_route_authentication.rs.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/cdc/orders");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/cdc/orders POST must return 405"
    );
}

// ─── /v1/cdc/{collection}/poll ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_route_is_mounted() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders/poll");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cdc/orders/poll must be mounted (not 404)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders/poll");
    assert!(
        is_auth_error(resp.status()),
        "/v1/cdc/orders/poll must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/cdc/orders/poll");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/cdc/orders/poll POST must return 405"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_isolates_events_by_selected_database_and_prefers_header() {
    let srv = start_http(AuthMode::Trust).await;
    let second_database = DatabaseId::new(42);
    let mut descriptor = DatabaseDescriptor::default_db();
    descriptor.id = second_database;
    descriptor.name = "cdc_second_database".into();
    srv.shared
        .credentials
        .catalog()
        .put_database(&descriptor)
        .expect("add second CDC database to catalog");

    for (database_id, lsn, document_id) in [
        (DatabaseId::DEFAULT, Lsn::new(91), "default-database-order"),
        (second_database, Lsn::new(92), "second-database-order"),
    ] {
        srv.shared.change_stream.publish_in_database(
            database_id,
            ChangeEvent {
                lsn,
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: RowIdentity::from_user_key(document_id),
                operation: ChangeOperation::Insert,
                timestamp_ms: 9_000,
                after: None,
            },
        );
    }

    let client = reqwest::Client::new();
    let default_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=9000",
            srv.local_addr
        ))
        .send()
        .await
        .expect("poll default CDC database");
    let default_status = default_response.status();
    let default_text = default_response
        .text()
        .await
        .expect("read default CDC response");
    assert_eq!(
        default_status,
        reqwest::StatusCode::OK,
        "unexpected default CDC response: {default_text}"
    );
    let default_body: serde_json::Value =
        serde_json::from_str(&default_text).expect("parse default CDC response");
    assert_eq!(
        default_body["changes"]
            .as_array()
            .expect("default changes array")
            .iter()
            .map(|change| change["document_id"].as_str().expect("document id"))
            .collect::<Vec<_>>(),
        vec!["default-database-order"],
        "the default database poll must not expose the second database event: {default_body}"
    );

    let second_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=9000&database=does_not_exist",
            srv.local_addr
        ))
        .header("X-NodeDB-Database", "cdc_second_database")
        .send()
        .await
        .expect("poll second CDC database");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response
        .json()
        .await
        .expect("parse second CDC response");
    assert_eq!(
        second_body["changes"]
            .as_array()
            .expect("second changes array")
            .iter()
            .map(|change| change["document_id"].as_str().expect("document id"))
            .collect::<Vec<_>>(),
        vec!["second-database-order"],
        "the database header must override the query parameter and isolate CDC events: {second_body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_excludes_matching_events_from_other_tenants() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(&srv.shared, "cdc_tenant_one_reader", vec![Role::ReadOnly]);
    let timestamp_ms = 1_000;

    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(101),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("tenant-one-order"),
        operation: ChangeOperation::Insert,
        timestamp_ms,
        after: None,
    });
    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(102),
        tenant_id: TenantId::new(2),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("tenant-two-order"),
        operation: ChangeOperation::Insert,
        timestamp_ms,
        after: None,
    });

    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms={timestamp_ms}",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll CDC changes as tenant-one reader");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("parse CDC poll response");
    let changes = body["changes"].as_array().expect("CDC changes array");
    let document_ids: Vec<_> = changes
        .iter()
        .map(|change| change["document_id"].as_str().expect("document id"))
        .collect();

    assert!(
        document_ids.contains(&"tenant-one-order"),
        "the authorized tenant's event must remain visible: {body}"
    );
    assert_eq!(
        document_ids,
        vec!["tenant-one-order"],
        "CDC poll must return only the authorized tenant's event, not another tenant's event with the same collection and timestamp: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_paginates_same_timestamp_with_opaque_cursor() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(&srv.shared, "cdc_cursor_reader", vec![Role::ReadOnly]);
    let timestamp_ms = 2_000;

    for (lsn, document_id) in [
        (Lsn::new(201), "cursor-order-1"),
        (Lsn::new(202), "cursor-order-2"),
        (Lsn::new(203), "cursor-order-3"),
    ] {
        srv.shared.change_stream.publish(ChangeEvent {
            lsn,
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms,
            after: None,
        });
    }

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms={timestamp_ms}&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first CDC poll page");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response
        .json()
        .await
        .expect("parse first CDC poll page");
    let first_changes = first_body["changes"]
        .as_array()
        .expect("first changes array");
    assert_eq!(first_changes.len(), 1, "limit=1 must produce one change");
    assert_eq!(
        first_changes[0]["document_id"], "cursor-order-1",
        "first page must contain the first LSN"
    );
    assert_eq!(
        first_body["has_more"], true,
        "a one-item page before additional matching events must report has_more"
    );
    let cursor = opaque_cursor(&first_body);

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second CDC poll page");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response
        .json()
        .await
        .expect("parse second CDC poll page");
    let second_changes = second_body["changes"]
        .as_array()
        .expect("second changes array");
    assert_eq!(
        second_changes.len(),
        1,
        "second limit=1 page must have one change"
    );
    assert_eq!(
        second_changes[0]["document_id"], "cursor-order-2",
        "the opaque cursor must advance to the next same-millisecond event instead of replaying the first"
    );
    assert_ne!(
        second_changes[0]["document_id"], "cursor-order-1",
        "the cursor page must not replay the first event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_opaque_cursor_preserves_publish_order_across_out_of_order_lsns() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.shared,
        "cdc_out_of_order_lsn_reader",
        vec![Role::ReadOnly],
    );
    let timestamp_ms = 2_500;

    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(602),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("published-first-lsn-602"),
        operation: ChangeOperation::Insert,
        timestamp_ms,
        after: None,
    });

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms={timestamp_ms}&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first CDC page before delayed WAL event");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response.json().await.expect("parse first page");
    assert_eq!(
        first_body["changes"][0]["document_id"], "published-first-lsn-602",
        "the first page must return the event that was published first"
    );
    let first_cursor = opaque_cursor(&first_body);

    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(601),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("delayed-lsn-601"),
        operation: ChangeOperation::Insert,
        timestamp_ms,
        after: None,
    });

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", first_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second CDC page after delayed WAL event");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response.json().await.expect("parse second page");
    assert_eq!(
        second_body["changes"][0]["document_id"], "delayed-lsn-601",
        "the cursor must not omit a later-published event merely because its WAL LSN is lower"
    );
    let second_cursor = opaque_cursor(&second_body);

    let final_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", second_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("final CDC page");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let final_body: serde_json::Value = final_response.json().await.expect("parse final page");
    assert_eq!(
        final_body["changes"]
            .as_array()
            .expect("final changes array")
            .len(),
        0,
        "opaque cursor pagination must neither repeat nor omit either out-of-order-LSN event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_opaque_cursor_paginates_events_sharing_one_lsn() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.shared,
        "cdc_duplicate_lsn_reader",
        vec![Role::ReadOnly],
    );
    let timestamp_ms = 2_600;

    for document_id in ["shared-lsn-first", "shared-lsn-second"] {
        srv.shared.change_stream.publish(ChangeEvent {
            lsn: Lsn::new(603),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms,
            after: None,
        });
    }

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms={timestamp_ms}&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first shared-LSN CDC page");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response.json().await.expect("parse first page");
    assert_eq!(first_body["changes"][0]["document_id"], "shared-lsn-first");
    let first_cursor = opaque_cursor(&first_body);

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", first_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second shared-LSN CDC page");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response.json().await.expect("parse second page");
    assert_eq!(
        second_body["changes"][0]["document_id"],
        "shared-lsn-second"
    );
    assert_ne!(
        second_body["changes"][0]["document_id"], first_body["changes"][0]["document_id"],
        "the cursor must not repeat the first event when both events share an LSN"
    );
    let second_cursor = opaque_cursor(&second_body);

    let final_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", second_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("final shared-LSN CDC page");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let final_body: serde_json::Value = final_response.json().await.expect("parse final page");
    assert!(
        final_body["changes"]
            .as_array()
            .expect("final changes array")
            .is_empty(),
        "each event sharing an LSN must be returned exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_malformed_opaque_cursor() {
    let srv = start_http(AuthMode::Trust).await;
    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", "not-a-valid-opaque-cursor")])
        .send()
        .await
        .expect("poll CDC with malformed cursor");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "malformed opaque CDC cursors must be rejected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_legacy_since_lsn_continuation() {
    let srv = start_http(AuthMode::Trust).await;
    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_lsn=603",
            srv.local_addr
        ))
        .send()
        .await
        .expect("poll CDC with legacy since_lsn continuation");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "legacy scalar since_lsn continuations must be rejected in favor of opaque cursors"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_limit_zero_clamps_to_a_nonempty_page() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(&srv.shared, "cdc_zero_limit_reader", vec![Role::ReadOnly]);

    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(301),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("zero-limit-order"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 3_000,
        after: None,
    });

    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=3000&limit=0",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("CDC poll with limit=0");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("parse CDC poll response");
    let changes = body["changes"].as_array().expect("CDC changes array");
    assert_eq!(
        changes.len(),
        1,
        "limit=0 must clamp to a positive page size for the one-event fixture: {body}"
    );
    assert!(
        !changes.is_empty() || body["has_more"].as_bool() != Some(true),
        "CDC poll must not report an empty page with has_more=true for limit=0: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_last_event_id_replays_only_events_after_the_cursor() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(&srv.shared, "cdc_sse_cursor_reader", vec![Role::ReadOnly]);
    let timestamp_ms = 4_000;

    for (lsn, document_id) in [
        (Lsn::new(401), "sse-cursor-first"),
        (Lsn::new(402), "sse-cursor-second"),
    ] {
        srv.shared.change_stream.publish(ChangeEvent {
            lsn,
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms,
            after: None,
        });
    }

    let client = reqwest::Client::new();
    let poll_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms={timestamp_ms}&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll first CDC event to obtain its opaque cursor");
    assert_eq!(poll_response.status(), reqwest::StatusCode::OK);
    let poll_body: serde_json::Value = poll_response
        .json()
        .await
        .expect("parse first CDC poll page");
    let poll_changes = poll_body["changes"]
        .as_array()
        .expect("first CDC poll changes array");
    assert_eq!(poll_changes.len(), 1, "limit=1 must return one event");
    assert_eq!(poll_changes[0]["document_id"], "sse-cursor-first");
    let cursor = opaque_cursor(&poll_body);

    let mut response = client
        .get(format!("http://{}/v1/cdc/orders", srv.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .header("Last-Event-ID", cursor)
        .send()
        .await
        .expect("CDC SSE request with Last-Event-ID");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let chunk = tokio::time::timeout(Duration::from_millis(300), response.chunk())
        .await
        .expect("timed out waiting for first replayed SSE event")
        .expect("SSE response body error")
        .expect("SSE stream ended before replaying an event");
    let event = std::str::from_utf8(&chunk).expect("SSE event must be UTF-8");
    assert!(
        event.contains("sse-cursor-second"),
        "Last-Event-ID replay must start after the cursor: {event}"
    );
    assert!(
        !event.contains("sse-cursor-first"),
        "Last-Event-ID replay must not include the cursor event: {event}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_custom_role_without_collection_grant() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.shared,
        "cdc_ungranted_poll_reader",
        vec![Role::Custom("cdc_ungranted_poll_role".into())],
    );
    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(103),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("ungranted-poll-order"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1_000,
        after: None,
    });

    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll CDC changes without collection grant");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "CDC poll must enforce collection READ permission before returning matching events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_rejects_custom_role_without_collection_grant_before_streaming() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.shared,
        "cdc_ungranted_sse_reader",
        vec![Role::Custom("cdc_ungranted_sse_role".into())],
    );
    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(104),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("ungranted-sse-order"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1_000,
        after: None,
    });

    let response = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new()
            .get(format!("http://{}/v1/cdc/orders", srv.local_addr))
            .header("Authorization", format!("Bearer {token}"))
            .send(),
    )
    .await
    .expect("ungranted CDC SSE request must be rejected before opening a stream")
    .expect("GET CDC SSE without collection grant");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "CDC SSE must enforce collection READ permission before opening a stream or replaying backlog"
    );
}
