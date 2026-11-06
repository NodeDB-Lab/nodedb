// SPDX-License-Identifier: BUSL-1.1

//! An audited action outside the DDL statement path refuses on a failed record.
//!
//! Each test covers one path shape: refused under a failing audit WAL, nothing landed,
//! and the same action succeeds once the WAL is back.
//! The catalog chokepoints have their own test in `ddl_audit_protocols`.
//! Statement handlers whose first effect precedes the catalog are in `audit_intent_refusal_ddl`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nodedb::config::server::BackupScheduleSettings;
use nodedb::control::backup::schedule::envelope_name;
use nodedb::control::backup::schedule::marks::settled_through;
use nodedb::control::security::audit::{AuditEntry, AuditEvent, AuditWalSink, lock_audit_log};
use nodedb::event::action::{ActionContext, ActionId, ActionKey, ActionPayload, FailedAction};
use nodedb::event::scheduler::backup_job::BackupJobs;
use nodedb::event::scheduler::dispatcher::{JobDispatcher, JobDispatcherConfig};
use nodedb::types::DatabaseId;
use nodedb_test_support::pgwire_harness::TestServer;

use super::ddl_audit_protocols::RefusingSink;

pub(super) fn refuse_audit(server: &TestServer) {
    lock_audit_log(&server.shared.audit)
        .set_wal_sink(Arc::new(RefusingSink) as Arc<dyn AuditWalSink>);
}

pub(super) fn restore_audit(server: &TestServer) {
    lock_audit_log(&server.shared.audit)
        .set_wal_sink(Arc::clone(&server.shared.wal) as Arc<dyn AuditWalSink>);
}

pub(super) async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Run `sql` and check that the audit record refused it, not some other error.
pub(super) async fn assert_refused(server: &TestServer, sql: &str) {
    let refusal = server
        .exec(sql)
        .await
        .expect_err("a failed audit record refuses the action");
    assert!(
        refusal.contains("audit"),
        "{sql} must be refused by its audit record: {refusal}"
    );
}

/// The intent rows that hold `needle`. A refused record stays in the buffer until a flush.
pub(super) fn intent_rows(server: &TestServer, needle: &str) -> usize {
    server
        .shared
        .recent_audit_entries(usize::MAX, |entry| {
            entry.event == AuditEvent::ActionIntent && entry.detail.contains(needle)
        })
        .expect("read the audit log")
        .len()
}

/// A key exists only behind a durable record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_issues_no_exchanged_key() {
    let server = TestServer::start().await;
    let url = format!("http://127.0.0.1:{}/v1/auth/exchange-key", server.http_port);
    let exchange = || async {
        reqwest::Client::new()
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect("POST exchange-key")
            .status()
    };

    refuse_audit(&server);
    let refused = exchange().await;
    assert!(!refused.is_success(), "a failed record refuses: {refused}");
    assert!(
        server.shared.auth_api_keys.list_all().is_empty(),
        "a refused exchange issues no key"
    );

    restore_audit(&server);
    let issued = exchange().await;
    assert!(issued.is_success(), "the exchange works again: {issued}");
    assert_eq!(server.shared.auth_api_keys.list_all().len(), 1);
    assert!(intent_rows(&server, "JWT exchange issues") >= 1);
}

/// One dead-lettered event action of the default database.
fn dead_lettered(sql: &str) -> FailedAction {
    FailedAction {
        key: ActionKey {
            source_lsn: 1,
            source_sequence: 1,
            source_vshard: 0,
            action: ActionId::EventAction {
                event_name: "refused_event".into(),
                index: 0,
            },
        },
        payload: ActionPayload::EventAction { sql: sql.into() },
        context: ActionContext {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            collection: "dlq_src".into(),
            row_id: "row".into(),
            cascade_depth: 0,
        },
        attempts: 5,
        last_error: "timeout".into(),
    }
}

/// A refused requeue leaves its entry unresolved, so the work stays in the DLQ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_keeps_the_dead_letter_entry() {
    let server = TestServer::start().await;
    let dlq = server
        .shared
        .trigger_dlq
        .get()
        .expect("the Event Plane runs");
    let entry_id = dlq
        .lock()
        .expect("DLQ lock")
        .enqueue(dead_lettered("SELECT 1"))
        .expect("enqueue");
    let requeue = format!("REQUEUE TRIGGER DLQ {entry_id}");

    refuse_audit(&server);
    assert_refused(&server, &requeue).await;
    assert_eq!(
        dlq.lock().expect("DLQ lock").list_unresolved().len(),
        1,
        "a refused requeue leaves the entry unresolved"
    );

    restore_audit(&server);
    exec(&server, &requeue).await;
    assert!(dlq.lock().expect("DLQ lock").list_unresolved().is_empty());
}

/// A DEFINE EVENT action stays unapplied while its record fails, and its retry lands it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_holds_the_event_action_until_a_retry_lands_it() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION ev_src",
        "CREATE COLLECTION ev_log",
        "DEFINE EVENT audited_log ON ev_src WHEN INSERT \
         THEN INSERT INTO ev_log (id) VALUES ($document_id)",
    ] {
        exec(&server, sql).await;
    }

    refuse_audit(&server);
    exec(&server, "INSERT INTO ev_src (id, v) VALUES ('held', 1)").await;

    // The refused run leaves its result row, which names the failure.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let failed = server
            .shared
            .recent_audit_entries(usize::MAX, |entry| {
                entry.source == "event_trigger_failed" && entry.detail.contains("audited_log")
            })
            .expect("read the audit log");
        if !failed.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "the action never ran");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let held = server
        .query_text("SELECT id FROM ev_log WHERE id = 'held'")
        .await
        .expect("read ev_log");
    assert!(
        held.is_empty(),
        "a refused action applies nothing: {held:?}"
    );

    restore_audit(&server);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let landed = server
            .query_text("SELECT id FROM ev_log WHERE id = 'held'")
            .await
            .expect("read ev_log");
        if !landed.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the retry never landed the action"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(intent_rows(&server, "audited_log") >= 1);
}

/// An audit WAL that refuses only entries holding `needle`, and passes the rest on.
///
/// A restore's statement record lands, so the refusal comes from its tombstone record.
struct NeedleSink {
    inner: Arc<dyn AuditWalSink>,
    needle: &'static str,
    refused: AtomicBool,
}

impl AuditWalSink for NeedleSink {
    fn append_batch(&self, entries: &[AuditEntry]) -> nodedb::Result<Vec<u64>> {
        if entries
            .iter()
            .any(|entry| entry.detail.contains(self.needle))
        {
            self.refused.store(true, Ordering::SeqCst);
            return Err(nodedb::Error::Storage {
                engine: "audit_wal".into(),
                detail: "injected audit WAL error".into(),
            });
        }
        self.inner.append_batch(entries)
    }

    fn sync_through(&self, position: u64) -> nodedb::Result<()> {
        self.inner.sync_through(position)
    }

    fn truncate_through(&self, position: u64) -> nodedb::Result<()> {
        self.inner.truncate_through(position)
    }
}

/// Create `collection` with one row, back it up to `object`, and hard-delete it.
///
/// Returns once the purge's tombstone is in the catalog.
async fn backed_up_then_purged(server: &TestServer, collection: &str, object: &str) -> String {
    exec(
        server,
        &format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        server,
        &format!("INSERT INTO {collection} (id, name) VALUES ('kept_row', 'kept')"),
    )
    .await;
    let uri = server.backup_uri(object);
    exec(server, &format!("BACKUP DATABASE default TO '{uri}'")).await;
    exec(server, &format!("DROP COLLECTION {collection} PURGE")).await;

    let catalog = server.shared.credentials.catalog();
    let deadline = Instant::now() + Duration::from_secs(30);
    while catalog
        .load_wal_tombstones()
        .expect("read tombstones")
        .is_empty()
    {
        assert!(Instant::now() < deadline, "the purge left no tombstone");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    uri
}

/// A restore that brings back a hard-deleted collection refuses before its first write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_refuses_the_restore_of_a_hard_deleted_collection() {
    const COLLECTION: &str = "tomb_docs";

    let server = TestServer::start_with_backup_root().await;
    let uri = backed_up_then_purged(&server, COLLECTION, "tomb/default.ndbb").await;

    let sink = Arc::new(NeedleSink {
        inner: Arc::clone(&server.shared.wal) as Arc<dyn AuditWalSink>,
        needle: "tombstoned collection",
        refused: AtomicBool::new(false),
    });
    lock_audit_log(&server.shared.audit).set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
    let restore = format!("RESTORE DATABASE default FROM '{uri}' FORCE");
    server
        .exec(&restore)
        .await
        .expect_err("a failed tombstone record refuses the restore");
    assert!(
        sink.refused.load(Ordering::SeqCst),
        "the refusal comes from the tombstone record"
    );
    let collections = server
        .shared
        .credentials
        .catalog()
        .load_all_collections(DatabaseId::DEFAULT)
        .expect("catalog read");
    assert!(
        !collections
            .iter()
            .any(|collection| collection.name == COLLECTION && collection.is_active),
        "a refused restore brings nothing back"
    );

    restore_audit(&server);
    exec(&server, &restore).await;
    let rows = server
        .query_text(&format!("SELECT id FROM {COLLECTION}"))
        .await
        .expect("read the restored collection");
    assert!(
        format!("{rows:?}").contains("kept_row"),
        "the restore landed: {rows:?}"
    );
}

/// A dry run of that restore writes nothing, so a failed record does not refuse it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_restore_needs_no_tombstone_record() {
    let server = TestServer::start_with_backup_root().await;
    let uri = backed_up_then_purged(&server, "tomb_dry", "tomb/dry.ndbb").await;

    refuse_audit(&server);
    exec(
        &server,
        &format!("RESTORE DATABASE default FROM '{uri}' FORCE DRY RUN"),
    )
    .await;
}

/// A refused `BACKUP DATABASE` writes no object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_writes_no_backup_object() {
    const OBJECT: &str = "refused/default.ndbb";

    let server = TestServer::start_with_backup_root().await;
    exec(
        &server,
        "CREATE COLLECTION bk_docs (id TEXT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    let backup = format!("BACKUP DATABASE default TO '{}'", server.backup_uri(OBJECT));

    refuse_audit(&server);
    assert_refused(&server, &backup).await;
    assert!(!server.backup_root().join(OBJECT).exists());

    restore_audit(&server);
    exec(&server, &backup).await;
    assert!(server.backup_root().join(OBJECT).exists());
}

/// Wait until `jobs` has nothing in flight.
async fn settle(jobs: &BackupJobs) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while jobs.in_flight() > 0 {
        assert!(
            Instant::now() < deadline,
            "a scheduled backup did not finish"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn envelope_exists(dir: &Path, database: &str, minute: u64) -> bool {
    dir.join(envelope_name(database, minute * 60_000)).exists()
}

/// A refused scheduled run writes no envelope, and its minute stays due.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_writes_no_scheduled_envelope() {
    const DATABASE: &str = "sched_refused";

    let server = TestServer::start_with_backup_root().await;
    for sql in [
        format!("CREATE DATABASE {DATABASE}"),
        format!("USE DATABASE {DATABASE}"),
        "CREATE COLLECTION sched_docs (id TEXT PRIMARY KEY, total INT) \
         WITH (engine='document_strict')"
            .to_owned(),
    ] {
        exec(&server, &sql).await;
    }
    let schedule = BackupScheduleSettings {
        database: DATABASE.into(),
        target: server.backup_uri("sched"),
        cron: "* * * * *".into(),
        keep: 10,
    };
    let jobs = BackupJobs::new(std::slice::from_ref(&schedule));
    let dispatcher = JobDispatcher::new(JobDispatcherConfig {
        max_concurrent_jobs: 4,
        max_result_bytes: u64::MAX,
    });
    let history = &server.shared.job_history;
    let dir = server.backup_root().join("sched");
    let base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
        / 60;

    // The first due minute settles the schedule's mark.
    jobs.fire(&server.shared, &dispatcher, history, base * 60 + 5);
    settle(&jobs).await;
    let settled = settled_through(&server.shared, &schedule).expect("read mark");
    assert_eq!(settled, Some(base));

    refuse_audit(&server);
    let due = base + 1;
    jobs.fire(&server.shared, &dispatcher, history, due * 60 + 5);
    settle(&jobs).await;
    assert!(
        !envelope_exists(&dir, DATABASE, due),
        "a refused run writes no envelope"
    );
    assert_eq!(
        settled_through(&server.shared, &schedule).expect("read mark"),
        settled,
        "a refused run leaves its minute due"
    );

    // A refused run retries one minute later. Missed minutes collapse into the newest one.
    restore_audit(&server);
    let retry = due + 1;
    jobs.fire(&server.shared, &dispatcher, history, retry * 60);
    settle(&jobs).await;
    assert!(
        envelope_exists(&dir, DATABASE, retry),
        "the retry runs the newest due minute"
    );
    assert_eq!(
        settled_through(&server.shared, &schedule).expect("read mark"),
        Some(retry)
    );
}
