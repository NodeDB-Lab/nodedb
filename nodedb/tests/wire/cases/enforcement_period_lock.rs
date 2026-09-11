// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the PERIOD LOCK constraint across every DML shape
//! that can rewrite a row: `UPDATE` (point), `MERGE`, `INSERT ... SELECT`,
//! `BulkUpdate`, and `BulkDelete`.
//!
//! A period lock refuses a write whose row names a period (via
//! `config.period_column`) whose reference-table status is not in the
//! declared allowed set. Every collection here uses `document_schemaless`
//! so its stored rows stay in MessagePack, matching the format the
//! Data-Plane period-lock check reads.

use crate::harness::TestServer;

/// Declare a reference table and a period-locked entries collection, joined
/// on `fiscal_period` / `period_key`, allowing writes only in status `OPEN`.
async fn setup(server: &TestServer, periods: &str, entries: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {periods} (period_key TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER COLLECTION {entries} ADD PERIOD LOCK ON fiscal_period \
             REFERENCES {periods}(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')"
        ))
        .await
        .unwrap();
}

async fn seed_period(server: &TestServer, periods: &str, key: &str, status: &str) {
    server
        .exec(&format!(
            "INSERT INTO {periods} (period_key, status) VALUES ('{key}', '{status}')"
        ))
        .await
        .unwrap();
}

fn assert_period_locked(result: &Result<(), String>) {
    let message = match result {
        Ok(()) => panic!("expected the write to be refused as a period lock violation"),
        Err(message) => message,
    };
    assert!(
        message.to_lowercase().contains("period locked"),
        "expected a period-locked refusal, got: {message}"
    );
}

/// A misconfigured `status_column` refuses as a config error — distinct from
/// both a locked period and an admitted write.
fn assert_period_lock_misconfigured(result: &Result<(), String>) {
    let message = match result {
        Ok(()) => panic!("expected the write to be refused as a period-lock config error"),
        Err(message) => message,
    };
    let lower = message.to_lowercase();
    assert!(
        lower.contains("misconfigured"),
        "expected a period-lock misconfiguration refusal, got: {message}"
    );
    assert!(
        !lower.contains("period locked"),
        "a misconfigured status column must not be reported as a locked period, got: {message}"
    );
}

/// A point `UPDATE` that assigns the period column into a CLOSED period is
/// refused, even though the row started in an OPEN one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_moving_a_row_into_a_locked_period_is_refused() {
    let server = TestServer::start().await;
    setup(&server, "plk_upd_ref_periods", "plk_upd_ref_entries").await;
    seed_period(&server, "plk_upd_ref_periods", "P-OPEN", "OPEN").await;
    seed_period(&server, "plk_upd_ref_periods", "P-CLOSED", "CLOSED").await;

    server
        .exec("INSERT INTO plk_upd_ref_entries (id, fiscal_period, amount) VALUES ('e1', 'P-OPEN', 100)")
        .await
        .expect("seeding into an open period must succeed");

    let result = server
        .exec("UPDATE plk_upd_ref_entries SET fiscal_period = 'P-CLOSED' WHERE id = 'e1'")
        .await;
    assert_period_locked(&result);

    let rows = server
        .query_text("SELECT fiscal_period FROM plk_upd_ref_entries WHERE id = 'e1'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["P-OPEN".to_string()],
        "a refused update must leave the row in its original period"
    );
}

/// A point `UPDATE` that moves a row between two OPEN periods is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_moving_a_row_into_an_open_period_is_admitted() {
    let server = TestServer::start().await;
    setup(&server, "plk_upd_ok_periods", "plk_upd_ok_entries").await;
    seed_period(&server, "plk_upd_ok_periods", "P-OPEN-1", "OPEN").await;
    seed_period(&server, "plk_upd_ok_periods", "P-OPEN-2", "OPEN").await;

    server
        .exec("INSERT INTO plk_upd_ok_entries (id, fiscal_period, amount) VALUES ('e1', 'P-OPEN-1', 100)")
        .await
        .expect("seeding into an open period must succeed");

    server
        .exec("UPDATE plk_upd_ok_entries SET fiscal_period = 'P-OPEN-2' WHERE id = 'e1'")
        .await
        .expect("moving a row between two open periods must be admitted");

    let rows = server
        .query_text("SELECT fiscal_period FROM plk_upd_ok_entries WHERE id = 'e1'")
        .await
        .unwrap();
    assert_eq!(rows, vec!["P-OPEN-2".to_string()]);
}

/// A `MERGE ... WHEN NOT MATCHED THEN INSERT` into an OPEN period is
/// admitted — the orchestrator must resolve the period-lock target before
/// it dispatches the apply pass, or the row is refused as an unknown period.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_into_an_open_period_is_admitted() {
    let server = TestServer::start().await;
    setup(&server, "plk_merge_periods", "plk_merge_entries").await;
    seed_period(&server, "plk_merge_periods", "P-OPEN", "OPEN").await;

    server
        .exec(
            "CREATE COLLECTION plk_merge_source (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO plk_merge_source (id, fiscal_period, amount) VALUES ('m1', 'P-OPEN', 50)",
        )
        .await
        .unwrap();

    server
        .exec(
            "MERGE INTO plk_merge_entries t USING plk_merge_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, fiscal_period, amount) \
             VALUES (s.id, s.fiscal_period, s.amount)",
        )
        .await
        .expect("a MERGE insert into an open period must be admitted");

    let rows = server
        .query_text("SELECT amount FROM plk_merge_entries WHERE id = 'm1'")
        .await
        .unwrap();
    assert_eq!(rows, vec!["50".to_string()]);
}

/// An `INSERT ... SELECT` into an OPEN period is admitted — the orchestrator
/// resolves the period-lock target for each paged `BatchInsert` it ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_into_an_open_period_is_admitted() {
    let server = TestServer::start().await;
    setup(&server, "plk_is_periods", "plk_is_entries").await;
    seed_period(&server, "plk_is_periods", "P-OPEN", "OPEN").await;

    server
        .exec(
            "CREATE COLLECTION plk_is_source (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO plk_is_source (id, fiscal_period, amount) VALUES ('s1', 'P-OPEN', 77)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO plk_is_entries SELECT * FROM plk_is_source")
        .await
        .expect("an INSERT ... SELECT into an open period must be admitted");

    let rows = server
        .query_text("SELECT amount FROM plk_is_entries WHERE id = 's1'")
        .await
        .unwrap();
    assert_eq!(rows, vec!["77".to_string()]);
}

/// A `BulkUpdate` (`UPDATE ... WHERE <non-key predicate>`) touching rows in
/// an OPEN period is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_touching_an_open_period_is_admitted() {
    let server = TestServer::start().await;
    setup(&server, "plk_bu_ok_periods", "plk_bu_ok_entries").await;
    seed_period(&server, "plk_bu_ok_periods", "P-OPEN", "OPEN").await;

    server
        .exec(
            "INSERT INTO plk_bu_ok_entries (id, fiscal_period, amount) VALUES ('e1', 'P-OPEN', 10)",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO plk_bu_ok_entries (id, fiscal_period, amount) VALUES ('e2', 'P-OPEN', 20)",
        )
        .await
        .unwrap();

    server
        .exec("UPDATE plk_bu_ok_entries SET amount = amount + 1 WHERE fiscal_period = 'P-OPEN'")
        .await
        .expect("a bulk update over an open period must be admitted");

    let rows = server
        .query_text("SELECT amount FROM plk_bu_ok_entries ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec!["11".to_string(), "21".to_string()]);
}

/// A `BulkUpdate` touching rows whose period was closed after they were
/// written is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_touching_a_locked_period_is_refused() {
    let server = TestServer::start().await;
    setup(&server, "plk_bu_lock_periods", "plk_bu_lock_entries").await;
    seed_period(&server, "plk_bu_lock_periods", "P-SOON-CLOSED", "OPEN").await;

    server
        .exec("INSERT INTO plk_bu_lock_entries (id, fiscal_period, amount) VALUES ('e1', 'P-SOON-CLOSED', 10)")
        .await
        .expect("seeding while the period is still open must succeed");
    server
        .exec("INSERT INTO plk_bu_lock_entries (id, fiscal_period, amount) VALUES ('e2', 'P-SOON-CLOSED', 20)")
        .await
        .unwrap();

    server
        .exec("UPDATE plk_bu_lock_periods SET status = 'CLOSED' WHERE period_key = 'P-SOON-CLOSED'")
        .await
        .expect("closing the reference period must succeed");

    let result = server
        .exec("UPDATE plk_bu_lock_entries SET amount = amount + 1 WHERE fiscal_period = 'P-SOON-CLOSED'")
        .await;
    assert_period_locked(&result);

    let rows = server
        .query_text("SELECT amount FROM plk_bu_lock_entries ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["10".to_string(), "20".to_string()],
        "a refused bulk update must leave every matched row unchanged"
    );
}

/// A `BulkDelete` removing rows from an OPEN period is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_delete_from_an_open_period_is_admitted() {
    let server = TestServer::start().await;
    setup(&server, "plk_bd_ok_periods", "plk_bd_ok_entries").await;
    seed_period(&server, "plk_bd_ok_periods", "P-OPEN", "OPEN").await;

    server
        .exec(
            "INSERT INTO plk_bd_ok_entries (id, fiscal_period, amount) VALUES ('e1', 'P-OPEN', 10)",
        )
        .await
        .unwrap();

    server
        .exec("DELETE FROM plk_bd_ok_entries WHERE fiscal_period = 'P-OPEN'")
        .await
        .expect("a bulk delete over an open period must be admitted");

    let rows = server
        .query_text("SELECT id FROM plk_bd_ok_entries")
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// A `BulkDelete` removing rows whose period was closed after they were
/// written is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_delete_from_a_locked_period_is_refused() {
    let server = TestServer::start().await;
    setup(&server, "plk_bd_lock_periods", "plk_bd_lock_entries").await;
    seed_period(&server, "plk_bd_lock_periods", "P-SOON-CLOSED", "OPEN").await;

    server
        .exec("INSERT INTO plk_bd_lock_entries (id, fiscal_period, amount) VALUES ('e1', 'P-SOON-CLOSED', 10)")
        .await
        .expect("seeding while the period is still open must succeed");

    server
        .exec("UPDATE plk_bd_lock_periods SET status = 'CLOSED' WHERE period_key = 'P-SOON-CLOSED'")
        .await
        .expect("closing the reference period must succeed");

    let result = server
        .exec("DELETE FROM plk_bd_lock_entries WHERE fiscal_period = 'P-SOON-CLOSED'")
        .await;
    assert_period_locked(&result);

    let rows = server
        .query_text("SELECT id FROM plk_bd_lock_entries")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["e1".to_string()],
        "a refused bulk delete must leave every matched row in place"
    );
}

/// A reference row that resolves but carries no `status` column at all is a
/// misconfigured `status_column`, not a locked period — a typo in the
/// configured column name must not read as every period being closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reference_row_missing_the_status_column_is_a_config_error() {
    let server = TestServer::start().await;
    setup(&server, "plk_cfg_periods", "plk_cfg_entries").await;

    // Seed the reference row through the real DDL surface, omitting `status`
    // entirely — a schemaless collection admits the row with no such field.
    server
        .exec("INSERT INTO plk_cfg_periods (period_key) VALUES ('P-NO-STATUS')")
        .await
        .expect("seeding a reference row without a status column must succeed");

    let result = server
        .exec(
            "INSERT INTO plk_cfg_entries (id, fiscal_period, amount) \
             VALUES ('e1', 'P-NO-STATUS', 10)",
        )
        .await;
    assert_period_lock_misconfigured(&result);

    let rows = server
        .query_text("SELECT id FROM plk_cfg_entries")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a write refused as misconfigured must leave no row behind"
    );
}
