// SPDX-License-Identifier: BUSL-1.1

//! RESTORE verifies every collection's row count and digest.
//!
//! A backup records one verification record per collection part. A clean
//! restore into a fresh server verifies. An envelope whose data sections lose
//! rows after the backup recorded them fails before the restore writes
//! anything, and the error names every mismatched collection. A DRY RUN runs
//! the envelope check only.

use bytes::Bytes;
use futures::SinkExt;
use nodedb::types::TenantDataSnapshot;
use nodedb_types::backup_envelope::{
    CollectionVerification, DEFAULT_MAX_TOTAL_BYTES, DatabaseDataSection, EnvelopeWriter,
    SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_VERIFICATION, VerifiedPart, parse_encrypted,
};

use super::backup_support::{drain_backup, push_restore};
use crate::harness::{TEST_BACKUP_KEK, TestServer};

const TENANT: u64 = 1;

const SETUP: &[&str] = &[
    "CREATE COLLECTION vr_people (id STRING PRIMARY KEY, city STRING) \
     WITH (engine='document_schemaless')",
    "INSERT INTO vr_people (id, city) VALUES ('alice', 'paris')",
    "INSERT INTO vr_people (id, city) VALUES ('bob', 'rome')",
    "INSERT INTO vr_people (id, city) VALUES ('carol', 'paris')",
    "GRAPH INSERT EDGE IN 'vr_people' FROM 'alice' TO 'bob' TYPE 'knows'",
    "GRAPH INSERT EDGE IN 'vr_people' FROM 'bob' TO 'carol' TYPE 'knows'",
    "CREATE COLLECTION vr_accounts (id STRING PRIMARY KEY, owner STRING, balance INT) \
     WITH (engine='document_strict')",
    "INSERT INTO vr_accounts (id, owner, balance) VALUES ('acc1', 'alice', 10)",
    "INSERT INTO vr_accounts (id, owner, balance) VALUES ('acc2', 'bob', 20)",
    "CREATE COLLECTION vr_ledger (id STRING PRIMARY KEY, value STRING) \
     WITH (engine='document_strict', bitemporal=true)",
    "INSERT INTO vr_ledger (id, value) VALUES ('e1', 'draft')",
    "UPDATE vr_ledger SET value = 'final' WHERE id = 'e1'",
    "CREATE COLLECTION vr_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
    "INSERT INTO vr_kv (key, value) VALUES ('k1', 'one')",
    "INSERT INTO vr_kv (key, value) VALUES ('k2', 'two')",
    "CREATE COLLECTION vr_cols COLUMNS (id TEXT, region TEXT, ts BIGINT) \
     WITH (engine='columnar')",
    "INSERT INTO vr_cols (id, region, ts) VALUES ('c1', 'eu', 1)",
    "INSERT INTO vr_cols (id, region, ts) VALUES ('c2', 'us', 2)",
    "CREATE COLLECTION vr_metrics (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
     WITH (engine='timeseries')",
    "INSERT INTO vr_metrics (captured_at, host, v) VALUES ('2020-03-05 10:00:00', 'h1', 1.5)",
    "CREATE COLLECTION vr_vec WITH (engine='document_schemaless')",
    "CREATE INDEX ON vr_vec (embedding)",
    "INSERT INTO vr_vec { id: 'v1', embedding: [1.0, 0.0, 0.0, 0.0] }",
    "INSERT INTO vr_vec { id: 'v2', embedding: [0.0, 1.0, 0.0, 0.0] }",
];

async fn source_backup() -> Vec<u8> {
    let source = TestServer::start().await;
    for sql in SETUP {
        source
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup")
}

/// Restore `envelope` as a DRY RUN. The error carries the server's text.
async fn push_dry_run(server: &TestServer, envelope: Vec<u8>) -> Result<(), String> {
    let sink = server
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN DRY RUN"))
        .await
        .map_err(|e| format!("copy_in: {e:?}"))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| format!("send: {e:?}"))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// The verification records of `envelope`.
fn verification(envelope: &[u8]) -> Vec<CollectionVerification> {
    let env = parse_encrypted(envelope, DEFAULT_MAX_TOTAL_BYTES, &TEST_BACKUP_KEK)
        .expect("parse the envelope");
    let sections: Vec<_> = env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_VERIFICATION)
        .collect();
    assert_eq!(sections.len(), 1, "one verification section");
    zerompk::from_msgpack(&sections[0].body).expect("decode the verification section")
}

fn count(records: &[CollectionVerification], collection: &str, part: VerifiedPart) -> u64 {
    records
        .iter()
        .find(|r| r.collection == collection && r.part == part)
        .unwrap_or_else(|| panic!("no {part} record for {collection}: {records:?}"))
        .tally
        .count
}

/// Re-encrypt `envelope` with one document row of each of `collections`
/// dropped from its data sections. The verification section is unchanged.
fn drop_one_document(envelope: &[u8], collections: &[&str]) -> Vec<u8> {
    let env = parse_encrypted(envelope, DEFAULT_MAX_TOTAL_BYTES, &TEST_BACKUP_KEK)
        .expect("parse the envelope");
    let mut pending: Vec<&str> = collections.to_vec();
    let mut writer = EnvelopeWriter::new(env.meta);
    for section in env.sections {
        let mut body = section.body;
        if section.origin_node_id < SECTION_ORIGIN_CATALOG_ROWS {
            let mut data: DatabaseDataSection =
                zerompk::from_msgpack(&body).expect("decode a data section");
            let mut snap: TenantDataSnapshot =
                zerompk::from_msgpack(&data.snapshot).expect("decode a tenant snapshot");
            pending.retain(|collection| {
                let needle = format!(":{collection}:");
                match snap
                    .documents_versioned
                    .iter()
                    .position(|(k, _)| k.contains(&needle))
                {
                    Some(at) => {
                        snap.documents_versioned.remove(at);
                        false
                    }
                    None => true,
                }
            });
            data.snapshot = zerompk::to_msgpack_vec(&snap).expect("encode the snapshot");
            body = zerompk::to_msgpack_vec(&data).expect("encode the data section");
        }
        writer
            .push_section(section.origin_node_id, body)
            .expect("push a section");
    }
    assert!(pending.is_empty(), "no document row of {pending:?} found");
    writer
        .finalize_encrypted(&TEST_BACKUP_KEK)
        .expect("encrypt the envelope")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_records_every_collection_part() {
    let records = verification(&source_backup().await);
    assert_eq!(count(&records, "vr_people", VerifiedPart::Documents), 3);
    assert!(count(&records, "vr_people", VerifiedPart::Edges) >= 2);
    assert_eq!(count(&records, "vr_accounts", VerifiedPart::Documents), 2);
    assert_eq!(count(&records, "vr_ledger", VerifiedPart::Documents), 2);
    assert_eq!(count(&records, "vr_kv", VerifiedPart::KeyValue), 2);
    assert_eq!(count(&records, "vr_cols", VerifiedPart::Columnar), 2);
    assert_eq!(count(&records, "vr_metrics", VerifiedPart::Timeseries), 1);
    assert_eq!(count(&records, "vr_vec", VerifiedPart::Vectors), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_restore_verifies() {
    let backup = source_backup().await;

    let target = TestServer::start().await;
    push_dry_run(&target, backup.clone())
        .await
        .unwrap_or_else(|e| panic!("dry run: {e}"));
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    let people = target
        .query_text("SELECT id FROM vr_people")
        .await
        .expect("read the restored rows");
    assert_eq!(people.len(), 3, "{people:?}");
}

/// A restore of the same envelope over its own restored data verifies again:
/// the destination rows match the backed-up ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_restore_verifies() {
    let backup = source_backup().await;

    let target = TestServer::start().await;
    for attempt in ["first", "second"] {
        push_restore(&target.client, TENANT, backup.clone())
            .await
            .unwrap_or_else(|e| panic!("{attempt} restore: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupted_section_fails_and_names_every_collection() {
    let corrupted = drop_one_document(&source_backup().await, &["vr_people", "vr_accounts"]);

    let target = TestServer::start().await;
    let err = push_dry_run(&target, corrupted.clone())
        .await
        .expect_err("a dry run checks the envelope");
    assert!(err.contains("restore verification failed"), "{err}");

    let err = push_restore(&target.client, TENANT, corrupted)
        .await
        .expect_err("the restore must fail verification");
    assert!(err.contains("restore verification failed"), "{err}");
    assert!(err.contains("'vr_people'"), "names vr_people: {err}");
    assert!(err.contains("'vr_accounts'"), "names vr_accounts: {err}");
    assert!(err.contains("nothing was restored"), "{err}");
    assert!(
        !err.contains("'vr_kv'"),
        "an intact collection is not named: {err}"
    );

    // The envelope check runs before the first write.
    if let Ok(rows) = target.query_text("SELECT id FROM vr_people").await {
        assert!(rows.is_empty(), "nothing was restored, got {rows:?}");
    }
}
