// SPDX-License-Identifier: BUSL-1.1

//! Shared setup for the permission-tree wire tests.
//!
//! The governed collection `pt_docs` holds `d1` and `d2`. The permission
//! table `pt_grants` feeds the tree. The probe user holds general read
//! access through `readwrite` and receives tree grants through the role
//! `pt_role`. A role name is the grantee because the numeric user id is
//! assigned by the catalog and the test cannot predict it.
//!
//! The probe is a non-superuser: a superuser skips the permission tree, so
//! only a non-superuser read exercises it.

use crate::harness::TestServer;

pub const PROBE_USER: &str = "pt_probe";
pub const PROBE_PASSWORD: &str = "pt-probe-password-7";

/// The read every test issues. Its text is fixed, so a repeat on one
/// connection is served from the session plan cache.
pub const SELECT_DOCS: &str = "SELECT id FROM pt_docs ORDER BY id";

/// Create the governed collection, the permission table, the tree, and the
/// probe user. No grant exists yet.
pub async fn create_tree(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION pt_docs (id TEXT PRIMARY KEY, title TEXT) \
         WITH (engine='document_strict')",
        "INSERT INTO pt_docs (id, title) VALUES ('d1', 'Doc One')",
        "INSERT INTO pt_docs (id, title) VALUES ('d2', 'Doc Two')",
        "CREATE COLLECTION pt_grants",
        "ALTER COLLECTION pt_docs SET PERMISSION_TREE = '{\
            \"resource_column\":\"id\",\
            \"graph_index\":\"pt_docs_tree\",\
            \"permission_table\":\"pt_grants\"\
         }'",
        "CREATE ROLE pt_role",
        "CREATE USER pt_probe PASSWORD 'pt-probe-password-7'",
        "GRANT ROLE readwrite TO pt_probe",
        "GRANT ROLE pt_role TO pt_probe",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Grant `pt_role` view access to `d1`.
pub async fn grant_d1(server: &TestServer) {
    server
        .exec(
            "INSERT INTO pt_grants (resource_id, grantee, level, inherited) \
             VALUES ('d1', 'pt_role', 'viewer', false)",
        )
        .await
        .unwrap_or_else(|e| panic!("grant d1: {e}"));
}

/// Revoke the grant on `d1`.
pub async fn revoke_d1(server: &TestServer) {
    server
        .exec("DELETE FROM pt_grants WHERE resource_id = 'd1' AND grantee = 'pt_role'")
        .await
        .unwrap_or_else(|e| panic!("revoke d1: {e}"));
}

/// Open a connection as the probe user.
pub async fn connect_probe(
    server: &TestServer,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .connect_as(PROBE_USER, PROBE_PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {PROBE_USER}: {e}"))
}

/// Run `sql` on `client` and return the first column of each row.
pub async fn select_ids(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            rows.push(row.get(0).unwrap_or("").to_string());
        }
    }
    rows
}
