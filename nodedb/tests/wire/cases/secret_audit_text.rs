// SPDX-License-Identifier: BUSL-1.1

//! A statement that carries a user password records a redacted audit text.
//! The password reaches no audit record and no file the server writes, the
//! metadata log among them, also when it shares a message with other DDL.

use crate::harness::TestServer;

use super::credential_ddl::files_containing;

const CREATE_PASSWORD: &str = "pw-create-7f3a91";
const ALTER_PASSWORD: &str = "pw-alter-2c8e44";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_password_never_reaches_the_audit_log_or_disk() {
    const COLLECTION_SQL: &str = "CREATE COLLECTION pw_audit_a";
    let server = TestServer::start().await;
    server
        .client
        .simple_query(&format!(
            "{COLLECTION_SQL}; CREATE USER pw_audit_user WITH PASSWORD '{CREATE_PASSWORD}' \
             ROLE readwrite"
        ))
        .await
        .expect("two-statement message");
    server
        .exec(&format!(
            "ALTER USER pw_audit_user SET PASSWORD '{ALTER_PASSWORD}'"
        ))
        .await
        .expect("ALTER USER SET PASSWORD");

    // The applier emits the DDL audit records off its apply loop.
    let created = "CREATE USER pw_audit_user WITH PASSWORD '<redacted>' ROLE readwrite";
    let altered = "ALTER USER pw_audit_user SET PASSWORD '<redacted>'";
    let mut audit = String::new();
    for _ in 0..100 {
        audit = server
            .query_rows("SHOW AUDIT LOG")
            .await
            .expect("SHOW AUDIT LOG")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        if audit.contains(created) && audit.contains(altered) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(audit.contains(created), "{audit}");
    assert!(audit.contains(altered), "{audit}");
    for password in [CREATE_PASSWORD, ALTER_PASSWORD] {
        assert!(
            !audit.contains(password),
            "the audit log shows {password}: {audit}"
        );
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    assert!(
        !files_containing(dir.path(), COLLECTION_SQL.as_bytes()).is_empty(),
        "the metadata log holds each statement's text in plain bytes"
    );
    for password in [CREATE_PASSWORD, ALTER_PASSWORD] {
        let leaks = files_containing(dir.path(), password.as_bytes());
        assert!(leaks.is_empty(), "{password} is on disk in {leaks:?}");
    }
}
