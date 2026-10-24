// SPDX-License-Identifier: BUSL-1.1

//! `CREATE CREDENTIAL`, `DROP CREDENTIAL`, `SHOW CREDENTIALS` and a COPY
//! that names a credential.
//!
//! A credential needs a superuser. Its secret is sealed at rest, survives a
//! restart, and never appears in SHOW, DESCRIBE or the audit log. A COPY
//! from an HTTP source sends a bearer credential's token, and the source
//! refuses the COPY without it.

use std::sync::atomic::Ordering;

use crate::harness::TestServer;

use super::array_geotiff::files::{fixture_bytes, qa};
use super::array_geotiff::fixture::{HEIGHT, WIDTH};
use super::array_geotiff::http_server::serve;
use super::array_geotiff::support::{PIXEL_GEOREF, aggregate, copy_sql, create, pixel_array};

const TOKEN: &str = "eyJ-earthdata-token-0451";
const ALL_PIXELS: u64 = (WIDTH * HEIGHT) as u64;

/// Every text cell of every row `sql` returns, joined.
async fn all_text(server: &TestServer, sql: &str) -> String {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The SQLSTATE `sql` fails with.
async fn sqlstate(client: &tokio_postgres::Client, sql: &str) -> String {
    let err = client.simple_query(sql).await.expect_err("refused");
    err.as_db_error()
        .map(|db| db.code().code().to_string())
        .unwrap_or_else(|| panic!("no SQLSTATE on {err}"))
}

async fn copy_qa(server: &TestServer, array: &str, port: u16, with: &str) -> Result<u64, String> {
    let sql =
        format!("COPY {array} FROM 'http://localhost:{port}/qa.tif' WITH (FORMAT geotiff{with})");
    copy_sql(server, &sql)
        .await
        .map_err(|e| format!("{}: {}", e.code, e.message))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bearer_credential_authenticates_a_copy_and_never_shows() {
    let server = TestServer::start().await;
    let (addr, served) = serve(vec![("qa.tif", fixture_bytes("band-qa.tif"))], Some(TOKEN)).await;
    create(
        &server,
        &pixel_array("cred_qa", "0..191", "qa UINT16", PIXEL_GEOREF),
    )
    .await;

    let refused = copy_qa(&server, "cred_qa", addr.port(), "").await;
    assert!(
        refused.as_ref().is_err_and(|e| e.starts_with("28000")),
        "{refused:?}"
    );

    server
        .exec(&format!(
            "CREATE CREDENTIAL earthdata (TYPE 'bearer', TOKEN '{TOKEN}')"
        ))
        .await
        .expect("CREATE CREDENTIAL");
    let dup = sqlstate(
        &server.client,
        "CREATE CREDENTIAL earthdata (TYPE 'bearer', TOKEN 'other')",
    )
    .await;
    assert_eq!(dup, "42710");

    let loaded = copy_qa(&server, "cred_qa", addr.port(), ", CREDENTIAL earthdata")
        .await
        .expect("COPY with the credential");
    assert_eq!(loaded, ALL_PIXELS);
    assert_eq!(
        aggregate(&server, "cred_qa", "qa", "max", "").await,
        f64::from(
            (0..HEIGHT)
                .flat_map(|r| (0..WIDTH).map(move |c| qa(r, c)))
                .max()
                .unwrap_or(0)
        )
    );
    assert!(served.requests.load(Ordering::SeqCst) > 1);

    let shown = all_text(&server, "SHOW CREDENTIALS").await;
    assert!(
        shown.contains("earthdata") && shown.contains("bearer"),
        "{shown}"
    );
    for sql in ["SHOW CREDENTIALS", "SHOW AUDIT LOG", "DESCRIBE cred_qa"] {
        let text = all_text(&server, sql).await;
        assert!(!text.contains(TOKEN), "{sql} shows the token: {text}");
    }

    server
        .exec("DROP CREDENTIAL earthdata")
        .await
        .expect("DROP CREDENTIAL");
    let gone = copy_qa(&server, "cred_qa", addr.port(), ", CREDENTIAL earthdata").await;
    assert!(
        gone.as_ref().is_err_and(|e| e.starts_with("42704")),
        "{gone:?}"
    );
    assert_eq!(
        sqlstate(&server.client, "DROP CREDENTIAL earthdata").await,
        "42704"
    );
    server
        .exec("DROP CREDENTIAL IF EXISTS earthdata")
        .await
        .expect("IF EXISTS");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credential_statements_need_a_superuser() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER cred_user WITH PASSWORD 'cred-user-secret-1' ROLE readwrite")
        .await
        .expect("CREATE USER");
    server
        .exec(&format!(
            "CREATE CREDENTIAL kept (TYPE 'bearer', TOKEN '{TOKEN}')"
        ))
        .await
        .expect("CREATE CREDENTIAL");
    let (client, _task) = server
        .connect_as("cred_user", "cred-user-secret-1")
        .await
        .expect("connect as cred_user");
    for sql in [
        "CREATE CREDENTIAL mine (TYPE 'sas', TOKEN 'sv=1')",
        "DROP CREDENTIAL kept",
        "SHOW CREDENTIALS",
    ] {
        assert_eq!(sqlstate(&client, sql).await, "42501", "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_credential_survives_a_restart() {
    let server = TestServer::start().await;
    let (addr, _served) = serve(vec![("qa.tif", fixture_bytes("band-qa.tif"))], Some(TOKEN)).await;
    server
        .exec(&format!(
            "CREATE CREDENTIAL earthdata (TYPE 'bearer', TOKEN '{TOKEN}')"
        ))
        .await
        .expect("CREATE CREDENTIAL");
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let shown = all_text(&server, "SHOW CREDENTIALS").await;
    assert!(shown.contains("earthdata"), "{shown}");
    create(
        &server,
        &pixel_array("cred_restart", "0..191", "qa UINT16", PIXEL_GEOREF),
    )
    .await;
    let loaded = copy_qa(
        &server,
        "cred_restart",
        addr.port(),
        ", CREDENTIAL earthdata",
    )
    .await
    .expect("the sealed secret opens after a restart");
    assert_eq!(loaded, ALL_PIXELS);
}

/// Every file under `dir` whose bytes contain `needle`.
pub(super) fn files_containing(dir: &std::path::Path, needle: &[u8]) -> Vec<std::path::PathBuf> {
    let mut hits = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            let entries = std::fs::read_dir(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
            pending.extend(entries.map(|e| e.expect("dir entry").path()));
        } else if let Ok(bytes) = std::fs::read(&path)
            && bytes.windows(needle.len()).any(|w| w == needle)
        {
            hits.push(path);
        }
    }
    hits
}

/// A message that runs `CREATE CREDENTIAL` after another DDL statement
/// records each statement's own text. The token reaches no audit record and
/// no file the server writes, the metadata log among them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_statement_message_never_records_the_token() {
    const COLLECTION_SQL: &str = "CREATE COLLECTION cred_multi_a";
    let server = TestServer::start().await;
    server
        .client
        .simple_query(&format!(
            "{COLLECTION_SQL}; CREATE CREDENTIAL cred_multi (TYPE 'bearer', TOKEN '{TOKEN}')"
        ))
        .await
        .expect("two-statement message");

    // The applier emits the DDL audit record off its apply loop.
    let own_text = format!("\"sql_statement\":\"{COLLECTION_SQL}\"");
    let mut audit = String::new();
    for _ in 0..100 {
        audit = all_text(&server, "SHOW AUDIT LOG").await;
        if audit.contains(&own_text) && audit.contains("cred_multi (TYPE 'bearer', <redacted>)") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(audit.contains(&own_text), "{audit}");
    assert!(
        audit.contains("cred_multi (TYPE 'bearer', <redacted>)"),
        "{audit}"
    );
    assert!(
        !audit.contains(TOKEN),
        "the audit log shows the token: {audit}"
    );

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    assert!(
        !files_containing(dir.path(), COLLECTION_SQL.as_bytes()).is_empty(),
        "the metadata log holds each statement's text in plain bytes"
    );
    let leaks = files_containing(dir.path(), TOKEN.as_bytes());
    assert!(leaks.is_empty(), "the token is on disk in {leaks:?}");
}
