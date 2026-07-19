// SPDX-License-Identifier: BUSL-1.1

//! Wire surfaces that announce a server version must source the value from
//! `crate::version::VERSION`, never an embedded literal. Format conventions
//! differ per protocol: pgwire uses `"15.0 (NodeDB X)"`, native uses
//! `"NodeDB/X"`, and RESP uses bare `X` after `nodedb_version:`.

mod common;

use std::collections::HashMap;

use common::pgwire_harness::TestServer;
use nodedb::version::VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn expected_pg_server_version() -> String {
    format!(
        "{} (NodeDB {VERSION})",
        nodedb_types::pg_compat::PG_COMPAT_VERSION
    )
}

async fn startup_parameters(port: u16) -> HashMap<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect to pgwire port");

    let mut fields = b"user\0nodedb\0database\0nodedb\0\0".to_vec();
    let len = 8 + fields.len();
    let mut startup = Vec::with_capacity(len);
    startup.extend_from_slice(&(len as i32).to_be_bytes());
    startup.extend_from_slice(&196_608i32.to_be_bytes());
    startup.append(&mut fields);
    stream.write_all(&startup).await.expect("send startup");

    let mut parameters = HashMap::new();
    loop {
        let mut tag = [0u8; 1];
        stream.read_exact(&mut tag).await.expect("read message tag");
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .await
            .expect("read message length");
        let message_len = i32::from_be_bytes(len);
        assert!(message_len >= 4, "invalid backend message length");
        let mut body = vec![0u8; message_len as usize - 4];
        stream
            .read_exact(&mut body)
            .await
            .expect("read message body");

        match tag[0] {
            b'S' => {
                assert_eq!(body.last(), Some(&0), "unterminated ParameterStatus");
                let payload = &body[..body.len() - 1];
                let separator = payload
                    .iter()
                    .position(|byte| *byte == 0)
                    .expect("ParameterStatus name terminator");
                let name = &payload[..separator];
                let value = &payload[separator + 1..];
                assert!(
                    !value.contains(&0),
                    "ParameterStatus contains trailing fields"
                );
                parameters.insert(
                    String::from_utf8(name.to_vec()).expect("UTF-8 parameter name"),
                    String::from_utf8(value.to_vec()).expect("UTF-8 parameter value"),
                );
            }
            b'Z' => return parameters,
            b'E' => panic!("startup error: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

#[tokio::test]
async fn pgwire_startup_server_version_is_libpq_parseable() {
    let srv = TestServer::start().await;
    let parameters = startup_parameters(srv.pg_port).await;

    assert_eq!(
        parameters.get("server_version"),
        Some(&expected_pg_server_version()),
        "libpq parses a leading numeric version from server_version: {parameters:?}"
    );
}

#[tokio::test]
async fn pgwire_show_server_version_is_libpq_parseable() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SHOW server_version").await.unwrap();
    assert_eq!(rows, vec![expected_pg_server_version()]);
}

#[tokio::test]
async fn pgwire_version_function_returns_postgres_compatible_string() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SELECT version()").await.unwrap();
    assert_eq!(
        rows,
        vec![nodedb_types::pg_compat::version_string()],
        "version() must use the canonical PostgreSQL-compatible string"
    );
}

#[tokio::test]
async fn pgwire_show_server_version_num_returns_pg_compat_number() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SHOW server_version_num").await.unwrap();
    assert_eq!(
        rows,
        vec![nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM],
        "SHOW must advertise the canonical PostgreSQL compatibility number"
    );
}

#[tokio::test]
async fn pgwire_current_setting_server_version_num() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT current_setting('server_version_num')")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM],
        "current_setting must advertise the canonical compatibility number"
    );
}

#[tokio::test]
async fn pgwire_current_setting_server_version_is_libpq_parseable() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT current_setting('server_version')")
        .await
        .unwrap();
    assert_eq!(rows, vec![expected_pg_server_version()]);
}

#[tokio::test]
async fn pgwire_show_all_server_version_is_libpq_parseable() {
    let srv = TestServer::start().await;
    let rows = srv.query_rows("SHOW ALL").await.unwrap();
    let version = rows
        .iter()
        .find(|row| row.first().map(String::as_str) == Some("server_version"))
        .expect("SHOW ALL must include server_version");

    assert_eq!(
        version.get(1),
        Some(&expected_pg_server_version()),
        "SHOW ALL must expose the same parseable server_version: {rows:?}"
    );
}

#[tokio::test]
async fn pgwire_show_all_server_version_stays_canonical_after_reset_attempt() {
    let srv = TestServer::start().await;

    let _reset_result = srv.query_text("RESET server_version").await;

    let rows = srv.query_rows("SHOW ALL").await.unwrap();
    let version = rows
        .iter()
        .find(|row| row.first().map(String::as_str) == Some("server_version"))
        .expect("SHOW ALL must include server_version");
    assert_eq!(version.get(1), Some(&expected_pg_server_version()));
}

#[tokio::test]
async fn pgwire_current_setting_unknown_missing_ok_true_is_null() {
    let srv = TestServer::start().await;
    // missing_ok = true → NULL (empty text over the wire), not an error.
    let rows = srv
        .query_text("SELECT current_setting('nodedb.does_not_exist', true)")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got {rows:?}");
}

/// No file under `src/control/server/` may embed digits directly inside a
/// `"NodeDB ..."`, `"NodeDB/..."`, or `nodedb_version:...` literal — every
/// wire-surface version must format `crate::version::VERSION` in.
#[test]
fn no_hardcoded_version_literal_in_server_wire_surfaces() {
    use std::path::PathBuf;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("control")
        .join("server");

    let patterns: &[&str] = &[
        r#""NodeDB "#,
        r#""NodeDB/"#,
        r#""nodedb_version:"#,
        r#"nodedb_version:"#,
    ];

    let mut offenders: Vec<String> = Vec::new();
    walk_rs(&root, &mut |path, contents| {
        for (lineno, line) in contents.lines().enumerate() {
            for pat in patterns {
                if let Some(idx) = line.find(pat) {
                    let after = &line[idx + pat.len()..];
                    if after.starts_with(|c: char| c.is_ascii_digit()) {
                        offenders.push(format!(
                            "{}:{}: {}",
                            path.display(),
                            lineno + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
    });

    assert!(
        offenders.is_empty(),
        "wire-surface version literal must source from `crate::version::VERSION`:\n  {}",
        offenders.join("\n  ")
    );
}

fn walk_rs(dir: &std::path::Path, f: &mut impl FnMut(&std::path::Path, &str)) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, f);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let contents = std::fs::read_to_string(&path).unwrap();
            f(&path, &contents);
        }
    }
}
