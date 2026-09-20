// SPDX-License-Identifier: BUSL-1.1

//! Byte-identity characterization for the protocol-neutral DDL result path.
//!
//! The pgwire entrypoint now consumes the shared `DdlResult` and re-encodes
//! it via `ddl_encode::ddl_results_to_pgwire`. The round-trip
//! `pgwire Response -> DdlResult -> pgwire Response` must reproduce the exact
//! wire shape the DDL router produced directly — in particular the
//! `RowDescription` column **type OIDs**, which `tokio_postgres`'
//! `simple_query` does not expose.
//!
//! `SHOW <...>` statements return `NoData` at extended-query `Describe`
//! time (they are dispatched at Execute time), so the extended protocol
//! never carries their result-column OIDs to a client. The OIDs are only on
//! the wire in the **simple-query** `RowDescription`. To observe them this
//! test speaks the PostgreSQL v3 wire protocol directly against the running
//! harness server: it captures each column's declared type OID and each
//! `DataRow`'s raw text bytes, then asserts they match the OIDs the DDL
//! field builders declare today.

use crate::harness::TestServer;
use crate::harness::raw_pgwire::RawPgConn;

/// PostgreSQL built-in type OIDs (stable, wire-level constants).
const OID_TEXT: u32 = 25;
const OID_INT8: u32 = 20;
const OID_INT4: u32 = 23;
const OID_INT2: u32 = 21;

/// A decoded simple-query result: the `RowDescription` columns as
/// `(name, type_oid)` pairs, plus each `DataRow`'s fields as raw text
/// (`None` for a SQL NULL / -1 length).
struct RawResult {
    columns: Vec<(String, u32)>,
    rows: Vec<Vec<Option<String>>>,
}

/// Read a NUL-terminated string starting at `*pos` in `body`, advancing
/// `*pos` past the terminator.
fn read_cstr(body: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while body[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&body[start..*pos]).into_owned();
    *pos += 1; // skip NUL
    s
}

fn read_i16(body: &[u8], pos: &mut usize) -> i16 {
    let v = i16::from_be_bytes([body[*pos], body[*pos + 1]]);
    *pos += 2;
    v
}

fn read_i32(body: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes([body[*pos], body[*pos + 1], body[*pos + 2], body[*pos + 3]]);
    *pos += 4;
    v
}

/// Open a fresh raw connection, complete a trust-mode startup, run one
/// simple query, and decode its `RowDescription` + `DataRow`s.
async fn raw_simple_query(port: u16, sql: &str) -> RawResult {
    let mut conn = RawPgConn::connect(port, "nodedb", "default").await;
    let messages = conn.simple_query(sql).await;

    let mut columns: Vec<(String, u32)> = Vec::new();
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();

    for (tag, body) in messages {
        match tag {
            b'T' => {
                // RowDescription: i16 field count, then per field:
                // name(cstr) table_oid(i32) col(i16) type_oid(i32)
                // type_len(i16) type_mod(i32) format(i16).
                let mut pos = 0usize;
                let n = read_i16(&body, &mut pos);
                for _ in 0..n {
                    let name = read_cstr(&body, &mut pos);
                    let _table_oid = read_i32(&body, &mut pos);
                    let _col = read_i16(&body, &mut pos);
                    let type_oid = read_i32(&body, &mut pos) as u32;
                    let _type_len = read_i16(&body, &mut pos);
                    let _type_mod = read_i32(&body, &mut pos);
                    let _format = read_i16(&body, &mut pos);
                    columns.push((name, type_oid));
                }
            }
            b'D' => {
                // DataRow: i16 field count, then per field i32 len (-1 =
                // NULL) + `len` bytes of text.
                let mut pos = 0usize;
                let n = read_i16(&body, &mut pos);
                let mut row = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let flen = read_i32(&body, &mut pos);
                    if flen < 0 {
                        row.push(None);
                    } else {
                        let flen = flen as usize;
                        let s = String::from_utf8_lossy(&body[pos..pos + flen]).into_owned();
                        pos += flen;
                        row.push(Some(s));
                    }
                }
                rows.push(row);
            }
            _ => {}
        }
    }

    RawResult { columns, rows }
}

/// An all-`TEXT`-column administrative SHOW: `SHOW SEQUENCES` returns
/// `name`, `current_value`, `called` — every column declared `text_field`.
/// The round-trip through `DdlResult` must preserve all three as `Type::TEXT`
/// (OID 25) and surface the created sequence's row verbatim.
#[tokio::test]
async fn show_sequences_row_description_is_all_text() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE ddl_rt_seq")
        .await
        .expect("CREATE SEQUENCE must succeed");

    let res = raw_simple_query(server.pg_port, "SHOW SEQUENCES").await;

    let expected = vec![
        ("name".to_string(), OID_TEXT),
        ("current_value".to_string(), OID_TEXT),
        ("called".to_string(), OID_TEXT),
    ];
    assert_eq!(
        res.columns, expected,
        "SHOW SEQUENCES RowDescription (name, OID) must round-trip byte-identically"
    );

    assert!(
        res.rows.iter().any(|r| r
            .first()
            .and_then(|c| c.as_deref())
            .map(|name| name == "ddl_rt_seq")
            .unwrap_or(false)),
        "SHOW SEQUENCES must surface the created sequence row: {:?}",
        res.rows
    );
}

/// A SHOW with a typed non-text column: `SHOW COLLECTIONS` declares
/// `name`(text), `owner`(text), `created_at`(int8), `partition_strategy`(text).
/// The `created_at` INT8 OID (20) is the load-bearing assertion — it proves
/// the neutral `DdlColType::Int8` round-trips back to `Type::INT8`, not a
/// text fallback.
#[tokio::test]
async fn show_collections_row_description_preserves_int8_column() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ddl_rt_coll (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION must succeed");

    let res = raw_simple_query(server.pg_port, "SHOW COLLECTIONS").await;

    let expected = vec![
        ("name".to_string(), OID_TEXT),
        ("owner".to_string(), OID_TEXT),
        ("created_at".to_string(), OID_INT8),
        ("partition_strategy".to_string(), OID_TEXT),
    ];
    assert_eq!(
        res.columns, expected,
        "SHOW COLLECTIONS RowDescription (name, OID) must round-trip byte-identically, \
         including the INT8 `created_at` column"
    );

    // The created collection's row must be present, with a non-NULL,
    // integer-parseable `created_at` (column index 2) proving the INT8
    // text value survived the round-trip unchanged.
    let row = res
        .rows
        .iter()
        .find(|r| {
            r.first()
                .and_then(|c| c.as_deref())
                .map(|name| name == "ddl_rt_coll")
                .unwrap_or(false)
        })
        .unwrap_or_else(|| {
            panic!(
                "SHOW COLLECTIONS must list the created collection: {:?}",
                res.rows
            )
        });

    let created_at = row
        .get(2)
        .and_then(|c| c.as_deref())
        .expect("created_at must be non-NULL");
    assert!(
        created_at.parse::<i64>().is_ok(),
        "created_at INT8 value must round-trip as a decimal integer, got {created_at:?}"
    );
}

/// Integer OID wire fidelity repro. A schemaless
/// `CREATE COLLECTION` (no `WITH (engine=...)` clause) declaring every
/// PostgreSQL integer width — `INT`, `INTEGER`, `INT4`, `BIGINT`, `INT8`,
/// `SMALLINT`, `INT2` — must advertise each column's *own* wire OID in
/// `RowDescription`: 23 (int4) for the 4-byte aliases, 20 (int8) for the
/// 8-byte aliases, 21 (int2) for the 2-byte aliases. Every declared width
/// must keep its own wire OID rather than collapsing to
/// `SqlDataType::Int64`'s single mapping —
/// `INT`/`INTEGER`/`INT4`/`BIGINT`/`INT8` all rendering as OID 20 (int8), and
/// `SMALLINT`/`INT2` (unlisted in the type-string parser) falling through to
/// the `String` default and rendering as OID 25 (text) instead of an integer
/// OID at all.
#[tokio::test]
async fn create_collection_int_widths_preserve_wire_oids() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION fixprobe_ints (\
                id TEXT PRIMARY KEY, \
                a INT, \
                b INTEGER, \
                c INT4, \
                d BIGINT, \
                e INT8, \
                f SMALLINT, \
                g INT2\
             )",
        )
        .await
        .expect("CREATE COLLECTION fixprobe_ints must succeed");
    server
        .exec(
            "INSERT INTO fixprobe_ints (id, a, b, c, d, e, f, g) \
             VALUES ('r1', 1, 2, 3, 4, 5, 6, 7)",
        )
        .await
        .expect("INSERT fixprobe_ints must succeed");

    let res = raw_simple_query(
        server.pg_port,
        "SELECT id, a, b, c, d, e, f, g FROM fixprobe_ints",
    )
    .await;

    let expected = vec![
        ("id".to_string(), OID_TEXT),
        ("a".to_string(), OID_INT4),
        ("b".to_string(), OID_INT4),
        ("c".to_string(), OID_INT4),
        ("d".to_string(), OID_INT8),
        ("e".to_string(), OID_INT8),
        ("f".to_string(), OID_INT2),
        ("g".to_string(), OID_INT2),
    ];
    assert_eq!(
        res.columns, expected,
        "fixprobe_ints RowDescription (name, OID) must preserve each declared \
         integer width — INT/INTEGER/INT4 -> int4 (23), BIGINT/INT8 -> int8 (20), \
         SMALLINT/INT2 -> int2 (21)"
    );

    let row = res
        .rows
        .first()
        .unwrap_or_else(|| panic!("expected 1 row, got: {:?}", res.rows));
    assert_eq!(
        row,
        &vec![
            Some("r1".to_string()),
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
            Some("4".to_string()),
            Some("5".to_string()),
            Some("6".to_string()),
            Some("7".to_string()),
        ],
        "fixprobe_ints row values must round-trip unchanged"
    );
}
