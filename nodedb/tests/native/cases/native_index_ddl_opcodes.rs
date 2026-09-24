// SPDX-License-Identifier: BUSL-1.1

//! Native index-DDL opcodes, end to end over the native wire.
//!
//! Each opcode runs the SQL DDL it names, so the index it makes is a catalog
//! index: `SHOW INDEXES` lists it, and inside an explicit transaction it is
//! visible to later statements and gone after ROLLBACK. Covers
//! `KvRegisterSortedIndex`, `KvDropSortedIndex`, `VectorSetParams`,
//! `DocumentDropIndex`, `KvRegisterIndex`, `KvDropIndex` and
//! `DocumentRegister`, in autocommit and inside a block.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// One native session with its own request sequence.
struct Client {
    stream: TcpStream,
    seq: u64,
}

impl Client {
    async fn connect(server: &NativeTestServer) -> Self {
        let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
            .await
            .expect("handshake");
        Self { stream, seq: 0 }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Run `sql` and require success.
    async fn sql(&mut self, sql: &str) -> NativeResponse {
        let seq = self.next_seq();
        let response = send_sql(&mut self.stream, seq, sql).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{sql} must succeed: {response:?}"
        );
        response
    }

    /// Send opcode `op` and require the plain opcode success reply.
    async fn op(&mut self, op: OpCode, fields: TextFields) {
        let seq = self.next_seq();
        let response = send_request(&mut self.stream, seq, op, fields).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{op:?} must succeed: {response:?}"
        );
        assert_eq!(
            (response.rows_affected, response.command.as_deref()),
            (None, None),
            "{op:?} answers like an opcode, with no count and no verb"
        );
    }

    /// Send read opcode `op` and require success.
    async fn read(&mut self, op: OpCode, fields: TextFields) -> NativeResponse {
        let seq = self.next_seq();
        let response = send_request(&mut self.stream, seq, op, fields).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{op:?} must succeed: {response:?}"
        );
        response
    }

    /// Every index name `SHOW INDEXES` lists to this session.
    async fn indexes(&mut self) -> Vec<String> {
        let response = self.sql("SHOW INDEXES").await;
        response
            .rows
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .filter_map(|cell| match cell {
                Value::String(text) => Some(text),
                _ => None,
            })
            .collect()
    }

    async fn lists(&mut self, index: &str) -> bool {
        self.indexes().await.iter().any(|name| name == index)
    }
}

fn sorted_index(collection: &str, name: &str) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        index_name: Some(name.to_string()),
        sort_columns: Some(vec![("score".to_string(), "DESC".to_string())]),
        key_column: Some("id".to_string()),
        ..TextFields::default()
    }
}

fn index_name(name: &str) -> TextFields {
    TextFields {
        index_name: Some(name.to_string()),
        ..TextFields::default()
    }
}

fn on_field(collection: &str, field: &str) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        field: Some(field.to_string()),
        ..TextFields::default()
    }
}

async fn kv_board(client: &mut Client, name: &str) {
    client
        .sql(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, score INT) WITH (engine='kv')"
        ))
        .await;
    client
        .sql(&format!("INSERT INTO {name} {{ id: 'p1', score: 10 }}"))
        .await;
}

#[tokio::test]
async fn sorted_index_opcodes_create_and_drop_a_catalog_index() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    kv_board(&mut client, "nat_sorted").await;

    client
        .op(
            OpCode::KvRegisterSortedIndex,
            sorted_index("nat_sorted", "nat_sorted_idx"),
        )
        .await;
    assert!(client.lists("nat_sorted_idx").await);
    client.sql("SELECT SORTED_COUNT(nat_sorted_idx)").await;

    client
        .op(OpCode::KvDropSortedIndex, index_name("nat_sorted_idx"))
        .await;
    assert!(!client.lists("nat_sorted_idx").await);
    server.shutdown().await;
}

#[tokio::test]
async fn sorted_index_opcodes_roll_back_inside_a_block() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    kv_board(&mut client, "nat_sorted_rb").await;

    client.sql("BEGIN").await;
    client
        .op(
            OpCode::KvRegisterSortedIndex,
            sorted_index("nat_sorted_rb", "nat_sorted_rb_idx"),
        )
        .await;
    assert!(
        client.lists("nat_sorted_rb_idx").await,
        "the block sees the index it created"
    );
    client.sql("SELECT SORTED_COUNT(nat_sorted_rb_idx)").await;
    client
        .sql("INSERT INTO nat_sorted_rb { id: 'p2', score: 20 }")
        .await;
    let top = client
        .read(
            OpCode::KvSortedIndexTopK,
            TextFields {
                index_name: Some("nat_sorted_rb_idx".to_string()),
                top_k_count: Some(10),
                ..TextFields::default()
            },
        )
        .await;
    assert_eq!(
        top.rows.as_ref().map(Vec::len),
        Some(2),
        "the TOPK opcode ranks the base row and the staged row: {top:?}"
    );
    client.sql("ROLLBACK").await;
    assert!(!client.lists("nat_sorted_rb_idx").await);

    // The name is free again, and a rolled-back drop keeps the index.
    client
        .op(
            OpCode::KvRegisterSortedIndex,
            sorted_index("nat_sorted_rb", "nat_sorted_rb_idx"),
        )
        .await;
    client.sql("BEGIN").await;
    client
        .op(OpCode::KvDropSortedIndex, index_name("nat_sorted_rb_idx"))
        .await;
    assert!(!client.lists("nat_sorted_rb_idx").await);
    client.sql("ROLLBACK").await;
    assert!(client.lists("nat_sorted_rb_idx").await);
    client.sql("SELECT SORTED_COUNT(nat_sorted_rb_idx)").await;
    server.shutdown().await;
}

fn vector_params(collection: &str) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        vector_dim: Some(3),
        metric: Some("l2".to_string()),
        ..TextFields::default()
    }
}

#[tokio::test]
async fn vector_set_params_creates_an_index_and_rolls_back_inside_a_block() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.sql("CREATE COLLECTION nat_vec").await;
    client.sql("CREATE COLLECTION nat_vec_rb").await;

    client
        .op(OpCode::VectorSetParams, vector_params("nat_vec"))
        .await;
    assert!(client.lists("vec_nat_vec").await);

    client.sql("BEGIN").await;
    client
        .op(OpCode::VectorSetParams, vector_params("nat_vec_rb"))
        .await;
    assert!(client.lists("vec_nat_vec_rb").await);
    client.sql("ROLLBACK").await;
    assert!(!client.lists("vec_nat_vec_rb").await);
    server.shutdown().await;
}

#[tokio::test]
async fn document_drop_index_opcode_drops_and_rolls_back_inside_a_block() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    client
        .sql(
            "CREATE COLLECTION nat_doc (id TEXT PRIMARY KEY, region TEXT) \
             WITH (engine='document_schemaless')",
        )
        .await;
    client
        .sql("CREATE INDEX nat_doc_region ON nat_doc (region)")
        .await;

    client.sql("BEGIN").await;
    client
        .op(OpCode::DocumentDropIndex, on_field("nat_doc", "region"))
        .await;
    assert!(!client.lists("nat_doc_region").await);
    client.sql("ROLLBACK").await;
    assert!(client.lists("nat_doc_region").await);

    client
        .op(OpCode::DocumentDropIndex, on_field("nat_doc", "region"))
        .await;
    assert!(!client.lists("nat_doc_region").await);
    server.shutdown().await;
}

#[tokio::test]
async fn kv_index_opcodes_make_a_catalog_index() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    client
        .sql("CREATE COLLECTION nat_kv (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await;
    client
        .sql("INSERT INTO nat_kv (key, bucket) VALUES ('s1', 'A')")
        .await;

    client.sql("BEGIN").await;
    client
        .op(OpCode::KvRegisterIndex, on_field("nat_kv", "bucket"))
        .await;
    assert!(client.lists("idx_nat_kv_bucket").await);
    client.sql("ROLLBACK").await;
    assert!(!client.lists("idx_nat_kv_bucket").await);

    client
        .op(OpCode::KvRegisterIndex, on_field("nat_kv", "bucket"))
        .await;
    assert!(client.lists("idx_nat_kv_bucket").await);
    client
        .sql("INSERT INTO nat_kv (key, bucket) VALUES ('s2', 'B')")
        .await;
    let rows = client
        .sql("SELECT key FROM nat_kv WHERE bucket = 'A'")
        .await
        .rows
        .unwrap_or_default();
    assert_eq!(rows, vec![vec![Value::String("s1".to_string())]]);

    client.sql("BEGIN").await;
    client
        .op(OpCode::KvDropIndex, on_field("nat_kv", "bucket"))
        .await;
    assert!(!client.lists("idx_nat_kv_bucket").await);
    client.sql("ROLLBACK").await;
    assert!(client.lists("idx_nat_kv_bucket").await);

    client
        .op(OpCode::KvDropIndex, on_field("nat_kv", "bucket"))
        .await;
    assert!(!client.lists("idx_nat_kv_bucket").await);
    server.shutdown().await;
}

fn register(collection: &str, paths: &[&str]) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        index_paths: Some(paths.iter().map(|p| p.to_string()).collect()),
        ..TextFields::default()
    }
}

#[tokio::test]
async fn document_register_opcode_creates_the_collection_and_its_indexes() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;

    client.sql("BEGIN").await;
    client
        .op(
            OpCode::DocumentRegister,
            register("nat_reg_rb", &["region"]),
        )
        .await;
    assert!(client.lists("idx_nat_reg_rb_region").await);
    client.sql("ROLLBACK").await;
    assert!(!client.lists("idx_nat_reg_rb_region").await);

    client
        .op(
            OpCode::DocumentRegister,
            register("nat_reg", &["region", "$.status"]),
        )
        .await;
    assert!(client.lists("idx_nat_reg_region").await);
    assert!(client.lists("idx_nat_reg_status").await);
    client
        .sql("INSERT INTO nat_reg (id, region, status) VALUES ('a', 'eu', 'open')")
        .await;
    let rows = client
        .sql("SELECT id FROM nat_reg WHERE region = 'eu'")
        .await
        .rows
        .unwrap_or_default();
    assert_eq!(rows, vec![vec![Value::String("a".to_string())]]);

    // A repeated register is a no-op.
    client
        .op(
            OpCode::DocumentRegister,
            register("nat_reg", &["region", "$.status"]),
        )
        .await;
    server.shutdown().await;
}
