//! Integration coverage for UTF-8 correctness in expression parsing.
//!
//! The expression tokenizer in `nodedb-query` iterates byte-by-byte but
//! slices `&str` — panicking on multi-byte UTF-8 codepoints. These tests
//! verify that non-ASCII characters in generated columns, check constraints,
//! and typeguard expressions do not cause panics or data corruption.

mod common;

use common::pgwire_harness::TestServer;

/// A GENERATED ALWAYS AS expression containing a CJK literal must not panic.
/// The tokenizer slices `&input[i..i+2]` which crosses a char boundary on
/// 3-byte UTF-8 codepoints.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_column_with_cjk_literal_does_not_panic() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE COLLECTION utf_gen TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                x TEXT, \
                y TEXT GENERATED ALWAYS AS ('你' || x))",
        )
        .await;
    assert!(
        result.is_ok(),
        "CJK literal in GENERATED expression should not panic: {:?}",
        result
    );

    server
        .exec("INSERT INTO utf_gen (id, x) VALUES ('u1', 'hello')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT y FROM utf_gen WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("你hello"),
        "generated column should concatenate CJK prefix: got {:?}",
        rows[0]
    );
}

/// Emoji (4-byte UTF-8) in an expression must not panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_column_with_emoji_does_not_panic() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE COLLECTION utf_emoji TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                tag TEXT, \
                display TEXT GENERATED ALWAYS AS ('🎉' || tag))",
        )
        .await;
    assert!(
        result.is_ok(),
        "emoji in GENERATED expression should not panic: {:?}",
        result
    );
}

/// Multi-byte characters followed by operators (`<=`, `>=`, `!=`, `||`)
/// are the specific trigger for the `&input[i..i+2]` boundary panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_with_utf8_near_operator() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION utf_ck TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                name TEXT)",
        )
        .await
        .unwrap();

    // Check constraint with a multi-byte literal adjacent to `!=`
    let result = server
        .exec(
            "ALTER COLLECTION utf_ck ADD CONSTRAINT no_forbidden \
             CHECK (NEW.name != '禁止')",
        )
        .await;
    assert!(
        result.is_ok(),
        "CHECK with UTF-8 literal near != operator should not panic: {:?}",
        result
    );

    // Valid insert should pass the constraint.
    server
        .exec("INSERT INTO utf_ck (id, name) VALUES ('c1', 'allowed')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM utf_ck WHERE id = 'c1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

/// Latin diacritics (2-byte UTF-8) in expression strings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_column_with_latin_diacritics() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE COLLECTION utf_lat TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                city TEXT, \
                greeting TEXT GENERATED ALWAYS AS ('café in ' || city))",
        )
        .await;
    assert!(
        result.is_ok(),
        "Latin diacritics in GENERATED expression should not panic: {:?}",
        result
    );

    server
        .exec("INSERT INTO utf_lat (id, city) VALUES ('l1', 'Paris')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT greeting FROM utf_lat WHERE id = 'l1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("café in Paris"),
        "generated column should preserve diacritics: got {:?}",
        rows[0]
    );
}
