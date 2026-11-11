// SPDX-License-Identifier: BUSL-1.1

use super::{
    holds_several_statements, split_sql_statements, split_statements, statement_position,
    statement_texts,
};

#[test]
fn a_statement_position_counts_characters_from_one() {
    let sql = "SELECT 1;  SELECT 2";
    assert_eq!(statement_position(sql, 0), Some(1));
    // Both spaces belong to the gap before the second statement, so its
    // first character is the twelfth of the request, one-based.
    assert_eq!(statement_position(sql, 1), Some(12));
    assert_eq!(statement_position(sql, 2), None);
}

#[test]
fn a_statement_position_skips_whitespace_and_counts_multibyte_characters() {
    let sql = "  SELECT 'é';\n SELECT 2";
    assert_eq!(statement_position(sql, 0), Some(3));
    assert_eq!(statement_position(sql, 1), Some(16));
}

#[test]
fn a_statement_position_skips_empty_statements() {
    assert_eq!(statement_position(";; SELECT 1", 0), Some(4));
}

#[test]
fn one_statement_with_a_trailing_semicolon_is_not_several() {
    assert!(!holds_several_statements(
        "INSERT INTO docs (id) VALUES ('a');"
    ));
    assert!(!holds_several_statements(" ; "));
    assert!(holds_several_statements("SELECT 1; SELECT 2"));
}

#[test]
fn single_statement_no_semicolon() {
    let stmts = split_sql_statements("SELECT 1");
    assert_eq!(stmts, vec!["SELECT 1"]);
}

#[test]
fn single_statement_with_semicolon() {
    let stmts = split_sql_statements("CREATE COLLECTION docs;");
    assert_eq!(stmts, vec!["CREATE COLLECTION docs"]);
}

#[test]
fn multi_statement_heredoc() {
    let sql = "CREATE COLLECTION docs;\nINSERT INTO docs (x) VALUES (1);\nSELECT * FROM docs;";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 3);
    assert_eq!(stmts[0], "CREATE COLLECTION docs");
    assert_eq!(stmts[1], "INSERT INTO docs (x) VALUES (1)");
    assert_eq!(stmts[2], "SELECT * FROM docs");
}

#[test]
fn semicolon_inside_string_not_split() {
    let sql = "INSERT INTO t (v) VALUES ('a;b');";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "INSERT INTO t (v) VALUES ('a;b')");
}

#[test]
fn empty_statements_discarded() {
    let stmts = split_sql_statements(";;  ;SELECT 1;;");
    assert_eq!(stmts, vec!["SELECT 1"]);
}

#[test]
fn procedural_begin_end_not_split() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS TEXT AS \
                BEGIN IF x > 0 THEN RETURN 'pos'; ELSE RETURN 'neg'; END IF; END";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].starts_with("CREATE FUNCTION"));
    assert!(stmts[0].ends_with("END"));
}

#[test]
fn procedural_with_trailing_semicolon() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS \
                BEGIN RETURN x; END;";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("BEGIN"));
    assert!(stmts[0].ends_with("END"));
}

#[test]
fn trigger_body_in_batch_splits_correctly() {
    let sql = "CREATE COLLECTION after_src;\n\
               CREATE COLLECTION after_log;\n\
               CREATE TRIGGER log_insert AFTER INSERT ON after_src FOR EACH ROW\n\
               BEGIN\n\
                   INSERT INTO after_log (id, src_id, action)\n\
                   VALUES (NEW.id || '_log', NEW.id, 'inserted');\n\
               END;\n\
               INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10);\n\
               SELECT * FROM after_log;";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 5);
    assert!(stmts[2].starts_with("CREATE TRIGGER log_insert"));
    assert!(stmts[2].contains("VALUES (NEW.id || '_log', NEW.id, 'inserted');"));
    assert!(stmts[2].ends_with("END"));
    assert_eq!(
        stmts[3],
        "INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10)"
    );
    assert_eq!(stmts[4], "SELECT * FROM after_log");
}

#[test]
fn trigger_body_with_if_block_remains_single_statement() {
    let sql = "CREATE TRIGGER normalize BEFORE INSERT ON users FOR EACH ROW \
               BEGIN \
                 IF NEW.active = TRUE THEN \
                   INSERT INTO audit (id) VALUES (NEW.id); \
                 END IF; \
               END; \
               SELECT 1;";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("END IF;"));
    assert!(stmts[0].ends_with("END"));
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn sync_trigger_body_in_batch_splits_correctly() {
    let sql = "CREATE SYNC TRIGGER log_insert AFTER INSERT ON after_src FOR EACH ROW \
               BEGIN \
                 INSERT INTO after_log (id) VALUES (NEW.id); \
               END; \
               SELECT 1;";
    let stmts = split_sql_statements(sql);
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].starts_with("CREATE SYNC TRIGGER log_insert"));
    assert!(stmts[0].contains("INSERT INTO after_log (id) VALUES (NEW.id);"));
    assert!(stmts[0].ends_with("END"));
    assert_eq!(stmts[1], "SELECT 1");
}

/// Each row is a request text and the statements it splits into.
const SPLIT_TABLE: &[(&str, &[&str])] = &[
    ("", &[]),
    (" ; ", &[]),
    (";;  ;", &[]),
    ("SELECT 1", &["SELECT 1"]),
    ("SELECT 1;", &["SELECT 1"]),
    ("SELECT 1; SELECT 2", &["SELECT 1", "SELECT 2"]),
    ("SELECT 1;; SELECT 2;", &["SELECT 1", "SELECT 2"]),
    (
        "INSERT INTO t (v) VALUES ('a;b'); SELECT 2",
        &["INSERT INTO t (v) VALUES ('a;b')", "SELECT 2"],
    ),
    (
        "SELECT \"a;b\" FROM t; SELECT 2",
        &["SELECT \"a;b\" FROM t", "SELECT 2"],
    ),
    ("SELECT $$a;b$$; SELECT 2", &["SELECT $$a;b$$", "SELECT 2"]),
    (
        "-- one; two\nSELECT 1; /* a; b */ SELECT 2",
        &["-- one; two\nSELECT 1", "/* a; b */ SELECT 2"],
    ),
    (
        "  BEGIN ;\n INSERT INTO t (id) VALUES ('a') ; COMMIT ",
        &["BEGIN", "INSERT INTO t (id) VALUES ('a')", "COMMIT"],
    ),
    (
        "CREATE TRIGGER t AFTER INSERT ON a FOR EACH ROW BEGIN SELECT 1; END; SELECT 2",
        &[
            "CREATE TRIGGER t AFTER INSERT ON a FOR EACH ROW BEGIN SELECT 1; END",
            "SELECT 2",
        ],
    ),
];

#[test]
fn the_carried_statements_equal_the_per_site_split() {
    for (sql, expected) in SPLIT_TABLE {
        let carried = split_statements(sql);
        assert_eq!(statement_texts(&carried), *expected, "{sql:?}");
        assert_eq!(split_sql_statements(sql), *expected, "{sql:?}");
        assert_eq!(holds_several_statements(sql), expected.len() > 1, "{sql:?}");
    }
}

#[test]
fn a_statement_starts_at_its_first_character_in_the_request_text() {
    for (sql, _) in SPLIT_TABLE {
        for statement in split_statements(sql) {
            assert!(
                sql.get(statement.start..)
                    .is_some_and(|rest| rest.starts_with(statement.text.as_str())),
                "{sql:?} at {}",
                statement.start
            );
        }
    }
}

#[test]
fn splitting_a_carried_statement_again_returns_it_alone() {
    for (sql, _) in SPLIT_TABLE {
        for statement in split_statements(sql) {
            assert_eq!(
                split_sql_statements(&statement.text),
                vec![statement.text.clone()],
                "{sql:?}"
            );
        }
    }
}

#[test]
fn a_statement_position_reads_the_carried_start() {
    let sql = "SELECT 1;  SELECT 2";
    let carried = split_statements(sql);
    assert_eq!(carried.first().and_then(|s| s.char_position(sql)), Some(1));
    assert_eq!(carried.get(1).and_then(|s| s.char_position(sql)), Some(12));
}
