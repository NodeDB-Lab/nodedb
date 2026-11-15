// SPDX-License-Identifier: BUSL-1.1

use super::{ddl_part, sequential_gate};
use crate::control::server::response_shape::types::{DmlOutcome, FoldedTag};
use crate::control::server::shared::ddl::DdlResult;
use crate::control::server::shared::sql_split::Statement;

fn request(statements: &[&str]) -> Vec<Statement> {
    statements
        .iter()
        .map(|text| Statement {
            text: (*text).to_owned(),
            start: 0,
        })
        .collect()
}

const INSERT: &str = "INSERT INTO docs (id, v) VALUES ('a', 'b')";
const SELECT: &str = "SELECT id, v FROM docs";
const CREATE: &str = "CREATE COLLECTION t (id TEXT PRIMARY KEY)";

#[test]
fn the_gate_takes_ddl_or_a_claimed_statement_beside_other_statements() {
    for statements in [
        [CREATE, INSERT, SELECT].as_slice(),
        &[INSERT, CREATE],
        &["UPSERT INTO docs (id, v) VALUES ('a', 'b')", INSERT],
        &[INSERT, "INSERT INTO docs { id: 'a', v: 'b' }"],
        &["SELECT KV_INCR('ctr', 'k', 1)", SELECT],
        &[INSERT, "BACKUP DATABASE default TO 'file:///tmp/x'"],
        &[CREATE, "SHOW COLLECTIONS"],
        &["DROP COLLECTION t", CREATE],
    ] {
        assert!(sequential_gate(&request(statements)), "{statements:?}");
    }
}

#[test]
fn the_gate_declines_dml_alone_and_one_statement() {
    for statements in [
        [INSERT, SELECT].as_slice(),
        &[
            INSERT,
            "UPDATE docs SET v = 'c' WHERE id = 'a'",
            "DELETE FROM docs",
        ],
        &[CREATE],
        &["UPSERT INTO docs (id, v) VALUES ('a', 'b')"],
        &[],
    ] {
        assert!(!sequential_gate(&request(statements)), "{statements:?}");
    }
}

#[test]
fn the_gate_declines_transaction_control_and_session_commands() {
    for statements in [
        ["BEGIN", CREATE, "COMMIT"].as_slice(),
        &[CREATE, INSERT, "COMMIT"],
        &["UPSERT INTO docs (id, v) VALUES ('a', 'b')", "SAVEPOINT s"],
        &["SET x = 1", CREATE],
        &[CREATE, "RESET x"],
    ] {
        assert!(!sequential_gate(&request(statements)), "{statements:?}");
    }
}

fn status(command: &str, rows_affected: Option<u64>) -> DdlResult {
    DdlResult::Status {
        command: command.to_owned(),
        rows_affected,
    }
}

#[test]
fn a_ddl_status_adds_no_row_and_no_count() {
    let part = ddl_part(vec![status("CREATE COLLECTION", None)]);
    assert!(part.rows.is_empty());
    assert_eq!(part.tag, Some(FoldedTag::Opaque));
}

#[test]
fn a_counted_router_write_adds_its_verb_and_count() {
    let part = ddl_part(vec![status("UPSERT", Some(1))]);
    assert!(part.rows.is_empty());
    assert_eq!(
        part.tag,
        Some(FoldedTag::Dml(DmlOutcome {
            verb: "UPSERT",
            affected: 1,
        }))
    );
}

#[test]
fn a_count_under_an_unknown_verb_adds_no_count() {
    let part = ddl_part(vec![status("GRAPH INSERT EDGE", Some(1))]);
    assert_eq!(part.tag, Some(FoldedTag::Opaque));
}

#[test]
fn an_empty_result_adds_nothing() {
    let part = ddl_part(vec![DdlResult::Empty]);
    assert!(part.rows.is_empty());
    assert_eq!(part.tag, None);
}
