// SPDX-License-Identifier: BUSL-1.1

//! Build a pgwire `CommandComplete` [`Tag`] that matches the Postgres wire
//! protocol's documented tag shapes, so `psql` and other strict clients can
//! parse it (`tokio_postgres` tolerates the malformed shapes this replaces,
//! which is why regressions here go unnoticed by the driver-based test
//! suite).
//!
//! Per the protocol's `CommandComplete` spec: `INSERT` alone carries an OID
//! ahead of the row count (`INSERT <oid> <rows>`); `UPDATE` / `DELETE` /
//! `SELECT` / `MERGE` / `MOVE` / `FETCH` / `COPY` carry `<cmd> <rows>`; a
//! literal SQL `TRUNCATE` carries no count at all. Every other command name
//! here is a NodeDB SQL-DSL extension with no Postgres equivalent (`UPSERT`,
//! `RESTORE TENANT`, `CREATE COLLECTION`, ...) — those keep whatever shape
//! their caller already used; the protocol has no rule to conform to.

use pgwire::api::results::{Response, Tag};

use crate::control::server::response_shape::types::{DmlOutcome, FoldedTag};

/// OID reported in the `INSERT <oid> <rows>` tag. Real Postgres has emitted
/// `0` here since 8.x (the OID-based tag only mattered for `oid`-typed
/// tables, long removed); NodeDB never had per-row OIDs, so `0` is the only
/// value a client should ever see.
const INSERT_TAG_OID: u32 = 0;

/// Build the `CommandComplete` tag for `command`, given the number of rows
/// it affected. `command` must already be the exact tag text (e.g.
/// `"INSERT"`, `"UPDATE"`, or a NodeDB-specific name like `"UPSERT"`).
pub(in crate::control::server::pgwire) fn dml_tag(command: &str, rows: usize) -> Tag {
    // A count-less verb (SQL `TRUNCATE`) renders bare, per the one rule
    // `DmlOutcome::verb_carries_count` owns for every protocol.
    if !DmlOutcome::verb_carries_count(command) {
        return Tag::new(command);
    }
    match command {
        "INSERT" => Tag::new(command).with_oid(INSERT_TAG_OID).with_rows(rows),
        _ => Tag::new(command).with_rows(rows),
    }
}

/// Render a folded statement outcome as its `CommandComplete` tag.
pub(in crate::control::server::pgwire) fn render(outcome: DmlOutcome) -> Tag {
    dml_tag(outcome.verb, outcome.affected as usize)
}

/// Push the statement's one folded tag onto `responses`, after its rows: its
/// DML tag when it folded a count-bearing task, `OK` when only opaque tasks
/// folded, nothing when it folded no task at all.
pub(in crate::control::server::pgwire) fn push_folded_tag(
    responses: &mut Vec<Response>,
    tag: Option<FoldedTag>,
) {
    match tag {
        Some(FoldedTag::Dml(outcome)) => responses.push(Response::Execution(render(outcome))),
        Some(FoldedTag::Opaque) => responses.push(Response::Execution(Tag::new("OK"))),
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_gets_oid_then_rows() {
        let tag: pgwire::messages::response::CommandComplete = dml_tag("INSERT", 3).into();
        assert_eq!(tag.tag, "INSERT 0 3");
    }

    #[test]
    fn update_gets_rows_only() {
        let tag: pgwire::messages::response::CommandComplete = dml_tag("UPDATE", 2).into();
        assert_eq!(tag.tag, "UPDATE 2");
    }

    #[test]
    fn truncate_drops_the_count() {
        let tag: pgwire::messages::response::CommandComplete = dml_tag("TRUNCATE", 9).into();
        assert_eq!(tag.tag, "TRUNCATE");
    }

    #[test]
    fn render_follows_the_outcome_verb() {
        let tag: pgwire::messages::response::CommandComplete = render(DmlOutcome {
            verb: "INSERT",
            affected: 3,
        })
        .into();
        assert_eq!(tag.tag, "INSERT 0 3");
    }

    #[test]
    fn nodedb_specific_command_keeps_its_count() {
        let tag: pgwire::messages::response::CommandComplete = dml_tag("UPSERT", 1).into();
        assert_eq!(tag.tag, "UPSERT 1");
    }
}
