// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` answer of a paged `INSERT ... SELECT`.
//!
//! A copy writes an unbounded row count, so its list keeps the paged path
//! only when no response limit applies and no item can fail (see
//! `returning::fallibility`). Every other copy with `RETURNING` runs in an
//! implicit transaction. As in PostgreSQL, the statement answers one result
//! set: every copied row's stored image, in copy order. Each page writes
//! with the list, so its reply holds the rows it stored.

use nodedb_types::{NativeCell, ReturningSpec, Value};

use crate::control::server::shared::returning::precommit::ReturningLimits;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{ReturningRowsReply, RowsPayload};

/// One paged copy's `RETURNING` answer, built page by page.
pub(super) struct CopyReturning {
    spec: ReturningSpec,
    rls_filters: Vec<u8>,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<NativeCell>>,
}

impl CopyReturning {
    /// The answer of a copy that answers `spec` under the target's read
    /// policy `rls_filters`. A node with any response limit is an error,
    /// raised before the first page writes: such a copy runs in a
    /// transaction, never page by page.
    pub(super) fn new(
        state: &SharedState,
        spec: ReturningSpec,
        rls_filters: Vec<u8>,
    ) -> crate::Result<Self> {
        if !ReturningLimits::from_tuning(&state.tuning.network).unlimited() {
            return Err(crate::Error::Internal {
                detail: "an INSERT ... SELECT whose RETURNING a response limit bounds reached \
                         the paged copy"
                    .into(),
            });
        }
        Ok(Self {
            spec,
            rls_filters,
            columns: None,
            rows: Vec::new(),
        })
    }

    /// The list each page writes with, and the read policy that gates it.
    ///
    /// A page writes the stored columns and carries no contract: the copy's
    /// list was judged once, for the whole statement, as a list that cannot
    /// fail. A page with the contract will read as a batch whose list can
    /// fail, and a data-group leader's gate will send it to Calvin.
    pub(super) fn page_returning(&self) -> (Option<ReturningSpec>, Vec<u8>) {
        let page = ReturningSpec {
            columns: self.spec.columns.clone(),
            precommit: None,
        };
        (Some(page), self.rls_filters.clone())
    }

    /// Add the rows one page's reply answers, after the rows before them.
    /// Its columns join the result set's by name: a column a row lacks
    /// answers NULL. Returns the rows the page answered.
    pub(super) fn absorb(&mut self, payload: &[u8]) -> crate::Result<usize> {
        let reply: ReturningRowsReply =
            zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("INSERT ... SELECT page RETURNING rows: {e}"),
            })?;
        let answered = reply.rows.len();
        let columns = self.columns.get_or_insert_with(|| reply.columns.clone());
        if *columns == reply.columns {
            self.rows.extend(reply.rows);
            return Ok(answered);
        }
        let positions: Vec<usize> = reply
            .columns
            .iter()
            .map(
                |name| match columns.iter().position(|known| known == name) {
                    Some(position) => position,
                    None => {
                        columns.push(name.clone());
                        columns.len() - 1
                    }
                },
            )
            .collect();
        let width = columns.len();
        for row in &mut self.rows {
            row.resize(width, NativeCell(Value::Null));
        }
        for cells in reply.rows {
            let mut row = vec![NativeCell(Value::Null); width];
            for (cell, position) in cells.into_iter().zip(positions.iter().copied()) {
                if let Some(slot) = row.get_mut(position) {
                    *slot = cell;
                }
            }
            self.rows.push(row);
        }
        Ok(answered)
    }

    /// The statement's one result set. Empty when no page answered a row:
    /// the shaper answers it with the announced columns and no row.
    pub(super) fn finish(self) -> crate::Result<Vec<u8>> {
        let Some(columns) = self.columns else {
            return Ok(Vec::new());
        };
        zerompk::to_msgpack_vec(&RowsPayload {
            columns,
            rows: self.rows,
        })
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("INSERT ... SELECT RETURNING rows: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::ReturningColumns;

    use super::*;

    fn payload(columns: &[&str], rows: Vec<Vec<Value>>) -> Vec<u8> {
        zerompk::to_msgpack_vec(&RowsPayload {
            columns: columns.iter().map(|name| (*name).to_owned()).collect(),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(NativeCell).collect())
                .collect(),
        })
        .expect("encode")
    }

    fn answer() -> CopyReturning {
        CopyReturning {
            spec: ReturningSpec {
                columns: ReturningColumns::Star,
                precommit: None,
            },
            rls_filters: Vec::new(),
            columns: None,
            rows: Vec::new(),
        }
    }

    /// Pages answer one result set in copy order, and a column a later page
    /// adds answers NULL on the rows before it.
    #[test]
    fn pages_join_one_result_set_in_copy_order() {
        let mut answer = answer();
        answer
            .absorb(&payload(&["id"], vec![vec![Value::Integer(1)]]))
            .expect("first page");
        answer
            .absorb(&payload(
                &["n", "id"],
                vec![vec![Value::Integer(9), Value::Integer(2)]],
            ))
            .expect("second page");
        let reply: ReturningRowsReply =
            zerompk::from_msgpack(&answer.finish().expect("finish")).expect("decode");
        assert_eq!(reply.columns, vec!["id".to_owned(), "n".to_owned()]);
        let cells: Vec<Vec<Value>> = reply
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(|cell| cell.0).collect())
            .collect();
        assert_eq!(
            cells,
            vec![
                vec![Value::Integer(1), Value::Null],
                vec![Value::Integer(2), Value::Integer(9)],
            ]
        );
    }

    /// A page answers the stored columns, and its write carries no
    /// contract a leader's gate reads.
    #[test]
    fn a_page_writes_the_stored_columns_with_no_contract() {
        let mut copy = answer();
        let named = ReturningColumns::Named(vec![nodedb_types::ReturningItem::Column {
            name: "id".into(),
            alias: None,
        }]);
        copy.spec = ReturningSpec {
            columns: named.clone(),
            precommit: Some(named.clone()),
        };
        let (page, _) = copy.page_returning();
        assert_eq!(
            page,
            Some(ReturningSpec {
                columns: named,
                precommit: None,
            })
        );
    }

    #[test]
    fn a_copy_that_answered_nothing_is_empty() {
        assert!(answer().finish().expect("finish").is_empty());
    }
}
