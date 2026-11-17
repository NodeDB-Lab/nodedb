// SPDX-License-Identifier: BUSL-1.1

//! The response limits a statement's staged `RETURNING` rows are held to.
//!
//! A write in a transaction answers `RETURNING` from the rows it stages, one
//! reply per staged write. An `INSERT ... SELECT`, `UPDATE ... FROM` or
//! `MERGE` stages one write per row, so its replies sum across the
//! statement. The statement is held to the limits an autocommit answer is
//! held to: rows, columns, cells and payload bytes. A reply past a limit
//! fails the statement with `54000`, and its transaction rolls back.
//!
//! The bytes counted are those of each staged reply, the rows as the Data
//! Plane encoded them.

use nodedb_types::config::tuning::NetworkTuning;

use crate::data::executor::response_codec::ReturningRowsReply;

use super::precommit::ReturningLimits;

/// What one statement's staged `RETURNING` replies have spent of the
/// response limits.
#[derive(Debug)]
pub(crate) struct StagedReturningBudget {
    limits: ReturningLimits,
    rows: usize,
    cells: usize,
    bytes: usize,
}

impl StagedReturningBudget {
    /// An unspent budget under the limits `network` sets.
    pub(crate) fn new(network: &NetworkTuning) -> Self {
        Self {
            limits: ReturningLimits::from_tuning(network),
            rows: 0,
            cells: 0,
            bytes: 0,
        }
    }

    /// Spend one staged reply, `payload`, a `RowsPayload`. A reply past a
    /// limit is an error. An empty reply spends nothing.
    pub(crate) fn admit(&mut self, payload: &[u8]) -> crate::Result<()> {
        // An empty reply answers columns and no row.
        if payload.is_empty() {
            return Ok(());
        }
        let reply: ReturningRowsReply =
            zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("staged RETURNING rows: {e}"),
            })?;
        let columns = reply.columns.len();
        limit("columns", columns, self.limits.columns)?;
        let rows = self.rows.saturating_add(reply.rows.len());
        let cells = self
            .cells
            .saturating_add(reply.rows.iter().map(Vec::len).sum::<usize>());
        let bytes = self.bytes.saturating_add(payload.len());
        limit("rows", rows, self.limits.rows)?;
        limit("cells", cells, self.limits.cells)?;
        limit("payload bytes", bytes, self.limits.payload_bytes)?;
        self.rows = rows;
        self.cells = cells;
        self.bytes = bytes;
        Ok(())
    }
}

/// Refuse `actual` past `maximum`, naming the limit.
fn limit(name: &str, actual: usize, maximum: usize) -> crate::Result<()> {
    if actual > maximum {
        return Err(crate::Error::ExecutionLimitExceeded {
            detail: format!("RETURNING {name} {actual} exceeds limit {maximum}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::{NativeCell, Value};

    use super::*;
    use crate::data::executor::response_codec::RowsPayload;

    fn reply(rows: usize) -> Vec<u8> {
        zerompk::to_msgpack_vec(&RowsPayload {
            columns: vec!["id".into()],
            rows: vec![vec![NativeCell(Value::Integer(1))]; rows],
        })
        .expect("encode")
    }

    #[test]
    fn replies_sum_toward_the_row_limit() {
        let mut budget = StagedReturningBudget::new(&NetworkTuning {
            max_returning_rows: 3,
            ..NetworkTuning::default()
        });
        budget.admit(&reply(2)).expect("two rows fit");
        assert!(matches!(
            budget.admit(&reply(2)),
            Err(crate::Error::ExecutionLimitExceeded { .. })
        ));
        budget
            .admit(&reply(1))
            .expect("the refused reply spent nothing");
    }

    #[test]
    fn replies_sum_toward_the_byte_limit() {
        let one = reply(1);
        let mut budget = StagedReturningBudget::new(&NetworkTuning {
            max_query_result_bytes: one.len() as u64,
            ..NetworkTuning::default()
        });
        budget.admit(&one).expect("one reply fits");
        assert!(budget.admit(&one).is_err());
    }
}
