// SPDX-License-Identifier: BUSL-1.1

//! The frozen `RETURNING` answer of one write the write gate routed through
//! Calvin.
//!
//! The routed write carries its `RETURNING` contract, so its participant
//! evaluates the list before it votes and answers the frozen rows. The
//! write's caller shapes the answer as it shapes a direct write's. The
//! answer travels framed as a MessagePack extension of its own type, which
//! no row payload starts with, so the shaper answers its rows as they are
//! and draws no sequence value.

use super::calvin_contract::contract_rows;
use super::calvin_fold::{CalvinBatchFold, CalvinFoldCtx};
use super::project::cell_keys;
use super::schema::OutputSchema;
use super::types::{ShapedRow, ShapedRows};
use crate::control::sequence::{SequenceAccess, SequenceRecorder};
use nodedb_physical::physical_plan::PhysicalPlan;

/// The MessagePack `ext 32` marker.
const EXT32: u8 = 0xc9;
/// The extension type of a framed frozen answer.
const FROZEN_ANSWER_TYPE: u8 = 0x46;
/// The marker, the four length bytes, and the type byte.
const HEADER: usize = 6;

/// `reply`, an encoded `CalvinFrozenReply`, framed as a frozen answer.
pub fn frame(reply: &[u8]) -> crate::Result<Vec<u8>> {
    let length = u32::try_from(reply.len()).map_err(|_| crate::Error::Internal {
        detail: format!(
            "calvin RETURNING: a frozen answer of {} bytes does not fit its frame",
            reply.len()
        ),
    })?;
    let mut framed = Vec::with_capacity(HEADER + reply.len());
    framed.push(EXT32);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.push(FROZEN_ANSWER_TYPE);
    framed.extend_from_slice(reply);
    Ok(framed)
}

/// The encoded `CalvinFrozenReply` `payload` frames, or `None` for any other
/// payload.
pub fn framed_reply(payload: &[u8]) -> Option<&[u8]> {
    let (header, reply) = payload.split_at_checked(HEADER)?;
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    let framed = header[0] == EXT32
        && header[5] == FROZEN_ANSWER_TYPE
        && usize::try_from(length).is_ok_and(|length| length == reply.len());
    framed.then_some(reply)
}

/// The rows of the frozen answer `reply` to `plan`, as one result set. The
/// session `sequences` records into takes the values the evaluation drew.
/// `projection`, the columns the statement announced, names and types the
/// set as it does a direct write's rows. A caller with no plan in scope
/// passes `None`: an answer with no row then names no column.
pub fn shape_frozen_answer(
    plan: Option<&PhysicalPlan>,
    reply: &[u8],
    projection: Option<&OutputSchema>,
    sequences: Option<&dyn SequenceAccess>,
) -> crate::Result<ShapedRows> {
    let session = sequences
        .and_then(|sequences| sequences.recorder())
        .unwrap_or(&NO_SESSION);
    let ctx = CalvinFoldCtx { session };
    let sets = contract_rows(plan, reply, &ctx)?;
    let shaped = CalvinBatchFold::one_result_set(sets).ok_or_else(|| crate::Error::Internal {
        detail: "calvin RETURNING: a frozen answer holds no result set".into(),
    })?;
    Ok(announced(shaped, projection))
}

/// A recorder for a caller with no session: it records nothing.
struct NoSession;

static NO_SESSION: NoSession = NoSession;

impl SequenceRecorder for NoSession {
    fn record_obtained(&self, _name: &str, _value: i64) {}

    fn obtained(&self, _name: &str) -> Option<i64> {
        None
    }
}

/// `shaped` under the names and types `projection` announced, one column per
/// announced column. A star, or a list of another width, keeps its own.
fn announced(shaped: ShapedRows, projection: Option<&OutputSchema>) -> ShapedRows {
    let Some(schema) =
        projection.filter(|schema| !schema.is_star && schema.columns.len() == shaped.columns.len())
    else {
        return shaped;
    };
    let old_keys = shaped.cell_keys();
    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|column| column.display_name.clone())
        .collect();
    let types = schema.columns.iter().map(|column| column.ty).collect();
    let new_keys = cell_keys(&columns);
    let notice = shaped.notice;
    let rows = shaped
        .rows
        .into_iter()
        .map(|mut row| {
            old_keys
                .iter()
                .zip(&new_keys)
                .filter_map(|(old, new)| row.remove(old).map(|cell| (new.clone(), cell)))
                .collect::<ShapedRow>()
        })
        .collect();
    let mut answered = ShapedRows::from_rows(columns, types, rows);
    answered.notice = notice;
    answered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_framed_reply_reads_back_and_nothing_else_does() {
        let reply = vec![0x84, 0x01, 0x02];
        let framed = frame(&reply).expect("frame");
        assert_eq!(framed_reply(&framed), Some(reply.as_slice()));
        assert_eq!(framed_reply(&reply), None);
        assert_eq!(framed_reply(&[]), None);
        let mut short = framed.clone();
        short.pop();
        assert_eq!(framed_reply(&short), None, "a length that does not fit");
        let mut other = framed;
        other[5] = 0x01;
        assert_eq!(framed_reply(&other), None, "another extension type");
    }

    #[test]
    fn announced_columns_name_and_type_the_set() {
        use super::super::schema::OutputColumn;
        use super::super::types::DdlColType;
        use nodedb_types::Value;
        let shaped = ShapedRows::from_rows(
            vec!["n".into()],
            vec![DdlColType::Text],
            vec![ShapedRow::from([("n".to_owned(), Value::Integer(4))])],
        );
        let schema = OutputSchema {
            columns: vec![OutputColumn {
                display_name: "next".into(),
                lookup_key: "next".into(),
                ty: DdlColType::Int8,
            }],
            is_star: false,
            cp_computed: Vec::new(),
            declared_key: None,
        };
        let answered = announced(shaped, Some(&schema));
        assert_eq!(answered.columns, ["next"]);
        assert_eq!(answered.column_types, [DdlColType::Int8]);
        assert_eq!(answered.rows[0]["next"], Value::Integer(4));
    }
}
