// SPDX-License-Identifier: BUSL-1.1

//! The reply a staged Calvin transaction answers with when its redo record
//! installs.

use nodedb_query::msgpack_scan::{write_kv_i64, write_kv_str, write_map_header};

use super::chained::ChainedPlan;
use super::crdt_writes::StagedCrdtWrite;
use crate::bridge::envelope::ErrorCode;
use crate::control::server::shared::sql::affected_payload::AffectedFields;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::{StagedPlanImages, StagedReturningImages};

/// The reply a Calvin transaction's install answers with.
///
/// The transaction answers with its last `RETURNING` plan's rows, or with
/// the sum of its plans' affected counts when no plan carries `RETURNING`. A
/// plan derived from the statement, such as a graph edge or a balance move,
/// therefore never replaces the rows the statement asked for, and adds no count.
#[derive(Debug)]
pub(in crate::data::executor) enum CalvinReply {
    /// An affected count, decided when the plan staged.
    Count(Vec<u8>),
    /// `RETURNING` rows, decided when the plan staged.
    Rows(Vec<u8>),
    /// The txn's frozen `RETURNING` replies, which the Control Plane reads
    /// from the slice's log after the install. The install renders nothing.
    Frozen,
}

impl Default for CalvinReply {
    fn default() -> Self {
        Self::Count(Vec::new())
    }
}

impl CalvinReply {
    /// Whether this reply carries `RETURNING` rows.
    pub(super) fn has_rows(&self) -> bool {
        !matches!(self, Self::Count(_))
    }
}

/// What staging a Calvin transaction's plans carries from one plan to the
/// next.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct CalvinStaging {
    /// The reply the plans staged so far answer with.
    pub(in crate::data::executor) reply: CalvinReply,
    /// Whether a plan the statement names, not a derived one, decided the
    /// count in `reply`.
    pub(super) user_count: bool,
    /// The running sum of the plans whose count reads their reply. It stands
    /// over `reply` unless a plan decided rows.
    pub(super) total: Option<CountTotal>,
    /// A resolved ingest staged. Its install answers its count, so its staged
    /// answer never joins the reply.
    pub(super) resolved_ingest: bool,
    /// The edges this home owns among the edge writes staged so far.
    pub(super) owned_edges: u64,
    /// The plans staged so far, which is the slice index of the next one.
    pub(super) plans_staged: u32,
    /// The `RETURNING` images of every plan staged so far, in plan order.
    /// They ride the stage response, beside `reply`.
    pub(super) images: Vec<StagedPlanImages>,
    /// The CRDT row writes of every plan staged so far, in plan order. A
    /// CRDT image folds the earlier writes to its row.
    pub(super) crdt_writes: Vec<StagedCrdtWrite>,
    /// The hash-chained `RETURNING` plans staged so far, in plan order.
    /// Their images settle once the whole slice staged.
    pub(super) chained: Vec<ChainedPlan>,
    /// The chained plan whose rows answer the slice: the last `RETURNING`
    /// plan, when it is chained.
    pub(super) chained_reply: Option<u32>,
}

impl CoreLoop {
    /// The reply a fully staged slice answers with, and the `RETURNING`
    /// images its stage response carries. Chained plans settle here.
    pub(in crate::data::executor) fn finish_calvin_staging(
        &self,
        txn_id: crate::types::TxnId,
        tid: u64,
        mut staging: CalvinStaging,
    ) -> Result<(CalvinReply, StagedReturningImages), ErrorCode> {
        self.settle_chained_plans(txn_id, tid, &mut staging)?;
        // Chained plans settle after the plans behind them. The stage
        // response carries every plan's images in plan order.
        staging.images.sort_by_key(|images| images.plan);
        Ok(staging.into_parts())
    }
}

impl CalvinStaging {
    /// The reply the staged plans answer with, and the `RETURNING` images
    /// the stage response carries.
    fn into_parts(mut self) -> (CalvinReply, StagedReturningImages) {
        let images = StagedReturningImages {
            plans: std::mem::take(&mut self.images),
        };
        (self.into_reply(), images)
    }

    /// The reply the staged plans answer with. The running total encodes once
    /// here. A slice of resolved ingests alone answers an empty count: its
    /// install counts replace it.
    pub(in crate::data::executor) fn into_reply(self) -> CalvinReply {
        if self.reply.has_rows() {
            return self.reply;
        }
        match self.total {
            Some(total) => CalvinReply::Count(total.encode()),
            None if self.resolved_ingest => CalvinReply::default(),
            None => self.reply,
        }
    }
}

/// The running sum of a slice's counted plans.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct CountTotal {
    affected: u64,
    rejected: u64,
    collection: Option<String>,
    inserted: bool,
    updated: bool,
}

impl CountTotal {
    /// Add one plan's answer. An `op` of `insert` on any plan answers `insert`,
    /// as an `ON CONFLICT DO UPDATE` of mixed rows tags `INSERT`. The first
    /// `collection` stays.
    pub(super) fn add(&mut self, fields: &AffectedFields<'_>) {
        self.affected = self.affected.saturating_add(fields.affected);
        self.rejected = self.rejected.saturating_add(fields.rejected);
        if self.collection.is_none() {
            self.collection = fields.collection.map(str::to_owned);
        }
        self.inserted |= fields.op == Some("insert");
        self.updated |= fields.op == Some("update");
    }

    /// The answer as a staging handler encodes one, in a fixed key order.
    fn encode(&self) -> Vec<u8> {
        let op = match (self.inserted, self.updated) {
            (true, _) => Some("insert"),
            (false, true) => Some("update"),
            (false, false) => None,
        };
        let rejected = (self.rejected > 0).then_some(self.rejected);
        let fields = 1
            + usize::from(self.collection.is_some())
            + usize::from(op.is_some())
            + usize::from(rejected.is_some());
        let mut payload = Vec::with_capacity(48);
        write_map_header(&mut payload, fields);
        write_kv_i64(&mut payload, "affected", saturating_i64(self.affected));
        if let Some(collection) = &self.collection {
            write_kv_str(&mut payload, "collection", collection);
        }
        if let Some(op) = op {
            write_kv_str(&mut payload, "op", op);
        }
        if let Some(rejected) = rejected {
            write_kv_i64(&mut payload, "rejected", saturating_i64(rejected));
        }
        payload
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::sql::affected_payload::read_affected_fields;

    fn fields(affected: u64, op: Option<&'static str>) -> AffectedFields<'static> {
        AffectedFields {
            affected,
            rejected: 0,
            collection: None,
            op,
        }
    }

    fn encoded(reply: CalvinReply) -> Vec<u8> {
        match reply {
            CalvinReply::Count(payload) => payload,
            other => panic!("a count reply, got {other:?}"),
        }
    }

    #[test]
    fn a_no_op_plan_adds_nothing_in_either_position() {
        let mut staging = CalvinStaging::default();
        staging.total.get_or_insert_default().add(&fields(0, None));
        staging.total.get_or_insert_default().add(&fields(1, None));
        let payload = encoded(staging.into_reply());
        assert_eq!(
            read_affected_fields(&payload).map(|read| read.affected),
            Some(1)
        );
    }

    #[test]
    fn an_insert_op_wins_over_an_update_op() {
        let mut total = CountTotal::default();
        total.add(&fields(1, Some("update")));
        total.add(&fields(1, Some("insert")));
        let payload = total.encode();
        let read = read_affected_fields(&payload).expect("a count");
        assert_eq!((read.affected, read.op), (2, Some("insert")));
    }

    #[test]
    fn rejected_counts_add_and_the_encoding_reads_back() {
        let mut total = CountTotal::default();
        total.add(&AffectedFields {
            affected: 2,
            rejected: 1,
            collection: None,
            op: None,
        });
        total.add(&AffectedFields {
            affected: 3,
            rejected: 4,
            collection: None,
            op: None,
        });
        let payload = total.encode();
        let read = read_affected_fields(&payload).expect("a count");
        assert_eq!((read.affected, read.rejected), (5, 5));
    }

    #[test]
    fn rows_stand_over_a_total() {
        let mut staging = CalvinStaging {
            reply: CalvinReply::Rows(vec![0x90]),
            ..CalvinStaging::default()
        };
        staging.total.get_or_insert_default().add(&fields(1, None));
        assert!(staging.into_reply().has_rows());
    }

    #[test]
    fn resolved_ingests_alone_answer_an_empty_count() {
        let staging = CalvinStaging {
            resolved_ingest: true,
            reply: CalvinReply::Count(vec![0x80]),
            ..CalvinStaging::default()
        };
        assert!(encoded(staging.into_reply()).is_empty());
    }
}
