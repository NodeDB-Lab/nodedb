// SPDX-License-Identifier: BUSL-1.1

//! The spooled form of each document row an install wrote, and the RAM
//! weight of each record an install spools.

use nodedb_types::RowIdentity;

use crate::data::executor::core_loop::held_events::{HEADER, SpooledOp, bytes_weight};
use crate::data::executor::handlers::spill::tiered::SpillWeight;
use crate::types::document_event::DocumentPutEvent;
use crate::wal::RedoPublish;

use super::super::state::AppliedDocWrite;

/// One document row an install wrote: an [`AppliedDocWrite`] as its spool
/// holds it.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct SpooledDocWrite {
    collection: String,
    identity: RowIdentity,
    op: SpooledOp,
    old_value: Option<Vec<u8>>,
    event: Option<DocumentPutEvent>,
    emits_event: bool,
}

impl From<AppliedDocWrite> for SpooledDocWrite {
    fn from(write: AppliedDocWrite) -> Self {
        Self {
            collection: write.collection,
            identity: write.identity,
            op: SpooledOp::of(write.op),
            old_value: write.old_value,
            event: write.event,
            emits_event: write.emits_event,
        }
    }
}

impl SpooledDocWrite {
    /// The write this record holds.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn into_write(
        self,
    ) -> crate::Result<AppliedDocWrite> {
        Ok(AppliedDocWrite {
            collection: self.collection,
            identity: self.identity,
            op: self.op.op()?,
            old_value: self.old_value,
            event: self.event,
            emits_event: self.emits_event,
        })
    }
}

impl SpillWeight for SpooledDocWrite {
    fn spill_weight(&self) -> usize {
        let event = self.event.as_ref().map_or(0, |event| {
            event.new_value.len() + bytes_weight(event.old_value.as_deref())
        });
        self.collection.spill_weight()
            + self.identity.spill_weight()
            + std::mem::size_of::<SpooledOp>()
            + bytes_weight(self.old_value.as_deref())
            + HEADER
            + event
            + 1
    }
}

impl SpillWeight for RedoPublish {
    fn spill_weight(&self) -> usize {
        self.owner.spill_weight()
            + self.topic.spill_weight()
            + self.payload.spill_weight()
            + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WriteOp;

    fn round_trip<T>(value: &T) -> T
    where
        T: zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
    {
        let bytes = zerompk::to_msgpack_vec(value).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    #[test]
    fn a_spooled_doc_write_round_trips() {
        let writes = [
            SpooledDocWrite {
                collection: "notes".into(),
                identity: RowIdentity::from_user_key("n1"),
                op: SpooledOp::of(WriteOp::Update),
                old_value: None,
                event: Some(DocumentPutEvent {
                    new_value: vec![1, 2, 3],
                    old_value: Some(vec![4]),
                }),
                emits_event: true,
            },
            SpooledDocWrite {
                collection: "notes".into(),
                identity: RowIdentity::from_user_key("n2"),
                op: SpooledOp::of(WriteOp::Delete),
                old_value: Some(vec![9, 9]),
                event: None,
                emits_event: false,
            },
        ];
        for write in writes {
            assert_eq!(round_trip(&write), write);
            assert!(write.spill_weight() > 0);
        }
    }

    #[test]
    fn a_publish_round_trips() {
        let publish = RedoPublish {
            owner: "trigger/0/notify".into(),
            database_id: 0,
            tenant_id: 1,
            topic: "feed".into(),
            payload: "hello".into(),
            metadata_floor: 3,
            position: None,
        };
        assert_eq!(round_trip(&publish), publish);
    }
}
