// SPDX-License-Identifier: BUSL-1.1

//! The storage-growth predicate for a replicated entry.
//!
//! A leader proposal gate classifies the entry a proposer built. A
//! plan-carrying entry decodes to its plan. A session transaction's redo
//! carries the flag its proposer computed from the buffered plans.

use crate::control::wal_replication::decode::decode_parsed_entry;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

use super::leader_gate::{EntryScope, entry_scope};
use super::predicate::plan_adds_storage;

/// Whether `entry` can add stored bytes.
///
/// A Calvin redo is exempt here: the Calvin submit classified the
/// transaction before it was sequenced.
pub fn entry_adds_storage(entry: &ReplicatedEntry) -> crate::Result<bool> {
    match &entry.write {
        ReplicatedWrite::TransactionRedo {
            calvin: Some(_), ..
        } => return Ok(false),
        ReplicatedWrite::TransactionRedo { adds_storage, .. } => return Ok(*adds_storage),
        // A put binds a cell surrogate. A delete or erasure names none.
        ReplicatedWrite::ArrayOp { cell_surrogate, .. } => return Ok(cell_surrogate.is_some()),
        _ => {}
    }
    let database_id = crate::types::DatabaseId::new(entry.database_id);
    match entry_scope(&entry.write, &entry.incarnations, database_id) {
        EntryScope::Exempt => Ok(false),
        // Reached by no entry: the whole-collection scopes are the two
        // variants above. A new one adds storage until it is classified.
        EntryScope::Collections(_) => Ok(true),
        EntryScope::Plan => Ok(decode_parsed_entry(entry)?
            .is_some_and(|(_, (_, _, plan, _))| plan_adds_storage(&plan))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::ReplicatedEventSource;
    use crate::control::wal_replication::types::RedoBody;

    fn redo_entry(
        body: RedoBody,
        adds_storage: bool,
        calvin: Option<crate::control::wal_replication::types::CalvinRedoMeta>,
    ) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::TransactionRedo {
                body,
                collections: vec!["c".into()],
                event_source: ReplicatedEventSource::from(crate::event::EventSource::User),
                origin: nodedb_physical::physical_plan::RedoOrigin::Commit,
                calvin,
                snapshot_ordinal: None,
                adds_storage,
            },
        )
    }

    fn chunked() -> RedoBody {
        RedoBody::Chunked {
            stream: crate::wal::RedoStreamId::Session {
                vshard: 0,
                idempotency_key: 1,
            },
            count: 4,
            len: 1 << 20,
        }
    }

    fn inline() -> RedoBody {
        RedoBody::Inline(Box::new(
            crate::control::wal_replication::types::RedoContent {
                redo: crate::wal::RedoRecord {
                    version: 1,
                    ops: Vec::new(),
                    calvin_stamp: None,
                    cross_shard_applied: None,
                    row_sources: Vec::new(),
                    publishes: Vec::new(),
                    row_changes: Vec::new(),
                    user_roles: Vec::new(),
                },
                sum_targets: Vec::new(),
                identities: Vec::new(),
                reads: Vec::new(),
            },
        ))
    }

    #[test]
    fn a_session_redo_reads_its_proposers_flag_inline() {
        assert!(entry_adds_storage(&redo_entry(inline(), true, None)).expect("classify"));
        assert!(!entry_adds_storage(&redo_entry(inline(), false, None)).expect("classify"));
    }

    #[test]
    fn a_chunked_delete_only_redo_adds_no_storage() {
        assert!(!entry_adds_storage(&redo_entry(chunked(), false, None)).expect("classify"));
        assert!(entry_adds_storage(&redo_entry(chunked(), true, None)).expect("classify"));
    }

    #[test]
    fn a_calvin_redo_is_exempt_whatever_its_flag() {
        let meta = crate::control::wal_replication::types::CalvinRedoMeta {
            epoch_system_ms: 1,
            reply: nodedb_physical::physical_plan::CalvinReplySpec::Count(Vec::new()),
            primary_write: true,
            user_write: true,
            returning: false,
        };
        assert!(!entry_adds_storage(&redo_entry(inline(), true, Some(meta))).expect("classify"));
    }
}
