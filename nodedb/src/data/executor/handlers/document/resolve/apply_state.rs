// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted resolved-statement effects and prepared publication.

use std::collections::BTreeMap;

use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::chain_guard::ChainGuard;
use crate::data::executor::enforcement::funnel::WriteEnforcementOutcome;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::memory::abort_error;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use crate::types::DocumentPutEvent;

#[derive(Default)]
pub(super) struct ResolvedApplyState {
    pub touched: Vec<(String, StorageKey)>,
    pub undo: Vec<UndoEntry>,
    pub chains: Vec<ChainGuard>,
    pub balanced: BTreeMap<String, Vec<BalancedEntry>>,
    pub events: Vec<ResolvedEvent>,
    pub write_set: Vec<WriteSetEntry>,
}

pub(super) enum ResolvedEvent {
    Put {
        collection: String,
        identity: RowIdentity,
        event: DocumentPutEvent,
    },
    Delete {
        collection: String,
        identity: RowIdentity,
        old_value: Vec<u8>,
    },
}

impl ResolvedApplyState {
    /// Retain derived effects immediately after their source, before another mutation runs.
    pub fn retain_enforcement(&mut self, collection: &str, outcome: WriteEnforcementOutcome) {
        self.balanced
            .entry(collection.to_owned())
            .or_default()
            .extend(outcome.balanced_entries);
        for mut target in outcome.target_writes {
            self.touched.push((
                target.collection.clone(),
                StorageKey::for_surrogate(target.surrogate),
            ));
            self.undo.append(&mut target.outcome.memory_undo);
            self.write_set.push(
                WriteSetEntry::installed_put(
                    target.surrogate.as_u32(),
                    target.identity.clone(),
                    target.outcome.installed,
                    target.event.clone(),
                )
                .in_collection(target.collection.clone()),
            );
            self.events.push(ResolvedEvent::Put {
                collection: target.collection,
                identity: target.identity,
                event: target.event,
            });
        }
    }

    /// Reverse memory effects while the caller's transaction still owns durable state.
    pub fn abort(
        mut self,
        core: &mut CoreLoop,
        database_id: u64,
        tid: u64,
        error: crate::Error,
    ) -> crate::Error {
        for chain in self.chains.iter_mut().rev() {
            chain.restore(core);
        }
        for (collection, key) in &self.touched {
            core.doc_cache.invalidate(database_id, tid, collection, key);
        }
        abort_error(error, core.undo_memory_effects(database_id, tid, self.undo))
    }
}

impl ResolvedEvent {
    pub fn publish(self, core: &mut CoreLoop, task: &ExecutionTask) {
        match self {
            Self::Put {
                collection,
                identity,
                event,
            } => core.emit_prepared_put_event(task, &collection, identity, &event),
            Self::Delete {
                collection,
                identity,
                old_value,
            } => core.emit_event_with_row_id(
                task,
                &collection,
                crate::event::WriteOp::Delete,
                crate::event::types::RowId::row(identity),
                None,
                Some(&old_value),
            ),
        }
    }
}
