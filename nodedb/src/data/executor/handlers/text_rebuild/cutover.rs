// SPDX-License-Identifier: BUSL-1.1

//! The journal replay of a full-text cutover.
//!
//! The cutover copies the rebuilt rows of every document but the ones
//! written during the last pass. Each of those derives again here, inside
//! the cutover transaction, from what the live database holds at cutover,
//! through [`rederive_doc`].

use redb::WriteTransaction;

use nodedb_types::Surrogate;

use super::rederive::{DocRef, rederive_doc};
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Derive each document of `touched` again into the live index inside
    /// `txn`, from its live versions or, with none, its live frames.
    pub(in crate::data::executor) fn replay_journaled_text(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
        collection: &str,
        touched: &[u32],
    ) -> crate::Result<()> {
        let target = self.live_replay_target(database_id, tid, collection);
        for &raw in touched {
            rederive_doc(
                txn,
                target,
                txn,
                DocRef {
                    database_id,
                    tid,
                    collection,
                    surrogate: Surrogate::new(raw),
                },
            )?;
        }
        Ok(())
    }
}
