// SPDX-License-Identifier: BUSL-1.1

//! The sequence results of a refused Calvin `RETURNING` evaluation, given
//! to the writer's session.
//!
//! A participant whose evaluation fails votes abort with the `nextval` and
//! `setval` results it drew. They stand, as in PostgreSQL: the writer's
//! `currval` takes them though the statement fails. The coordinator that
//! decodes the verdict gives them to the session of the statement now
//! running, the session whose `currval` snapshot the contract carries.
//!
//! A sequencer leader that runs a forwarded submit runs no statement of
//! its own. It collects the results with [`collecting`], and its reply
//! carries them to the forwarding coordinator, which records them here.

use std::cell::RefCell;
use std::future::Future;

use crate::control::cluster::calvin::refusal::RefusedSequences;
use crate::control::server::shared::session::read_scope::current_sequence_values;

tokio::task_local! {
    static COLLECTED: RefCell<Option<RefusedSequences>>;
}

/// Give `sequences` to the writer: the collector [`collecting`] installed,
/// else the session of the statement now running. Outside both, no session
/// holds a `currval`, and this records nothing.
pub fn record(sequences: RefusedSequences) {
    if sequences.effects.is_empty() {
        return;
    }
    let mut pending = Some(sequences);
    let _ = COLLECTED.try_with(|collected| {
        let mut collected = collected.borrow_mut();
        match (collected.as_mut(), pending.take()) {
            (Some(held), Some(sequences)) => held.effects.extend(sequences.effects),
            (None, sequences) => *collected = sequences,
            (Some(_), None) => {}
        }
    });
    // Left unconsumed only when no collector is installed.
    let Some(sequences) = pending else {
        return;
    };
    let Some(session) = current_sequence_values() else {
        return;
    };
    for effect in &sequences.effects {
        let (name, value) = effect.name_value();
        session.record(
            sequences.database_id.as_u64(),
            sequences.tenant_id.as_u64(),
            name,
            value,
        );
    }
}

/// Run `future` and collect the sequence results every refusal it decodes
/// records, in place of any session.
pub async fn collecting<F: Future>(future: F) -> (F::Output, Option<RefusedSequences>) {
    COLLECTED
        .scope(RefCell::new(None), async move {
            let output = future.await;
            let collected = COLLECTED
                .try_with(|collected| collected.borrow_mut().take())
                .ok()
                .flatten();
            (output, collected)
        })
        .await
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::control::wal_replication::types::FrozenSequenceEffect;

    fn drawn(values: &[i64]) -> RefusedSequences {
        RefusedSequences {
            database_id: DatabaseId::new(1),
            tenant_id: TenantId::new(7),
            effects: values
                .iter()
                .map(|value| FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: *value,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn a_collector_takes_every_recorded_result_in_order() {
        let ((), collected) = collecting(async {
            record(drawn(&[1]));
            record(drawn(&[]));
            record(drawn(&[2, 3]));
        })
        .await;
        assert_eq!(collected, Some(drawn(&[1, 2, 3])));
    }

    /// Inside a statement scope, the results reach that statement's session:
    /// its `currval` is the last of them, in the txn's database and tenant.
    #[tokio::test]
    async fn a_statement_session_takes_the_results() {
        use std::sync::Arc;

        use crate::control::server::shared::session::SessionStore;
        use crate::control::server::shared::session::conn_scope::scoped;
        use crate::control::server::shared::session::read_scope::enter;

        let addr: std::net::SocketAddr = ([127, 0, 0, 1], 5001).into();
        let sessions = Arc::new(SessionStore::new());
        sessions.ensure_session(addr);
        scoped(async {
            let _scope = enter(&sessions, addr.into());
            record(drawn(&[4, 5]));
        })
        .await;
        let values = sessions.sequence_values(addr).expect("the session");
        assert_eq!(values.last(1, 7, "s"), Some(5));
        assert_eq!(values.last(2, 7, "s"), None, "another database holds none");
    }

    #[tokio::test]
    async fn no_record_collects_nothing() {
        let ((), collected) = collecting(async {}).await;
        assert_eq!(collected, None);
        // Outside a collector and a statement, nothing holds a `currval`.
        record(drawn(&[1]));
    }
}
