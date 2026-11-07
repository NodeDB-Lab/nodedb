// SPDX-License-Identifier: BUSL-1.1

//! A committed write's wait for its sync mirrors.
//!
//! `MIRROR DATABASE ... MODE = sync` asks the source to answer a write only once
//! the mirror holds it. The pin carries the mode, and each ack carries the cap of
//! the cut the mirror's groups committed. The wait ends when every sync pin of the
//! database reports a cap at or above the write.
//!
//! The target is the cap a read would take right after the write. Such a read sees
//! the write, so the write sits at or below the cap, and at or below every later
//! cut's cap.
//!
//! The write is durable before the wait starts. A wait that runs out answers
//! `MIRROR_SYNC_TIMEOUT` and undoes nothing: there is no silent downgrade to async,
//! and the row stays. A pin that expired confirms nothing more, so a write times
//! out until the link bootstraps again, or its row leaves.
//!
//! A link joins the wait at its first cut. A mirror in bootstrap holds no cut to
//! confirm with.

use std::time::Instant;

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::DatabaseId;

use crate::control::read_cut::statement_cap;
use crate::control::server::shared::session::{SessionId, SessionStore};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;

/// The database a statement of `tasks` waits on, when a write of it targets a
/// database with a sync mirror.
pub(crate) fn sync_database_of(state: &SharedState, tasks: &[PhysicalTask]) -> Option<DatabaseId> {
    tasks
        .iter()
        .filter(|task| plan_is_write(&task.plan))
        .map(|task| task.database_id)
        .find(|database| state.mirror.pins.holds_sync(database.as_u64()))
}

/// The database a session's COMMIT waits on: a buffered write's, or a held
/// `PUBLISH TO`'s, since a mirror applies a topic message as a write.
pub(crate) fn commit_sync_database(
    state: &SharedState,
    sessions: &SessionStore,
    session: impl Into<SessionId>,
) -> Option<DatabaseId> {
    let session = session.into();
    sync_database_of(state, &sessions.buffered_tasks(session)).or_else(|| {
        sessions
            .pending_publish_databases(session)
            .into_iter()
            .find(|database| state.mirror.pins.holds_sync(*database))
            .map(DatabaseId::new)
    })
}

/// Wait for the sync mirrors of `database`, when it has any. A write path with
/// no planned tasks to read calls this once its write committed.
pub(crate) async fn await_database_sync(
    state: &SharedState,
    database: DatabaseId,
) -> crate::Result<()> {
    if !state.mirror.pins.holds_sync(database.as_u64()) {
        return Ok(());
    }
    await_mirror_sync(state, database).await
}

/// Wait until every sync mirror of `database` holds what this node committed.
/// The caller's write committed already, so every error here says so.
pub(crate) async fn await_mirror_sync(
    state: &SharedState,
    database: DatabaseId,
) -> crate::Result<()> {
    let timeout = state.tuning.mirror.sync_timeout();
    let started = Instant::now();
    let reached = tokio::time::timeout(timeout, confirmed_by_every_mirror(state, database)).await;
    let waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match reached {
        Ok(Ok(())) => return Ok(()),
        Ok(Err(error)) => {
            tracing::warn!(
                database = database.as_u64(),
                %error,
                "mirror sync: the wait for a committed write stopped"
            );
        }
        Err(_) => {
            tracing::warn!(
                database = database.as_u64(),
                waited_ms,
                "mirror sync: a sync mirror did not confirm a committed write"
            );
        }
    }
    let name = state
        .credentials
        .catalog()
        .get_database(database)
        .ok()
        .flatten()
        .map_or_else(|| database.as_u64().to_string(), |db| db.name);
    Err(crate::Error::MirrorSyncTimeout {
        database: name,
        waited_ms,
    })
}

async fn confirmed_by_every_mirror(state: &SharedState, database: DatabaseId) -> crate::Result<()> {
    let now_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let target = statement_cap(state, now_ms).await?.cap;
    let pins = &state.mirror.pins;
    loop {
        // The registration comes before the read, so no apply slips between.
        let changed = pins.changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if pins.sync_confirmed(database.as_u64(), target) {
            return Ok(());
        }
        changed.await;
    }
}
