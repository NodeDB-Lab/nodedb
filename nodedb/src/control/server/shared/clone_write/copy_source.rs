// SPDX-License-Identifier: BUSL-1.1

//! The source version a copy-up copies.
//!
//! A copy-up copies the source version the clone reads, with its stamps, so
//! the copy installs as that same logical version. The version comes from
//! the source shard's owner at the clone's cut. The caller's row-level
//! security then judges that exact version: a read pinned at the version's
//! own ordinal must see it, or the copy-up treats the row as absent.

use nodedb_types::{CloneOrigin, SystemTimeScope};

use crate::control::maintenance::clone_materializer::{
    SourceKey, SourceRow, clone_scope, read_source_key, read_source_row,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::probes::{SourceView, fetch_kv_source_value, fetch_source_row};

/// The view of `origin`'s source a clone read takes: its cut, and its KV
/// surrogate ceiling. `source_coll_qualified` names the source collection.
pub(super) fn clone_source_view<'a>(
    state: &SharedState,
    tenant_id: TenantId,
    origin: &CloneOrigin,
    bitemporal: bool,
    source_coll_qualified: &'a str,
) -> SourceView<'a> {
    let system_as_of_ms =
        crate::control::clone::lsn_resolve::source_as_of_ms(state, bitemporal, origin.as_of_lsn);
    SourceView {
        tenant_id,
        source_db_id: origin.source_database,
        source_coll_qualified,
        system_time: clone_scope(system_as_of_ms),
        kv_surrogate_ceiling: origin.kv_surrogate_ceiling,
    }
}

/// A read pinned at `ordinal`: it sees the version stamped there.
fn pinned_at(ordinal: i64) -> SystemTimeScope {
    SystemTimeScope::Pinned {
        ordinal,
        cut_marker: 0,
    }
}

/// The source row `src` names, as the clone reads it, when the caller can
/// see that version. `None` when the clone reads no such row or the caller's
/// row-level security hides it.
pub(super) async fn copy_source_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    view: SourceView<'_>,
    document_id: &str,
    src: nodedb_types::Surrogate,
) -> crate::Result<Option<SourceRow>> {
    let Some(row) = read_source_row(
        state,
        view.tenant_id,
        view.source_db_id,
        view.source_coll_qualified,
        src,
        view.system_time,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(stamp) = row.stamp else {
        return Err(crate::Error::Storage {
            engine: "clone_copyup".into(),
            detail: format!(
                "source row of document '{document_id}' in '{}' carries no version stamp",
                view.source_coll_qualified
            ),
        });
    };
    let gate = SourceView {
        system_time: pinned_at(stamp.sys_from),
        ..view
    };
    let visible = fetch_source_row(state, identity, gate, document_id, src).await?;
    Ok(visible.map(|_| row))
}

/// The source key `key`, as the clone reads it, when the caller can see that
/// version. `None` when the clone reads no such key or the caller's
/// row-level security hides it.
pub(super) async fn copy_source_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    view: SourceView<'_>,
    key: &[u8],
) -> crate::Result<Option<SourceKey>> {
    let Some(source) = read_source_key(
        state,
        view.tenant_id,
        view.source_db_id,
        view.source_coll_qualified,
        key,
    )
    .await?
    else {
        return Ok(None);
    };
    let gate = SourceView {
        system_time: pinned_at(source.stamp.sys_from),
        ..view
    };
    let visible = fetch_kv_source_value(state, identity, gate, key).await?;
    Ok(visible.map(|_| source))
}
