// SPDX-License-Identifier: BUSL-1.1

//! Reversal of transactional DDL that `finalize_pending` already applied, after
//! the buffered DML that depended on it failed to dispatch.
//!
//! The reversal is DDL like any other: it takes the preparation lease, is
//! stamped from committed state, and applies only under the token that owns
//! the lease. No other DDL interleaves between the stamp and the apply. A
//! descriptor another DDL changed after the finalize is refused, never
//! overwritten.

use std::sync::atomic::Ordering;

use nodedb_cluster::{MetadataEntry, PendingDdlObject};

use crate::control::catalog_entry::incarnation::RowKey;
use crate::control::catalog_entry::incarnation::target::{carried_target, delete_key, written_row};
use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::metadata_proposer::MetadataRaftHandle;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

use super::ddl_flush::{propose_and_await, reverse_create};

/// Undo every object `finalize_pending` already applied to the catalog, as one
/// fenced batch. A fresh create is deleted, an alter is restored from the
/// `before_image` captured at propose time. Every failure is returned: the
/// caller surfaces it alongside the original dispatch failure.
///
/// The authorization barrier runs after the preparation lease is released,
/// as on the single-statement path.
pub(super) async fn compensate_finalized(
    state: &SharedState,
    objects: &[PendingDdlObject],
) -> crate::Result<()> {
    let handle = state.metadata_raft_handle()?;
    let _local_guard = crate::control::metadata_proposer::lock_ddl_preparation_async(state).await;
    let lease =
        crate::control::metadata_proposer::acquire_ddl_prepare_lease_async(state, handle.as_ref())
            .await?;
    let reversed = propose_reversals(state, handle.as_ref(), lease.token(), objects).await;
    lease.release().await;
    let log_index = reversed?;
    // The compensation restores prior authorization state, which binds every
    // node like any other authorization change.
    if super::ddl_authorization::objects_bear_authorization(objects)? {
        super::ddl_authorization::barrier_at(state, log_index).await?;
    }
    Ok(())
}

/// Plan, stamp, and propose the reversal batch under the preparation lease
/// `token`, then check that every reversal applied. Returns the batch's log
/// index.
async fn propose_reversals(
    state: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
    objects: &[PendingDdlObject],
) -> crate::Result<u64> {
    let catalog = state.credentials.catalog();
    let reversals = plan_reversals(objects, catalog)?;
    let stamped =
        catalog_entry::descriptor_stamp::stamp_batch(reversals, &state.hlc_clock, catalog)?;
    let mut entries = Vec::with_capacity(stamped.len());
    for entry in &stamped {
        entries.push(MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(entry)?,
        });
    }
    let prepared = MetadataEntry::DdlPrepared {
        token,
        entry: Box::new(MetadataEntry::Batch { entries }),
    };
    let log_index = propose_and_await(state, handle, &prepared).await?;
    if state.metadata_ddl.applied_token.load(Ordering::Acquire) != token {
        return Err(crate::Error::Config {
            detail: "commit compensation: DDL preparation ownership was superseded before apply"
                .into(),
        });
    }
    for entry in &stamped {
        verify_applied(entry, catalog)?;
    }
    Ok(log_index)
}

/// One finalized object, decoded, with the image it replaced.
struct Applied<'a> {
    entry: CatalogEntry,
    /// `None` when the transaction created the descriptor.
    before_image: Option<&'a [u8]>,
}

/// One reversal per descriptor, in first-mention order.
///
/// Within one transaction every object of a descriptor is a create, or every
/// one is an alter, because each was classified against the same committed
/// state. A created descriptor is deleted at the incarnation the transaction
/// left. An altered one is restored from that shared `before_image`. Objects
/// of an unversioned kind reverse one by one.
fn plan_reversals(
    objects: &[PendingDdlObject],
    catalog: &SystemCatalog,
) -> crate::Result<Vec<CatalogEntry>> {
    let mut applied = Vec::with_capacity(objects.len());
    for object in objects {
        applied.push(match object {
            PendingDdlObject::Create { entry } => Applied {
                entry: catalog_entry::decode(wire_payload(entry)?)?,
                before_image: None,
            },
            PendingDdlObject::Alter {
                entry,
                before_image,
            } => Applied {
                entry: catalog_entry::decode(wire_payload(entry)?)?,
                before_image: Some(before_image.as_slice()),
            },
        });
    }

    // `(row key, first index, last index)` per descriptor.
    let mut groups: Vec<(Option<RowKey<'_>>, usize, usize)> = Vec::new();
    for (index, object) in applied.iter().enumerate() {
        let key = written_row(&object.entry).map(|(key, _)| key);
        let existing = key.and_then(|key| {
            groups
                .iter_mut()
                .find(|(grouped, _, _)| *grouped == Some(key))
        });
        match existing {
            Some(group) => group.2 = index,
            None => groups.push((key, index, index)),
        }
    }

    let mut reversals = Vec::with_capacity(groups.len());
    for (key, first, last) in groups {
        if let Some(key) = key {
            refuse_if_changed(&key, &applied[last].entry, catalog)?;
        }
        reversals.push(match applied[first].before_image {
            Some(before_image) => catalog_entry::decode(before_image)?,
            None => reverse_create(&applied[last].entry)?,
        });
    }
    Ok(reversals)
}

/// Refuse the reversal when the row no longer holds the incarnation this
/// transaction left: a later DDL owns it, and a reversal will overwrite it.
fn refuse_if_changed(
    key: &RowKey<'_>,
    last: &CatalogEntry,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let expected = written_row(last).map(|(_, incarnation)| incarnation);
    if key.read(catalog)? == expected {
        return Ok(());
    }
    Err(crate::Error::Internal {
        detail: format!(
            "commit compensation: '{}' changed after this transaction's DDL was finalized; \
             the reversal is refused",
            key.name()
        ),
    })
}

/// Fail when the applier acknowledged a reversal instead of applying it.
fn verify_applied(entry: &CatalogEntry, catalog: &SystemCatalog) -> crate::Result<()> {
    let applied = if let (Some(key), Some(target)) = (delete_key(entry), carried_target(entry)) {
        // A purge keeps its inactive row, at the target's clock, until the
        // storage reclaim finishes.
        (
            key,
            key.read(catalog)?.is_none_or(|row| row.hlc == target.hlc),
        )
    } else if let Some((key, written)) = written_row(entry) {
        (key, key.read(catalog)? == Some(written))
    } else {
        return Ok(());
    };
    match applied {
        (_, true) => Ok(()),
        (key, false) => Err(crate::Error::Internal {
            detail: format!(
                "commit compensation: the reversal of '{}' was acknowledged as superseded \
                 instead of applied",
                key.name()
            ),
        }),
    }
}

/// The opaque catalog payload `entry` carries, regardless of audit wrapping.
fn wire_payload(entry: &MetadataEntry) -> crate::Result<&[u8]> {
    match entry {
        MetadataEntry::CatalogDdl { payload }
        | MetadataEntry::CatalogDdlAudited { payload, .. } => Ok(payload),
        other => Err(crate::Error::Internal {
            detail: format!(
                "commit compensation: pending DDL wire shape is not CatalogDdl: {other:?}"
            ),
        }),
    }
}
