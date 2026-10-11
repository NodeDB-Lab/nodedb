// SPDX-License-Identifier: BUSL-1.1

//! Cutover phase for `MOVE TENANT`.
//!
//! A collection's storage key and its home vShard both hash its database, so
//! moving a collection to another database moves its rows to another vShard:
//! another core, and in a cluster another Raft group on other nodes. The
//! cutover therefore moves the rows as data, then the namespace:
//!
//! 1. Re-issue the captured rows into the target database as durable writes.
//!    Each write routes by the target key and replicates to every replica of
//!    the target vShard's group. Any node or core error fails the cutover
//!    here, before the catalog changes. Then the target is captured and every
//!    collection's row count and digest checked against the source capture.
//!    A mismatch fails the cutover too, with the source untouched.
//! 2. Propose one metadata commit: the array rekey entries (see
//!    [`super::arrays`]), then `CatalogEntry::MoveTenantCutover`, which moves
//!    every collection's catalog row to the target database. Their apply ends
//!    the drain phase's drain on every node.
//! 3. Every node that applies the commit renames each array store and
//!    reclaims the source-keyed collection storage on each of its cores, in
//!    post-apply.

use crate::control::backup::restore::reissue_into_database;
use crate::control::backup::verify::destination::verify_destination;
use crate::control::backup::verify::expect::expect_capture;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_batch_async;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_types::NodeDbError;
use nodedb_types::backup_envelope::VerificationPhase;

use super::snapshot::SourceCapture;

/// Run the cutover phase with the rows the snapshot phase captured.
pub async fn run(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
    capture: SourceCapture,
) -> Result<(), NodeDbError> {
    let failed = |detail: String| {
        NodeDbError::move_tenant_cutover_failed(tenant_id.as_u64().to_string(), detail)
    };
    let SourceCapture {
        collections: moved_collections,
        data,
    } = capture;

    // The phase code stays the statement's verdict, and the typed error
    // rides as its cause with its own class. The detail carries the cause's
    // text too, since a client reads only the message.
    for (owner, snap) in data {
        let expectation = expect_capture(
            state,
            owner.as_u64(),
            source_db_id,
            &moved_collections,
            &snap,
        )
        .map_err(|e| {
            failed(format!(
                "digest of tenant {}'s captured rows in database {}: {e}",
                owner.as_u64(),
                source_db_id.as_u64()
            ))
            .with_cause(crate::error_classify::classify(&e))
        })?;
        reissue_into_database(state, owner.as_u64(), source_db_id, target_db_id, snap)
            .await
            .map_err(|e| {
                failed(format!(
                    "re-issue of tenant {}'s rows from database {} into database {}: {e}",
                    owner.as_u64(),
                    source_db_id.as_u64(),
                    target_db_id.as_u64()
                ))
                .with_cause(crate::error_classify::classify(&e))
            })?;
        // The target must hold every captured row before the catalog moves.
        // A mismatch fails the move here, and the source stays untouched.
        verify_destination(
            state,
            owner.as_u64(),
            expectation,
            |_| Ok(target_db_id),
            VerificationPhase::Move,
        )
        .await
        .map_err(|e| {
            failed(format!(
                "verification of tenant {}'s rows in database {}: {e}",
                owner.as_u64(),
                target_db_id.as_u64()
            ))
            .with_cause(crate::error_classify::classify(&e))
        })?;
    }

    // Fails the cutover after the re-issue and before the proposal.
    crate::fail_point_err!(
        crate::fail_point::FailScope::Node(state.node_id),
        "move_tenant::cutover::before_proposal",
        |detail: String| failed(format!("fail point: {detail}"))
    );

    // The arrays are read under the DDL preparation lease, so the commit
    // rekeys exactly the arrays the catalog holds when it is proposed. Its apply rekeyed the arrays and
    // reclaimed the source storage in post-apply on every node, this one
    // included, before the proposal returned.
    propose_catalog_batch_async(state, |catalog| {
        let mut entries =
            super::arrays::rekey_entries(catalog, tenant_id, source_db_id, target_db_id)?;
        entries.push(CatalogEntry::MoveTenantCutover {
            tenant_id: tenant_id.as_u64(),
            source_db_id: source_db_id.as_u64(),
            target_db_id: target_db_id.as_u64(),
            collections: moved_collections,
        });
        Ok(entries)
    })
    .await
    .map_err(|e| failed(format!("Raft proposal failed: {e}")))?;

    Ok(())
}
