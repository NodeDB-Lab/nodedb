// SPDX-License-Identifier: BUSL-1.1

//! The pass that turns every passed KV TTL into a replicated reap.
//!
//! A core never reaps on its own (`engine::kv::engine::expiry_due`). Each
//! pass asks every local core for its due expiries
//! (`MetaOp::KvDueExpiries`). For each key whose vShard's data group this
//! node leads, it proposes one resolved KV write of `Reap` mutations,
//! through write admission like every other write:
//!
//! - the leader's write gate takes the key's lock, so the reap orders
//!   against every other write of the key in the group's log;
//! - the gate folds the expiry's last ordinal into the leader's clock before
//!   it reserves (`ReplicatedWrite::ordinal_floor`), so every later write of
//!   the key stamps above the reaped version's end;
//! - every replica applies the reap at the same log position, against the
//!   same stored expiry, and ends the version at the same ordinal.
//!
//! A follower's due set holds the same keys. It proposes nothing: the
//! leader's reaps clear the followers' entries when they apply. A failed
//! proposal leaves the entry due, and the next pass proposes it again. A
//! reap the key outgrew is a no-op that clears the entry.
//!
//! One key's error never stops the others. The pass continues, records the
//! error, and reports it. The key stays due.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation, MetaOp};
use nodedb_types::{CollectionKey, QualifiedCollection, RlsWriteCheck};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::engine::kv::DueExpiry;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Most reap mutations one proposed entry carries.
const REAPS_PER_ENTRY: usize = 256;

/// How long one reap proposal waits for its leader's gate and its apply.
const PROPOSE_DEADLINE: Duration = Duration::from_secs(30);

/// What one pass did.
#[derive(Debug, Default)]
pub struct ReapPass {
    /// Due entries every local core answered.
    pub due: usize,
    /// Reaps this node proposed as its groups' leader.
    pub proposed: usize,
    /// Keys the pass did not reap because of an error. They stay due.
    pub failed: usize,
    /// The first error the pass met. Set when `failed` is above zero.
    pub first_error: Option<crate::Error>,
}

impl ReapPass {
    /// Count `keys` keys that `error` stopped.
    fn record_failure(&mut self, keys: usize, error: crate::Error) {
        self.failed += keys;
        self.first_error.get_or_insert(error);
    }
}

/// The data group a reap of `entry` writes: its database, tenant and vShard.
type ReapTarget = (DatabaseId, TenantId, VShardId);

/// The reap batches of a due set, and the entries that fit none.
struct ReapBatches {
    batches: Vec<(ReapTarget, Vec<KvResolvedMutation>)>,
    unusable: Vec<crate::Error>,
}

/// The vShard and the qualified name of a due entry's collection.
fn reap_home(
    database_id: DatabaseId,
    stored: String,
) -> crate::Result<(VShardId, QualifiedCollection)> {
    let vshard = CollectionKey::from_qualified_str(database_id, &stored)?.vshard();
    let collection = QualifiedCollection::from_stored(database_id, stored)?;
    Ok((vshard, collection))
}

/// Group `due` by the vShard each key homes to, `REAPS_PER_ENTRY` per batch,
/// keeping only the vShards `leads` names. An entry whose collection name does
/// not resolve goes in `unusable`, and every other entry still batches.
fn reap_batches(due: Vec<DueExpiry>, leads: impl Fn(VShardId) -> bool) -> ReapBatches {
    let mut by_target: BTreeMap<(u64, u64, u32), Vec<KvResolvedMutation>> = BTreeMap::new();
    let mut unusable = Vec::new();
    for entry in due {
        let database_id = DatabaseId::new(entry.database_id);
        let (vshard, collection) = match reap_home(database_id, entry.collection) {
            Ok(home) => home,
            Err(error) => {
                crate::diag::kv_reap_entry_unusable(&error, entry.database_id, entry.tenant_id);
                unusable.push(error);
                continue;
            }
        };
        if !leads(vshard) {
            continue;
        }
        by_target
            .entry((entry.database_id, entry.tenant_id, vshard.as_u32()))
            .or_default()
            .push(KvResolvedMutation::Reap {
                collection,
                key: entry.key,
                expire_at_ms: entry.expire_at_ms,
            });
    }
    let mut batches = Vec::new();
    for ((database_id, tenant_id, vshard), mutations) in by_target {
        let target = (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            VShardId::new(vshard),
        );
        for chunk in mutations.chunks(REAPS_PER_ENTRY) {
            batches.push((target, chunk.to_vec()));
        }
    }
    ReapBatches { batches, unusable }
}

/// The resolved write that reaps `mutations`. An expiry is the system's
/// act: no write policy governs it.
fn reap_plan(mutations: Vec<KvResolvedMutation>) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::ResolvedWrite {
        mutations,
        response_payload: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// Ask every local core for at most `limit` due expiries each, and propose
/// a reap of each key whose data group this node leads. Every batch is
/// proposed. A key's error, or a batch's, is recorded on the returned pass,
/// and its entries stay due for the next pass. `Err` means no key was
/// attempted: the cores did not answer, or no proposer is installed.
pub async fn run_reap_pass(state: &SharedState, limit: u32) -> crate::Result<ReapPass> {
    let Some(multi_raft) = state.multi_raft.get() else {
        // Raft is not serving yet: no group has a leader here to propose.
        return Ok(ReapPass::default());
    };
    let response = crate::control::server::broadcast::broadcast_to_all_cores(
        state,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        PhysicalPlan::Meta(MetaOp::KvDueExpiries { limit }),
        TraceId::generate(),
    )
    .await?;
    let due: Vec<DueExpiry> = zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("kv due expiries: {e}"),
        }
    })?;
    let mut pass = ReapPass {
        due: due.len(),
        ..ReapPass::default()
    };
    let ReapBatches { batches, unusable } = {
        let mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        reap_batches(due, |vshard| mr.vshard_role_is_leader(vshard.as_u32()))
    };
    for error in unusable {
        pass.record_failure(1, error);
    }
    if batches.is_empty() {
        return Ok(pass);
    }
    let proposer = state.async_raft_proposer()?;
    for ((database_id, tenant_id, vshard), mutations) in batches {
        let count = mutations.len();
        let plan = reap_plan(mutations);
        let proposed = async {
            let entry = crate::control::wal_replication::to_replicated_entry(
                tenant_id,
                database_id,
                vshard,
                &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
            )?
            .ok_or_else(|| crate::Error::Internal {
                detail: "kv reap: a resolved reap did not map to a replicated write".into(),
            })?;
            let deadline = tokio::time::Instant::now() + PROPOSE_DEADLINE;
            crate::control::wal_replication::propose_replicated_entry(
                state, proposer, entry, deadline,
            )
            .await
        }
        .await;
        match proposed {
            Ok(_) => pass.proposed += count,
            Err(e) => {
                tracing::debug!(
                    vshard_id = vshard.as_u32(),
                    reaps = count,
                    error = %e,
                    "kv reap proposal failed; the keys stay due for the next pass"
                );
                pass.record_failure(count, e);
            }
        }
    }
    Ok(pass)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn due(collection: &str, key: &[u8]) -> DueExpiry {
        DueExpiry {
            database_id: 0,
            tenant_id: 1,
            collection: collection.into(),
            key: key.to_vec(),
            expire_at_ms: 1_500,
        }
    }

    fn vshard_of(collection: &str) -> VShardId {
        CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard()
    }

    /// One bad collection name stops only its own key.
    #[test]
    fn an_unusable_entry_is_recorded_and_the_rest_still_batch() {
        let ReapBatches { batches, unusable } = reap_batches(
            vec![due("a", b"k1"), due("a/b/c", b"bad"), due("a", b"k2")],
            |_| true,
        );
        assert_eq!(unusable.len(), 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.len(), 2);

        let mut pass = ReapPass::default();
        for error in unusable {
            pass.record_failure(1, error);
        }
        assert_eq!(pass.failed, 1);
        assert!(pass.first_error.is_some());
    }

    /// Only the vShards this node leads get reaps, one batch per vShard, each
    /// mutation naming the expiry the core handed out.
    #[test]
    fn a_pass_reaps_only_the_vshards_it_leads() {
        let led = vshard_of("a");
        let other = (0..512)
            .map(|i| format!("o{i}"))
            .find(|name| vshard_of(name) != led)
            .expect("a collection on another vShard");
        let ReapBatches { batches, unusable } = reap_batches(
            vec![due("a", b"k1"), due(&other, b"k2"), due("a", b"k3")],
            |vshard| vshard == led,
        );
        assert!(unusable.is_empty());
        assert_eq!(batches.len(), 1);
        let ((_, tenant, vshard), mutations) = &batches[0];
        assert_eq!((*tenant, *vshard), (TenantId::new(1), led));
        assert_eq!(mutations.len(), 2);
        assert!(mutations.iter().all(|m| matches!(
            m,
            KvResolvedMutation::Reap {
                expire_at_ms: 1_500,
                ..
            }
        )));
    }

    #[test]
    fn a_large_due_set_splits_into_bounded_entries() {
        let keys: Vec<DueExpiry> = (0..(REAPS_PER_ENTRY + 1))
            .map(|i| due("a", format!("k{i}").as_bytes()))
            .collect();
        let sizes: Vec<usize> = reap_batches(keys, |_| true)
            .batches
            .iter()
            .map(|(_, m)| m.len())
            .collect();
        assert_eq!(sizes, vec![REAPS_PER_ENTRY, 1]);
    }
}
