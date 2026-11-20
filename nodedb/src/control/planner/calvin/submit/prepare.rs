// SPDX-License-Identifier: BUSL-1.1

//! Prepare a Calvin transaction for its sequencer submit.
//!
//! Every submit stamps its class the same way: raise the metadata floor,
//! stamp incarnations and UNIQUE claims, slice edge batches by home pair,
//! and split the plans into parts when one sequencer entry cannot carry
//! them. The plan batch decodes once here, and every stage reads the
//! decoded plans. The class's plan bytes change only when edge slicing
//! changes the plans, and then encode once. A class whose plans carry a full
//! `RETURNING` list also gets its response contract here.

use nodedb_cluster::calvin::types::TxClass;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::reported_results::ReplyFold;
use super::stream::PartStream;
use crate::Error;
use crate::control::state::SharedState;

#[cfg(test)]
thread_local! {
    /// Plan batch decodes this thread ran through [`decode_plans`].
    static PLAN_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A class ready for its sequencer submit.
pub(crate) struct Prepared {
    /// The parts to stream after the header, when the class travels as
    /// parts.
    pub stream: Option<PartStream>,
    /// How the coordinator folds the participants' answers.
    pub fold: ReplyFold,
}

/// Stamp `tx_class` for its submit on this node, and split its plans into
/// parts when one sequencer entry cannot carry them.
///
/// A class already split into parts was stamped and split by its
/// coordinator. Only its metadata floor rises here.
pub(crate) fn prepare_submit(
    state: &SharedState,
    tx_class: &mut TxClass,
) -> crate::Result<Prepared> {
    check_carried_bytes(tx_class)?;
    super::local::raise_metadata_floor(state, tx_class);
    if tx_class.is_multi_part() {
        return Ok(Prepared {
            stream: None,
            fold: ReplyFold::of_prepared(tx_class, &[]),
        });
    }
    // The first stage that reads the plans names a decode error.
    let stage = if tx_class.incarnations.is_empty() {
        "calvin incarnation stamp"
    } else {
        "calvin unique claims"
    };
    let plans = bind_volatile_defaults(state, tx_class, decode_plans(&tx_class.plans, stage)?)?;
    super::local::stamp_incarnations(state, tx_class, &plans)?;
    super::unique_claims::stamp_unique_claims(state, tx_class, &plans)?;
    super::chain_heads::stamp_chain_heads(state, tx_class, &plans)?;
    super::period_lock_reads::stamp_period_lock_reads(state, tx_class, &plans)?;
    super::sum_target_rows::stamp_sum_target_rows(state, tx_class, &plans)?;
    // A batch edge op spans the homes of all its edges. Split into one batch
    // per home pair, it spans at most two, which every part can carry.
    let sliced = super::edge_slices::slice_edge_batches(tx_class, plans)?;
    // The contract keys each plan by its index in the plans the class
    // carries, which are the sliced plans.
    super::returning_contract::stamp_returning_contract(state, tx_class, &sliced.plans)?;
    let stream = super::parts::split_into_parts(state, tx_class, &sliced)?;
    let fold = ReplyFold::of_prepared(tx_class, &sliced.plans);
    Ok(Prepared { stream, fold })
}

/// Refuse a class whose published messages or applied key do not decode.
///
/// A participant decodes both when it builds its redo, after the COMMIT
/// verdict. A decode error there halts its vShard. Here it refuses the
/// submit before the class is sequenced.
fn check_carried_bytes(tx_class: &TxClass) -> crate::Result<()> {
    crate::wal::RedoPublish::decode_all(&tx_class.publishes)?;
    if !tx_class.applied_key.is_empty() {
        zerompk::from_msgpack::<crate::wal::CrossShardAppliedKey>(&tx_class.applied_key).map_err(
            |e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("Calvin transaction applied key: {e}"),
            },
        )?;
    }
    Ok(())
}

/// `plans` with every volatile DEFAULT their bodies leave unset bound once,
/// here on the submitter. Every participant replica executes the sequenced
/// plans on its own, so a DEFAULT left for the apply would draw a different
/// value on each. The class's plan bytes re-encode only when a body changed.
/// A class an earlier submit already bound binds nothing again. Each KV
/// write's pending declared columns resolve here too.
fn bind_volatile_defaults(
    state: &SharedState,
    tx_class: &mut TxClass,
    mut plans: Vec<PhysicalPlan>,
) -> crate::Result<Vec<PhysicalPlan>> {
    let mut changed = false;
    for plan in &mut plans {
        if let Some(bound) = crate::control::write_resolve::bind_volatile_defaults(
            state,
            tx_class.tenant_id,
            tx_class.database_id,
            plan,
        )? {
            *plan = bound;
            changed = true;
        }
        // Every participant re-types a KV value under the declared columns
        // resolved here, on the submitter.
        changed |= crate::control::planner::kv_declared_columns::resolve_kv_declared_columns(
            state,
            tx_class.tenant_id,
            plan,
        )?;
    }
    if changed {
        tx_class.plans =
            nodedb_physical::physical_plan::wire::encode_batch(&plans).map_err(|e| {
                Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("calvin default binding: plan encode: {e}"),
                }
            })?;
    }
    Ok(plans)
}

/// Decode the plan batch `bytes`. `stage` names the reader in a decode
/// error.
fn decode_plans(bytes: &[u8], stage: &str) -> crate::Result<Vec<PhysicalPlan>> {
    #[cfg(test)]
    PLAN_DECODES.with(|count| count.set(count.get() + 1));
    nodedb_physical::physical_plan::wire::decode_batch(bytes).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{stage}: plan decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::types::{
        EngineKeySet, HomedPair, ReadWriteSet, SortedVec, VersionedReadSet,
    };
    use nodedb_physical::physical_plan::{BatchEdge, DocumentOp, GraphOp};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create submit prepare test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("submit-prepare.wal"))
                .expect("open submit prepare test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct submit prepare state");
        (state, directory)
    }

    fn decodes() -> usize {
        PLAN_DECODES.with(std::cell::Cell::get)
    }

    fn edge(i: usize) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: format!("src{i}"),
            label: "L".into(),
            dst_id: format!("dst{i}"),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
            properties: Vec::new(),
        }
    }

    fn truncate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// A class over `plans` whose write set names an edge, so every stage
    /// of the prepare reads the plans.
    fn edge_class(plans: &[PhysicalPlan], body_plans: Vec<u32>) -> TxClass {
        let bytes =
            nodedb_physical::physical_plan::wire::encode_batch(&plans.to_vec()).expect("encode");
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "g".to_owned(),
            edges: SortedVec::new(vec![HomedPair::everywhere((1, 2))]),
            home_vshards: SortedVec::new(vec![1, 2]),
        }]);
        let mut class = TxClass::new_single_vshard(
            ReadWriteSet::new(Vec::new()),
            write_set,
            bytes,
            nodedb_types::TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("tx class");
        class.set_body_plans(body_plans);
        class
    }

    /// One submit decodes its plan batch once, through every stage, and
    /// re-encodes it once when edge slicing changes the plans.
    #[tokio::test]
    async fn one_submit_decodes_its_plan_batch_once() {
        let (state, _directory) = test_state();
        let plans = vec![
            truncate("before"),
            PhysicalPlan::Graph(GraphOp::EdgePutBatch {
                edges: (0..40).map(edge).collect(),
            }),
        ];
        let mut class = edge_class(&plans, vec![1, 1, 0]);

        let before = decodes();
        let prepared = prepare_submit(&state, &mut class).expect("prepares");
        assert_eq!(decodes() - before, 1, "one decode per submit");

        assert!(prepared.stream.is_none(), "the class fits one entry");
        let stamped = nodedb_physical::physical_plan::wire::decode_batch(&class.plans)
            .expect("the sliced plans decode");
        assert!(stamped.len() > plans.len(), "the batch splits by home pair");
        let body: Vec<u32> = (0u32..).take(stamped.len()).collect();
        assert_eq!(
            class.body_plans, body,
            "every slice of a body task is a body task"
        );
        assert!(!class.incarnations.is_empty(), "the plans name collections");
    }

    /// A class whose messages or applied key do not decode is refused before
    /// it is sequenced. A participant decodes both after the COMMIT verdict,
    /// where an error halts its vShard.
    #[tokio::test]
    async fn undecodable_carried_bytes_are_refused_before_sequencing() {
        let (state, _directory) = test_state();
        // 0xc1 is never a valid MessagePack byte.
        let mut class = edge_class(&[truncate("a")], Vec::new());
        class.publishes = vec![0xc1];
        assert!(matches!(
            prepare_submit(&state, &mut class),
            Err(Error::Serialization { .. })
        ));

        let mut class = edge_class(&[truncate("a")], Vec::new());
        class.applied_key = vec![0xc1];
        assert!(matches!(
            prepare_submit(&state, &mut class),
            Err(Error::Serialization { .. })
        ));
    }

    /// A class split into parts by its coordinator decodes nothing.
    #[tokio::test]
    async fn a_split_class_decodes_nothing() {
        let (state, _directory) = test_state();
        let mut class = edge_class(&[truncate("a")], Vec::new());
        class.multi_part = Some(nodedb_cluster::calvin::types::MultiPartPlans::default());
        class.plans = Vec::new();

        let before = decodes();
        prepare_submit(&state, &mut class).expect("prepares");
        assert_eq!(decodes(), before);
    }
}
