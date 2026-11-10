// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` images of a hash-chained document plan, decided once the
//! whole slice staged.
//!
//! The install links every row it puts into a `HASH_CHAIN` collection after
//! the chain head it finds, in the order the slice's redo carries the puts:
//! ascending row identity, every plan of the slice together
//! (`resolve::document`). A later plan of the slice can stage a row that
//! sorts before an earlier plan's, so a link is known only once every plan
//! staged.
//!
//! The head the install finds is the head the stage reads. Every Calvin
//! writer of a chained collection locks it whole `Exclusive`
//! (`planner::calvin::submit::chain_heads`), and a gated insert takes the
//! collection `Intent`. So no other write to the chain lands between this
//! slice's grant and its install, and every write admitted before the grant
//! reached this core before the stage.

use std::collections::{BTreeMap, HashMap};

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::{RowIdentity, Surrogate};

use super::images::{ReturningImageRow, RowLocation};
use super::reply::{CalvinReply, CalvinStaging};
use super::staged_images::plan_images;
use super::target::{CollKey, RowEngine};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::hash_chain;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::handlers::point::apply_put::stored_body::StoredBodyInput;
use crate::data::executor::handlers::transaction::overlay::{Staged, TxnOverlay};
use crate::data::executor::handlers::transaction::resolve::redo_put_value;
use crate::types::TxnId;
use crate::types::hash_chain::ChainHead;

/// A `RETURNING` plan over a hash-chained collection whose images wait for
/// the whole slice.
#[derive(Debug)]
pub(super) struct ChainedPlan {
    /// The plan's index in the stage's local slice.
    pub index: u32,
    pub coll_key: CollKey,
    pub spec: ReturningSpec,
    pub rls_filters: Vec<u8>,
    /// Each row the plan wrote, in write order.
    pub rows: Vec<(RowIdentity, Surrogate)>,
}

impl CoreLoop {
    /// Decide the images of every chained plan of `staging`, and the reply
    /// of the one that answers the slice.
    pub(super) fn settle_chained_plans(
        &self,
        txn_id: TxnId,
        tid: u64,
        staging: &mut CalvinStaging,
    ) -> Result<(), ErrorCode> {
        let chained = std::mem::take(&mut staging.chained);
        if chained.is_empty() {
            return Ok(());
        }
        let overlay = self
            .txn_overlays
            .get(&txn_id)
            .ok_or_else(|| ErrorCode::Internal {
                detail: "calvin RETURNING: the transaction overlay is missing after staging".into(),
            })?;
        let mut linked: HashMap<CollKey, BTreeMap<RowIdentity, Vec<u8>>> = HashMap::new();
        for plan in chained {
            if !linked.contains_key(&plan.coll_key) {
                let rows = self.chain_linked_rows(overlay, &plan.coll_key)?;
                linked.insert(plan.coll_key.clone(), rows);
            }
            let stored = linked
                .get(&plan.coll_key)
                .ok_or_else(|| ErrorCode::Internal {
                    detail: "calvin RETURNING: the chain links were not derived".into(),
                })?;
            let rows = plan
                .rows
                .iter()
                .map(|(identity, surrogate)| {
                    let bytes = stored.get(identity).ok_or_else(|| ErrorCode::Internal {
                        detail: format!(
                            "calvin RETURNING: row '{}' of '{}' has no staged chain link",
                            identity.as_str(),
                            plan.coll_key.2
                        ),
                    })?;
                    Ok(ReturningImageRow {
                        identity: identity.clone(),
                        surrogate: *surrogate,
                        bytes: bytes.clone(),
                        vector: None,
                    })
                })
                .collect::<Result<Vec<_>, ErrorCode>>()?;
            let at = RowLocation {
                engine: RowEngine::Document,
                database_id: plan.coll_key.0.as_u64(),
                tid,
                collection: &plan.coll_key.2,
                vector_field: None,
            };
            let docs = self.calvin_image_docs(&at, &rows)?;
            if staging.chained_reply == Some(plan.index) {
                staging.reply = CalvinReply::Rows(self.calvin_render_docs(
                    RowEngine::Document,
                    &plan.spec,
                    &plan.rls_filters,
                    &docs.docs,
                )?);
            }
            staging.images.push(plan_images(
                plan.index,
                &plan.coll_key.2,
                &plan.rls_filters,
                docs,
                &self.crs_context(),
            )?);
        }
        staging.chained_reply = None;
        Ok(())
    }

    /// The stored bytes the install writes for every row the slice staged
    /// into the chained collection `coll_key`, keyed by row identity. Each
    /// row links after the one before it in the redo's order, the first
    /// after this core's chain head.
    fn chain_linked_rows(
        &self,
        overlay: &TxnOverlay,
        coll_key: &CollKey,
    ) -> Result<BTreeMap<RowIdentity, Vec<u8>>, ErrorCode> {
        let config = self
            .doc_configs
            .get(coll_key)
            .ok_or_else(|| ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: chained collection '{}' has no config",
                    coll_key.2
                ),
            })?;
        let strict_schema =
            self.resolve_strict_schema(coll_key.0.as_u64(), coll_key.1.as_u64(), &coll_key.2);
        let mut puts: BTreeMap<&RowIdentity, (u32, &[u8])> = BTreeMap::new();
        for (doc_id, staged) in overlay.iter_doc_entries_for_collection(coll_key) {
            // A chained collection removes no row, so no tombstone links.
            let Staged::Put(body) = staged else {
                continue;
            };
            let surrogate = overlay
                .surrogate_for_doc_id(coll_key, doc_id)
                .ok_or_else(|| ErrorCode::Internal {
                    detail: format!(
                        "calvin RETURNING: staged row '{}' has no bound surrogate",
                        doc_id.as_str()
                    ),
                })?;
            puts.insert(doc_id, (surrogate, body.as_slice()));
        }
        let mut head = self
            .chain
            .heads
            .get(coll_key)
            .cloned()
            .unwrap_or_else(ChainHead::genesis);
        let mut linked = BTreeMap::new();
        for (doc_id, (surrogate, body)) in puts {
            let value =
                redo_put_value(body, strict_schema.as_ref(), doc_id).map_err(ErrorCode::from)?;
            // A committed redo install stamps every document row with the
            // stamp its record carries. A read never shows the stamp, so the
            // stage's ordinal stands in for it.
            let stored = self
                .build_linked_body(
                    StoredBodyInput {
                        config_key: coll_key,
                        surrogate: Surrogate::new(surrogate),
                        value: &value,
                        bitemporal: true,
                        sys_from: self.bitemporal_ordinal(),
                        valid_from_ms: i64::MIN,
                        valid_until_ms: i64::MAX,
                        guards: GuardScope::InjectOnly,
                    },
                    &head,
                )
                .map_err(ErrorCode::from)?
                .stored;
            let view = self
                .decode_stored_document(config, &stored)
                .map_err(ErrorCode::from)?;
            head = hash_chain::stored_link(&view).ok_or_else(|| ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: the linked row '{}' of '{}' carries no link",
                    doc_id.as_str(),
                    coll_key.2
                ),
            })?;
            linked.insert(doc_id.clone(), stored);
        }
        Ok(linked)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, RlsWriteCheck, StorageKey, Surrogate, Value};

    use super::super::fixtures::{TID, coll, object, returning, text};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::enforcement::hash_chain::{CHAIN_HASH_FIELD, CHAIN_SEQ_FIELD};
    use crate::data::executor::response_codec::{RowsPayload, StagedReturningImages};
    use crate::engine::document::store::CollectionConfig;
    use crate::types::TenantId;

    const LEDGER: &str = "ledger";

    /// A link as `(seq, hash)`.
    type Link = (u64, String);

    /// Declare the ledger `HASH_CHAIN` on `core`.
    fn chain_ledger(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(LEDGER);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), LEDGER.to_string()),
            config,
        );
    }

    /// An insert of `id` into the chained ledger, returning its link.
    fn insert(id: &str, surrogate: u32, memo: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: coll(LEDGER),
            document_id: id.to_string(),
            value: object(&[("memo", text(memo))]),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: returning(&["memo", CHAIN_HASH_FIELD, CHAIN_SEQ_FIELD]),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn link_of(fields: &Value) -> Link {
        let seq = match fields.get(CHAIN_SEQ_FIELD) {
            Some(Value::Integer(seq)) => u64::try_from(*seq).expect("a positive seq"),
            other => panic!("a chain seq, got {other:?}"),
        };
        let hash = match fields.get(CHAIN_HASH_FIELD) {
            Some(Value::String(hash)) => hash.clone(),
            other => panic!("a chain hash, got {other:?}"),
        };
        (seq, hash)
    }

    /// The link base stores for the row at `surrogate`.
    fn stored_link(core: &CoreLoop, surrogate: u32) -> Link {
        let bytes = core
            .current_row_body(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                LEDGER,
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read")
            .expect("the install stored the row");
        let config = core
            .doc_configs
            .get(&(DatabaseId::DEFAULT, TenantId::new(TID), LEDGER.to_string()))
            .expect("config");
        let view = core.decode_stored_document(config, &bytes).expect("decode");
        let link = crate::data::executor::enforcement::hash_chain::stored_link(&view)
            .expect("a linked row");
        (link.seq, link.hash)
    }

    /// Commit `plans` as the slice at `epoch`. Returns the link of each
    /// plan's staged image, in plan order, and the links of the rows the
    /// install answered.
    fn commit(core: &mut CoreLoop, plans: &[PhysicalPlan], epoch: u64) -> (Vec<Link>, Vec<Link>) {
        let (staged, installed) = core.calvin_commit_staged_for_test(
            &make_default_task(),
            TID,
            plans,
            (epoch, 0),
            epoch * 100,
        );
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let installed = installed.expect("the stage admitted the slice");
        assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
        assert!(installed.error_code.is_none(), "{:?}", installed.error_code);
        let images: StagedReturningImages =
            zerompk::from_msgpack(staged.payload.as_bytes()).expect("stage images");
        let image_links = images
            .plans
            .iter()
            .flat_map(|plan| plan.rows.iter().map(|row| link_of(&row.fields)))
            .collect();
        let payload: RowsPayload =
            zerompk::from_msgpack(installed.payload.as_bytes()).expect("RETURNING rows");
        let reply_links = payload
            .rows
            .iter()
            .map(|row| {
                let fields = Value::Object(
                    payload
                        .columns
                        .iter()
                        .cloned()
                        .zip(row.iter().map(|cell| cell.0.clone()))
                        .collect(),
                );
                link_of(&fields)
            })
            .collect();
        (image_links, reply_links)
    }

    /// The reply of a chained insert carries the link its install stores.
    #[test]
    fn a_chained_returning_carries_the_installed_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        chain_ledger(&mut core);

        let (images, reply) = commit(&mut core, &[insert("l1", 7, "first")], 1);

        let stored = stored_link(&core, 7);
        assert_eq!(stored.0, 1);
        assert_eq!(reply, vec![stored.clone()]);
        assert_eq!(images, vec![stored]);
    }

    /// Two chained inserts, one after the other in Calvin order, each answer
    /// their own link: the second links after the first.
    #[test]
    fn two_chained_inserts_each_answer_their_own_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        chain_ledger(&mut core);

        let (_, first) = commit(&mut core, &[insert("l1", 7, "first")], 1);
        let (_, second) = commit(&mut core, &[insert("l2", 8, "second")], 2);

        let (stored_first, stored_second) = (stored_link(&core, 7), stored_link(&core, 8));
        assert_eq!(first, vec![stored_first.clone()]);
        assert_eq!(second, vec![stored_second.clone()]);
        assert_eq!(stored_second.0, stored_first.0 + 1);
        assert_ne!(stored_first.1, stored_second.1);
    }

    /// The install links a slice's puts in row-identity order. A later plan
    /// that writes a row sorting first links first, and each plan's image
    /// carries the link the install stores for its row.
    #[test]
    fn plans_of_one_slice_answer_the_links_of_the_install_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        chain_ledger(&mut core);

        let (images, reply) = commit(
            &mut core,
            &[
                insert("l2", 8, "written first"),
                insert("l1", 7, "sorts first"),
            ],
            1,
        );

        let (l1, l2) = (stored_link(&core, 7), stored_link(&core, 8));
        assert_eq!((l1.0, l2.0), (1, 2), "the install links l1 first");
        assert_eq!(images, vec![l2, l1.clone()], "each plan keeps its own row");
        assert_eq!(reply, vec![l1], "the last RETURNING plan answers");
    }
}
