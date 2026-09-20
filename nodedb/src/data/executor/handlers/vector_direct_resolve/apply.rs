// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `VectorOp::ResolvedDirectWrite`. Runs on every
//! replica; the plan carries the decided verdict and mutations, nothing
//! recomputed.
//!
//! Drift check: every mutation's pre-image is checked against the stored
//! sidecar before the first mutation runs, all-or-nothing; a failure returns
//! `OllpRetryRequired` — same contract the KV resolved-write apply uses.
//! WAL replay applies the same mutations through
//! [`CoreLoop::apply_vector_resolved_mutations`] with no drift check: a
//! replayed record is the committed history, not a proposal.

use std::collections::HashMap;

use nodedb_physical::physical_plan::VectorResolvedMutation;
use nodedb_types::{RlsWriteCheck, Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::vector_direct_row::{
    VectorDirectIndexSpec, VectorDirectRowWrite, VectorIndexKey,
};
use crate::data::executor::handlers::vector_direct_update::VectorPlannedRow;
use crate::data::executor::handlers::vector_upsert::decode_payload_lowercased;
use crate::data::executor::task::ExecutionTask;

/// The index a resolved write lands in, with the settings a first write
/// registers on a new index. The vector width comes from each mutation.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct VectorResolvedIndexSpec<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

impl VectorResolvedIndexSpec<'_> {
    fn with_dim(&self, dim: usize) -> VectorDirectIndexSpec<'_> {
        VectorDirectIndexSpec {
            collection: self.collection,
            field: self.field,
            dim,
            quantization: self.quantization,
            storage_dtype: self.storage_dtype,
            payload_indexes: self.payload_indexes,
        }
    }
}

/// Parameters for [`CoreLoop::execute_vector_resolved_direct_write`].
pub(in crate::data::executor) struct VectorResolvedApplyParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub index: VectorResolvedIndexSpec<'a>,
    pub mutations: &'a [VectorResolvedMutation],
    pub response_payload: &'a [u8],
    /// Carries the verdict the Control Plane decided; the gate stays on
    /// every write path even as a no-op here.
    pub rls_write_check: &'a RlsWriteCheck,
}

impl CoreLoop {
    /// Handle `VectorOp::ResolvedDirectWrite`: check every pre-image, apply
    /// every mutation, and return the shipped payload verbatim.
    pub(in crate::data::executor) fn execute_vector_resolved_direct_write(
        &mut self,
        params: VectorResolvedApplyParams<'_>,
    ) -> Response {
        let VectorResolvedApplyParams {
            task,
            tid,
            index,
            mutations,
            response_payload,
            rls_write_check,
        } = params;
        debug!(
            core = self.core_id,
            collection = %index.collection,
            count = mutations.len(),
            "vector resolved direct write"
        );
        let database_id = task.request.database_id.as_u64();
        if let Err(e) = self.check_vector_resolved_preconditions(database_id, tid, index, mutations)
        {
            return self.response_error(task, e);
        }
        let touched = match self.apply_vector_resolved_mutations(
            task,
            tid,
            index,
            mutations,
            rls_write_check,
        ) {
            Ok(touched) => touched,
            Err(e) => return self.response_error(task, e),
        };
        if !touched.is_empty() {
            let index_key =
                CoreLoop::vector_index_key(database_id, tid, index.collection, index.field);
            self.finish_vector_direct_write(task, &index_key, tid, index.collection, &touched);
        }
        self.response_with_payload(task, response_payload.to_vec())
    }

    /// Refuse the whole write when any row moved past what the resolve read.
    /// A vector that no longer fits the index is the same constraint error
    /// the live write reports.
    fn check_vector_resolved_preconditions(
        &self,
        database_id: u64,
        tid: u64,
        index: VectorResolvedIndexSpec<'_>,
        mutations: &[VectorResolvedMutation],
    ) -> Result<(), ErrorCode> {
        let index_key = CoreLoop::vector_index_key(database_id, tid, index.collection, index.field);
        for mutation in mutations {
            if let Some(vector) = mutation.stored_vector() {
                self.check_vector_direct_index(database_id, tid, &index.with_dim(vector.len()))?;
            }
            let surrogate = mutation.surrogate();
            let bound = self.vector_direct_node(&index_key, surrogate).is_some();
            let stored = if bound {
                self.vector_sidecar_bytes(database_id, tid, index.collection, surrogate)?
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let unchanged = match mutation {
                VectorResolvedMutation::Delete { old_payload, .. }
                | VectorResolvedMutation::Update { old_payload, .. } => {
                    bound && stored == *old_payload
                }
                VectorResolvedMutation::Upsert {
                    old_payload: None, ..
                } => !bound,
                VectorResolvedMutation::Upsert {
                    old_payload: Some(old),
                    ..
                } => bound && stored == *old,
            };
            if !unchanged {
                return Err(ErrorCode::OllpRetryRequired);
            }
        }
        Ok(())
    }

    /// Apply every mutation in order and return the surrogates touched. A
    /// `Delete` / `Update` whose node is gone applies nothing, so a replay
    /// over a restored checkpoint is idempotent; an `Upsert` always lands.
    pub(in crate::data::executor) fn apply_vector_resolved_mutations(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        index: VectorResolvedIndexSpec<'_>,
        mutations: &[VectorResolvedMutation],
        rls_write_check: &RlsWriteCheck,
    ) -> Result<Vec<Surrogate>, ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        let collection = index.collection;
        let probe_key = CoreLoop::vector_index_key(database_id, tid, collection, index.field);
        let mut touched: Vec<Surrogate> = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            match mutation {
                VectorResolvedMutation::Delete { surrogate, .. } => {
                    if self
                        .remove_vector_direct_row(&probe_key, tid, collection, *surrogate)?
                        .is_some()
                    {
                        touched.push(*surrogate);
                    }
                }
                VectorResolvedMutation::Update {
                    surrogate,
                    new_vector,
                    merged_payload,
                    old_payload,
                } => {
                    if self.vector_direct_node(&probe_key, *surrogate).is_none() {
                        continue;
                    }
                    let index_key: VectorIndexKey = match new_vector {
                        Some(vector) => {
                            self.vector_direct_index(task, tid, &index.with_dim(vector.len()))?
                        }
                        None => probe_key.clone(),
                    };
                    let fields =
                        decode_resolved_payload(merged_payload, rls_write_check, tid, collection)?;
                    let row = VectorPlannedRow {
                        surrogate: *surrogate,
                        fields,
                        sidecar: merged_payload.clone(),
                        old_sidecar: old_payload.clone(),
                    };
                    self.apply_vector_direct_update_row(
                        task,
                        &index_key,
                        tid,
                        collection,
                        &row,
                        new_vector.as_deref(),
                    )?;
                    touched.push(*surrogate);
                }
                VectorResolvedMutation::Upsert {
                    surrogate,
                    pk_bytes: _,
                    vector,
                    payload,
                    old_payload: _,
                } => {
                    let index_key =
                        self.vector_direct_index(task, tid, &index.with_dim(vector.len()))?;
                    let fields =
                        decode_resolved_payload(payload, rls_write_check, tid, collection)?;
                    if self.vector_direct_node(&index_key, *surrogate).is_some() {
                        self.remove_vector_direct_row(&index_key, tid, collection, *surrogate)?;
                    }
                    self.write_vector_direct_row(VectorDirectRowWrite {
                        task,
                        index_key: &index_key,
                        tid,
                        collection,
                        surrogate: *surrogate,
                        vector,
                        fields: &fields,
                        sidecar: payload,
                    })?;
                    touched.push(*surrogate);
                }
            }
        }
        Ok(touched)
    }
}

/// Decode a shipped sidecar into the field map the bitmap indexes are built
/// from, running the (already decided) write gate over the same image.
fn decode_resolved_payload(
    payload: &[u8],
    rls_write_check: &RlsWriteCheck,
    tid: u64,
    collection: &str,
) -> Result<HashMap<String, Value>, ErrorCode> {
    let fields = decode_payload_lowercased(payload).map_err(|e| ErrorCode::Internal {
        detail: format!("vector resolved write on '{collection}': sidecar decode: {e}"),
    })?;
    rls_write_gate::admit_document_value(
        rls_write_check,
        &Value::Object(fields.clone()),
        tid,
        collection,
    )?;
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{
        VectorDirectWriteIntent, VectorOp, VectorResolveOutcome, VectorResolvedMutation,
        VectorWriteTargets,
    };
    use nodedb_types::{RlsWriteCheck, Surrogate, Value};

    use crate::bridge::envelope::{
        Admission, ErrorCode, ExemptReason, PhysicalPlan, Priority, Request, Response, Status,
    };
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::vector_upsert::VectorDirectUpsertParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    use super::{VectorResolvedApplyParams, VectorResolvedIndexSpec};

    const TID: u64 = 1;
    const COLLECTION: &str = "vp_resolved";
    const FIELD: &str = "emb";

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(VectorOp::Search {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                metric: nodedb_types::vector_distance::DistanceMetric::L2,
                filter_bitmap: None,
                field_name: String::new(),
                rls_filters: Vec::new(),
                inline_prefilter_plan: None,
                ann_options: Default::default(),
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: Some(Lsn::new(7)),
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    fn payload(owner: &str) -> Vec<u8> {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert("owner".into(), Value::String(owner.into()));
        zerompk::to_msgpack_vec(&map).expect("encode payload")
    }

    /// `owner = <owner>` as the injected write predicate.
    fn owner_policy(owner: &str) -> RlsWriteCheck {
        let filter = ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        RlsWriteCheck::from_injected(zerompk::to_msgpack_vec(&vec![filter]).expect("encode"))
    }

    fn seed(h: &mut CoreHarness, surrogate: Surrogate, vector: &[f32], owner: &str) {
        let t = task();
        let resp = h
            .core
            .execute_vector_direct_upsert(VectorDirectUpsertParams {
                task: &t,
                tid: TID,
                collection: COLLECTION,
                field: FIELD,
                surrogate,
                vector,
                payload: &payload(owner),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
                intent: VectorDirectWriteIntent::Insert,
                on_conflict_updates: &[],
                rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
                returning: None,
                rls_filters: &[],
            });
        assert_eq!(
            resp.status,
            Status::Ok,
            "seed {surrogate:?}: {:?}",
            resp.error_code
        );
    }

    fn stored_owner(h: &CoreHarness, surrogate: Surrogate) -> Option<String> {
        let row = h
            .core
            .vector_sidecar_row(DatabaseId::DEFAULT.as_u64(), TID, COLLECTION, surrogate)
            .expect("read sidecar")?;
        match row.fields.get("owner") {
            Some(Value::String(s)) => Some(s.to_string()),
            _ => None,
        }
    }

    fn delete_everything(rls_write_check: RlsWriteCheck) -> VectorOp {
        VectorOp::DirectDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            field: FIELD.into(),
            targets: VectorWriteTargets::Predicate(Vec::new()),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check,
        }
    }

    fn resolve(h: &CoreHarness, op: &VectorOp) -> Response {
        let t = task();
        h.core.execute_vector_resolve_direct_write(&t, TID, op)
    }

    fn decode(resp: &Response) -> VectorResolveOutcome {
        assert_eq!(
            resp.status,
            Status::Ok,
            "resolve failed: {:?}",
            resp.error_code
        );
        zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode resolve outcome")
    }

    fn apply(h: &mut CoreHarness, outcome: &VectorResolveOutcome) -> Response {
        let t = task();
        h.core
            .execute_vector_resolved_direct_write(VectorResolvedApplyParams {
                task: &t,
                tid: TID,
                index: VectorResolvedIndexSpec {
                    collection: COLLECTION,
                    field: FIELD,
                    quantization: nodedb_types::VectorQuantization::None,
                    storage_dtype: nodedb_types::VectorStorageDtype::F32,
                    payload_indexes: &[],
                },
                mutations: &outcome.mutations,
                response_payload: &outcome.response_payload,
                rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
            })
    }

    fn affected(resp: &Response) -> u64 {
        crate::control::server::shared::sql::staging_predicates::extract_affected_count(
            resp.payload.as_bytes(),
        )
        .expect("affected count")
    }

    #[test]
    fn resolve_reads_only_and_apply_removes_the_resolved_rows() {
        let mut h = make_core();
        seed(&mut h, Surrogate::new(1), &[1.0, 0.0], "alice");
        seed(&mut h, Surrogate::new(2), &[0.0, 1.0], "alice");

        let outcome = decode(&resolve(&h, &delete_everything(owner_policy("alice"))));
        assert_eq!(outcome.mutations.len(), 2);
        assert_eq!(
            stored_owner(&h, Surrogate::new(1)).as_deref(),
            Some("alice")
        );
        assert_eq!(
            stored_owner(&h, Surrogate::new(2)).as_deref(),
            Some("alice")
        );

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(affected(&resp), 2, "the shipped reply is returned verbatim");
        assert_eq!(stored_owner(&h, Surrogate::new(1)), None);
        assert_eq!(stored_owner(&h, Surrogate::new(2)), None);
    }

    #[test]
    fn a_row_outside_the_policy_refuses_the_whole_resolve() {
        let mut h = make_core();
        seed(&mut h, Surrogate::new(1), &[1.0, 0.0], "alice");
        seed(&mut h, Surrogate::new(2), &[0.0, 1.0], "bob");

        let resp = resolve(&h, &delete_everything(owner_policy("alice")));
        assert_eq!(resp.status, Status::Error);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedAuthz { .. })
        ));
        assert_eq!(
            stored_owner(&h, Surrogate::new(1)).as_deref(),
            Some("alice")
        );
        assert_eq!(stored_owner(&h, Surrogate::new(2)).as_deref(), Some("bob"));
    }

    #[test]
    fn drifted_pre_image_retries_and_mutates_nothing() {
        let mut h = make_core();
        seed(&mut h, Surrogate::new(1), &[1.0, 0.0], "alice");
        seed(&mut h, Surrogate::new(2), &[0.0, 1.0], "alice");

        let outcome = decode(&resolve(&h, &delete_everything(owner_policy("alice"))));

        // A concurrent write lands on the SECOND row between resolve and apply.
        let t = task();
        let resp = h
            .core
            .execute_vector_direct_upsert(VectorDirectUpsertParams {
                task: &t,
                tid: TID,
                collection: COLLECTION,
                field: FIELD,
                surrogate: Surrogate::new(2),
                vector: &[0.5, 0.5],
                payload: &payload("carol"),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
                intent: VectorDirectWriteIntent::Upsert,
                on_conflict_updates: &[],
                rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
                returning: None,
                rls_filters: &[],
            });
        assert_eq!(resp.status, Status::Ok);

        let resp = apply(&mut h, &outcome);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "a drifted pre-image must yield OllpRetryRequired"
        );
        assert_eq!(
            stored_owner(&h, Surrogate::new(1)).as_deref(),
            Some("alice"),
            "the first row must survive: nothing applies when any row drifted"
        );
        assert_eq!(
            stored_owner(&h, Surrogate::new(2)).as_deref(),
            Some("carol")
        );
    }

    #[test]
    fn an_upsert_resolved_against_an_absent_row_requires_it_to_stay_absent() {
        let mut h = make_core();
        seed(&mut h, Surrogate::new(1), &[1.0, 0.0], "alice");

        let outcome = VectorResolveOutcome {
            mutations: vec![VectorResolvedMutation::Upsert {
                surrogate: Surrogate::new(9),
                pk_bytes: b"r9".to_vec(),
                vector: vec![0.0, 1.0],
                payload: payload("alice"),
                old_payload: None,
            }],
            response_payload: Vec::new(),
        };
        seed(&mut h, Surrogate::new(9), &[0.0, 1.0], "bob");

        let resp = apply(&mut h, &outcome);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert_eq!(stored_owner(&h, Surrogate::new(9)).as_deref(), Some("bob"));
    }

    #[test]
    fn a_resolved_update_rewrites_the_sidecar_and_keeps_the_node() {
        let mut h = make_core();
        seed(&mut h, Surrogate::new(1), &[1.0, 0.0], "alice");
        let index_key =
            CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, COLLECTION, FIELD);
        let node_before = h
            .core
            .vector_direct_node(&index_key, Surrogate::new(1))
            .expect("bound");

        let op = VectorOp::DirectUpdate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            field: FIELD.into(),
            targets: VectorWriteTargets::Surrogates(vec![Surrogate::new(1)]),
            new_vector: None,
            payload_patch: vec![(
                "owner".into(),
                nodedb_physical::physical_plan::UpdateValue::Literal(
                    nodedb_types::value_to_msgpack(&Value::String("alice2".into()))
                        .expect("encode literal"),
                ),
            )],
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: owner_policy("alice2"),
        };
        let outcome = decode(&resolve(&h, &op));
        assert_eq!(outcome.mutations.len(), 1);
        assert_eq!(
            stored_owner(&h, Surrogate::new(1)).as_deref(),
            Some("alice"),
            "resolve is read-only"
        );

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(affected(&resp), 1);
        assert_eq!(
            stored_owner(&h, Surrogate::new(1)).as_deref(),
            Some("alice2")
        );
        assert_eq!(
            h.core.vector_direct_node(&index_key, Surrogate::new(1)),
            Some(node_before),
            "a payload-only update keeps the HNSW node"
        );
    }
}
