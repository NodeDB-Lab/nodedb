// SPDX-License-Identifier: BUSL-1.1

//! KV atomic operation handlers: Incr, IncrFloat, Cas, GetSet.

use nodedb_physical::physical_plan::KvCounterShape;
use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_shape};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::engine::kv::{AtomicError, Incremented};

/// Shared identity context for a single-key KV atomic operation
/// (INCR_FLOAT / GETSET) dispatched to this core.
pub(in crate::data::executor) struct KvAtomicCtx<'a> {
    pub(in crate::data::executor) task: &'a ExecutionTask,
    pub(in crate::data::executor) did: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    pub(in crate::data::executor) key: &'a [u8],
    pub(in crate::data::executor) surrogate: nodedb_types::Surrogate,
    /// Compiled row-level-security WRITE predicate from the plan. Every
    /// handler reading this field decides the image it is about to persist
    /// against it.
    pub(in crate::data::executor) rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// The `ErrorCode` an atomic that computed or stored no value answers with.
pub(in crate::data::executor) fn atomic_error_code(
    error: AtomicError,
    collection: &str,
) -> ErrorCode {
    match error {
        AtomicError::TypeMismatch { detail } => ErrorCode::TypeMismatch {
            collection: collection.to_string(),
            detail,
        },
        AtomicError::Counter(fault) => ErrorCode::CounterFault {
            collection: collection.to_string(),
            fault,
        },
        AtomicError::Encode { detail } => ErrorCode::Internal { detail },
        // Nothing was written: the engine consults the gate before it
        // installs the computed value.
        AtomicError::Rejected(error) => (*error).into(),
    }
}

/// The reply to an `INCR_FLOAT`: the new value as a number, and for a raw
/// body also the stored text. RESP answers with the text, so its reply is
/// byte for byte what `GET` returns.
pub(in crate::data::executor) fn incr_float_reply(value: f64, written: &[u8]) -> serde_json::Value {
    match std::str::from_utf8(written) {
        Ok(text) if kv_body_shape(written) == KvBodyShape::Raw => {
            serde_json::json!({ "value": value, "text": text })
        }
        _ => serde_json::json!({ "value": value }),
    }
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_incr(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        shape: &KvCounterShape,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, delta, "kv incr");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        // `Incr` carries a TTL that installs a new absolute `expire_at_ms`
        // when `ttl_ms > 0` (see `atomic_put`), so the live-installed instant
        // must be the same one `wal_append_kv_op` resolved and recorded —
        // see `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms: u64 = self.kv_ttl_now_ms(task);
        // The engine computes the post-image and installs it in one pass, so
        // the write policy is handed in and decided on the computed bytes
        // rather than on a duplicate of the increment arithmetic out here.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        match self.kv_engine.incr(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
            ttl_ms,
            shape,
            &admit,
        ) {
            Ok(Incremented { value, written }) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                // The event carries the bytes the engine stored: the whole row
                // for a typed row, the decimal text for a raw body.
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    Some(written.as_slice()),
                    None,
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
                match response_codec::encode_json_as_msgpack(&serde_json::json!({ "value": value }))
                {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(error) => self.response_atomic_error(task, collection, error),
        }
    }

    pub(in crate::data::executor) fn execute_kv_incr_float(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: &str,
        shape: &KvCounterShape,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, %delta, "kv incr_float");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        // Same engine-internal compute-and-persist as `Incr` — see there.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        match self.kv_engine.incr_float(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
            shape,
            &admit,
        ) {
            Ok(Incremented { value, written }) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                // The event carries the bytes the engine stored: the whole row
                // for a typed row, the decimal text for a raw body.
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    Some(written.as_slice()),
                    None,
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
                match response_codec::encode_json_as_msgpack(&incr_float_reply(value, &written)) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(error) => self.response_atomic_error(task, collection, error),
        }
    }

    pub(in crate::data::executor) fn execute_kv_cas(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv cas");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        // A swap into a typed row stores the row with one column replaced, not
        // `new_value` itself, so the policy decides the image the engine
        // computes — see `Incr`.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        let result = match self.kv_engine.cas(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            expected,
            new_value,
            &admit,
        ) {
            Ok(result) => result,
            Err(error) => return self.response_atomic_error(task, collection, error),
        };

        if let Some(written) = &result.written {
            if let Some(ref m) = self.metrics {
                m.record_kv_put();
            }
            let key_str = String::from_utf8_lossy(key);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Update,
                crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                Some(written.as_slice()),
                None,
            );
            self.note_kv_write_lsn(task, did, tid, collection, key);
        }

        let current_b64 = result
            .current_value
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json_as_msgpack(&serde_json::json!({
            "success": result.success(),
            "current_value": current_b64,
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// `rls_filters` decides the OLD value handed back: `GETSET` is a read as
    /// much as a write, so a row the read policy hides must come back absent
    /// rather than being disclosed by the write that replaced it. The write
    /// half is a separate decision on the image the write stores.
    pub(in crate::data::executor) fn execute_kv_getset(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        new_value: &[u8],
        rls_filters: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv getset");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        // A write into a typed row stores the row with one column replaced, so
        // the policy decides the image the engine computes — see `Incr`.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        let crate::engine::kv::GetSetResult { old, written } = match self.kv_engine.getset(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            new_value,
            &admit,
        ) {
            Ok(result) => result,
            Err(error) => return self.response_atomic_error(task, collection, error),
        };

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(written.as_slice()),
            old.as_deref(),
        );
        self.note_kv_write_lsn(task, did, tid, collection, key);

        // A row the read policy excludes is reported exactly as an absent row,
        // the same convention `execute_kv_get` uses — the caller cannot tell it
        // apart from a key that never existed, so the reply discloses nothing.
        // A filter that fails to evaluate withholds the value too: an old value
        // the policy could not be decided against is not one it cleared.
        let disclosable_old = match &old {
            Some(bytes) => match self.row_passes_rls(bytes, rls_filters) {
                Ok(true) => old.as_deref(),
                Ok(false) => None,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            },
            None => None,
        };

        let old_b64 = disclosable_old
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "old_value": old_b64 })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// The error response for an atomic that computed or stored no value.
    pub(in crate::data::executor) fn response_atomic_error(
        &self,
        task: &ExecutionTask,
        collection: &str,
        error: AtomicError,
    ) -> Response {
        self.response_error(task, atomic_error_code(error, collection))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{KvCounterShape, KvOp};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};

    use super::KvAtomicCtx;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::event::WriteOp;
    use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
    use crate::types::{DatabaseId, TenantId, VShardId};

    const TID: u64 = 1;
    const COLLECTION: &str = "kv_counters";

    struct CoreHarness {
        core: CoreLoop,
        events: EventConsumerRx,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    /// A core whose write events land on `events`.
    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let mut core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        CoreHarness {
            core,
            events: consumers.pop().expect("consumer"),
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn did() -> u64 {
        DatabaseId::DEFAULT.as_u64()
    }

    fn task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Kv(KvOp::Get {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"seed".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            }),
            None,
        )
    }

    fn seed(core: &mut CoreLoop, key: &[u8], value: &[u8]) {
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did(),
            tenant_id: TID,
            collection: COLLECTION,
            key,
            value,
            ttl_ms: 0,
            now_ms: crate::engine::kv::current_ms(),
            surrogate: Surrogate::new(1),
        });
    }

    fn stored(core: &CoreLoop, key: &[u8]) -> Vec<u8> {
        core.kv_engine
            .get(did(), TID, COLLECTION, key, crate::engine::kv::current_ms())
            .expect("the key holds a value")
    }

    fn typed_row(fields: &[(&str, Value)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode row")
    }

    fn columns(bytes: &[u8]) -> HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(bytes).expect("decode row") {
            Value::Object(map) => map,
            other => panic!("expected a typed row, got {other:?}"),
        }
    }

    fn ctx<'a>(
        task: &'a ExecutionTask,
        key: &'a [u8],
        check: &'a RlsWriteCheck,
    ) -> KvAtomicCtx<'a> {
        KvAtomicCtx {
            task,
            did: did(),
            tid: TID,
            collection: COLLECTION,
            key,
            surrogate: Surrogate::new(1),
            rls_write_check: check,
        }
    }

    #[test]
    fn incr_on_a_typed_row_emits_the_whole_stored_row() {
        let mut h = make_core();
        seed(
            &mut h.core,
            b"player",
            &typed_row(&[
                ("label", Value::String("gold".into())),
                ("n", Value::Integer(5)),
            ]),
        );

        let t = task();
        let check = RlsWriteCheck::already_decided_elsewhere();
        let resp = h
            .core
            .execute_kv_incr(ctx(&t, b"player", &check), 3, 0, &KvCounterShape::Raw);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        let event = h.events.try_recv().expect("INCR emits a write event");
        assert_eq!(event.op, WriteOp::Update);
        let new_value = event.new_value.expect("the event carries the new row");
        assert_eq!(
            new_value.as_ref(),
            stored(&h.core, b"player").as_slice(),
            "the event carries exactly the bytes the engine stored"
        );
        let row = columns(&new_value);
        assert_eq!(row.get("n"), Some(&Value::Integer(8)));
        assert_eq!(row.get("label"), Some(&Value::String("gold".into())));
    }

    #[test]
    fn incr_float_on_a_typed_row_emits_the_whole_stored_row() {
        let mut h = make_core();
        seed(
            &mut h.core,
            b"player",
            &typed_row(&[
                ("label", Value::String("gold".into())),
                ("score", Value::Float(1.5)),
            ]),
        );

        let t = task();
        let check = RlsWriteCheck::already_decided_elsewhere();
        let resp =
            h.core
                .execute_kv_incr_float(ctx(&t, b"player", &check), "1", &KvCounterShape::Raw);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        let event = h.events.try_recv().expect("INCR_FLOAT emits a write event");
        let new_value = event.new_value.expect("the event carries the new row");
        assert_eq!(new_value.as_ref(), stored(&h.core, b"player").as_slice());
        let row = columns(&new_value);
        assert_eq!(row.get("score"), Some(&Value::Float(2.5)));
        assert_eq!(row.get("label"), Some(&Value::String("gold".into())));
    }

    #[test]
    fn incr_on_a_raw_body_emits_the_stored_decimal_text() {
        let mut h = make_core();
        seed(&mut h.core, b"hits", b"41");

        let t = task();
        let check = RlsWriteCheck::already_decided_elsewhere();
        let resp = h
            .core
            .execute_kv_incr(ctx(&t, b"hits", &check), 1, 0, &KvCounterShape::Raw);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        let event = h.events.try_recv().expect("INCR emits a write event");
        assert_eq!(event.new_value.as_deref(), Some(b"42".as_slice()));
        assert_eq!(stored(&h.core, b"hits"), b"42".to_vec());
    }

    #[test]
    fn incr_on_raw_text_that_is_not_an_integer_answers_the_counter_fault() {
        let mut h = make_core();
        seed(&mut h.core, b"name", b"abc");

        let t = task();
        let check = RlsWriteCheck::already_decided_elsewhere();
        let resp = h
            .core
            .execute_kv_incr(ctx(&t, b"name", &check), 1, 0, &KvCounterShape::Raw);
        assert_eq!(resp.status, Status::Error);
        assert_eq!(
            resp.error_code.map(|code| *code),
            Some(crate::bridge::envelope::ErrorCode::CounterFault {
                collection: COLLECTION.into(),
                fault: crate::bridge::envelope::CounterFault::NotAnInteger,
            })
        );
        assert!(
            h.events.try_recv().is_none(),
            "a refused INCR emits no event"
        );
        assert_eq!(stored(&h.core, b"name"), b"abc".to_vec());
    }
}
