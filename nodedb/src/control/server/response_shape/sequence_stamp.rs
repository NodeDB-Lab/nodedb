// SPDX-License-Identifier: BUSL-1.1

//! Sequence stamps: one `nextval` value per output row, written at the
//! response boundary.
//!
//! A projection item `nextval('<literal>')` is carried by the plan as a
//! sequence projection and announced in the output schema through
//! [`OutputColumn::sequence`](super::schema::OutputColumn). The Data Plane
//! evaluates nothing for it: it holds no sequence state and answered NULL for
//! every row before this path existed.
//!
//! [`SequenceStamper`] allocates the values on the control plane and writes
//! one cell per row, in output order, after every predicate and projection —
//! the same boundary the declared-instant conversion uses, so one place
//! decides what a client sees.
//!
//! Allocation goes through [`SequenceRegistry::nextval_batch`]: one atomic
//! batch per stamped column per row set. A retried statement can leave a gap;
//! PostgreSQL accepts gaps for the same reason.
//!
//! `currval` is not recorded here: the shaper holds no session map. Threading
//! the session recorder from the pgwire dispatch is a later slice.

use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::control::sequence::{SequenceError, SequenceRegistry};

/// Allocates and writes sequence stamps for one shaped row set.
pub struct SequenceStamper {
    registry: Arc<SequenceRegistry>,
    database_id: u64,
    tenant_id: u64,
}

impl SequenceStamper {
    pub(in crate::control::server) fn new(
        registry: Arc<SequenceRegistry>,
        database_id: u64,
        tenant_id: u64,
    ) -> Self {
        Self {
            registry,
            database_id,
            tenant_id,
        }
    }

    /// Write one freshly allocated value into every row's cell for each
    /// stamped column, in row order.
    pub(super) fn stamp(
        &self,
        rows: &mut [Map<String, JsonValue>],
        keys: &[String],
        sequences: &[Option<String>],
    ) -> crate::Result<()> {
        for (key, sequence) in keys.iter().zip(sequences.iter()) {
            let Some(name) = sequence else {
                continue;
            };
            let values = self
                .registry
                .nextval_batch(self.database_id, self.tenant_id, name, rows.len())
                .map_err(|e| match e {
                    SequenceError::NotFound { .. } => crate::Error::UndefinedObject {
                        kind: "sequence",
                        name: name.clone(),
                    },
                    other => crate::Error::Internal {
                        detail: format!("nextval('{name}'): {other}"),
                    },
                })?;
            for (row, value) in rows.iter_mut().zip(values) {
                row.insert(key.clone(), JsonValue::from(value));
            }
        }
        Ok(())
    }
}
