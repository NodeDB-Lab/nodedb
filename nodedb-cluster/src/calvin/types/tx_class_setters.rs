// SPDX-License-Identifier: BUSL-1.1

//! Field setters of [`TxClass`], and the order check of its `body_plans`.

use crate::error::CalvinError;

use super::lock_wire::TxnIdWire;
use super::transaction::{CalvinIncarnation, TxClass};

impl TxClass {
    /// Set the lock-table owner id propagated to `SequencedTxn.lock_owner`.
    pub fn set_lock_owner(&mut self, owner: Option<TxnIdWire>) {
        self.lock_owner = owner;
    }

    /// Set the WAL event-source code the transaction's writes carry.
    pub fn set_event_source(&mut self, code: u8) {
        self.event_source = code;
    }

    /// Set the roles of the identity whose writes the transaction commits.
    pub fn set_user_roles(&mut self, user_roles: Vec<String>) {
        self.user_roles = user_roles;
    }

    /// Mark the transaction as the re-issue of RESTORE `restore_id`.
    pub fn set_restore_id(&mut self, restore_id: u64) {
        self.restore_id = restore_id;
    }

    /// Set the indexes into `plans` of the plans a trigger body buffered.
    /// Sorts and deduplicates them, so readers find an index by binary search.
    pub fn set_body_plans(&mut self, mut body_plans: Vec<u32>) {
        body_plans.sort_unstable();
        body_plans.dedup();
        self.body_plans = body_plans;
    }

    /// Set the encoded messages the transaction's trigger bodies published.
    pub fn set_publishes(&mut self, publishes: Vec<u8>) {
        self.publishes = publishes;
    }

    /// Set the encoded dedup key of the cross-shard request the transaction
    /// applies, and the vShard the request addresses.
    pub fn set_applied_key(&mut self, applied_key: Vec<u8>, vshard: u32) {
        self.applied_key = applied_key;
        self.applied_key_vshard = vshard;
    }

    /// Set the collections the plans name, with their planned incarnations.
    pub fn set_incarnations(&mut self, incarnations: Vec<CalvinIncarnation>) {
        self.incarnations = incarnations;
    }

    /// Refuse `body_plans` that are out of order or repeat an index.
    pub(super) fn check_body_plans(&self) -> Result<(), CalvinError> {
        match self
            .body_plans
            .windows(2)
            .position(|pair| pair[0] >= pair[1])
        {
            Some(at) => Err(CalvinError::BodyPlansUnsorted { index: at + 1 }),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::TenantId;

    use super::*;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, VersionedReadSet};

    fn class() -> TxClass {
        TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![EngineKeySet::Document {
                collection: "c".to_owned(),
                surrogates: SortedVec::new(vec![1]),
            }]),
            Vec::new(),
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid class")
    }

    #[test]
    fn set_body_plans_sorts_and_deduplicates() {
        let mut tx = class();
        tx.set_body_plans(vec![4, 1, 4, 0]);
        assert_eq!(tx.body_plans, vec![0, 1, 4]);
        assert!(tx.restore_derived().is_ok());
    }

    #[test]
    fn a_decoded_class_with_disordered_body_plans_is_refused() {
        let mut tx = class();
        tx.body_plans = vec![2, 1];
        assert_eq!(
            tx.restore_derived(),
            Err(CalvinError::BodyPlansUnsorted { index: 1 })
        );
        tx.body_plans = vec![1, 3, 3];
        assert_eq!(
            tx.restore_derived(),
            Err(CalvinError::BodyPlansUnsorted { index: 2 })
        );
    }
}
