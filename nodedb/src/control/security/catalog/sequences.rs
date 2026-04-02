//! Sequence metadata operations for the system catalog.

use super::sequence_types::{SequenceState, StoredSequence};
use super::types::{SEQUENCE_STATE, SEQUENCES, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Store a sequence definition.
    ///
    /// Key format: `"{tenant_id}:{name}"`.
    pub fn put_sequence(&self, seq: &StoredSequence) -> crate::Result<()> {
        let key = sequence_key(seq.tenant_id, &seq.name);
        let bytes = rmp_serde::to_vec(seq).map_err(|e| catalog_err("serialize sequence", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SEQUENCES)
                .map_err(|e| catalog_err("open sequences", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert sequence", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a sequence definition by tenant and name.
    pub fn get_sequence(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<StoredSequence>> {
        let key = sequence_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get sequence", e))?
        {
            Some(value) => {
                let seq = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deserialize sequence", e))?;
                Ok(Some(seq))
            }
            None => Ok(None),
        }
    }

    /// Delete a sequence definition. Returns true if it existed.
    pub fn delete_sequence(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = sequence_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(SEQUENCES)
                .map_err(|e| catalog_err("open sequences", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete sequence", e))?
                .is_some();
            // Also remove the runtime state.
            let mut state_table = write_txn
                .open_table(SEQUENCE_STATE)
                .map_err(|e| catalog_err("open sequence_state", e))?;
            let _ = state_table.remove(key.as_str());
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all sequences for a tenant.
    pub fn load_sequences_for_tenant(&self, tenant_id: u32) -> crate::Result<Vec<StoredSequence>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;

        let mut sequences = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range sequences", e))?;
        while let Some(Ok((key, value))) = range.next() {
            if key.value().starts_with(&prefix)
                && let Ok(seq) = rmp_serde::from_slice::<StoredSequence>(value.value())
            {
                sequences.push(seq);
            }
        }
        Ok(sequences)
    }

    /// Load all sequences across all tenants.
    pub fn load_all_sequences(&self) -> crate::Result<Vec<StoredSequence>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;

        let mut sequences = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range sequences", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(seq) = rmp_serde::from_slice::<StoredSequence>(value.value()) {
                sequences.push(seq);
            }
        }
        Ok(sequences)
    }

    /// Store sequence runtime state (current value, epoch).
    pub fn put_sequence_state(&self, state: &SequenceState) -> crate::Result<()> {
        let key = sequence_key(state.tenant_id, &state.name);
        let bytes =
            rmp_serde::to_vec(state).map_err(|e| catalog_err("serialize sequence state", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SEQUENCE_STATE)
                .map_err(|e| catalog_err("open sequence_state", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert sequence state", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load sequence runtime state.
    pub fn get_sequence_state(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<SequenceState>> {
        let key = sequence_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCE_STATE)
            .map_err(|e| catalog_err("open sequence_state", e))?;
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get sequence state", e))?
        {
            Some(value) => {
                let state = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deserialize sequence state", e))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }
}

fn sequence_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_get_sequence() {
        let cat = make_catalog();
        let seq = StoredSequence {
            tenant_id: 1,
            name: "order_seq".into(),
            owner: "admin".into(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
            cache_size: 1,
            epoch: 1,
            created_at: 1000,
            format_template: None,
            reset_scope: crate::control::sequence::ResetScope::Never,
            gap_free: false,
        };
        cat.put_sequence(&seq).unwrap();

        let loaded = cat.get_sequence(1, "order_seq").unwrap().unwrap();
        assert_eq!(loaded.name, "order_seq");
        assert_eq!(loaded.increment, 1);
        assert_eq!(loaded.start_value, 1);
    }

    #[test]
    fn delete_sequence() {
        let cat = make_catalog();
        let seq = StoredSequence {
            tenant_id: 1,
            name: "s1".into(),
            owner: "admin".into(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: 100,
            cycle: true,
            cache_size: 1,
            epoch: 1,
            created_at: 0,
            format_template: None,
            reset_scope: crate::control::sequence::ResetScope::Never,
            gap_free: false,
        };
        cat.put_sequence(&seq).unwrap();
        assert!(cat.delete_sequence(1, "s1").unwrap());
        assert!(!cat.delete_sequence(1, "s1").unwrap());
        assert!(cat.get_sequence(1, "s1").unwrap().is_none());
    }

    #[test]
    fn load_for_tenant() {
        let cat = make_catalog();
        for i in 0..3 {
            let seq = StoredSequence {
                tenant_id: 1,
                name: format!("s{i}"),
                owner: "admin".into(),
                start_value: 1,
                increment: 1,
                min_value: 1,
                max_value: i64::MAX,
                cycle: false,
                cache_size: 1,
                epoch: 1,
                created_at: 0,
                format_template: None,
                reset_scope: crate::control::sequence::ResetScope::Never,
                gap_free: false,
            };
            cat.put_sequence(&seq).unwrap();
        }
        // Different tenant.
        let other = StoredSequence {
            tenant_id: 2,
            name: "other".into(),
            owner: "admin".into(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
            cache_size: 1,
            epoch: 1,
            created_at: 0,
            format_template: None,
            reset_scope: crate::control::sequence::ResetScope::Never,
            gap_free: false,
        };
        cat.put_sequence(&other).unwrap();

        let t1 = cat.load_sequences_for_tenant(1).unwrap();
        assert_eq!(t1.len(), 3);
        let all = cat.load_all_sequences().unwrap();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn sequence_state_roundtrip() {
        let cat = make_catalog();
        let state = SequenceState::new(1, "s1".into(), 1, 1);
        cat.put_sequence_state(&state).unwrap();

        let loaded = cat.get_sequence_state(1, "s1").unwrap().unwrap();
        assert_eq!(loaded.current_value, 1);
        assert!(!loaded.is_called);
    }
}
