// SPDX-License-Identifier: BUSL-1.1

//! Give a bootstrapping mirror the capture's conversion records.
//!
//! A core takes a collection's row encoding from its conversion record and the
//! catalog row's stamp together. A stamp with no record says the rows still hold
//! the source mode. A capture's rows hold the target mode, so every replica of
//! the collection's group needs the record before the rows arrive.
//!
//! The record reaches them as a `CollectionConvert` entry at the same generation.
//! The collection holds no row yet, so the entry rewrites nothing: a schemaless
//! request over no rows commits the record and takes its target mode. The tail's
//! next conversion carries a higher generation and runs as usual.

use std::collections::HashMap;

use futures::future::try_join_all;
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

use crate::control::backup::snapshot_keys::extract_db_tenant_scoped_collection;
use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::control::wal_replication::propose::propose_replicated_entry;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::engine::sparse::btree::storage_conversion::StorageConversionRecord;
use crate::event::EventSource;

/// The entry that installs `record` on the replicas of `name`'s group.
fn record_entry(
    tenant: u64,
    database: DatabaseId,
    name: &str,
    record: StorageConversionRecord,
) -> ReplicatedEntry {
    let vshard = CollectionKey::from_bare(database, name).vshard();
    let qualified = QualifiedCollection::new(database, name);
    ReplicatedEntry::new(
        tenant,
        database.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::CollectionConvert {
            collection: name.to_string(),
            target_type: "document_schemaless".to_string(),
            schema_json: String::new(),
            source_storage_mode: StorageMode::Schemaless,
            target_storage_mode: record.target,
            generation: record.generation,
            incarnation: record.incarnation,
        },
    )
    .naming([qualified.as_str()])
    .with_event_source(EventSource::RaftFollower)
}

/// The record of `key`, when its incarnation is the one the catalog holds. A
/// record of an earlier incarnation describes rows that are gone.
fn live_record(
    tenant: u64,
    database: DatabaseId,
    key: &str,
    bytes: &[u8],
    incarnations: &HashMap<&str, nodedb_types::Hlc>,
) -> crate::Result<Option<(String, StorageConversionRecord)>> {
    let Some(stored) = extract_db_tenant_scoped_collection(key, tenant) else {
        return Err(crate::Error::Internal {
            detail: format!(
                "mirror bootstrap: the conversion record key '{key}' names no collection of \
                 tenant {tenant}"
            ),
        });
    };
    let bare = CollectionKey::from_qualified_str(database, stored)?
        .name()
        .to_string();
    let record = StorageConversionRecord::from_bytes(bytes)?;
    Ok((incarnations.get(bare.as_str()) == Some(&record.incarnation)).then_some((bare, record)))
}

/// Install each of `records` on every replica, and wait until each applied here.
pub(super) async fn install_conversions(
    state: &SharedState,
    tenant: u64,
    database: DatabaseId,
    records: &[(String, Vec<u8>)],
    collections: &[StoredCollection],
) -> crate::Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let incarnations: HashMap<&str, nodedb_types::Hlc> = collections
        .iter()
        .filter(|collection| collection.tenant_id == tenant)
        .map(|collection| (collection.name.as_str(), collection.incarnation))
        .collect();
    let proposer = state.async_raft_proposer()?;
    let deadline = StatementDeadline::of_statement(state).tokio_instant();
    let mut proposals = Vec::with_capacity(records.len());
    for (key, bytes) in records {
        if let Some((name, record)) = live_record(tenant, database, key, bytes, &incarnations)? {
            let entry = record_entry(tenant, database, &name, record);
            proposals.push(propose_replicated_entry(state, proposer, entry, deadline));
        }
    }
    try_join_all(proposals).await.map(drop)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Hlc;

    use super::*;

    const DB: DatabaseId = DatabaseId::new(1900);
    const TENANT: u64 = 1;
    const INCARNATION: Hlc = Hlc::new(5, 0);

    fn record(incarnation: Hlc) -> Vec<u8> {
        StorageConversionRecord {
            generation: 2,
            incarnation,
            target: StorageMode::Schemaless,
        }
        .to_bytes()
        .expect("encode")
    }

    fn incarnations() -> HashMap<&'static str, Hlc> {
        HashMap::from([("orders", INCARNATION)])
    }

    #[test]
    fn a_record_of_the_held_incarnation_installs_under_the_bare_name() {
        let found = live_record(
            TENANT,
            DB,
            "1900:1:1900/orders",
            &record(INCARNATION),
            &incarnations(),
        )
        .expect("read")
        .expect("a live record");
        assert_eq!(found.0, "orders");
        assert_eq!(found.1.generation, 2);
    }

    #[test]
    fn a_record_of_an_earlier_incarnation_stays_out() {
        assert!(
            live_record(
                TENANT,
                DB,
                "1900:1:1900/orders",
                &record(Hlc::new(1, 0)),
                &incarnations(),
            )
            .expect("read")
            .is_none()
        );
    }

    #[test]
    fn a_record_of_a_collection_the_catalog_lacks_stays_out() {
        assert!(
            live_record(
                TENANT,
                DB,
                "1900:1:1900/gone",
                &record(INCARNATION),
                &incarnations(),
            )
            .expect("read")
            .is_none()
        );
    }

    /// The entry names the record's generation and target, and passes the gates
    /// as a follower's write.
    #[test]
    fn the_entry_carries_the_record() {
        let entry = record_entry(
            TENANT,
            DB,
            "orders",
            StorageConversionRecord {
                generation: 7,
                incarnation: INCARNATION,
                target: StorageMode::Schemaless,
            },
        );
        match entry.write {
            ReplicatedWrite::CollectionConvert {
                generation,
                incarnation,
                ref collection,
                ..
            } => {
                assert_eq!(generation, 7);
                assert_eq!(incarnation, INCARNATION);
                assert_eq!(collection, "orders");
            }
            other => panic!("not a conversion: {other:?}"),
        }
        assert_eq!(
            entry.event_source,
            crate::control::wal_replication::types::ReplicatedEventSource::from(
                EventSource::RaftFollower
            )
        );
    }
}
