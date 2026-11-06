// SPDX-License-Identifier: BUSL-1.1

//! What leaves this cluster for one database.
//!
//! A data group and the metadata group hold every database of every tenant. The
//! filter runs on the source, before any byte reaches the wire, and it passes
//! by allow: an entry leaves only when it names the link's database.
//!
//! Data groups pass an entry of the database, and a mirror cut's barrier, which
//! names no database and carries only positions. These stay home:
//!
//! - entries of any other database, and bytes that are not an entry;
//! - a Calvin read result or read timeout: a slice's redo carries its outcome,
//!   and the mirror's log needs neither;
//! - any other cut barrier: a backup's names another database's capture;
//! - a mirror's own stamped entries: this cluster is not their source.
//!
//! The metadata group passes only the catalog entry inside a DDL envelope, and
//! only when it names the database. The envelope's audit fields stay home.

use nodedb_cluster::{MetadataEntry, decode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::DatabaseId;

/// Whether the data-group entry `data` leaves for `database`.
pub fn data_entry_leaves(data: &[u8], database: u64) -> bool {
    if data.is_empty() {
        return false;
    }
    let Some(entry) = ReplicatedEntry::from_bytes(data) else {
        return false;
    };
    if entry.mirror.is_some() {
        return false;
    }
    if let ReplicatedWrite::CutBarrier { mirror, .. } = &entry.write {
        return mirror.is_some();
    }
    let stays_home = matches!(
        entry.write,
        ReplicatedWrite::CalvinReadResult { .. }
            | ReplicatedWrite::CalvinReadTimeout { .. }
            | ReplicatedWrite::MirrorSeal { .. }
    );
    !stays_home && entry.database_id == database
}

/// Every catalog payload of the metadata entry `data` that names `database`, in
/// log order. A batch or a prepared DDL unwraps. Any other metadata entry
/// yields nothing.
pub fn metadata_payloads(data: &[u8], database: u64) -> Vec<Vec<u8>> {
    let Ok(entry) = decode_entry(data) else {
        return Vec::new();
    };
    let mut payloads = Vec::new();
    collect_payloads(&entry, database, &mut payloads);
    payloads
}

fn collect_payloads(entry: &MetadataEntry, database: u64, payloads: &mut Vec<Vec<u8>>) {
    if let MetadataEntry::Batch { entries } = entry {
        for inner in entries {
            collect_payloads(inner, database, payloads);
        }
    } else if let MetadataEntry::DdlPrepared { entry, .. } = entry {
        collect_payloads(entry, database, payloads);
    } else if let MetadataEntry::CatalogDdl { payload }
    | MetadataEntry::CatalogDdlAudited { payload, .. } = entry
        && payload_names(payload, database)
    {
        payloads.push(payload.clone());
    }
}

/// Whether a catalog payload changes `database`'s objects, or drops it. A
/// payload that does not decode names nothing.
fn payload_names(payload: &[u8], database: u64) -> bool {
    let Ok(entry) = catalog_entry::decode(payload) else {
        return false;
    };
    if let CatalogEntry::DeleteDatabase { db_id } = entry {
        return db_id == database;
    }
    entry
        .mirror_scope()
        .into_iter()
        .flatten()
        .any(|scope| scope == DatabaseId::new(database))
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::encode_entry;
    use nodedb_types::{MirrorCutMark, MirrorEntryStamp};

    use super::*;
    use crate::control::wal_replication::types::MirrorBarrier;

    const DB: u64 = 1500;
    const OTHER: u64 = 1501;

    fn entry(database: u64, write: ReplicatedWrite) -> Vec<u8> {
        ReplicatedEntry::new(1, database, 2, write)
            .encode()
            .expect("encode")
    }

    fn put(database: u64) -> Vec<u8> {
        entry(
            database,
            ReplicatedWrite::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: vec![1],
                surrogate: 1,
                resolved_sum_targets: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
        )
    }

    fn barrier(mirror: Option<MirrorBarrier>) -> Vec<u8> {
        entry(
            DatabaseId::DEFAULT.as_u64(),
            ReplicatedWrite::CutBarrier {
                hlc: 1,
                restore_point: 0,
                capture: None,
                mirror,
            },
        )
    }

    #[test]
    fn only_the_links_database_leaves() {
        assert!(data_entry_leaves(&put(DB), DB));
        assert!(!data_entry_leaves(&put(OTHER), DB));
    }

    #[test]
    fn a_calvin_read_result_and_timeout_stay_home() {
        let result = entry(
            DB,
            ReplicatedWrite::CalvinReadResult {
                epoch: 1,
                position: 0,
                passive_vshard: 2,
                tenant_id: 1,
                values: Vec::new(),
            },
        );
        assert!(!data_entry_leaves(&result, DB));
        let timeout = entry(
            DB,
            ReplicatedWrite::CalvinReadTimeout {
                epoch: 1,
                position: 0,
                tenant_id: 1,
            },
        );
        assert!(!data_entry_leaves(&timeout, DB));
    }

    #[test]
    fn only_a_mirror_cuts_barrier_leaves() {
        let mark = MirrorCutMark {
            seq: 1,
            cap_ordinal: 1,
            metadata_index: 1,
        };
        assert!(data_entry_leaves(
            &barrier(Some(MirrorBarrier {
                mark,
                calvin_floors: Vec::new(),
            })),
            DB
        ));
        assert!(!data_entry_leaves(&barrier(None), DB));
    }

    #[test]
    fn a_no_op_and_foreign_bytes_stay_home() {
        assert!(!data_entry_leaves(&[], DB));
        assert!(!data_entry_leaves(b"not an entry", DB));
    }

    /// This cluster is no source of a mirror's own entries.
    #[test]
    fn a_stamped_entry_and_a_seal_stay_home() {
        let mut stamped = ReplicatedEntry::new(
            1,
            DB,
            2,
            ReplicatedWrite::MirrorSeal {
                last: false,
                calvin_floors: Vec::new(),
            },
        );
        assert!(!data_entry_leaves(&stamped.encode().expect("encode"), DB));
        stamped.mirror = Some(MirrorEntryStamp {
            source_group: 0,
            source_index: 0,
            cut: 1,
            epoch: 1,
        });
        assert!(!data_entry_leaves(&stamped.encode().expect("encode"), DB));
    }

    fn drop_collection(database: u64) -> Vec<u8> {
        catalog_entry::encode(&CatalogEntry::DeactivateCollection {
            database_id: database,
            tenant_id: 1,
            name: "c".into(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        })
        .expect("encode")
    }

    fn ddl(payload: Vec<u8>) -> MetadataEntry {
        MetadataEntry::CatalogDdl { payload }
    }

    #[test]
    fn only_the_databases_ddl_leaves_and_every_envelope_unwraps() {
        let mine = drop_collection(DB);
        let theirs = drop_collection(OTHER);
        let plain = encode_entry(&ddl(mine.clone())).expect("encode");
        assert_eq!(metadata_payloads(&plain, DB), vec![mine.clone()]);
        assert!(metadata_payloads(&plain, OTHER).is_empty());

        let batch = encode_entry(&MetadataEntry::Batch {
            entries: vec![
                ddl(theirs.clone()),
                MetadataEntry::DdlPrepared {
                    token: 1,
                    entry: Box::new(ddl(mine.clone())),
                },
            ],
        })
        .expect("encode");
        assert_eq!(metadata_payloads(&batch, DB), vec![mine]);
    }

    /// The audit envelope names who ran the DDL. Only the catalog entry leaves.
    #[test]
    fn an_audited_ddl_leaves_without_its_audit_fields() {
        let mine = drop_collection(DB);
        let audited = encode_entry(&MetadataEntry::CatalogDdlAudited {
            payload: mine.clone(),
            auth_user_id: "1".into(),
            auth_user_name: "operator".into(),
            sql_text: "DROP COLLECTION c".into(),
        })
        .expect("encode");
        let leaving = metadata_payloads(&audited, DB);
        assert_eq!(leaving, vec![mine]);
        assert!(
            !leaving[0]
                .windows(b"operator".len())
                .any(|window| window == b"operator")
        );
    }

    #[test]
    fn a_source_drop_leaves_and_another_entry_stays_home() {
        let drop =
            catalog_entry::encode(&CatalogEntry::DeleteDatabase { db_id: DB }).expect("encode");
        let entry = encode_entry(&ddl(drop.clone())).expect("encode");
        assert_eq!(metadata_payloads(&entry, DB), vec![drop]);
        assert!(metadata_payloads(&entry, OTHER).is_empty());
        let mark = encode_entry(&MetadataEntry::ArchiveMark).expect("encode");
        assert!(metadata_payloads(&mark, DB).is_empty());
        assert!(metadata_payloads(b"not an entry", DB).is_empty());
    }
}
