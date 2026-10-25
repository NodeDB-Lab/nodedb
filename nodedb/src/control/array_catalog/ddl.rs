// SPDX-License-Identifier: BUSL-1.1

//! Array DDL through the replicated catalog.
//!
//! `CREATE`, `ALTER`, and `DROP ARRAY` each propose one catalog entry through
//! the metadata group, built from committed state under the DDL preparation
//! lease. Every node applies it: the `_system.arrays` row, the in-memory
//! mirror, and the per-core open or drop in post-apply. With no metadata
//! group this node applies the same entry through the same apply path.
//!
//! SQL conversion only validates and emits the DDL task. The front doors hand
//! the authorized task here instead of dispatching it to a core.

use nodedb_physical::physical_plan::{ArrayOp, MetaOp};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::Hlc;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::retention::check_retention_floor;
use crate::control::array_catalog::schema_recode::check_same_content;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_batch_async;
use crate::control::security::catalog::SystemCatalog;
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId};

/// Whether `plan` is array DDL, which runs through [`run_authorized_array_ddl`]
/// and never reaches a core as a task.
pub(crate) fn is_array_ddl(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Array(ArrayOp::OpenArray { .. } | ArrayOp::DropArray { .. })
            | PhysicalPlan::Meta(MetaOp::AlterArray { .. })
    )
}

/// Propose the catalog entry an authorized array DDL task names, and answer
/// with the status the statement reports. The front doors call this with the
/// authorization they hold, so none of them unwraps it.
pub(crate) async fn run_authorized_array_ddl(
    state: &SharedState,
    authorized: AuthorizedTask,
) -> crate::Result<Response> {
    run_trusted_array_ddl(state, authorized.into_physical_task()).await
}

/// [`run_authorized_array_ddl`] for a task whose authority comes from an
/// already admitted operation.
pub(crate) async fn run_trusted_array_ddl(
    state: &SharedState,
    task: PhysicalTask,
) -> crate::Result<Response> {
    let PhysicalTask {
        tenant_id,
        database_id,
        plan,
        ..
    } = task;
    let (statement, key, payload) = match &plan {
        PhysicalPlan::Array(ArrayOp::OpenArray { .. }) => ("CREATE ARRAY", "opened", None),
        PhysicalPlan::Array(ArrayOp::DropArray { .. }) => ("DROP ARRAY", "dropped", None),
        PhysicalPlan::Meta(MetaOp::AlterArray {
            audit_retain_ms, ..
        }) => {
            // The acknowledgement a core answered ALTER ARRAY with: the new
            // retention, or 0 when it is cleared.
            let ack = audit_retain_ms
                .flatten()
                .and_then(|ms| u64::try_from(ms).ok())
                .unwrap_or(0);
            ("ALTER ARRAY", "altered", Some(ack.to_le_bytes().to_vec()))
        }
        _ => {
            return Err(crate::Error::PlanError {
                detail: "array DDL path received a non-DDL plan".into(),
            });
        }
    };
    // No catalog overlay replays uncommitted array DDL, so a transaction
    // cannot read the array it created.
    if crate::control::server::shared::session::ddl_buffer::is_active() {
        return Err(crate::Error::NotInTransactionBlock {
            statement: statement.into(),
        });
    }
    propose_array_entries(state, |catalog| {
        Ok(vec![entry_for(catalog, tenant_id, database_id, &plan)?])
    })
    .await?;
    let payload = match payload {
        Some(bytes) => bytes,
        None => crate::data::executor::response_codec::encode_count(key, 1)?,
    };
    Ok(Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_versions: crate::types::ReadVersions::new(),
        write_set: Vec::new(),
    })
}

/// Propose the entries `plan` builds from the committed catalog.
///
/// `plan` reads committed state under the DDL preparation lease, and the
/// apply follows it under the same guard. Two statements never both find an identity absent and both
/// create it. Every core opens or drops the array before this returns.
pub(crate) async fn propose_array_entries(
    state: &SharedState,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> crate::Result<()> {
    propose_catalog_batch_async(state, plan).await?;
    Ok(())
}

/// The entry `plan` makes of the committed catalog. CREATE requires the
/// identity absent, ALTER and DROP require it present.
fn entry_for(
    catalog: &SystemCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<CatalogEntry> {
    let committed = |name: &str| catalog.get_array_in_database(tenant_id, database_id, name);
    match plan {
        PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id,
            schema_msgpack,
            schema_hash,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        }) => {
            if committed(&array_id.name)?.is_some() {
                return Err(crate::Error::PlanError {
                    detail: format!("CREATE ARRAY {}: already exists", array_id.name),
                });
            }
            Ok(CatalogEntry::PutArray(Box::new(ArrayCatalogEntry {
                array_id: array_id.clone(),
                name: array_id.name.clone(),
                schema_msgpack: schema_msgpack.clone(),
                schema_hash: *schema_hash,
                created_at_ms: now_epoch_ms(),
                prefix_bits: *prefix_bits,
                audit_retain_ms: *audit_retain_ms,
                minimum_audit_retain_ms: *minimum_audit_retain_ms,
                // Frozen by the proposer's stamp.
                modification_hlc: Hlc::ZERO,
                incarnation: nodedb_types::Hlc::ZERO,
            })))
        }
        PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id,
            audit_retain_ms,
            minimum_audit_retain_ms,
            schema_msgpack,
        }) => {
            let current = committed(array_id)?.ok_or_else(|| crate::Error::PlanError {
                detail: format!("ALTER ARRAY {array_id}: not found"),
            })?;
            check_retention_floor(
                array_id,
                &current,
                *audit_retain_ms,
                *minimum_audit_retain_ms,
            )?;
            // A georef or attr metadata change keeps every field
            // `schema_hash` covers. Checked
            // against the committed schema, so the kept hash stays true.
            let schema_msgpack = match schema_msgpack {
                Some(altered) => {
                    check_same_content(
                        &format!("ALTER ARRAY {array_id}"),
                        &current.schema_msgpack,
                        altered,
                    )?;
                    altered.clone()
                }
                None => current.schema_msgpack.clone(),
            };
            Ok(CatalogEntry::PutArray(Box::new(ArrayCatalogEntry {
                audit_retain_ms: audit_retain_ms.unwrap_or(current.audit_retain_ms),
                minimum_audit_retain_ms: minimum_audit_retain_ms
                    .unwrap_or(current.minimum_audit_retain_ms),
                schema_msgpack,
                ..current
            })))
        }
        PhysicalPlan::Array(ArrayOp::DropArray { array_id }) => {
            if committed(&array_id.name)?.is_none() {
                return Err(crate::Error::PlanError {
                    detail: format!("DROP ARRAY {}: not found", array_id.name),
                });
            }
            Ok(CatalogEntry::DeleteArray {
                database_id: database_id.as_u64(),
                tenant_id: tenant_id.as_u64(),
                name: array_id.name.clone(),
                // Frozen by the proposer's stamp.
                target_hlc: Hlc::ZERO,
                moved_to: None,
            })
        }
        _ => Err(crate::Error::PlanError {
            detail: "array DDL path received a non-DDL plan".into(),
        }),
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_array::types::ArrayId;

    use super::*;
    use crate::control::security::credential::CredentialStore;

    fn catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn open_plan(name: &str) -> PhysicalPlan {
        PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, name),
            schema_msgpack: vec![0x90],
            schema_hash: 7,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        })
    }

    fn entry(catalog: &SystemCatalog, plan: &PhysicalPlan) -> crate::Result<CatalogEntry> {
        entry_for(catalog, TenantId::new(1), DatabaseId::DEFAULT, plan)
    }

    #[test]
    fn create_requires_absence_and_drop_requires_presence() {
        let (store, _tmp) = catalog();
        let catalog = store.catalog();
        let drop = PhysicalPlan::Array(ArrayOp::DropArray {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, "grid"),
        });
        assert!(entry(catalog, &drop).is_err());

        let CatalogEntry::PutArray(created) = entry(catalog, &open_plan("grid")).expect("create")
        else {
            unreachable!("CREATE ARRAY builds a put");
        };
        catalog.put_array(&created).expect("apply the create");
        assert!(entry(catalog, &open_plan("grid")).is_err());
        assert!(matches!(
            entry(catalog, &drop).expect("drop"),
            CatalogEntry::DeleteArray { moved_to: None, .. }
        ));
    }

    /// ALTER rewrites only the retention fields it names.
    #[test]
    fn alter_keeps_every_field_it_does_not_name() {
        let (store, _tmp) = catalog();
        let catalog = store.catalog();
        let CatalogEntry::PutArray(created) = entry(catalog, &open_plan("grid")).expect("create")
        else {
            unreachable!("CREATE ARRAY builds a put");
        };
        catalog.put_array(&created).expect("apply the create");

        let alter = PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id: "grid".to_string(),
            audit_retain_ms: Some(Some(60_000)),
            minimum_audit_retain_ms: None,
            schema_msgpack: None,
        });
        let CatalogEntry::PutArray(altered) = entry(catalog, &alter).expect("alter") else {
            unreachable!("ALTER ARRAY builds a put");
        };
        assert_eq!(altered.audit_retain_ms, Some(60_000));
        assert_eq!(altered.minimum_audit_retain_ms, None);
        assert_eq!(altered.schema_hash, created.schema_hash);
        assert_eq!(altered.schema_msgpack, created.schema_msgpack);
        assert_eq!(altered.array_id, created.array_id);
    }

    fn raster_schema() -> nodedb_array::schema::ArraySchema {
        use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
        use nodedb_array::types::domain::{Domain, DomainBound};
        let dim = |name: &str| {
            DimSpec::new(
                name,
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            )
        };
        ArraySchemaBuilder::new("scene")
            .dim(dim("row"))
            .dim(dim("col"))
            .attr(AttrSpec::new("b1", AttrType::UInt16, true))
            .tile_extents(vec![256, 256])
            .build()
            .expect("raster schema")
    }

    fn put_raster(catalog: &SystemCatalog) -> ArrayCatalogEntry {
        let plan = PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, "scene"),
            schema_msgpack: zerompk::to_msgpack_vec(&raster_schema()).expect("encode"),
            schema_hash: 0xFEED,
            prefix_bits: 8,
            audit_retain_ms: Some(1_000),
            minimum_audit_retain_ms: Some(10),
        });
        let CatalogEntry::PutArray(created) = entry(catalog, &plan).expect("create") else {
            unreachable!("CREATE ARRAY builds a put");
        };
        catalog.put_array(&created).expect("apply the create");
        *created
    }

    fn alter_schema(schema_msgpack: Vec<u8>) -> PhysicalPlan {
        PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id: "scene".to_string(),
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            schema_msgpack: Some(schema_msgpack),
        })
    }

    /// A georef change replaces the schema bytes and keeps every other field.
    #[test]
    fn alter_georef_keeps_hash_and_every_other_field() {
        let (store, _tmp) = catalog();
        let catalog = store.catalog();
        let created = put_raster(catalog);
        let georef = nodedb_array::schema::ArrayGeoref {
            srid: nodedb_types::geometry::Srid::new(32647).expect("srid"),
            x_dim: 1,
            y_dim: 0,
            transform: Some(nodedb_array::schema::GeoTransform([
                600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0,
            ])),
        };
        let with_georef = raster_schema()
            .with_georef(Some(georef.clone()))
            .expect("georef");
        let bytes = zerompk::to_msgpack_vec(&with_georef).expect("encode");
        let CatalogEntry::PutArray(altered) =
            entry(catalog, &alter_schema(bytes.clone())).expect("alter")
        else {
            unreachable!("ALTER ARRAY builds a put");
        };
        assert_eq!(altered.schema_msgpack, bytes);
        assert_eq!(altered.schema_hash, created.schema_hash);
        assert_eq!(altered.prefix_bits, created.prefix_bits);
        assert_eq!(altered.audit_retain_ms, created.audit_retain_ms);
        assert_eq!(
            altered.minimum_audit_retain_ms,
            created.minimum_audit_retain_ms
        );
        assert_eq!(altered.array_id, created.array_id);
        assert_eq!(altered.created_at_ms, created.created_at_ms);
        let decoded: nodedb_array::schema::ArraySchema =
            zerompk::from_msgpack(&altered.schema_msgpack).expect("decode");
        assert_eq!(decoded.georef, Some(georef));
    }

    /// The apply refuses a schema that changes a field `schema_hash` covers.
    #[test]
    fn alter_refuses_a_content_change() {
        let (store, _tmp) = catalog();
        let catalog = store.catalog();
        put_raster(catalog);
        let mut changed = raster_schema();
        changed.tile_extents = vec![128, 128];
        let bytes = zerompk::to_msgpack_vec(&changed).expect("encode");
        assert!(entry(catalog, &alter_schema(bytes)).is_err());
    }

    #[test]
    fn only_array_ddl_takes_the_catalog_path() {
        assert!(is_array_ddl(&open_plan("grid")));
        assert!(!is_array_ddl(&PhysicalPlan::Array(
            ArrayOp::PurgeArrayDrop {
                array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, "grid"),
            }
        )));
    }
}
