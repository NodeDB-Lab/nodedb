// SPDX-License-Identifier: BUSL-1.1

//! Audit description of a stamped `CatalogEntry`.

use crate::control::catalog_entry;

/// Return `(descriptor_name, version_after, hlc_string)` for a
/// stamped `CatalogEntry`. Delete* variants return `version_after = 0`
/// since the object is being removed. A soft delete keeps its row, so it
/// reports the version and HLC it stamped.
pub(super) fn describe_entry(e: &catalog_entry::CatalogEntry) -> (String, u64, String) {
    use catalog_entry::CatalogEntry as E;
    match e {
        E::PutCollection(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::PutCollectionIfAbsent(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::DeactivateCollection {
            name,
            descriptor_version,
            modification_hlc,
            ..
        } => (
            name.clone(),
            *descriptor_version,
            format!("{modification_hlc:?}"),
        ),
        E::PurgeCollection { name, .. } => (name.clone(), 0, String::new()),
        E::RecordWalTombstone { collection, .. } => (collection.clone(), 0, String::new()),
        E::PutSequence(s) => (
            s.name.clone(),
            s.descriptor_version,
            format!("{:?}", s.modification_hlc),
        ),
        E::DeleteSequence { name, .. } => (name.clone(), 0, String::new()),
        E::PutSequenceState(s) => (s.name.clone(), 0, String::new()),
        E::PutTrigger(t) => (
            t.name.clone(),
            t.descriptor_version,
            format!("{:?}", t.modification_hlc),
        ),
        E::DeleteTrigger { name, .. } => (name.clone(), 0, String::new()),
        E::PutFunction(f) => (
            f.name.clone(),
            f.descriptor_version,
            format!("{:?}", f.modification_hlc),
        ),
        E::DeleteFunction { name, .. } => (name.clone(), 0, String::new()),
        E::PutProcedure(p) => (
            p.name.clone(),
            p.descriptor_version,
            format!("{:?}", p.modification_hlc),
        ),
        E::DeleteProcedure { name, .. } => (name.clone(), 0, String::new()),
        E::PutSchedule(s) => (s.name.clone(), 0, String::new()),
        E::DeleteSchedule { name, .. } => (name.clone(), 0, String::new()),
        E::PutChangeStream(cs) => (cs.name.clone(), 0, String::new()),
        E::DeleteChangeStream { name, .. } => (name.clone(), 0, String::new()),
        E::PutUser(u) => (u.username.clone(), 0, String::new()),
        E::DropUser { username, .. } => (username.clone(), 0, String::new()),
        E::PutRole(r) => (r.name.clone(), 0, String::new()),
        E::DeleteRole { name, .. } => (name.clone(), 0, String::new()),
        E::PutApiKey(k) => (k.key_id.clone(), 0, String::new()),
        E::RevokeApiKey { key_id, .. } => (key_id.clone(), 0, String::new()),
        E::PutAuthUser(u) => (u.id.clone(), 0, String::new()),
        E::PutMaterializedView(m) => (m.name.clone(), 0, String::new()),
        E::DeleteMaterializedView { name, .. } => (name.clone(), 0, String::new()),
        E::PutStreamingMaterializedView(m) => (m.name.clone(), 0, String::new()),
        E::DeleteStreamingMaterializedView { name, .. } => (name.clone(), 0, String::new()),
        E::PutContinuousAggregate(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::DeleteContinuousAggregate { name, .. } => (name.clone(), 0, String::new()),
        E::PutTenant(t) => (t.name.clone(), 0, String::new()),
        E::PutTenantWithAdmin { tenant, admin } => (tenant.name.clone(), 0, admin.username.clone()),
        E::DeleteTenant { tenant_id, .. } => (tenant_id.to_string(), 0, String::new()),
        E::PutRlsPolicy(p) => (p.name.clone(), 0, String::new()),
        E::DeleteRlsPolicy { name, .. } => (name.clone(), 0, String::new()),
        E::PutRedactionPolicy(p) => (p.name.clone(), 0, String::new()),
        E::DeleteRedactionPolicy { for_role, .. } => (for_role.clone(), 0, String::new()),
        E::PutPermission(p) => (
            format!("{}@{}:{}", p.grantee, p.target, p.permission),
            0,
            String::new(),
        ),
        E::DeletePermission {
            target,
            grantee,
            permission,
        } => (format!("{grantee}@{target}:{permission}"), 0, String::new()),
        E::PutScopeGrant(g) => (
            format!("{}:{}@{}", g.grantee_type, g.grantee_id, g.scope_name),
            0,
            String::new(),
        ),
        E::DeleteScopeGrant {
            scope_name,
            grantee_type,
            grantee_id,
        } => (
            format!("{grantee_type}:{grantee_id}@{scope_name}"),
            0,
            String::new(),
        ),
        E::PutDatabaseQuota { db_id, .. } => (format!("quota:db:{db_id}"), 0, String::new()),
        E::DeleteDatabaseQuota { db_id } => (format!("quota:db:{db_id}"), 0, String::new()),
        E::PutTenantQuota {
            db_id, tenant_id, ..
        } => (
            format!("quota:db:{db_id}:tenant:{tenant_id}"),
            0,
            String::new(),
        ),
        E::DeleteTenantQuota { db_id, tenant_id } => (
            format!("quota:db:{db_id}:tenant:{tenant_id}"),
            0,
            String::new(),
        ),
        E::PutScopeQuota(q) => (format!("quota:scope:{}", q.scope_name), 0, String::new()),
        E::DeleteScopeQuota { scope_name } => {
            (format!("quota:scope:{scope_name}"), 0, String::new())
        }
        E::PutRetentionPolicy(p) => (
            format!("retention:{}:{}:{}", p.database_id, p.tenant_id, p.name),
            0,
            String::new(),
        ),
        E::DeleteRetentionPolicy {
            database_id,
            tenant_id,
            name,
            ..
        } => (
            format!("retention:{database_id}:{tenant_id}:{name}"),
            0,
            String::new(),
        ),
        E::PutAlertRule(a) => (
            format!("alert:{}:{}:{}", a.database_id, a.tenant_id, a.name),
            0,
            String::new(),
        ),
        E::DeleteAlertRule {
            database_id,
            tenant_id,
            name,
        } => (
            format!("alert:{database_id}:{tenant_id}:{name}"),
            0,
            String::new(),
        ),
        E::CreateTopicIfAbsent(t) => (
            format!("topic:{}:{}:{}", t.database_id, t.tenant_id, t.name),
            0,
            String::new(),
        ),
        E::DeleteTopicWithConsumerGroups {
            database_id,
            tenant_id,
            name,
            ..
        } => (
            format!("topic:{database_id}:{tenant_id}:{name}"),
            0,
            String::new(),
        ),
        E::PutConsumerGroupIfAbsent(g) => (
            format!(
                "consumer_group:{}:{}:{}:{}",
                g.database_id, g.tenant_id, g.stream_name, g.name
            ),
            0,
            String::new(),
        ),
        E::DeleteConsumerGroup {
            database_id,
            tenant_id,
            stream_name,
            name,
            ..
        } => (
            format!("consumer_group:{database_id}:{tenant_id}:{stream_name}:{name}"),
            0,
            String::new(),
        ),
        E::MigrateConsumerGroupStream { def, legacy_stream } => (
            format!(
                "consumer_group:{}:{}:{legacy_stream}:{}",
                def.database_id, def.tenant_id, def.name
            ),
            0,
            String::new(),
        ),
        E::PutBackupScheduleMark(m) => (
            format!("backup_schedule:{}:{:016x}", m.job, m.incarnation),
            0,
            String::new(),
        ),
        E::PutSpatialRefSys(def) => (format!("spatial_ref_sys:{}", def.srid), 0, String::new()),
        E::DropSpatialRefSys { srid } => (format!("spatial_ref_sys:{srid}"), 0, String::new()),
        E::PutCredential(row) => (
            format!("credential:{}:{}", row.tenant_id, row.name),
            0,
            row.kind.type_name().to_string(),
        ),
        E::DropCredential { tenant_id, name } => {
            (format!("credential:{tenant_id}:{name}"), 0, String::new())
        }
        E::CommitConsumerOffsets(c) => (
            format!(
                "consumer_group:{}:{}:{}:{}",
                c.database_id, c.tenant_id, c.stream_name, c.group_name
            ),
            0,
            format!("{:?}", c.group_hlc),
        ),
        E::PutCheckpoint(c) => (
            format!(
                "checkpoint:{}:{}:{}:{}:{}",
                c.database_id, c.tenant_id, c.collection, c.doc_id, c.checkpoint_name
            ),
            0,
            String::new(),
        ),
        E::DeleteCheckpoint {
            database_id,
            tenant_id,
            collection,
            doc_id,
            checkpoint_name,
        } => (
            format!("checkpoint:{database_id}:{tenant_id}:{collection}:{doc_id}:{checkpoint_name}"),
            0,
            String::new(),
        ),
        E::CompactHistory {
            database_id,
            tenant_id,
            collection,
            doc_id,
            before_timestamp,
            ..
        } => (
            format!(
                "checkpoint:{database_id}:{tenant_id}:{collection}:{doc_id}:<{before_timestamp}"
            ),
            0,
            String::new(),
        ),
        E::PutVectorModel(m) => (
            format!(
                "vector_model:{}:{}:{}:{}",
                m.database_id, m.tenant_id, m.collection, m.column
            ),
            0,
            String::new(),
        ),
        E::DeleteVectorModel {
            database_id,
            tenant_id,
            collection,
            column,
        } => (
            format!("vector_model:{database_id}:{tenant_id}:{collection}:{column}"),
            0,
            String::new(),
        ),
        E::PutCloneCopyup {
            database_id,
            tenant_id,
            collection,
            source_surrogate,
            ..
        }
        | E::PutCloneTombstone {
            database_id,
            tenant_id,
            collection,
            source_surrogate,
        } => (
            format!("clone_cow:{database_id}:{tenant_id}:{collection}:{source_surrogate}"),
            0,
            String::new(),
        ),
        E::PutKvCloneTombstone {
            database_id,
            tenant_id,
            collection,
            kv_key,
        } => (
            format!("clone_kv_tombstone:{database_id}:{tenant_id}:{collection}:{kv_key}"),
            0,
            String::new(),
        ),
        E::PutCloneSourceDrain(row) => (
            format!(
                "clone_source_drain:{}:{}:{}",
                row.clone_database, row.tenant_id, row.clone_collection
            ),
            0,
            String::new(),
        ),
        E::DeleteCloneSourceDrain {
            clone_database,
            tenant_id,
            clone_collection,
        } => (
            format!("clone_source_drain:{clone_database}:{tenant_id}:{clone_collection}"),
            0,
            String::new(),
        ),
        E::PutColumnStats(rows) => (
            rows.first().map_or_else(String::new, |r| {
                format!(
                    "column_stats:{}:{}:{}",
                    r.database_id, r.tenant_id, r.collection
                )
            }),
            0,
            String::new(),
        ),
        E::PutVectorIndexParams(p) => (
            format!(
                "vector_index_params:{}:{}:{}:{}",
                p.database_id, p.tenant_id, p.collection, p.field_name
            ),
            0,
            String::new(),
        ),
        E::DeleteVectorIndexParams {
            database_id,
            tenant_id,
            collection,
            field_name,
            ..
        } => (
            format!("vector_index_params:{database_id}:{tenant_id}:{collection}:{field_name}"),
            0,
            String::new(),
        ),
        E::PutOwner(o) => (o.object_name.clone(), 0, String::new()),
        E::DeleteOwner { object_name, .. } => (object_name.clone(), 0, String::new()),
        E::PutSynonymGroup(g) => (g.name.clone(), 0, String::new()),
        E::DeleteSynonymGroup { name, .. } => (name.clone(), 0, String::new()),
        E::PutArray(a) => (a.name.clone(), 0, String::new()),
        E::DeleteArray { name, .. } => (name.clone(), 0, String::new()),
        E::PutCustomType(t) => (t.name.clone(), 0, String::new()),
        E::DeleteCustomType { name, .. } => (name.clone(), 0, String::new()),
        E::PutDatabase(d) => (d.name.clone(), 0, String::new()),
        E::DeleteDatabase { db_id } => (db_id.to_string(), 0, String::new()),
        E::PutDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => (
            format!("db:{db_id}:user:{user_id}:{privilege}"),
            0,
            String::new(),
        ),
        E::DeleteDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => (
            format!("db:{db_id}:user:{user_id}:{privilege}"),
            0,
            String::new(),
        ),
        E::CloneDatabase {
            target_descriptor, ..
        } => (target_descriptor.name.clone(), 0, String::new()),
        E::MoveTenantCutover { tenant_id, .. } => (format!("tenant:{tenant_id}"), 0, String::new()),
        E::PutIndexRecord(r) => (r.name.clone(), 0, String::new()),
        E::DeleteIndexRecord { name, .. } => (name.clone(), 0, String::new()),
        E::PutOidcProvider(p) => (p.provider_name.clone(), 0, String::new()),
        E::DeleteOidcProvider { name } => (name.clone(), 0, String::new()),
    }
}
