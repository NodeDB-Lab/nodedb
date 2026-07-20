// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for `DROP USER` ownership/grant cleanup and recovery.

mod catalog_integrity_helpers;
mod common;

use catalog_integrity_helpers::{
    TENANT, make_collection, make_function, make_mv_sourced, make_procedure, make_schedule,
    make_sequence, make_stream, make_trigger,
};
use common::pgwire_harness::TestServer;
use nodedb::control::cluster::recovery_check::divergence::DivergenceKind;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::cluster::verify_and_repair;
use nodedb::control::security::catalog::auth_types::{StoredOwner, StoredPermission};
use nodedb::control::security::catalog::{StoredContinuousAggregate, SystemCatalog};
use nodedb::control::security::identity::Role;
use nodedb::types::{DatabaseId, TenantId};

const VICTIM: &str = "victim_owner";
const ADMIN_TARGET: &str = "nodedb";

fn plant_owner(catalog: &SystemCatalog, object_type: &str, name: &str, owner: &str) {
    catalog
        .put_owner(&StoredOwner {
            object_type: object_type.to_string(),
            object_name: name.to_string(),
            tenant_id: TENANT,
            owner_username: owner.to_string(),
        })
        .unwrap();
}

fn owner_of(catalog: &SystemCatalog, object_type: &str, name: &str) -> String {
    catalog
        .load_all_owners()
        .unwrap()
        .into_iter()
        .find(|o| o.object_type == object_type && o.object_name == name)
        .unwrap_or_else(|| panic!("owner row for {object_type} '{name}' vanished"))
        .owner_username
}

async fn create_collection_as(server: &TestServer, user: &str, name: &str) {
    let (client, connection) = server
        .connect_as(user, "probe-secret-99")
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    client
        .simple_query(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, body TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("{user} creates {name}: {e}"));
    drop(client);
    connection.abort();
}

fn move_collection(catalog: &SystemCatalog, name: &str, database_id: DatabaseId) {
    let mut collection = catalog
        .get_collection(DatabaseId::DEFAULT, TENANT, name)
        .unwrap()
        .unwrap();
    catalog
        .delete_collection(DatabaseId::DEFAULT, TENANT, name)
        .unwrap();
    collection.database_id = database_id;
    catalog.put_collection(database_id, &collection).unwrap();
}

fn assert_no_dangling_user_references(catalog: &SystemCatalog) {
    let dangling: Vec<_> = verify_redb_integrity(catalog)
        .into_iter()
        .filter(|d| {
            matches!(
                d.kind,
                DivergenceKind::DanglingReference {
                    to_kind: "user",
                    ..
                }
            )
        })
        .collect();
    assert!(
        dangling.is_empty(),
        "catalog must not retain references to nonexistent users: {dangling:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_reassigns_non_default_database_collection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER global_owner PASSWORD 'probe-secret-99'")
        .await
        .expect("create ordinary user");
    create_collection_as(&server, "global_owner", "global_notes").await;
    let catalog = server.shared.credentials.catalog();
    move_collection(catalog, "global_notes", DatabaseId::new(9));

    server
        .exec("DROP USER global_owner")
        .await
        .expect("DROP USER should succeed when ownership can be reassigned");

    assert_no_dangling_user_references(server.shared.credentials.catalog());
    assert_eq!(
        catalog
            .get_collection(DatabaseId::new(9), TENANT, "global_notes")
            .unwrap()
            .unwrap()
            .owner,
        "nodedb"
    );
    assert_eq!(owner_of(catalog, "collection", "global_notes"), "nodedb");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_reassigns_to_the_named_tenants_provisioned_admin() {
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 42").await.unwrap();
    server
        .exec("CREATE USER tenant_owner PASSWORD 'probe-secret-99' TENANT acme")
        .await
        .unwrap();
    create_collection_as(&server, "tenant_owner", "tenant_notes").await;

    server.exec("DROP USER tenant_owner").await.unwrap();

    assert_no_dangling_user_references(server.shared.credentials.catalog());
    assert_eq!(
        owner_of(
            server.shared.credentials.catalog(),
            "collection",
            "tenant_notes"
        ),
        "acme_admin"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_honors_the_tenants_explicit_admin_principal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT custom_scope ID 43 WITH ADMIN steward")
        .await
        .unwrap();
    server
        .exec("CREATE USER custom_owner PASSWORD 'probe-secret-99' TENANT custom_scope")
        .await
        .unwrap();
    create_collection_as(&server, "custom_owner", "custom_notes").await;

    server.exec("DROP USER custom_owner").await.unwrap();

    assert_no_dangling_user_references(server.shared.credentials.catalog());
    assert_eq!(
        owner_of(
            server.shared.credentials.catalog(),
            "collection",
            "custom_notes"
        ),
        "steward"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_reassigns_retention_tombstoned_collections() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER retired_owner PASSWORD 'probe-secret-99'")
        .await
        .unwrap();
    create_collection_as(&server, "retired_owner", "retired_notes").await;
    server.exec("DROP COLLECTION retired_notes").await.unwrap();
    server.exec("DROP USER retired_owner").await.unwrap();

    assert_no_dangling_user_references(server.shared.credentials.catalog());
    assert_eq!(
        owner_of(
            server.shared.credentials.catalog(),
            "collection",
            "retired_notes"
        ),
        "nodedb"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_repair_recovers_preexisting_dangling_owners() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER lost_owner PASSWORD 'probe-secret-99'")
        .await
        .unwrap();
    create_collection_as(&server, "lost_owner", "recoverable_notes").await;
    move_collection(
        server.shared.credentials.catalog(),
        "recoverable_notes",
        DatabaseId::new(10),
    );
    server
        .shared
        .credentials
        .catalog()
        .delete_user("lost_owner")
        .unwrap();

    let report = verify_and_repair(&server.shared).await.unwrap();

    assert!(
        report.is_acceptable(),
        "startup repair must recover dangling owners: {report}"
    );
    assert_no_dangling_user_references(server.shared.credentials.catalog());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_repair_preserves_an_existing_inactive_canonical_primary_owner() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER canonical_owner PASSWORD 'probe-secret-99'")
        .await
        .unwrap();
    create_collection_as(&server, "canonical_owner", "canonical_notes").await;
    let mut canonical_user = server
        .shared
        .credentials
        .catalog()
        .get_user("canonical_owner")
        .unwrap()
        .unwrap();
    canonical_user.is_active = false;
    server
        .shared
        .credentials
        .catalog()
        .put_user(&canonical_user)
        .unwrap();
    plant_owner(
        server.shared.credentials.catalog(),
        "collection",
        "canonical_notes",
        "missing_stale_owner",
    );

    let report = verify_and_repair(&server.shared).await.unwrap();

    assert!(
        report.is_acceptable(),
        "startup repair must restore the canonical owner: {report}"
    );
    assert_eq!(
        owner_of(
            server.shared.credentials.catalog(),
            "collection",
            "canonical_notes"
        ),
        "canonical_owner"
    );
    assert_eq!(
        server
            .shared
            .credentials
            .catalog()
            .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "canonical_notes")
            .unwrap()
            .unwrap()
            .owner,
        "canonical_owner"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_repair_revokes_preexisting_grants_to_missing_users() {
    let server = TestServer::start().await;
    let catalog = server.shared.credentials.catalog();
    server
        .exec("CREATE USER lost_grantee PASSWORD 'probe-secret-99'")
        .await
        .unwrap();
    let grant = StoredPermission {
        target: "collection:1:any_notes".to_string(),
        grantee: "user:lost_grantee".to_string(),
        permission: "read".to_string(),
        granted_by: "nodedb".to_string(),
        granted_at: 0,
    };
    catalog.put_permission(&grant).unwrap();
    catalog.delete_user("lost_grantee").unwrap();

    let report = verify_and_repair(&server.shared).await.unwrap();

    assert!(
        report.is_acceptable(),
        "startup repair must revoke dangling grants: {report}"
    );
    assert_no_dangling_user_references(catalog);
    assert!(
        catalog
            .load_all_permissions()
            .unwrap()
            .iter()
            .all(|p| p.grantee != "user:lost_grantee")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_reassigns_every_owner_bearing_kind_and_sweeps_grants() {
    let server = TestServer::start().await;
    let catalog = server.shared.credentials.catalog().clone();

    server
        .shared
        .credentials
        .create_user(VICTIM, "pw", TenantId::new(TENANT), vec![Role::ReadWrite])
        .expect("create victim user");

    // Plant a representative spread of owner-bearing objects owned by the
    // victim: a collection (the only kind the pre-fix code handled), plus
    // a sequence and a function (kinds it did NOT handle). Each needs its
    // primary row AND its owner row.
    let mut coll = make_collection("victim_coll");
    coll.owner = VICTIM.to_string();
    catalog
        .put_collection(nodedb_types::DatabaseId::DEFAULT, &coll)
        .unwrap();
    plant_owner(&catalog, "collection", "victim_coll", VICTIM);

    let mut seq = make_sequence("victim_seq");
    seq.owner = VICTIM.to_string();
    catalog.put_sequence(&seq).unwrap();
    plant_owner(&catalog, "sequence", "victim_seq", VICTIM);

    let mut func = make_function("victim_fn");
    func.owner = VICTIM.to_string();
    catalog.put_function(&func).unwrap();
    plant_owner(&catalog, "function", "victim_fn", VICTIM);

    let mut proc = make_procedure("victim_proc");
    proc.owner = VICTIM.to_string();
    catalog.put_procedure(&proc).unwrap();
    plant_owner(&catalog, "procedure", "victim_proc", VICTIM);

    let mut trigger = make_trigger("victim_trigger", "victim_coll");
    trigger.owner = VICTIM.to_string();
    catalog.put_trigger(&trigger).unwrap();
    plant_owner(&catalog, "trigger", "victim_trigger", VICTIM);

    let mut mv = make_mv_sourced("victim_mv", "victim_coll");
    mv.owner = VICTIM.to_string();
    catalog.put_materialized_view(&mv).unwrap();
    plant_owner(&catalog, "materialized_view", "victim_mv", VICTIM);

    let mut schedule = make_schedule("victim_schedule");
    schedule.owner = VICTIM.to_string();
    catalog.put_schedule(&schedule).unwrap();
    plant_owner(&catalog, "schedule", "victim_schedule", VICTIM);

    let mut stream = make_stream("victim_stream");
    stream.owner = VICTIM.to_string();
    catalog.put_change_stream(&stream).unwrap();
    plant_owner(&catalog, "change_stream", "victim_stream", VICTIM);

    let aggregate = StoredContinuousAggregate {
        database_id: 11,
        tenant_id: TENANT,
        name: "victim_aggregate".to_string(),
        source: "victim_coll".to_string(),
        def_bytes: Vec::new(),
        owner: VICTIM.to_string(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    };
    catalog.put_continuous_aggregate(&aggregate).unwrap();
    plant_owner(&catalog, "continuous_aggregate", "victim_aggregate", VICTIM);
    plant_owner(&catalog, "index", "victim_index", VICTIM);

    let grant = StoredPermission {
        target: "collection:1:victim_coll".to_string(),
        grantee: format!("user:{VICTIM}"),
        permission: "read".to_string(),
        granted_by: "nodedb".to_string(),
        granted_at: 0,
    };
    catalog.put_permission(&grant).unwrap();
    server
        .shared
        .permissions
        .install_replicated_permission(&grant);

    assert!(
        verify_redb_integrity(&catalog).is_empty(),
        "planted state should be integrity-clean before the drop: {:?}",
        verify_redb_integrity(&catalog)
    );

    // The operation under test: a real DROP USER over pgwire as the
    // bootstrap superuser.
    server
        .exec(&format!("DROP USER {VICTIM}"))
        .await
        .expect("DROP USER should succeed");

    // 1. No dangling owner/permission references remain — this is the
    //    exact condition the boot check aborts on.
    let violations = verify_redb_integrity(&catalog);
    let dangling: Vec<_> = violations
        .iter()
        .filter(|v| {
            matches!(
                &v.kind,
                DivergenceKind::DanglingReference { from_kind, .. }
                    if *from_kind == "owner" || *from_kind == "permission"
            )
        })
        .collect();
    assert!(
        dangling.is_empty(),
        "DROP USER must leave no dangling owner/permission references — \
         every owned object reassigned and every grant swept. Got: {dangling:?}"
    );

    // 2. Boot repair pass would accept this catalog (startup would not
    //    abort).
    let report = verify_and_repair(&server.shared)
        .await
        .expect("verify_and_repair");
    assert!(
        report.is_acceptable(),
        "boot catalog sanity check must accept the post-drop catalog; \
         integrity_violations: {:?}",
        report.integrity_violations
    );

    // 3. Every owned object is now owned by the tenant admin, not the
    //    deleted user.
    assert_eq!(
        owner_of(&catalog, "collection", "victim_coll"),
        ADMIN_TARGET
    );
    for (kind, name) in [
        ("sequence", "victim_seq"),
        ("function", "victim_fn"),
        ("procedure", "victim_proc"),
        ("trigger", "victim_trigger"),
        ("materialized_view", "victim_mv"),
        ("schedule", "victim_schedule"),
        ("change_stream", "victim_stream"),
        ("continuous_aggregate", "victim_aggregate"),
        ("index", "victim_index"),
    ] {
        assert_eq!(
            owner_of(&catalog, kind, name),
            ADMIN_TARGET,
            "{kind} {name}"
        );
    }

    // 3b. Representative in-band owners are rewritten in lockstep with
    // the separate StoredOwner rows.
    assert_eq!(
        catalog
            .get_sequence(TENANT, "victim_seq")
            .unwrap()
            .unwrap()
            .owner,
        ADMIN_TARGET
    );
    assert_eq!(
        catalog
            .get_function(TENANT, "victim_fn")
            .unwrap()
            .unwrap()
            .owner,
        ADMIN_TARGET
    );
    assert_eq!(
        catalog
            .get_continuous_aggregate(11, TENANT, "victim_aggregate")
            .unwrap()
            .unwrap()
            .owner,
        ADMIN_TARGET
    );

    // 4. The grant to the victim is gone (no dangling grantee).
    assert!(
        !catalog
            .load_all_permissions()
            .unwrap()
            .iter()
            .any(|p| p.grantee == format!("user:{VICTIM}")),
        "every grant made to the dropped user must be swept"
    );
}
