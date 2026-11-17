// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SEARCH INDEX` builds its index off the Data Plane core.
//!
//! The build is the REINDEX full-text rebuild. A failpoint holds the
//! rebuild thread. While it is held, the core that owns the collection
//! still answers a query on another collection, a write to the indexed
//! collection lands, and the statement has not answered. After the release
//! the statement answers, and the index holds the rows stored before the
//! build and the row written during it.

#[cfg(feature = "failpoints")]
mod held {
    use std::time::{Duration, Instant};

    use nodedb::fail_point::{FailAction, FailGuard};
    use nodedb_test_support::pgwire_harness::TestServer;

    use super::super::reindex_concurrent_writes::{events, text_ids};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_search_index_leaves_the_core_serving_while_its_build_is_held() {
        let log = events();
        let server = TestServer::start().await;
        server
            .exec("CREATE COLLECTION tb_held WITH (engine='document_schemaless')")
            .await
            .unwrap();
        for i in 1..=3 {
            server
                .exec(&format!(
                    "INSERT INTO tb_held {{ id: 'h{i}', body: 'alpha stored' }}"
                ))
                .await
                .unwrap();
        }
        server
            .exec("CREATE COLLECTION tb_other WITH (engine='document_schemaless')")
            .await
            .unwrap();
        server
            .exec("INSERT INTO tb_other { id: 'o1', body: 'other row' }")
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let release = dir.path().join("release");
        let _guard = FailGuard::install(
            "reindex::fts_build_hold",
            FailAction::WaitForFile(release.clone()),
        );

        // The CREATE runs on its own connection: it answers only after its
        // build cuts over, and the cutover waits for the release.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            server.pg_port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let create = tokio::spawn(async move {
            client
                .simple_query("CREATE SEARCH INDEX ON tb_held (body)")
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        });

        let deadline = Instant::now() + Duration::from_secs(30);
        while log.count("rebuild_started", "fts", "tb_held") == 0 {
            assert!(Instant::now() < deadline, "the build never started");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The core that holds the build answers other requests.
        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            server.query_text("SELECT id FROM tb_other"),
        )
        .await
        .expect("a query on another collection must answer while the build is held")
        .unwrap();
        assert_eq!(rows, ["o1"]);
        tokio::time::timeout(
            Duration::from_secs(10),
            server.exec("INSERT INTO tb_held { id: 'h4', body: 'alpha during' }"),
        )
        .await
        .expect("a write to the indexed collection must land while the build is held")
        .unwrap();
        assert!(
            !create.is_finished(),
            "CREATE SEARCH INDEX must not answer before its build cuts over"
        );
        assert_eq!(log.count("atomic_cutover", "fts", "tb_held"), 0);

        std::fs::write(&release, b"").unwrap();
        tokio::time::timeout(Duration::from_secs(60), create)
            .await
            .expect("CREATE SEARCH INDEX must answer after the release")
            .unwrap()
            .expect("CREATE SEARCH INDEX must succeed");
        assert_eq!(
            log.count("atomic_cutover", "fts", "tb_held"),
            log.count("rebuild_started", "fts", "tb_held"),
            "the statement answers only after its cutover"
        );
        assert_eq!(
            text_ids(&server, "tb_held", "alpha").await,
            ["h1", "h2", "h3", "h4"],
            "the build indexed the stored rows, and the row written during it"
        );
    }
}
