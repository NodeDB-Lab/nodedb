// SPDX-License-Identifier: BUSL-1.1

//! A write parked in one data group never holds back another group.
//!
//! The apply loop applies every data group this node hosts. A write to
//! collection A is parked at the fail gate `funnel::before_dispatch::<A>`,
//! after its record is appended and before its core holds it. A's group waits
//! on it, because every later entry of that group must reach its core after
//! it. A write to collection B, applied in another group, must apply and be
//! acknowledged while A's write is parked. Once released, A's write applies.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::Duration;

use crash_harness::CrashHarness;
use crash_harness::vshards::{names_in_distinct_data_groups, single_node_data_group};

/// How long the parked write must stay unacknowledged.
const PARKED_FOR: Duration = Duration::from_millis(1500);

/// How long the write in the other group may take. Well under the request
/// deadline a write waiting behind the parked one would wait out.
const OTHER_GROUP_BUDGET: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn a_parked_write_in_one_data_group_never_holds_back_another_group() {
    let [parked, other] = names_in_distinct_data_groups(["group_parked", "group_other"]);
    assert_ne!(
        single_node_data_group(&parked),
        single_node_data_group(&other)
    );
    let h = CrashHarness::new();
    let release = h.data_dir().join("release-parked-write");
    std::fs::write(&release, b"open").expect("open the gate for the setup");
    let mut h = h.with_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "funnel::before_dispatch::{parked}=wait_file({})",
            release.display()
        ),
    );
    h.spawn();
    h.wait_ready();
    for name in [&parked, &other] {
        h.exec(&format!(
            "CREATE COLLECTION {name} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await;
    }

    // Park the write to `parked` between its record and its core.
    std::fs::remove_file(&release).expect("close the gate");
    let conn_str = h.pgwire_conn_str();
    let insert = format!("INSERT INTO {parked} (k, v) VALUES ('p', 'held')");
    let parked_write = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        client
            .simple_query(&insert)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !parked_write.is_finished(),
        "the write to {parked} was not parked at the gate"
    );

    tokio::time::timeout(
        OTHER_GROUP_BUDGET,
        h.exec(&format!("INSERT INTO {other} (k, v) VALUES ('o', 'x')")),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "a write to {other} stalled behind the parked write to {parked}, \
             though the two apply in different data groups"
        )
    });
    assert_eq!(
        h.query_col_idx(&format!("SELECT v FROM {other} WHERE k = 'o'"), 0)
            .await,
        vec!["x".to_string()]
    );
    assert!(
        !parked_write.is_finished(),
        "the write to {parked} left the gate before its release"
    );

    std::fs::write(&release, b"release").expect("release the parked write");
    parked_write
        .await
        .expect("parked write task")
        .unwrap_or_else(|e| panic!("parked write: {e}"));
    assert_eq!(
        h.query_col_idx(&format!("SELECT v FROM {parked} WHERE k = 'p'"), 0)
            .await,
        vec!["held".to_string()]
    );
}
