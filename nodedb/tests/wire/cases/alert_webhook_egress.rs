// SPDX-License-Identifier: BUSL-1.1

//! A `CREATE ALERT ... NOTIFY WEBHOOK` URL is user-supplied. The `[egress]`
//! policy checks it when the alert is defined, so a URL that names a private
//! or IP-literal host never becomes a stored rule.

use crate::harness::TestServer;

fn alert_sql(name: &str, url: &str) -> String {
    format!(
        "CREATE ALERT {name} ON alert_egress_metrics CONDITION avg(temperature) > 90 \
         WINDOW '5m' NOTIFY WEBHOOK '{url}'"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_webhook_the_egress_policy_refuses_is_not_defined() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION alert_egress_metrics")
        .await
        .expect("CREATE COLLECTION");

    for (name, url) in [
        ("alert_ip_literal", "https://127.0.0.1:9/hook"),
        ("alert_metadata", "http://169.254.169.254/latest/meta-data/"),
        ("alert_plain_http", "http://hooks.example.com/hook"),
    ] {
        server.expect_error(&alert_sql(name, url), "[egress]").await;
    }

    server
        .exec(&alert_sql("alert_public", "https://hooks.example.com/hook"))
        .await
        .expect("an https webhook to a DNS name is defined");
    let shown = server
        .query_rows("SHOW ALERTS")
        .await
        .expect("SHOW ALERTS")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(shown.contains("alert_public"), "{shown}");
    for refused in ["alert_ip_literal", "alert_metadata", "alert_plain_http"] {
        assert!(!shown.contains(refused), "{refused} was stored: {shown}");
    }
}
