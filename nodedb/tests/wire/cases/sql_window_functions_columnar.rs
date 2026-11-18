// SPDX-License-Identifier: BUSL-1.1

//! Window functions answer the same values on a columnar collection as on a
//! document collection. A window column must never come back NULL for every
//! row with no error.

use crate::harness::TestServer;

async fn seed(server: &TestServer, table: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {table} (id INT PRIMARY KEY, dept TEXT, salary INT) \
             WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {table} (id, dept, salary) VALUES \
             (1, 'eng', 100), (2, 'eng', 200), (3, 'ops', 300), \
             (4, 'ops', NULL), (5, 'hr', 150), (6, 'eng', 100)"
        ))
        .await
        .unwrap();
}

/// Each row as `(id, window cell)`. A NULL cell reads as the empty string.
async fn pairs(server: &TestServer, sql: &str) -> Vec<(String, String)> {
    let rows = server.query_rows(sql).await.unwrap();
    rows.into_iter()
        .map(|r| {
            (
                r.first().cloned().unwrap_or_default(),
                r.get(1).cloned().unwrap_or_default(),
            )
        })
        .collect()
}

fn cells(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Read a numeric cell as an integer, so `850` and `850.0` compare equal.
fn int_cells(rows: &[(String, String)]) -> Vec<(String, Option<i64>)> {
    rows.iter()
        .map(|(id, v)| (id.clone(), v.parse::<f64>().ok().map(|f| f as i64)))
        .collect()
}

async fn check_windows(server: &TestServer, table: &str) {
    let rn = pairs(
        server,
        &format!("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM {table} ORDER BY id"),
    )
    .await;
    assert_eq!(
        rn,
        cells(&[
            ("1", "1"),
            ("2", "2"),
            ("3", "3"),
            ("4", "4"),
            ("5", "5"),
            ("6", "6")
        ]),
        "{table}: ROW_NUMBER"
    );

    let rk = pairs(
        server,
        &format!(
            "SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rk \
             FROM {table} WHERE salary IS NOT NULL ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        rk,
        cells(&[("1", "2"), ("2", "1"), ("3", "1"), ("5", "1"), ("6", "2")]),
        "{table}: RANK"
    );

    let run = pairs(
        server,
        &format!("SELECT id, SUM(salary) OVER (ORDER BY id) AS run FROM {table} ORDER BY id"),
    )
    .await;
    assert_eq!(
        int_cells(&run),
        vec![
            ("1".to_string(), Some(100)),
            ("2".to_string(), Some(300)),
            ("3".to_string(), Some(600)),
            ("4".to_string(), Some(600)),
            ("5".to_string(), Some(750)),
            ("6".to_string(), Some(850)),
        ],
        "{table}: SUM OVER, raw {run:?}"
    );

    let prev = pairs(
        server,
        &format!("SELECT id, LAG(salary) OVER (ORDER BY id) AS prev FROM {table} ORDER BY id"),
    )
    .await;
    assert_eq!(
        int_cells(&prev),
        vec![
            ("1".to_string(), None),
            ("2".to_string(), Some(100)),
            ("3".to_string(), Some(200)),
            ("4".to_string(), Some(300)),
            ("5".to_string(), None),
            ("6".to_string(), Some(150)),
        ],
        "{table}: LAG, raw {prev:?}"
    );

    // The LIMIT cuts rows after the window pass, never before it.
    let top = pairs(
        server,
        &format!(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM {table} \
             ORDER BY id DESC LIMIT 3"
        ),
    )
    .await;
    assert_eq!(
        top,
        cells(&[("6", "6"), ("5", "5"), ("4", "4")]),
        "{table}: ROW_NUMBER with ORDER BY DESC LIMIT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_functions_answer_values_on_columnar() {
    let server = TestServer::start().await;
    seed(&server, "w_columnar", "columnar").await;
    check_windows(&server, "w_columnar").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_functions_answer_values_on_document_strict() {
    let server = TestServer::start().await;
    seed(&server, "w_strict", "document_strict").await;
    check_windows(&server, "w_strict").await;
}
