// SPDX-License-Identifier: BUSL-1.1

//! Arrays, COPY runs and reads shared by the GeoTIFF COPY cases.

use std::collections::HashMap;

use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

/// The pixel-mode GEOREF of the scene grid.
pub const PIXEL_GEOREF: &str =
    "GEOREF (SRID 32647, X col, Y row, TRANSFORM (600000, 30, 0, 700000, 0, -30))";

/// The three scene bands as UINT16 attrs.
pub const SCENE_ATTRS: &str = "b1 UINT16, b2 UINT16, b3 UINT16";

/// A pixel-mode array over the scene: INT64 `row` and `col`.
pub fn pixel_array(name: &str, rows: &str, attrs: &str, georef: &str) -> String {
    format!(
        "CREATE ARRAY {name} DIMS (row INT64 [{rows}], col INT64 [0..255]) \
         ATTRS ({attrs}) TILE_EXTENTS (64, 64) {georef}"
    )
}

/// A world-mode array over the scene: FLOAT64 `y` and `x` in EPSG:32647.
pub fn world_array(name: &str) -> String {
    format!(
        "CREATE ARRAY {name} \
         DIMS (y FLOAT64 [694240.0..700000.0], x FLOAT64 [600000.0..607680.0]) \
         ATTRS ({SCENE_ATTRS}) TILE_EXTENTS (1920, 1920) GEOREF (SRID 32647, X x, Y y)"
    )
}

pub async fn create(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A refused statement: its SQLSTATE and message.
#[derive(Debug)]
pub struct Refusal {
    pub code: String,
    pub message: String,
}

/// Run `COPY <array> FROM '<path>' WITH (FORMAT geotiff<extra>)` and
/// return the row count of its `COPY <n>` tag.
pub async fn copy_geotiff(
    server: &TestServer,
    array: &str,
    path: &str,
    extra: &str,
) -> Result<u64, Refusal> {
    copy_sql(
        server,
        &format!("COPY {array} FROM '{path}' WITH (FORMAT geotiff{extra})"),
    )
    .await
}

/// Run the COPY statement `sql` and return the row count of its
/// `COPY <n>` tag.
pub async fn copy_sql(server: &TestServer, sql: &str) -> Result<u64, Refusal> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .map_err(|e| match e.as_db_error() {
            Some(db) => Refusal {
                code: db.code().code().to_string(),
                message: db.message().to_string(),
            },
            None => Refusal {
                code: String::new(),
                message: e.to_string(),
            },
        })?;
    let count = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::CommandComplete(rows) => Some(*rows),
        _ => None,
    });
    Ok(count.unwrap_or_else(|| panic!("{sql}: no command tag")))
}

/// Run a COPY that must be refused with `code` and commit no row.
pub async fn assert_refused(server: &TestServer, array: &str, path: &str, extra: &str, code: &str) {
    let err = copy_geotiff(server, array, path, extra)
        .await
        .expect_err("COPY must be refused");
    assert_eq!(err.code, code, "{array}: {err:?}");
    assert!(
        err.message.ends_with("(0 rows committed)"),
        "{array}: {err:?}"
    );
    assert_eq!(
        aggregate(server, array, "b1", "count", "").await,
        0.0,
        "{array} holds no cell"
    );
}

/// `ARRAY_AGG(array, attr, reducer<tail>)` as a number. No row or a NULL
/// result is 0.
pub async fn aggregate(
    server: &TestServer,
    array: &str,
    attr: &str,
    reducer: &str,
    tail: &str,
) -> f64 {
    let sql = format!("SELECT * FROM ARRAY_AGG('{array}', '{attr}', '{reducer}'{tail})");
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let Some(text) = rows.first().and_then(|row| row.get("result")) else {
        return 0.0;
    };
    if text.is_empty() {
        return 0.0;
    }
    text.parse()
        .unwrap_or_else(|_| panic!("{sql}: result '{text}' is not a number"))
}

/// The cells of pixel-mode `array` in a row and col window, keyed by
/// `(row, col)`, with their `b1` text.
pub async fn pixel_cells(
    server: &TestServer,
    array: &str,
    rows: (u32, u32),
    cols: (u32, u32),
) -> HashMap<(u32, u32), String> {
    let sql = format!(
        "SELECT * FROM ARRAY_SLICE('{array}', '{{\"row\":[{},{}],\"col\":[{},{}]}}', '*', 100000)",
        rows.0, rows.1, cols.0, cols.1
    );
    let found = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    found
        .iter()
        .map(|row| {
            let index = |column: &str| -> u32 {
                row.get(column)
                    .and_then(|text| text.parse::<f64>().ok())
                    .unwrap_or_else(|| panic!("{column} in {row:?}")) as u32
            };
            let b1 = row.get("b1").cloned().unwrap_or_default();
            ((index("row"), index("col")), b1)
        })
        .collect()
}
