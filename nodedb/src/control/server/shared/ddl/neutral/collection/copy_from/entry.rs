// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_from_file`, the privilege and path checks, and the
//! engine's bulk-load route, which `EngineRules::plan_bulk_load` answers.
//!
//! A `COPY ... FROM` reads a file as the server's OS user, or fetches a
//! remote raster from the server's network, so only a superuser may run it.
//! A server file is opened once, and every later check and read uses that
//! handle. Text formats read one server file. A raster reads one or more
//! sources: server files, `http(s)://` URLs or `s3://` objects.
//!
//! The result type is [`DdlResult`] / [`DdlError`] throughout. Each error
//! site builds a `DdlError` directly via [`ddl_err`] so the whole call chain
//! speaks one error type.

use nodedb_types::DatabaseId;

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::statement::{CopyFormat, CopyRasterOptions, CopySource, CopyStdinFormat};

use crate::control::planner::context::{BulkLoadRoute, QueryContext};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::dml::authorize_write_target;
use crate::control::server::shared::ddl::neutral::collection::server_path::{
    require_superuser, validate_path,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use crate::control::trigger::statement_txn::{fires_joined_body, in_block, with_statement_txn};
use crate::control::trigger::{DmlEvent, TriggerScope};

use super::array_load::raster::copy_raster;
use super::array_load::{ArrayLoadCtx, ArrayLoader, LoadInput, TextFormat};
use super::csv_import::{CsvOptions, import_csv};
use super::import_ctx::ImportCtx;
use super::json_import::{import_json_array, import_ndjson};
use super::source_file::CopySourceFile;

/// How many bytes one read of an array load's file takes.
const FILE_READ_BYTES: usize = 1024 * 1024;

/// COPY FROM format, text, and GeoTIFF options.
#[derive(Clone, Copy, Debug)]
pub struct CopyFromOptions<'a> {
    pub format: Option<&'a CopyFormat>,
    pub delimiter: Option<char>,
    pub header: bool,
    /// The raster options. Empty unless `format` is `GeoTiff`.
    pub raster: &'a CopyRasterOptions,
}

/// The row format of a collection load.
enum RowFormat {
    Ndjson,
    JsonArray,
    Csv(CsvOptions),
}

/// Execute `COPY <collection> FROM <sources> [WITH (...)]`.
pub async fn copy_from_file(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    sources: &[CopySource],
    options: CopyFromOptions<'_>,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyFromOptions {
        format,
        delimiter,
        header,
        raster: _,
    } = options;

    require_superuser(identity, "COPY ... FROM '<path>'")?;
    authorize_write_target(state, identity, database_id, collection)?;
    for source in sources {
        if let CopySource::File(path) = source {
            validate_path(path, "COPY")?;
        }
    }
    let first = sources
        .first()
        .ok_or_else(|| ddl_err("42601", "COPY: FROM names no source"))?;
    let shown = first.display();

    // Determine format (caller has already auto-detected from extension; this is a safety net).
    let resolved_format = format.ok_or_else(|| {
        ddl_err(
            "42601",
            format!(
                "COPY: cannot infer format for '{shown}'; \
                 add WITH (FORMAT ndjson|json|csv|geotiff)"
            ),
        )
    })?;

    let tenant_id = identity.tenant_id;

    // The engine's rules decide how it loads: row by row, in array cell
    // chunks, or not at all (timeseries, spatial).
    let route = QueryContext::for_state(state)
        .bulk_load_route(collection, tenant_id, database_id)
        .map_err(|e| bulk_load_refusal(e, tenant_id))?;
    if let BulkLoadRoute::ArrayCells(_) = route {
        let ctx = ArrayLoadCtx {
            state,
            identity,
            database_id,
            txn_ctx,
        };
        return copy_array_from_file(&ctx, collection, sources, options).await;
    }

    let row_format = match resolved_format {
        CopyFormat::Ndjson => RowFormat::Ndjson,
        CopyFormat::JsonArray => RowFormat::JsonArray,
        CopyFormat::Csv => RowFormat::Csv(CsvOptions {
            delimiter: delimiter.unwrap_or(','),
            has_header: header,
        }),
        CopyFormat::GeoTiff => {
            return Err(ddl_err(
                "0A000",
                format!("COPY: FORMAT geotiff loads into an array; '{collection}' is not an array"),
            ));
        }
    };
    let path = single_file(sources)?;
    let bytes = CopySourceFile::open(path).await?.read_all().await?;

    // Each row fires its collection's BEFORE, INSTEAD OF and SYNC AFTER
    // bodies, which join the statement's transaction. Outside a transaction
    // block a COPY into such a collection runs in an implicit one: its rows
    // and the bodies' writes commit together, or none of them do.
    let implicit = !in_block(txn_ctx)
        && fires_joined_body(
            state,
            TriggerScope {
                database_id,
                tenant_id,
            },
            collection,
            DmlEvent::Insert,
        );
    let row_count = with_statement_txn(
        state,
        identity,
        txn_ctx,
        implicit,
        async |txn_ctx: &DmlTxnCtx<'_>| {
            let import_ctx = ImportCtx {
                state,
                identity,
                tenant_id,
                database_id,
                txn_ctx,
            };
            match row_format {
                RowFormat::Ndjson => import_ndjson(&import_ctx, collection, path, &bytes).await,
                RowFormat::JsonArray => {
                    import_json_array(&import_ctx, collection, path, &bytes).await
                }
                RowFormat::Csv(csv) => import_csv(&import_ctx, collection, path, &bytes, csv).await,
            }
        },
    )
    .await?;

    // The count is baked into `command` (not `rows_affected`) because the
    // native and HTTP encoders (`ddl_result_to_native`, `ddl_results_to_json`)
    // read only `command` for a `Status` result and drop `rows_affected` —
    // this is the only shape that surfaces the row count on every protocol.
    // pgwire's `ddl_result_to_response` renders it bare via the `None` arm,
    // which already produces the spec-correct `COPY <rows>` tag text.
    Ok(vec![DdlResult::Status {
        command: format!("COPY {row_count}"),
        rows_affected: None,
    }])
}

/// The DDL error of a refused bulk-load route. An engine's refusal keeps its
/// exact text under `0A000`.
pub(super) fn bulk_load_refusal(e: SqlError, tenant_id: nodedb_types::TenantId) -> DdlError {
    match e {
        SqlError::Unsupported { detail } => ddl_err("0A000", detail),
        other => DdlError::from_sql_error(other, tenant_id),
    }
}

/// `COPY <array> FROM '<path>'`: the file's CSV rows stream through the
/// array bulk-load core in reads of [`FILE_READ_BYTES`], as
/// `COPY <array> FROM STDIN` does. A GeoTIFF streams its decoded blocks.
async fn copy_array_from_file(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    sources: &[CopySource],
    options: CopyFromOptions<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyFromOptions {
        format,
        delimiter,
        header,
        raster,
    } = options;
    match format {
        Some(CopyFormat::Csv) => {}
        Some(CopyFormat::GeoTiff) => {
            let rows = copy_raster(ctx, array, sources, raster).await?;
            return Ok(vec![DdlResult::Status {
                command: format!("COPY {rows}"),
                rows_affected: None,
            }]);
        }
        Some(CopyFormat::Ndjson | CopyFormat::JsonArray) | None => {
            return Err(ddl_err(
                "0A000",
                format!("COPY: array '{array}' loads CSV rows; use WITH (FORMAT csv)"),
            ));
        }
    }
    let path = single_file(sources)?;
    let format = TextFormat {
        format: CopyStdinFormat::Csv,
        delimiter,
        header,
    };
    let mut source = CopySourceFile::open(path).await?;
    let mut loader = ArrayLoader::open(ctx, array, LoadInput::Text(format))?;
    let mut buf = vec![0u8; FILE_READ_BYTES];
    loop {
        let n = source.read_some(&mut buf).await?;
        if n == 0 {
            break;
        }
        loader.feed_text(ctx, &buf[..n]).await?;
    }
    let rows = loader.finish(ctx).await?;
    Ok(vec![DdlResult::Status {
        command: format!("COPY {rows}"),
        rows_affected: None,
    }])
}

/// The one server file a text format reads.
fn single_file(sources: &[CopySource]) -> Result<&str, DdlError> {
    match sources {
        [CopySource::File(path)] => Ok(path),
        _ => Err(ddl_err(
            "0A000",
            "COPY: text formats read one server file; HTTP and S3 sources and source lists \
             need FORMAT geotiff",
        )),
    }
}

/// Wrap a row-level error to include the row number in the message.
///
/// Row-level import helpers (`import_csv`, `import_ndjson`, `import_json_array`)
/// call `plan_and_dispatch`, which returns a protocol-neutral [`DdlError`] (not
/// a pgwire `PgWireError`), so this wraps the same type — only the message is
/// decorated with the row number.
pub(super) fn wrap_row_error(e: DdlError, line_no: usize, fmt: &str) -> DdlError {
    DdlError::new(
        e.sqlstate,
        format!("COPY: {fmt} row {line_no}: {}", e.message),
    )
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
