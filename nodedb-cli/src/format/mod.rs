pub mod csv;
pub mod json;
pub mod table;

use nodedb_types::result::QueryResult;

use crate::args::OutputFormat;

/// Format a query result according to the output format.
pub fn format_result(qr: &QueryResult, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => table::format(qr),
        OutputFormat::Json => json::format(qr),
        OutputFormat::Csv => csv::format(qr),
    }
}
