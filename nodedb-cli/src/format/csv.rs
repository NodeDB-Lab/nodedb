//! CSV output format.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

/// Format a QueryResult as CSV with a header row.
pub fn format(qr: &QueryResult) -> String {
    if qr.columns.is_empty() && qr.rows.is_empty() {
        return String::new();
    }

    let mut out = String::new();

    // Header.
    out.push_str(&qr.columns.join(","));
    out.push('\n');

    // Rows.
    for row in &qr.rows {
        let cells: Vec<String> = row.iter().map(csv_value).collect();
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    out
}

fn csv_value(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        Value::Bytes(b) => format!(
            "\\x{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
        Value::Array(_) | Value::Object(_) => {
            let json = serde_json::to_string(v).unwrap_or_default();
            format!("\"{}\"", json.replace('"', "\"\""))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_output() {
        let qr = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::Integer(1), Value::String("Alice".into())],
                vec![Value::Integer(2), Value::String("Bob, Jr.".into())],
            ],
            rows_affected: 0,
        };
        let out = format(&qr);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,Alice");
        assert!(lines[2].contains("\"Bob, Jr.\"")); // quoted because of comma
    }
}
