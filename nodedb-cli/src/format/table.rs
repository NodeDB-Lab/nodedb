//! Table-formatted output with aligned columns and box-drawing characters.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

/// Format a QueryResult as an aligned table.
pub fn format(qr: &QueryResult) -> String {
    if qr.columns.is_empty() && qr.rows.is_empty() {
        if qr.rows_affected > 0 {
            return format!("({} row(s) affected)", qr.rows_affected);
        }
        return "(empty result)".to_string();
    }

    let ncols = qr.columns.len();

    // Compute column widths.
    let mut widths: Vec<usize> = qr.columns.iter().map(|c| c.len()).collect();
    for row in &qr.rows {
        for (i, val) in row.iter().enumerate() {
            if i < ncols {
                let w = display_value(val).len();
                if w > widths[i] {
                    widths[i] = w;
                }
            }
        }
    }

    // Cap column widths.
    for w in &mut widths {
        if *w > 60 {
            *w = 60;
        }
    }

    let mut out = String::new();

    // Header.
    let header: Vec<String> = qr
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| pad(c, widths[i]))
        .collect();
    out.push_str(&format!(" {} \n", header.join(" | ")));

    // Separator.
    let sep: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
    out.push_str(&format!("─{}─\n", sep.join("─┼─")));

    // Rows.
    for row in &qr.rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, val)| {
                let s = display_value(val);
                if i < ncols {
                    pad_or_truncate(&s, widths[i])
                } else {
                    s
                }
            })
            .collect();
        out.push_str(&format!(" {} \n", cells.join(" | ")));
    }

    // Footer.
    out.push_str(&format!("({} row(s))\n", qr.rows.len()));

    out
}

fn display_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => format!("{f:.6}"),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => format!("\\x{}", hex(b)),
        Value::Array(arr) => {
            let inner: Vec<String> = arr.iter().map(display_value).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Object(obj) => {
            let pairs: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{k}: {}", display_value(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn pad(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(width - s.len()))
    }
}

fn pad_or_truncate(s: &str, width: usize) -> String {
    if s.len() > width {
        format!("{}...", &s[..width.saturating_sub(3)])
    } else {
        pad(s, width)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::result::QueryResult;

    #[test]
    fn format_simple_table() {
        let qr = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::String("u1".into()), Value::String("Alice".into())],
                vec![Value::String("u2".into()), Value::String("Bob".into())],
            ],
            rows_affected: 0,
        };
        let out = format(&qr);
        assert!(out.contains("id"));
        assert!(out.contains("Alice"));
        assert!(out.contains("Bob"));
        assert!(out.contains("(2 row(s))"));
    }

    #[test]
    fn format_empty() {
        let qr = QueryResult::empty();
        let out = format(&qr);
        assert!(out.contains("empty result"));
    }

    #[test]
    fn display_null() {
        assert_eq!(display_value(&Value::Null), "NULL");
    }

    #[test]
    fn display_truncation() {
        let long = "a".repeat(100);
        let truncated = pad_or_truncate(&long, 20);
        assert!(truncated.len() <= 20);
        assert!(truncated.ends_with("..."));
    }
}
