//! Field-level change diffing for CDC events.
//!
//! Compares `old_value` and `new_value` JSON objects to produce per-field diffs.
//! Supports nested objects and arrays (block-level changes for collaborative docs).

use nodedb_types::json_msgpack::JsonValue;
use serde::{Deserialize, Serialize};

/// A single field-level change within a document mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDiff {
    /// Dot-path to the field: `"title"`, `"blocks[3].content"`, `"metadata.tags"`.
    pub field: String,
    /// Type of change.
    pub op: DiffOp,
    /// Previous value (for `Modified` and `Removed`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_value: Option<serde_json::Value>,
    /// New value (for `Modified` and `Added`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_value: Option<serde_json::Value>,
}

/// Type of field-level change.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiffOp {
    /// Field value changed (old → new). Used for non-string or complex changes.
    Modified,
    /// Field added (not present in old, present in new).
    Added,
    /// Field removed (present in old, not present in new).
    Removed,
    /// Array element inserted at index.
    ArrayInsert,
    /// Array element removed at index.
    ArrayRemove,
    /// Text inserted at a position within a string field.
    /// `new_value` contains the inserted text, `old_value` contains
    /// position as JSON number (character offset).
    TextInsert,
    /// Text deleted at a position within a string field.
    /// `old_value` contains the deleted text, `new_value` contains
    /// position as JSON number (character offset).
    TextDelete,
}

// ─── zerompk impls for DiffOp ─────────────────────────────────────────────

impl zerompk::ToMessagePack for DiffOp {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        let tag: u8 = match self {
            DiffOp::Modified => 0,
            DiffOp::Added => 1,
            DiffOp::Removed => 2,
            DiffOp::ArrayInsert => 3,
            DiffOp::ArrayRemove => 4,
            DiffOp::TextInsert => 5,
            DiffOp::TextDelete => 6,
        };
        writer.write_u8(tag)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for DiffOp {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(DiffOp::Modified),
            1 => Ok(DiffOp::Added),
            2 => Ok(DiffOp::Removed),
            3 => Ok(DiffOp::ArrayInsert),
            4 => Ok(DiffOp::ArrayRemove),
            5 => Ok(DiffOp::TextInsert),
            6 => Ok(DiffOp::TextDelete),
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

// ─── zerompk impls for FieldDiff ──────────────────────────────────────────

impl zerompk::ToMessagePack for FieldDiff {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        // Determine how many fields to write (omit None option fields).
        let field_count =
            2 + usize::from(self.old_value.is_some()) + usize::from(self.new_value.is_some());
        writer.write_map_len(field_count)?;
        writer.write_string("field")?;
        writer.write_string(&self.field)?;
        writer.write_string("op")?;
        self.op.write(writer)?;
        if let Some(ref v) = self.old_value {
            writer.write_string("old_value")?;
            JsonValue(v.clone()).write(writer)?;
        }
        if let Some(ref v) = self.new_value {
            writer.write_string("new_value")?;
            JsonValue(v.clone()).write(writer)?;
        }
        Ok(())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for FieldDiff {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_map_len()?;
        let mut field: Option<String> = None;
        let mut op: Option<DiffOp> = None;
        let mut old_value: Option<serde_json::Value> = None;
        let mut new_value: Option<serde_json::Value> = None;
        for _ in 0..len {
            let key = reader.read_string()?.into_owned();
            match key.as_str() {
                "field" => field = Some(reader.read_string()?.into_owned()),
                "op" => op = Some(DiffOp::read(reader)?),
                "old_value" => old_value = Some(JsonValue::read(reader)?.0),
                "new_value" => new_value = Some(JsonValue::read(reader)?.0),
                _ => {
                    // Skip unknown field values by reading as JsonValue.
                    JsonValue::read(reader)?;
                }
            }
        }
        Ok(FieldDiff {
            field: field.unwrap_or_default(),
            op: op.unwrap_or(DiffOp::Modified),
            old_value,
            new_value,
        })
    }
}

/// Compute field-level diffs between two JSON objects.
///
/// Returns an empty Vec if both values are identical or if either is not
/// an object (non-objects are opaque — we can't produce field-level diffs).
pub fn compute_field_diffs(old: &serde_json::Value, new: &serde_json::Value) -> Vec<FieldDiff> {
    let mut diffs = Vec::new();
    diff_values(&mut diffs, "", old, new);
    diffs
}

/// Recursive diff of two JSON values at a given path prefix.
fn diff_values(
    diffs: &mut Vec<FieldDiff>,
    prefix: &str,
    old: &serde_json::Value,
    new: &serde_json::Value,
) {
    if old == new {
        return;
    }

    match (old, new) {
        (serde_json::Value::Object(old_map), serde_json::Value::Object(new_map)) => {
            diff_objects(diffs, prefix, old_map, new_map);
        }
        (serde_json::Value::Array(old_arr), serde_json::Value::Array(new_arr)) => {
            diff_arrays(diffs, prefix, old_arr, new_arr);
        }
        // String fields: try to produce TextInsert/TextDelete for simple edits.
        (serde_json::Value::String(old_s), serde_json::Value::String(new_s))
            if !prefix.is_empty() =>
        {
            diff_strings(diffs, prefix, old_s, new_s);
        }
        _ => {
            // Scalar or type mismatch — emit a Modified diff.
            if !prefix.is_empty() {
                diffs.push(FieldDiff {
                    field: prefix.to_owned(),
                    op: DiffOp::Modified,
                    old_value: Some(old.clone()),
                    new_value: Some(new.clone()),
                });
            }
        }
    }
}

/// Diff two JSON objects field-by-field.
fn diff_objects(
    diffs: &mut Vec<FieldDiff>,
    prefix: &str,
    old_map: &serde_json::Map<String, serde_json::Value>,
    new_map: &serde_json::Map<String, serde_json::Value>,
) {
    // Fields in old that are modified or removed in new.
    for (key, old_val) in old_map {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match new_map.get(key) {
            Some(new_val) => diff_values(diffs, &path, old_val, new_val),
            None => {
                diffs.push(FieldDiff {
                    field: path,
                    op: DiffOp::Removed,
                    old_value: Some(old_val.clone()),
                    new_value: None,
                });
            }
        }
    }

    // Fields in new that were added (not in old).
    for (key, new_val) in new_map {
        if !old_map.contains_key(key) {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            diffs.push(FieldDiff {
                field: path,
                op: DiffOp::Added,
                old_value: None,
                new_value: Some(new_val.clone()),
            });
        }
    }
}

/// Diff two JSON arrays element-by-element.
///
/// For short arrays or same-length arrays, diffs per index.
/// For length changes, emits ArrayInsert/ArrayRemove for trailing elements.
fn diff_arrays(
    diffs: &mut Vec<FieldDiff>,
    prefix: &str,
    old_arr: &[serde_json::Value],
    new_arr: &[serde_json::Value],
) {
    let min_len = old_arr.len().min(new_arr.len());

    // Compare overlapping elements.
    for i in 0..min_len {
        let path = format!("{prefix}[{i}]");
        diff_values(diffs, &path, &old_arr[i], &new_arr[i]);
    }

    // Elements removed from old (old is longer).
    for (i, item) in old_arr.iter().enumerate().skip(min_len) {
        let path = format!("{prefix}[{i}]");
        diffs.push(FieldDiff {
            field: path,
            op: DiffOp::ArrayRemove,
            old_value: Some(item.clone()),
            new_value: None,
        });
    }

    // Elements added in new (new is longer).
    for (i, item) in new_arr.iter().enumerate().skip(min_len) {
        let path = format!("{prefix}[{i}]");
        diffs.push(FieldDiff {
            field: path,
            op: DiffOp::ArrayInsert,
            old_value: None,
            new_value: Some(item.clone()),
        });
    }
}

/// Diff two strings, producing TextInsert/TextDelete for simple edits
/// or Modified for complex changes.
///
/// Finds the longest common prefix and suffix, then examines the middle.
/// If only text was inserted (old middle is empty), emits TextInsert.
/// If only text was deleted (new middle is empty), emits TextDelete.
/// Otherwise, falls back to Modified.
fn diff_strings(diffs: &mut Vec<FieldDiff>, prefix: &str, old_s: &str, new_s: &str) {
    let old_chars: Vec<char> = old_s.chars().collect();
    let new_chars: Vec<char> = new_s.chars().collect();

    // Common prefix length.
    let common_prefix = old_chars
        .iter()
        .zip(new_chars.iter())
        .take_while(|(a, b)| a == b)
        .count();

    // Common suffix length (from the end, not overlapping with prefix).
    let old_remaining = old_chars.len() - common_prefix;
    let new_remaining = new_chars.len() - common_prefix;
    let common_suffix = old_chars[common_prefix..]
        .iter()
        .rev()
        .zip(new_chars[common_prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();

    let old_mid_len = old_remaining - common_suffix;
    let new_mid_len = new_remaining - common_suffix;

    if old_mid_len == 0 && new_mid_len > 0 {
        // Pure insertion.
        let inserted: String = new_chars[common_prefix..common_prefix + new_mid_len]
            .iter()
            .collect();
        diffs.push(FieldDiff {
            field: prefix.to_owned(),
            op: DiffOp::TextInsert,
            old_value: Some(serde_json::Value::Number(serde_json::Number::from(
                common_prefix,
            ))),
            new_value: Some(serde_json::Value::String(inserted)),
        });
    } else if new_mid_len == 0 && old_mid_len > 0 {
        // Pure deletion.
        let deleted: String = old_chars[common_prefix..common_prefix + old_mid_len]
            .iter()
            .collect();
        diffs.push(FieldDiff {
            field: prefix.to_owned(),
            op: DiffOp::TextDelete,
            old_value: Some(serde_json::Value::String(deleted)),
            new_value: Some(serde_json::Value::Number(serde_json::Number::from(
                common_prefix,
            ))),
        });
    } else {
        // Replacement or complex change — fall back to Modified.
        diffs.push(FieldDiff {
            field: prefix.to_owned(),
            op: DiffOp::Modified,
            old_value: Some(serde_json::Value::String(old_s.to_owned())),
            new_value: Some(serde_json::Value::String(new_s.to_owned())),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identical_objects_no_diffs() {
        let v = json!({"title": "Hello", "count": 42});
        let diffs = compute_field_diffs(&v, &v);
        assert!(diffs.is_empty());
    }

    #[test]
    fn scalar_field_modified() {
        let old = json!({"title": "Draft", "count": 1});
        let new = json!({"title": "Final", "count": 1});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "title");
        assert_eq!(diffs[0].op, DiffOp::Modified);
        assert_eq!(diffs[0].old_value, Some(json!("Draft")));
        assert_eq!(diffs[0].new_value, Some(json!("Final")));
    }

    #[test]
    fn field_added_and_removed() {
        let old = json!({"a": 1, "b": 2});
        let new = json!({"a": 1, "c": 3});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 2);

        let removed = diffs.iter().find(|d| d.op == DiffOp::Removed).unwrap();
        assert_eq!(removed.field, "b");

        let added = diffs.iter().find(|d| d.op == DiffOp::Added).unwrap();
        assert_eq!(added.field, "c");
    }

    #[test]
    fn nested_object_diff() {
        let old = json!({"meta": {"author": "Alice", "draft": true}});
        let new = json!({"meta": {"author": "Bob", "draft": true}});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "meta.author");
    }

    #[test]
    fn array_element_modified() {
        let old = json!({"blocks": [{"type": "h1", "text": "Old"}, {"type": "p", "text": "Same"}]});
        let new = json!({"blocks": [{"type": "h1", "text": "New"}, {"type": "p", "text": "Same"}]});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "blocks[0].text");
    }

    #[test]
    fn array_element_inserted() {
        let old = json!({"items": [1, 2]});
        let new = json!({"items": [1, 2, 3]});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "items[2]");
        assert_eq!(diffs[0].op, DiffOp::ArrayInsert);
    }

    #[test]
    fn array_element_removed() {
        let old = json!({"items": [1, 2, 3]});
        let new = json!({"items": [1, 2]});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "items[2]");
        assert_eq!(diffs[0].op, DiffOp::ArrayRemove);
    }

    #[test]
    fn deep_nested_block_change() {
        let old = json!({
            "blocks": [
                {"id": "blk-1", "type": "heading", "content": "Intro"},
                {"id": "blk-2", "type": "list", "children": [
                    {"id": "blk-3", "content": "Item A"},
                    {"id": "blk-4", "content": "Item B"}
                ]}
            ]
        });
        let new = json!({
            "blocks": [
                {"id": "blk-1", "type": "heading", "content": "Introduction"},
                {"id": "blk-2", "type": "list", "children": [
                    {"id": "blk-3", "content": "Item A"},
                    {"id": "blk-4", "content": "Item C"}
                ]}
            ]
        });
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().any(|d| d.field == "blocks[0].content"));
        assert!(
            diffs
                .iter()
                .any(|d| d.field == "blocks[1].children[1].content")
        );
    }

    #[test]
    fn insert_only_no_old_value() {
        let old = json!(null);
        let new = json!({"title": "New Doc"});
        let diffs = compute_field_diffs(&old, &new);
        // Type mismatch (null vs object) — whole thing is a modify at root.
        assert_eq!(diffs.len(), 0); // Root-level diff with empty prefix is skipped.
    }

    #[test]
    fn field_diff_serialization() {
        let diff = FieldDiff {
            field: "title".into(),
            op: DiffOp::Modified,
            old_value: Some(json!("Draft")),
            new_value: Some(json!("Final")),
        };
        let json = serde_json::to_string(&diff).unwrap();
        assert!(json.contains("\"modified\""));
        let parsed: FieldDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.field, "title");
    }

    #[test]
    fn text_insert_detected() {
        let old = json!({"content": "Hello"});
        let new = json!({"content": "Hello World"});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "content");
        assert_eq!(diffs[0].op, DiffOp::TextInsert);
        // old_value = position (5), new_value = inserted text
        assert_eq!(diffs[0].old_value, Some(json!(5)));
        assert_eq!(diffs[0].new_value, Some(json!(" World")));
    }

    #[test]
    fn text_delete_detected() {
        let old = json!({"content": "Hello World"});
        let new = json!({"content": "Hello"});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "content");
        assert_eq!(diffs[0].op, DiffOp::TextDelete);
        // old_value = deleted text, new_value = position (5)
        assert_eq!(diffs[0].old_value, Some(json!(" World")));
        assert_eq!(diffs[0].new_value, Some(json!(5)));
    }

    #[test]
    fn text_replacement_falls_back_to_modified() {
        let old = json!({"content": "Hello World"});
        let new = json!({"content": "Hello Earth"});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].op, DiffOp::Modified);
    }

    #[test]
    fn text_insert_at_beginning() {
        let old = json!({"title": "World"});
        let new = json!({"title": "Hello World"});
        let diffs = compute_field_diffs(&old, &new);
        assert_eq!(diffs[0].op, DiffOp::TextInsert);
        assert_eq!(diffs[0].old_value, Some(json!(0))); // Position 0.
        assert_eq!(diffs[0].new_value, Some(json!("Hello ")));
    }
}
