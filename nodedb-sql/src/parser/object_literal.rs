//! Parser for `{ key: value }` object literal syntax.

use std::collections::HashMap;

use nodedb_types::Value;

/// Parse a `{ key: value, ... }` object literal into a field map.
///
/// Returns `None` if the input doesn't start with `{` (not an object literal).
/// Returns `Some(Err(msg))` on parse errors (malformed object literal).
/// Returns `Some(Ok(fields))` on success.
pub fn parse_object_literal(s: &str) -> Option<Result<HashMap<String, Value>, String>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('{') {
        return None;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let mut pos = 0;
    Some(parse_object(&chars, &mut pos))
}

/// Parse `[{ ... }, { ... }]` — an array of object literals for batch insert.
///
/// Returns `None` if the input doesn't start with `[` (not an array literal).
/// Returns `Some(Err(msg))` on parse errors.
/// Returns `Some(Ok(vec))` on success — each element must be an object.
pub fn parse_object_literal_array(s: &str) -> Option<Result<Vec<HashMap<String, Value>>, String>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') {
        return None;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let mut pos = 0;

    // Consume '['
    pos += 1;
    let mut objects = Vec::new();
    loop {
        skip_ws(&chars, &mut pos);
        if pos >= chars.len() {
            return Some(Err("unterminated array of objects".to_string()));
        }
        if chars[pos] == ']' {
            break;
        }
        if chars[pos] == ',' {
            pos += 1;
            continue;
        }
        if chars[pos] != '{' {
            return Some(Err(format!(
                "expected '{{' at position {pos}, found '{}'",
                chars[pos]
            )));
        }
        match parse_object(&chars, &mut pos) {
            Ok(obj) => objects.push(obj),
            Err(e) => return Some(Err(e)),
        }
        skip_ws(&chars, &mut pos);
        if pos < chars.len() && chars[pos] == ',' {
            pos += 1;
        }
    }
    Some(Ok(objects))
}

fn skip_ws(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn parse_ident(chars: &[char], pos: &mut usize) -> String {
    let mut s = String::new();
    while *pos < chars.len() {
        let c = chars[*pos];
        if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
            s.push(c);
            *pos += 1;
        } else {
            break;
        }
    }
    s
}

fn parse_string(chars: &[char], pos: &mut usize) -> Result<String, String> {
    // Expect opening single-quote
    if *pos >= chars.len() || chars[*pos] != '\'' {
        return Err(format!(
            "expected single quote at position {}, found {:?}",
            pos,
            chars.get(*pos)
        ));
    }
    *pos += 1; // consume opening quote
    let mut s = String::new();
    loop {
        if *pos >= chars.len() {
            return Err("unterminated string literal".to_string());
        }
        if chars[*pos] == '\'' {
            *pos += 1; // consume quote
            // SQL escaped quote: '' → '
            if *pos < chars.len() && chars[*pos] == '\'' {
                s.push('\'');
                *pos += 1;
            } else {
                break; // end of string
            }
        } else {
            s.push(chars[*pos]);
            *pos += 1;
        }
    }
    Ok(s)
}

fn parse_number(chars: &[char], pos: &mut usize) -> Result<Value, String> {
    let start = *pos;
    if *pos < chars.len() && chars[*pos] == '-' {
        *pos += 1;
    }
    while *pos < chars.len() && chars[*pos].is_ascii_digit() {
        *pos += 1;
    }
    let is_float = *pos < chars.len() && chars[*pos] == '.';
    if is_float {
        *pos += 1; // consume '.'
        while *pos < chars.len() && chars[*pos].is_ascii_digit() {
            *pos += 1;
        }
    }
    let raw: String = chars[start..*pos].iter().collect();
    if is_float {
        raw.parse::<f64>()
            .map(Value::Float)
            .map_err(|_| format!("invalid float: {raw}"))
    } else {
        raw.parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| format!("invalid integer: {raw}"))
    }
}

fn parse_array(chars: &[char], pos: &mut usize) -> Result<Vec<Value>, String> {
    // Expect '['
    if *pos >= chars.len() || chars[*pos] != '[' {
        return Err(format!(
            "expected '[' at position {pos}, found {:?}",
            chars.get(*pos)
        ));
    }
    *pos += 1; // consume '['
    let mut items = Vec::new();
    loop {
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err("unterminated array literal".to_string());
        }
        if chars[*pos] == ']' {
            *pos += 1; // consume ']'
            break;
        }
        // trailing comma already consumed; skip it
        if chars[*pos] == ',' {
            *pos += 1;
            continue;
        }
        let val = parse_value(chars, pos)?;
        items.push(val);
        skip_ws(chars, pos);
        if *pos < chars.len() && chars[*pos] == ',' {
            *pos += 1; // consume ','
        }
    }
    Ok(items)
}

fn parse_object(chars: &[char], pos: &mut usize) -> Result<HashMap<String, Value>, String> {
    // Expect '{'
    if *pos >= chars.len() || chars[*pos] != '{' {
        return Err(format!(
            "expected '{{' at position {pos}, found {:?}",
            chars.get(*pos)
        ));
    }
    *pos += 1; // consume '{'
    let mut map = HashMap::new();
    loop {
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err("unterminated object literal".to_string());
        }
        if chars[*pos] == '}' {
            *pos += 1; // consume '}'
            break;
        }
        // Trailing comma: skip and re-check for '}'
        if chars[*pos] == ',' {
            *pos += 1;
            continue;
        }

        // Parse key (must be an unquoted identifier)
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err("expected key, reached end of input".to_string());
        }
        let first = chars[*pos];
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(format!(
                "expected identifier key at position {pos}, found '{first}'"
            ));
        }
        let key = parse_ident(chars, pos);
        if key.is_empty() {
            return Err(format!("expected non-empty key at position {pos}"));
        }

        // Expect ':'
        skip_ws(chars, pos);
        if *pos >= chars.len() || chars[*pos] != ':' {
            return Err(format!(
                "expected ':' after key '{key}' at position {pos}, found {:?}",
                chars.get(*pos)
            ));
        }
        *pos += 1; // consume ':'

        // Parse value
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err(format!(
                "expected value for key '{key}', reached end of input"
            ));
        }
        if chars[*pos] == '}' || chars[*pos] == ',' {
            return Err(format!(
                "expected value for key '{key}', found '{}'",
                chars[*pos]
            ));
        }
        let val = parse_value(chars, pos)?;
        map.insert(key, val);

        // Optional comma
        skip_ws(chars, pos);
        if *pos < chars.len() && chars[*pos] == ',' {
            *pos += 1;
        }
    }
    Ok(map)
}

fn parse_value(chars: &[char], pos: &mut usize) -> Result<Value, String> {
    skip_ws(chars, pos);
    if *pos >= chars.len() {
        return Err("unexpected end of input while parsing value".to_string());
    }
    match chars[*pos] {
        '\'' => parse_string(chars, pos).map(Value::String),
        '{' => parse_object(chars, pos).map(Value::Object),
        '[' => parse_array(chars, pos).map(Value::Array),
        '-' | '0'..='9' => parse_number(chars, pos),
        _ => {
            // bare word: true / false / null / identifier
            let word = parse_ident(chars, pos);
            match word.to_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                "null" => Ok(Value::Null),
                _ if word.is_empty() => Err(format!(
                    "unexpected character '{}' at position {pos}",
                    chars[*pos]
                )),
                _ => Err(format!("unknown bare word: '{word}'")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> HashMap<String, Value> {
        parse_object_literal(s).unwrap().unwrap()
    }

    #[test]
    fn simple_string_and_int() {
        let m = parse("{ name: 'Alice', age: 30 }");
        assert_eq!(m["name"], Value::String("Alice".to_string()));
        assert_eq!(m["age"], Value::Integer(30));
    }

    #[test]
    fn nested_object() {
        let m = parse("{ addr: { city: 'NYC' } }");
        let inner = match &m["addr"] {
            Value::Object(o) => o,
            _ => panic!("expected Object"),
        };
        assert_eq!(inner["city"], Value::String("NYC".to_string()));
    }

    #[test]
    fn array_value() {
        let m = parse("{ tags: ['a', 'b'] }");
        assert_eq!(
            m["tags"],
            Value::Array(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ])
        );
    }

    #[test]
    fn mixed_types() {
        let m = parse("{ a: 'str', b: 42, c: 2.78, d: true, e: false, f: null }");
        assert_eq!(m["a"], Value::String("str".to_string()));
        assert_eq!(m["b"], Value::Integer(42));
        assert_eq!(m["c"], Value::Float(2.78));
        assert_eq!(m["d"], Value::Bool(true));
        assert_eq!(m["e"], Value::Bool(false));
        assert_eq!(m["f"], Value::Null);
    }

    #[test]
    fn escaped_quotes() {
        let m = parse("{ name: 'O''Brien' }");
        assert_eq!(m["name"], Value::String("O'Brien".to_string()));
    }

    #[test]
    fn empty_object() {
        let m = parse("{ }");
        assert!(m.is_empty());
    }

    #[test]
    fn trailing_comma() {
        let m = parse("{ name: 'Alice', }");
        assert_eq!(m["name"], Value::String("Alice".to_string()));
    }

    #[test]
    fn not_an_object_returns_none() {
        assert!(parse_object_literal("not an object").is_none());
    }

    #[test]
    fn missing_value_returns_err() {
        let result = parse_object_literal("{ name: }");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn missing_key_returns_err() {
        let result = parse_object_literal("{ : 'val' }");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn negative_numbers() {
        let m = parse("{ x: -42, y: -2.78 }");
        assert_eq!(m["x"], Value::Integer(-42));
        assert_eq!(m["y"], Value::Float(-2.78));
    }

    #[test]
    fn nested_array_in_object() {
        let m = parse("{ data: { items: [1, 2, 3] } }");
        let inner = match &m["data"] {
            Value::Object(o) => o,
            _ => panic!("expected Object"),
        };
        assert_eq!(
            inner["items"],
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ])
        );
    }

    #[test]
    fn dotted_key() {
        let m = parse("{ metadata.source: 'web' }");
        assert_eq!(m["metadata.source"], Value::String("web".to_string()));
    }

    #[test]
    fn parse_array_of_objects() {
        let result = parse_object_literal_array("[{ name: 'Alice' }, { name: 'Bob' }]")
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["name"], Value::String("Alice".to_string()));
        assert_eq!(result[1]["name"], Value::String("Bob".to_string()));
    }

    #[test]
    fn parse_array_empty() {
        let result = parse_object_literal_array("[]").unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_array_not_array_returns_none() {
        assert!(parse_object_literal_array("{ name: 'Alice' }").is_none());
    }

    #[test]
    fn parse_array_non_object_element_returns_err() {
        let result = parse_object_literal_array("[42]");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn parse_array_trailing_comma() {
        let result = parse_object_literal_array("[{ a: 1 }, { b: 2 },]")
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 2);
    }
}
