/// SQL LIKE pattern matching.
///
/// Supports `%` (zero or more characters) and `_` (exactly one character).
/// When `case_insensitive` is true, both input and pattern are lowercased (ILIKE).
pub fn sql_like_match(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (input, pattern) = if case_insensitive {
        (input.to_lowercase(), pattern.to_lowercase())
    } else {
        (input.to_string(), pattern.to_string())
    };

    let input = input.as_bytes();
    let pattern = pattern.as_bytes();

    let (mut i, mut j) = (0usize, 0usize);
    let (mut star_j, mut star_i) = (usize::MAX, 0usize);

    while i < input.len() {
        if j < pattern.len() && (pattern[j] == b'_' || pattern[j] == input[i]) {
            i += 1;
            j += 1;
        } else if j < pattern.len() && pattern[j] == b'%' {
            star_j = j;
            star_i = i;
            j += 1;
        } else if star_j != usize::MAX {
            star_i += 1;
            i = star_i;
            j = star_j + 1;
        } else {
            return false;
        }
    }

    while j < pattern.len() && pattern[j] == b'%' {
        j += 1;
    }

    j == pattern.len()
}
