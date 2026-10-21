#!/usr/bin/env python3
"""Reject parser and decoder patterns that invalidate byte/UTF-8 safety proofs.

This is deliberately a conservative source gate, not a Rust parser.  It masks
non-code faithfully, scopes checks to production functions, and reports every
remaining suspicious construct for remediation rather than carrying a baseline.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APPROVED_CAPACITY_HELPERS = {
    ("nodedb-array/src/codec/limits.rs", "checked_capacity"),
    ("nodedb-codec/src/bounds.rs", "checked_capacity"),
    ("nodedb-codec/src/bounds.rs", "decoded_len"),
    ("nodedb-raster/src/limits.rs", "checked_raster_capacity"),
    ("nodedb-spatial/src/wkb/read.rs", "checked_wkb_capacity"),
    ("nodedb-types/src/backup_envelope/read.rs", "checked_section_capacity"),
    ("nodedb-types/src/decode_bounds.rs", "checked_decode_capacity"),
}
APPROVED_CAPACITY_NAMES = {name for _, name in APPROVED_CAPACITY_HELPERS}
PROTECTED_PATH = re.compile(r"(?:parser|parse|wire|ddl|trigger|check)", re.I)
BYTE_TYPES = {"u8", "i8", "c_uchar", "c_char"}
# Source roots under these directory names belong to vendored dependencies or build output,
# rather than to the first-party Cargo workspace.
EXCLUDED_SOURCE_PATH_PARTS = frozenset({
    "vendor",
    "target",
    "deps",
    "build",
    "node_modules",
    "third_party",
    "third-party",
})


def mask_rust(source: str) -> str:
    """Mask comments and literals, preserving byte positions and newlines."""
    out = list(source)
    i = 0
    n = len(source)
    depth = 0
    while i < n:
        if depth:
            if source.startswith("/*", i):
                out[i:i + 2] = "  "; depth += 1; i += 2
            elif source.startswith("*/", i):
                out[i:i + 2] = "  "; depth -= 1; i += 2
            else:
                if source[i] != "\n": out[i] = " "
                i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i); end = n if end < 0 else end
            out[i:end] = " " * (end - i); i = end; continue
        if source.startswith("/*", i):
            out[i:i + 2] = "  "; depth = 1; i += 2; continue
        raw = re.match(r'(?:b)?r(#{0,255})"', source[i:])
        if raw:
            marker = '"' + raw.group(1)
            end = source.find(marker, i + raw.end())
            end = n if end < 0 else end + len(marker)
            for j in range(i, end):
                if source[j] != "\n": out[j] = " "
            i = end; continue
        if source[i] == '"' or source.startswith('b"', i):
            start = i; i += 2 if source.startswith('b"', start) else 1
            escaped = False
            while i < n:
                c = source[i]
                if c == '"' and not escaped:
                    i += 1; break
                escaped = c == "\\" and not escaped
                if c != "\\": escaped = False
                i += 1
            for j in range(start, i):
                if source[j] != "\n": out[j] = " "
            continue
        char = re.match(r"(?:b)?'(?:\\.|[^'\\\n])'", source[i:])
        if char:
            end = i + char.end(); out[i:end] = " " * (end - i); i = end; continue
        i += 1
    return "".join(out)


def matching_brace(code: str, opening: int) -> int | None:
    depth = 0
    for pos in range(opening, len(code)):
        if code[pos] == "{": depth += 1
        elif code[pos] == "}":
            depth -= 1
            if depth == 0: return pos
    return None


def mask_test_items(source: str, code: str) -> str:
    """Mask each #[cfg(test)] module/function, including nested braces."""
    out = list(code)
    for attr in re.finditer(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]", code):
        brace = code.find("{", attr.end())
        if brace < 0: continue
        # Attributes are only test items when the declaration before its body is
        # a function/module; never hide an unrelated conditional expression.
        header = code[attr.end():brace]
        if not re.search(r"\b(?:mod|fn)\b", header): continue
        end = matching_brace(code, brace)
        if end is None: continue
        for pos in range(attr.start(), end + 1):
            if source[pos] != "\n": out[pos] = " "
    return "".join(out)


def line_of(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def functions(code: str):
    pattern = re.compile(r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)\s*\((.*?)\)\s*(?:->[^\{]+)?\{", re.S)
    for match in pattern.finditer(code):
        end = matching_brace(code, match.end() - 1)
        if end is not None:
            yield match.group(1), match.group(2), match.start(), match.end(), end


def aliases_for(body: str, roots: set[str]) -> set[str]:
    aliases = set(roots)
    changed = True
    while changed:
        changed = False
        for match in re.finditer(r"\blet\s+(\w+)\s*=\s*(\w+)(?:\s*\.\s*(?:trim|trim_start|trim_end|strip_prefix|strip_suffix)\s*\([^;]*?\))?\s*;", body):
            if match.group(2) in aliases and match.group(1) not in aliases:
                aliases.add(match.group(1)); changed = True
    return aliases


def case_fold_violations(code: str, source: str):
    found = []
    for _, _, start, body_start, end in functions(code):
        body = code[body_start:end]
        aliases: set[str] = set()
        # Include chained simple aliases of the original string root.
        for binding in re.finditer(r"\blet\s+(\w+)\s*=\s*(\w+)\s*\.\s*to_(?:upper|lower)case\s*\(\s*\)", body):
            folded, root = binding.groups()
            roots = aliases_for(body[:binding.start()], {root})
            for candidate in roots:
                indexed = re.search(rf"\b{re.escape(candidate)}\s*\[", body[binding.end():])
                if indexed:
                    found.append(line_of(source, body_start + binding.end() + indexed.start()))
            aliases.add(folded)
    return found


def fixed_range_violations(rel: str, code: str, source: str):
    if not PROTECTED_PATH.search(rel): return []
    found = []
    numeric_range = re.compile(r"\[\s*(?:\d+\s*)?(?:\.\.=|\.\.)\s*(?:\d+\s*)?\]")
    for _, params, _, body_start, end in functions(code):
        roots = set(re.findall(r"(?:^|,)\s*(\w+)\s*:\s*&\s*str\b", params))
        if not roots: continue
        body = code[body_start:end]
        for name in aliases_for(body, roots):
            for hit in re.finditer(rf"\b{re.escape(name)}\s*{numeric_range.pattern}", body):
                found.append(line_of(source, body_start + hit.start()))
    return found


def byte_pointer_violations(code: str, source: str):
    found = []
    for _, params, _, body_start, end in functions(code):
        roots = set(re.findall(r"(?:^|,)\s*(\w+)\s*:\s*&\s*\[\s*u8\s*\]", params))
        if not roots: continue
        body = code[body_start:end]
        aliases = aliases_for(body, roots)
        for name in aliases:
            # `loadu(foo.as_ptr() as *const T)` is an explicit unaligned load
            # primitive; it does not create a typed Rust reference and is safe.
            for hit in re.finditer(rf"\b{re.escape(name)}\s*\.\s*as_ptr\s*\(\s*\)\s*as\s*\*\s*(?:const|mut)\s*([A-Za-z_:][\w:]*)", body):
                typ = hit.group(1).split("::")[-1]
                prefix = body[max(0, hit.start() - 80):hit.start()]
                if typ not in BYTE_TYPES and "loadu" not in prefix:
                    found.append(line_of(source, body_start + hit.start()))
            for hit in re.finditer(rf"\b{re.escape(name)}\s*\.\s*as_ptr\s*\(\s*\)\s*\.\s*cast\s*::\s*<\s*([A-Za-z_:][\w:]*)", body):
                if hit.group(1).split("::")[-1] not in BYTE_TYPES:
                    found.append(line_of(source, body_start + hit.start()))
            for hit in re.finditer(rf"(?:from_raw_parts(?:_mut)?|\*\s*{re.escape(name)}\s*\.\s*as_ptr)", body):
                window = body[max(0, hit.start() - 120):hit.end() + 120]
                if name in window:
                    found.append(line_of(source, body_start + hit.start()))
    return found


def allocation_violations(rel: str, code: str, source: str):
    found = []
    for name, params, _, body_start, end in functions(code):
        if not re.search(r"(?:decode|read|parse|from_bytes)", name, re.I): continue
        roots = set(re.findall(r"(?:^|,)\s*(\w+)\s*:\s*&\s*\[\s*u8\s*\]", params))
        if not roots: continue
        body = code[body_start:end]
        safe_vars = set()
        for helper in APPROVED_CAPACITY_NAMES:
            safe_vars.update(
                re.findall(
                    rf"\blet\s+(?:Some\s*\(\s*)?(\w+)\s*\)?\s*=\s*(?:[A-Za-z_]\w*::)*{re.escape(helper)}\s*\(",
                    body,
                )
            )
        patterns = [
            re.compile(
                r"\b(?:Vec|VecDeque|HashMap|HashSet)\s*::\s*with_capacity\s*\(\s*([^;\n]+)\s*\)"
            ),
            re.compile(r"\bvec\s*!\s*\[[^;\]]*;\s*([^\]]+?)\s*\]"),
        ]
        for pattern in patterns:
            for hit in pattern.finditer(body):
                expr = hit.group(1).strip()
                safe = expr in safe_vars or bool(re.fullmatch(r"\d+", expr))
                safe = safe or bool(re.fullmatch(r"\w+\s*\.\s*len\s*\(\s*\)(?:\s*/\s*\d+)?", expr))
                if not safe:
                    found.append(line_of(source, body_start + hit.start()))
    return found


def violations(rel: str, source: str):
    masked = mask_test_items(source, mask_rust(source))
    result = []
    result += [(line, "CASE_FOLD_INDEX_ORIGINAL") for line in case_fold_violations(masked, source)]
    result += [(line, "FIXED_STRING_RANGE") for line in fixed_range_violations(rel, masked, source)]
    result += [(line, "UNTRUSTED_BYTE_POINTER_CAST") for line in byte_pointer_violations(masked, source)]
    result += [(line, "UNCHECKED_DECODE_ALLOCATION") for line in allocation_violations(rel, masked, source)]
    return sorted(set(result))


def self_test() -> None:
    assert is_first_party_source_path(ROOT / "fuzz/src/target.rs")
    assert not is_first_party_source_path(ROOT / "fuzz/vendor/sonic-rs/src/value.rs")
    assert not is_first_party_source_path(ROOT / "target/debug/build/generated/src/lib.rs")
    assert not is_first_party_source_path(ROOT / "nodedb/deps/example/src/lib.rs")
    assert not violations("nodedb/src/parser/x.rs", '// let u = s.to_uppercase();\n')
    assert violations("nodedb/src/parser/x.rs", 'fn f(s: &str) { let u = s.to_uppercase(); let x = s[0..1]; }')
    assert not violations("nodedb/src/parser/x.rs", 'fn f(s: &str) { let u = r#"s[0..1]"#; }')
    nested = '#[cfg(test)] mod tests { #[cfg(test)] fn f(s: &str) { let x=s[0..1]; } }\nfn g(s: &str) { let x=s[0..1]; }'
    assert violations("nodedb/src/parser/x.rs", nested) == [(2, "FIXED_STRING_RANGE")]
    assert not violations("nodedb/src/parser/x.rs", 'fn f(s: &str) { let x = s[at..]; }')
    assert violations("nodedb/src/wire/x.rs", 'fn f(bytes: &[u8]) { let p = bytes.as_ptr() as *const u64; }')
    assert not violations("nodedb/src/wire/x.rs", 'fn f(bytes: &[u8]) { _mm_loadu_si128(bytes.as_ptr() as *const __m128i); }')
    assert violations("nodedb/src/wire/x.rs", 'fn decode(bytes: &[u8], n: usize) { let v = Vec::with_capacity(n); }')
    assert violations("nodedb/src/wire/x.rs", 'fn from_bytes(bytes: &[u8], n: usize) { let v = HashMap::with_capacity(n); }')
    assert not violations("nodedb/src/wire/x.rs", 'fn decode(bytes: &[u8]) { let v = Vec::with_capacity(bytes.len() / 4); }')
    assert not violations("nodedb-spatial/src/wkb/read.rs", 'fn checked_wkb_capacity() -> usize { 1 }\nfn read(bytes: &[u8]) { let n = checked_wkb_capacity(); let v = Vec::with_capacity(n); }')
    print("OK: robust-parsing gate self-tests passed.")


def is_first_party_source_path(path: Path) -> bool:
    """Return whether a path is in a workspace source tree, not dependency output."""
    try:
        parts = path.relative_to(ROOT).parts
    except ValueError:
        return False
    return not any(part in EXCLUDED_SOURCE_PATH_PARTS for part in parts)


def workspace_sources():
    return sorted(
        path
        for src in ROOT.rglob("src")
        if src.is_dir() and is_first_party_source_path(src)
        for path in src.rglob("*.rs")
        if is_first_party_source_path(path)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test(); return 0
    for rel, helper in APPROVED_CAPACITY_HELPERS:
        path = ROOT / rel
        if not path.is_file() or not re.search(rf"\bfn\s+{re.escape(helper)}\b", path.read_text(encoding="utf-8")):
            print(f"ERROR: approved capacity helper missing: {rel}::{helper}", file=sys.stderr)
            return 1
    errors = []
    for path in workspace_sources():
        rel = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        for line, rule in violations(rel, source):
            errors.append(f"{rel}:{line}:{rule}")
    if errors:
        print("ERROR: robust parsing violations:", file=sys.stderr)
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("OK: robust-parsing gate clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
