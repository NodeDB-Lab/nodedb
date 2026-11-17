#!/usr/bin/env python3
"""Reject unquoted dynamic SQL reconstruction at execution/planning boundaries.

This dependency-free structural gate is intentionally conservative. It masks Rust
lexical opaque regions before locating cfg(test) modules and Rust function braces,
then inspects reconstructed SQL formats that reach planning/execution sinks.
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
RUST_ROOT = ROOT / "nodedb" / "src"
SCAN_ROOTS = tuple(RUST_ROOT / plane for plane in ("control", "event", "data"))
MARKER = "reconstructed-sql: parser-only "
SINK_CALL_RE = re.compile(
    r"\b(?P<name>plan_sql(?:_with_rls)?|plan_authorized_sql|dispatch_sql|"
    r"execute_sql|parse_block)\s*\("
)
PARSE_SQL_RE = re.compile(r"(?:\bParser\s*::|\bparser::statement::)parse_sql\s*\(")
CONSTRUCTION_RE = re.compile(
    r"\b(?:format|format_args|write)\s*!\s*\(|\.(?:push_str|replace)\s*\("
)
CANONICAL_CALL_RE = re.compile(
    r"::\s*nodedb_types\s*::\s*(?:quote_ident|quote_literal|Value\s*::\s*to_sql_literal)\s*\("
)
PATH_CANONICAL_HELPERS = {
    "control/server/shared/check_constraint/subquery_match.rs": {
        "canonical_check_expr_sql",
        "canonical_check_from_sql",
    },
    "control/server/shared/ddl/neutral/materialized_view/refresh.rs": {
        "json_value_to_sql_literal",
        "value_to_sql_literal",
    },
    "control/event_trigger.rs": {"canonical_trigger_template_sql"},
}
FUNCTION_RE = re.compile(
    r"(?:^|\n)\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+"
    r"[A-Za-z_][A-Za-z0-9_]*[^;{]*\{"
)
CFG_START_RE = re.compile(r"#\s*\[\s*cfg\s*\(")
MOD_AFTER_CFG_RE = re.compile(r"\s*mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*([\{;])")
LET_RE = re.compile(
    r"\blet\s+(?:mut\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*(?::[^=;]+)?=\s*"
)
ASSIGN_RE = re.compile(r"(?<![=!<>])\b([A-Za-z_][A-Za-z0-9_]*)\s*=(?!=)")
PATH_ATTRIBUTE_RE = re.compile(r"#\s*\[\s*path\s*=\s*\"([^\"]+)\"\s*\]")


class Finding:
    def __init__(self, path: Path, line: int, message: str) -> None:
        self.path = path
        self.line = line
        self.message = message

    def __str__(self) -> str:
        try:
            relative = self.path.relative_to(ROOT)
        except ValueError:
            relative = self.path
        return f"{relative}:{self.line}: {self.message}"


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def blank(text: str, start: int, end: int, output: list[str]) -> None:
    for index in range(start, min(end, len(text))):
        if output[index] != "\n":
            output[index] = " "


def scan_quoted(text: str, start: int, quote: str) -> int:
    cursor = start + 1
    while cursor < len(text):
        if text[cursor] == "\\":
            cursor += 2
        elif text[cursor] == quote:
            return cursor + 1
        else:
            cursor += 1
    return len(text)


def char_literal_end(text: str, start: int) -> int | None:
    """Return a Rust character literal end without mistaking lifetimes for chars."""
    if text[start] != "'":
        return None
    cursor = start + 1
    if cursor >= len(text) or text[cursor] == "\n":
        return None
    if text[cursor] == "\\":
        cursor += 1
        if cursor >= len(text):
            return None
        if text[cursor] == "u" and cursor + 1 < len(text) and text[cursor + 1] == "{":
            close = text.find("}", cursor + 2)
            if close < 0:
                return None
            cursor = close + 1
        elif text[cursor] == "x":
            cursor += 3
        else:
            cursor += 1
    else:
        cursor += 1
    return cursor + 1 if cursor < len(text) and text[cursor] == "'" else None


def raw_string_end(text: str, start: int) -> int | None:
    """Return the end of r###"..."### / br###"..."###, if it starts here."""
    cursor = start
    if text.startswith("br", cursor):
        cursor += 2
    elif text.startswith("r", cursor):
        cursor += 1
    else:
        return None
    hashes = 0
    while cursor < len(text) and text[cursor] == "#":
        hashes += 1
        cursor += 1
    if cursor >= len(text) or text[cursor] != '"':
        return None
    terminator = '"' + ('#' * hashes)
    close = text.find(terminator, cursor + 1)
    return len(text) if close < 0 else close + len(terminator)


def rust_mask(text: str) -> str:
    """Mask Rust comments and literals without changing offsets/newlines.

    Nested block comments, normal/byte strings, raw/byte raw strings, and
    character/byte-character literals are opaque.  The result is suitable for
    brace counting; the original text remains authoritative for content checks.
    """
    output = list(text)
    cursor = 0
    while cursor < len(text):
        if text.startswith("//", cursor):
            end = text.find("\n", cursor)
            end = len(text) if end < 0 else end
            blank(text, cursor, end, output)
            cursor = end
            continue
        if text.startswith("/*", cursor):
            start = cursor
            cursor += 2
            depth = 1
            while cursor < len(text) and depth:
                if text.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif text.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            blank(text, start, cursor, output)
            continue
        raw_end = raw_string_end(text, cursor)
        if raw_end is not None:
            blank(text, cursor, raw_end, output)
            cursor = raw_end
            continue
        if text.startswith('b"', cursor):
            end = scan_quoted(text, cursor + 1, '"')
            blank(text, cursor, end, output)
            cursor = end
            continue
        if text[cursor] == '"':
            end = scan_quoted(text, cursor, '"')
            blank(text, cursor, end, output)
            cursor = end
            continue
        char_start = cursor + 1 if text.startswith("b'", cursor) else cursor
        if char_start < len(text) and text[char_start] == "'":
            end = char_literal_end(text, char_start)
            if end is not None:
                blank(text, cursor, end, output)
                cursor = end
                continue
        cursor += 1
    return "".join(output)


def matching_brace(masked: str, open_brace: int) -> int:
    depth = 0
    for cursor in range(open_brace, len(masked)):
        if masked[cursor] == "{":
            depth += 1
        elif masked[cursor] == "}":
            depth -= 1
            if depth == 0:
                return cursor + 1
    return len(masked)


def matching_delimiter(masked: str, open_delimiter: int, close_delimiter: str) -> int:
    depth = 0
    for cursor in range(open_delimiter, len(masked)):
        if masked[cursor] == masked[open_delimiter]:
            depth += 1
        elif masked[cursor] == close_delimiter:
            depth -= 1
            if depth == 0:
                return cursor
    return -1


def test_module_ranges(path: Path, text: str, masked: str) -> tuple[list[tuple[int, int]], set[Path]]:
    inline: list[tuple[int, int]] = []
    external: set[Path] = set()
    for match in CFG_START_RE.finditer(masked):
        open_paren = masked.find("(", match.start(), match.end())
        close_paren = matching_delimiter(masked, open_paren, ")")
        if close_paren < 0 or not re.search(r"\btest\b", masked[open_paren + 1 : close_paren]):
            continue
        cursor = close_paren + 1
        while cursor < len(masked) and masked[cursor].isspace():
            cursor += 1
        if cursor >= len(masked) or masked[cursor] != "]":
            continue
        attribute_end = cursor + 1
        path_override: Path | None = None
        while True:
            while attribute_end < len(masked) and masked[attribute_end].isspace():
                attribute_end += 1
            if not masked.startswith("#[", attribute_end):
                break
            attribute_close = matching_delimiter(masked, attribute_end + 1, "]")
            if attribute_close < 0:
                break
            attribute_text = text[attribute_end : attribute_close + 1]
            path_match = PATH_ATTRIBUTE_RE.fullmatch(attribute_text.strip())
            if path_match:
                path_override = path.parent / path_match.group(1)
            attribute_end = attribute_close + 1
        module = MOD_AFTER_CFG_RE.match(masked, attribute_end)
        if module is None:
            continue
        name, tail = module.groups()
        if tail == "{":
            open_brace = masked.find("{", module.start(2), module.end(2))
            inline.append((match.start(), matching_brace(masked, open_brace)))
            continue
        candidates = (
            (path_override,) if path_override is not None else (path.with_name(f"{name}.rs"), path.parent / name / "mod.rs")
        )
        external.update(candidate.resolve() for candidate in candidates if candidate.is_file())
    return inline, external


def mask_ranges(text: str, ranges: Iterable[tuple[int, int]]) -> str:
    output = list(text)
    for start, end in ranges:
        blank(text, start, end, output)
    return "".join(output)


def function_spans(masked: str) -> Iterable[tuple[int, int]]:
    for match in FUNCTION_RE.finditer(masked):
        open_brace = masked.find("{", match.start(), match.end())
        if open_brace >= 0:
            yield match.start(), matching_brace(masked, open_brace)


def has_reasoned_parser_marker(original: str, offset: int) -> bool:
    line_start = original.rfind("\n", 0, offset) + 1
    previous_end = max(0, line_start - 1)
    previous_start = original.rfind("\n", 0, previous_end) + 1
    line_end = original.find("\n", offset)
    if line_end < 0:
        line_end = len(original)
    for line in (original[previous_start:previous_end], original[line_start:line_end]):
        stripped = line.lstrip()
        if not stripped.startswith("// " + MARKER):
            continue
        reason = stripped[len("// " + MARKER) :].strip()
        if len(reason) >= 10:
            return True
    return False


def invocation_end(masked: str, open_paren: int) -> int:
    depth = 0
    for cursor in range(open_paren, len(masked)):
        if masked[cursor] == "(":
            depth += 1
        elif masked[cursor] == ")":
            depth -= 1
            if depth == 0:
                return cursor + 1
    return len(masked)


def split_top_level(original: str, masked: str) -> list[str]:
    pieces: list[str] = []
    start = 0
    parens = brackets = braces = 0
    for index, char in enumerate(masked):
        if char == "(":
            parens += 1
        elif char == ")":
            parens -= 1
        elif char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
        elif char == "{":
            braces += 1
        elif char == "}":
            braces -= 1
        elif char == "," and parens == brackets == braces == 0:
            pieces.append(original[start:index].strip())
            start = index + 1
    pieces.append(original[start:].strip())
    return pieces


def format_literal_content(argument: str) -> str | None:
    argument = argument.strip()
    raw = re.fullmatch(r"(?:br|r)(?:#*)\"(.*)\"(?:#*)", argument, re.DOTALL)
    if raw:
        return raw.group(1)
    normal = re.fullmatch(r"(?:b)?\"(.*)\"", argument, re.DOTALL)
    return normal.group(1) if normal else None


def capture_expressions(format_text: str) -> list[str]:
    captures: list[str] = []
    cursor = 0
    implicit = 0
    while cursor < len(format_text):
        if format_text.startswith("{{", cursor) or format_text.startswith("}}", cursor):
            cursor += 2
            continue
        if format_text[cursor] != "{":
            cursor += 1
            continue
        close = format_text.find("}", cursor + 1)
        if close < 0:
            return ["<malformed>"]
        capture = format_text[cursor + 1 : close].split(":", 1)[0].split("!", 1)[0].strip()
        if not capture:
            capture = str(implicit)
            implicit += 1
        captures.append(capture)
        cursor = close + 1
    return captures


def direct_value_constructor(expression: str) -> bool:
    prefix = re.sub(r"\.to_sql_literal\s*\(\s*\)\s*$", "", expression).strip()
    qualifier = r"::\s*nodedb_types\s*::\s*Value\s*::\s*"
    if re.fullmatch(qualifier + r"Null", prefix):
        return True
    variants = (
        "Bool|Integer|Float|String|Bytes|Array|Object|Uuid|Ulid|DateTime|"
        "NaiveDateTime|Duration|Decimal|Geometry|Set|Regex|ArrayCell|Vector"
    )
    match = re.match(qualifier + rf"(?:{variants})\s*\(", prefix)
    if not match:
        return False
    open_paren = prefix.find("(", match.start())
    return invocation_end(rust_mask(prefix), open_paren) == len(prefix)


def direct_canonical(expression: str, extra_helpers: set[str] | None = None) -> bool:
    expression = expression.strip()
    if re.search(r"\.to_sql_literal\s*\(\s*\)\s*$", expression):
        return direct_value_constructor(expression)
    helper_match = re.match(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)
    helper_allowed = bool(
        helper_match and extra_helpers and helper_match.group(1) in extra_helpers
    )
    if not CANONICAL_CALL_RE.match(expression) and not helper_allowed:
        return False
    # The canonical helper invocation must consume the whole expression. This
    # rejects `quote_ident(x) + hand_escaped` while permitting nested arguments.
    open_paren = expression.find("(")
    return open_paren >= 0 and invocation_end(rust_mask(expression), open_paren) == len(expression)


def canonical_collection_suffix(suffix: str) -> bool:
    remaining = suffix.strip()
    while remaining:
        if remaining.startswith("?"):
            remaining = remaining[1:].strip()
            continue
        method = re.match(r"\.(collect|join)\b", remaining)
        if not method:
            return False
        name = method.group(1)
        cursor = method.end()
        if name == "collect" and remaining[cursor:].lstrip().startswith("::"):
            cursor += len(remaining[cursor:]) - len(remaining[cursor:].lstrip()) + 2
            if remaining[cursor:].lstrip().startswith("<"):
                cursor += len(remaining[cursor:]) - len(remaining[cursor:].lstrip())
                depth = 0
                while cursor < len(remaining):
                    if remaining[cursor] == "<":
                        depth += 1
                    elif remaining[cursor] == ">":
                        depth -= 1
                        if depth == 0:
                            cursor += 1
                            break
                    cursor += 1
                if depth != 0:
                    return False
        cursor += len(remaining[cursor:]) - len(remaining[cursor:].lstrip())
        if cursor >= len(remaining) or remaining[cursor] != "(":
            return False
        end = invocation_end(rust_mask(remaining), cursor)
        if end <= cursor:
            return False
        arguments = remaining[cursor + 1 : end - 1].strip()
        if name == "collect" and arguments:
            return False
        if name == "join" and format_literal_content(arguments) is None:
            return False
        remaining = remaining[end:].strip()
    return True


def has_top_level_value_operator(expression: str) -> bool:
    masked = rust_mask(expression)
    paren = bracket = brace = 0
    first_nonspace = next((i for i, ch in enumerate(masked) if not ch.isspace()), -1)
    for index, ch in enumerate(masked):
        if ch == "(":
            paren += 1
        elif ch == ")":
            paren = max(0, paren - 1)
        elif ch == "[":
            bracket += 1
        elif ch == "]":
            bracket = max(0, bracket - 1)
        elif ch == "{":
            brace += 1
        elif ch == "}":
            brace = max(0, brace - 1)
        elif paren == bracket == brace == 0:
            if ch in "+-*/%|^<>=!":
                return True
            if ch == "&" and index != first_nonspace:
                return True
    return False


def canonical_binding_rhs(rhs: str, extra_helpers: set[str] | None = None) -> bool:
    rhs = rhs.strip()
    if "format!" in rhs or ".replace(" in rhs:
        return False
    if direct_canonical(rhs, extra_helpers):
        return True
    # Canonical collections built solely by mapping every value through the
    # shared literal helper are safe to interpolate as one VALUES fragment.
    all_maps = list(re.finditer(r"\.map\s*\(", rhs))
    canonical_maps = list(
        re.finditer(
            r"\.map\s*\(\s*::\s*nodedb_types\s*::\s*(?:Value\s*::\s*to_sql_literal|quote_ident|quote_literal)\s*\)",
            rhs,
        )
    )
    if extra_helpers:
        helpers = "|".join(re.escape(helper) for helper in sorted(extra_helpers))
        canonical_maps.extend(
            re.finditer(rf"\.map\s*\(\s*(?:{helpers})\s*\)", rhs)
        )
    # Every element's final value-producing map must be canonical. Earlier maps
    # may select/convert inputs, but no later map may replace escaped output.
    if not all_maps or not canonical_maps:
        return False
    canonical = max(canonical_maps, key=lambda match: match.start())
    if all_maps[-1].start() != canonical.start():
        return False
    if has_top_level_value_operator(rhs[: canonical.start()]):
        return False
    return canonical_collection_suffix(rhs[canonical.end() :])


def brace_depth(masked: str, offset: int) -> int:
    """Return lexical brace depth before `offset` (opaque regions already masked)."""
    return masked[:offset].count("{") - masked[:offset].count("}")


def binding_events(original: str, masked: str, before: int) -> list[tuple[int, str, str, str]]:
    """Return ordered local binding events before `before`.

    `let` shadows the visible binding at its lexical depth; plain assignment
    updates the currently visible binding.  The RHS is retained so callers can
    decide whether it re-establishes canonical provenance or invalidates it.
    """
    events: list[tuple[int, str, str, str]] = []
    for match in LET_RE.finditer(masked):
        if match.start() >= before:
            break
        end = masked.find(";", match.end(), before)
        if end >= 0:
            events.append((match.start(), "let", match.group(1), original[match.end() : end]))
    for match in ASSIGN_RE.finditer(masked):
        if match.start() >= before:
            break
        # A let initializer is represented by LET_RE, never as an assignment.
        prefix = masked[max(0, match.start() - 12) : match.start()]
        if re.search(r"\blet\s+(?:mut\s+)?$", prefix):
            continue
        end = masked.find(";", match.end(), before)
        if end >= 0:
            events.append((match.start(), "assign", match.group(1), original[match.end() : end]))
    return sorted(events, key=lambda event: event[0])


def discard_out_of_scope(
    bindings: dict[str, list[tuple[int, bool]]], depth: int
) -> None:
    for values in bindings.values():
        while values and values[-1][0] > depth:
            values.pop()


def rhs_is_canonical(
    rhs: str,
    bindings: dict[str, list[tuple[int, bool]]],
    extra_helpers: set[str] | None = None,
) -> bool:
    rhs = rhs.strip()
    if canonical_binding_rhs(rhs, extra_helpers):
        return True
    # A direct alias of an already canonical local preserves provenance.
    alias = re.fullmatch(r"&?\s*([A-Za-z_][A-Za-z0-9_]*)", rhs)
    return bool(alias and bindings.get(alias.group(1), []) and bindings[alias.group(1)][-1][1])


def canonical_bindings(
    original: str,
    masked: str,
    before: int,
    extra_helpers: set[str] | None = None,
) -> set[str]:
    """Compute canonical locals at one construction, honoring reassignment/scope."""
    bindings: dict[str, list[tuple[int, bool]]] = {}
    for offset, kind, name, rhs in binding_events(original, masked, before):
        event_depth = brace_depth(masked, offset)
        discard_out_of_scope(bindings, event_depth)
        canonical = rhs_is_canonical(rhs, bindings, extra_helpers)
        if kind == "let":
            bindings.setdefault(name, []).append((event_depth, canonical))
        elif bindings.get(name):
            binding_depth, _ = bindings[name][-1]
            bindings[name][-1] = (binding_depth, canonical)
        else:
            # Assignment to an outer/local binding that predates the function
            # is conservatively noncanonical until a canonical RHS establishes it.
            bindings[name] = [(event_depth, canonical)]
    discard_out_of_scope(bindings, brace_depth(masked, before))
    return {name for name, values in bindings.items() if values and values[-1][1]}


def sink_invocations(masked: str) -> list[tuple[int, int]]:
    """Return planning/execution calls that consume reconstructed SQL.

    `parse_block` is a sink only in a function that subsequently executes the
    resulting block. This preserves detection of the procedural alert path
    while allowing parser-only validation to use its separate marker rule.
    """
    executes_block = bool(re.search(r"\bexecute_block\s*\(", masked))
    sinks: list[tuple[int, int]] = []
    for match in SINK_CALL_RE.finditer(masked):
        if match.group("name") == "parse_block" and not executes_block:
            continue
        open_paren = masked.find("(", match.start())
        sinks.append((match.start(), invocation_end(masked, open_paren)))
    return sinks


def local_name_before_construction(masked: str, start: int) -> str | None:
    """Return the local receiving this construction in its let statement."""
    statement_start = max(masked.rfind(";", 0, start), masked.rfind("{", 0, start)) + 1
    prefix = masked[statement_start:start]
    match = re.search(
        r"\blet\s+(?:mut\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(?::[^=;]+)?=\s*$",
        prefix,
        re.DOTALL,
    )
    if match:
        return match.group(1)
    assigned_let = re.search(
        r"\blet\s+(?:mut\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*(?::[^=;]+)?=",
        prefix,
        re.DOTALL,
    )
    if assigned_let:
        return assigned_let.group(1)
    assignment = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=", prefix)
    return assignment.group(1) if assignment else None


def buffer_name_for_construction(masked: str, match: re.Match[str]) -> str | None:
    """Return the local buffer mutated by push_str!/write!, if recognizable."""
    if ".push_str" in match.group(0):
        receiver = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*$", masked[: match.start()])
        return receiver.group(1) if receiver else None
    if not match.group(0).lstrip().startswith("write"):
        return None
    open_paren = masked.find("(", match.start())
    end = invocation_end(masked, open_paren)
    arguments = split_top_level(masked[open_paren + 1 : end - 1], masked[open_paren + 1 : end - 1])
    if not arguments:
        return None
    target = arguments[0].strip()
    target_match = re.fullmatch(r"&\s*(?:mut\s+)?([A-Za-z_][A-Za-z0-9_]*)", target)
    return target_match.group(1) if target_match else None


def local_reaches_sink(masked: str, name: str, after: int, sinks: Iterable[tuple[int, int]]) -> bool:
    """Follow local aliases with lexical shadowing until a later sink."""
    bindings: dict[str, list[tuple[int, bool]]] = {
        name: [(brace_depth(masked, after), True)]
    }
    sink_list = sorted((start, end) for start, end in sinks if start > after)
    events = binding_events(masked, masked, len(masked))
    event_index = 0
    for sink_start, sink_end in sink_list:
        while event_index < len(events) and events[event_index][0] < sink_start:
            offset, kind, target, rhs = events[event_index]
            event_index += 1
            if offset < after:
                continue
            event_depth = brace_depth(masked, offset)
            discard_out_of_scope(bindings, event_depth)
            rhs_identifiers = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", rhs))
            source_reachable = any(
                candidate in rhs_identifiers and values and values[-1][1]
                for candidate, values in bindings.items()
            )
            if kind == "let":
                bindings.setdefault(target, []).append((event_depth, source_reachable))
            elif bindings.get(target):
                binding_depth, _ = bindings[target][-1]
                bindings[target][-1] = (binding_depth, source_reachable)
            else:
                bindings[target] = [(event_depth, source_reachable)]
        discard_out_of_scope(bindings, brace_depth(masked, sink_start))
        reachable = {
            candidate
            for candidate, values in bindings.items()
            if values and values[-1][1]
        }
        arguments = masked[sink_start:sink_end]
        if any(
            re.search(rf"(?<![A-Za-z0-9_]){re.escape(candidate)}\b", arguments)
            for candidate in reachable
        ):
            return True
    return False


def construction_reaches_sink(masked: str, match: re.Match[str], sinks: Iterable[tuple[int, int]]) -> bool:
    open_paren = masked.find("(", match.start())
    end = invocation_end(masked, open_paren)
    for sink_start, sink_end in sinks:
        if sink_start <= match.start() and end <= sink_end:
            return True
    local = local_name_before_construction(masked, match.start())
    if local and local_reaches_sink(masked, local, end, sinks):
        return True
    buffer = buffer_name_for_construction(masked, match)
    return bool(buffer and local_reaches_sink(masked, buffer, end, sinks))


def construction_is_safe(
    original: str,
    masked: str,
    match: re.Match[str],
    names: set[str],
    extra_helpers: set[str] | None = None,
) -> bool:
    open_paren = masked.find("(", match.start())
    end = invocation_end(masked, open_paren)
    invocation = original[match.start() : end]
    invocation_mask = masked[match.start() : end]
    if ".replace" in match.group(0):
        return False
    if ".push_str" in match.group(0):
        argument = invocation[invocation.find("(") + 1 : -1].strip().lstrip("&").strip()
        return (
            argument.startswith('"')
            or direct_canonical(argument, extra_helpers)
            or argument in names
        )
    is_write = match.group(0).lstrip().startswith("write")
    inner_start = invocation.find("(") + 1
    args = split_top_level(invocation[inner_start:-1], invocation_mask[inner_start:-1])
    if not args:
        return True
    if match.group(0).lstrip().startswith("write"):
        # The first write! argument is the destination buffer, not the format.
        args = args[1:]
    if not args:
        return True
    format_text = format_literal_content(args[0])
    if format_text is None:
        return False
    positional: list[str] = []
    named: dict[str, str] = {}
    for argument in args[1:]:
        named_match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\Z", argument, re.DOTALL)
        if named_match:
            named[named_match.group(1)] = named_match.group(2)
        else:
            positional.append(argument)
    for capture in capture_expressions(format_text):
        expression: str | None
        if capture.isdigit():
            index = int(capture)
            expression = positional[index] if index < len(positional) else None
        else:
            expression = named.get(capture, capture)
        if expression is None or not (
            direct_canonical(expression, extra_helpers) or expression.strip() in names
        ):
            return False
    return True


def scan_text(path: Path, raw_text: str) -> list[Finding]:
    try:
        path_key = path.resolve().relative_to(RUST_ROOT.resolve()).as_posix()
    except (OSError, ValueError):
        path_key = ""
    extra_helpers = PATH_CANONICAL_HELPERS.get(path_key, set())
    lexical = rust_mask(raw_text)
    inline_ranges, _ = test_module_ranges(path, raw_text, lexical)
    masked = mask_ranges(lexical, inline_ranges)
    findings: list[Finding] = []
    for start, end in function_spans(masked):
        body = raw_text[start:end]
        body_masked = masked[start:end]
        sinks = sink_invocations(body_masked)
        reaches_execution = bool(sinks)
        header_end = body_masked.find("{")
        header = body_masked[:header_end] if header_end >= 0 else body_masked
        is_sql_builder = bool(
            re.search(
                r"\bfn\s+(?:sql|(?:build|reconstruct|render|compose|make)[A-Za-z0-9_]*(?:sql|query|statement|command))\b",
                header,
            )
        )
        parser_sinks = []
        for parse in PARSE_SQL_RE.finditer(body_masked):
            open_paren = body_masked.find("(", parse.start())
            parser_sinks.append((parse.start(), invocation_end(body_masked, open_paren)))
        constructions = list(CONSTRUCTION_RE.finditer(body_masked))
        if not reaches_execution and not parser_sinks and not is_sql_builder:
            continue
        for construction in constructions:
            if not is_sql_builder and not construction_reaches_sink(body_masked, construction, sinks):
                continue
            names = canonical_bindings(
                body, body_masked, construction.start(), extra_helpers
            )
            if not construction_is_safe(
                body, body_masked, construction, names, extra_helpers
            ):
                findings.append(Finding(path, line_number(raw_text, start + construction.start()), "dynamic SQL construction reaches an execution/planning sink without canonical quoting"))
        # Parser validation is independent of execution/planning sinks in the
        # same function: every dynamically reconstructed parser input needs its
        # own exact-site, reasoned parser-only marker.
        for parse, parser_sink in zip(PARSE_SQL_RE.finditer(body_masked), parser_sinks):
            if any(construction_reaches_sink(body_masked, construction, [parser_sink]) for construction in constructions):
                offset = start + parse.start()
                if not has_reasoned_parser_marker(raw_text, offset):
                    findings.append(Finding(path, line_number(raw_text, offset), "parser-only parse_sql requires an exact '// reconstructed-sql: parser-only <reason>' comment"))
    return findings


def scan_paths(paths: Iterable[Path]) -> list[Finding]:
    paths = list(paths)
    excluded: set[Path] = set()
    for path in paths:
        text = path.read_text(encoding="utf-8")
        _, external = test_module_ranges(path, text, rust_mask(text))
        excluded.update(external)
    findings: list[Finding] = []
    for path in paths:
        if path.resolve() not in excluded:
            findings.extend(scan_text(path, path.read_text(encoding="utf-8")))
    return findings


def self_test() -> int:
    cases = [
        ("multiline signature", "fn f(\n value: &str,\n) { let sql = format!(\"SELECT {value}\"); plan_sql(&sql); }", 1),
        ("unsafe assigned sql", "fn f() { let sql = format!(\"SELECT * FROM {table}\"); plan_sql(&sql); }", 1),
        ("indirect keyword", "fn f() { let prefix = \"SELECT * FROM\"; let sql = format!(\"{prefix} {table}\"); plan_sql(&sql); }", 1),
        ("unsafe graph DSL", "fn build_graph_sql() { format!(\"GRAPH TRAVERSE FROM {node}\") }", 1),
        ("unsafe publish DSL", "fn f() { let sql = format!(\"PUBLISH TO {topic}\"); plan_sql(&sql); }", 1),
        ("direct unsafe sink", "fn f() { plan_sql(&format!(\"SELECT * FROM {table}\")); }", 1),
        ("safe execution plus diagnostic", "fn f() { let table = ::nodedb_types::quote_ident(input); let sql = format!(\"SELECT * FROM {table}\"); plan_sql(&sql); let message = format!(\"ANALYZE scan failed: {error}\"); log(message); }", 0),
        ("unrelated SQL-looking diagnostic", "fn f() { let message = format!(\"DELETE failed for {table}\"); log(message); }", 0),
        ("executor is not builder", "fn execute_sql() { let message = format!(\"invalid SHOW target {name}\"); log(message); }", 0),
        ("unsafe SQL builder helper", "fn build_sql() -> String { format!(\"SELECT * FROM {table}\") }", 1),
        ("unsafe query builder helper", "fn build_match_query() -> String { format!(\"{prefix} {table}\") }", 1),
        ("mixed safe unsafe", "fn f() { let t = quote_ident(table); let sql = format!(\"SELECT {t}, {value}\"); plan_sql(&sql); }", 1),
        ("positional mixed", "fn f() { let sql = format!(\"SELECT {} {}\", quote_ident(t), value); plan_sql(&sql); }", 1),
        ("post-canonical map is unsafe", "fn f() { let values = xs.iter().map(Value::to_sql_literal).map(|_| attacker).collect::<Vec<_>>().join(\",\"); let sql = format!(\"SELECT {values}\"); plan_sql(&sql); }", 1),
        ("canonical captures", "fn f() { let t = ::nodedb_types::quote_ident(table); let v = ::nodedb_types::quote_literal(value); let sql = format!(\"SELECT {t} WHERE x = {v}\"); plan_sql(&sql); }", 0),
        ("attacker method is not canonical", "fn f() { let t = attacker.to_sql_literal(); let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("chained Value spoof is not canonical", "fn f() { let t = Value::from(attacker).into_raw().to_sql_literal(); let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("direct Value constructor is canonical", "fn f() { let t = ::nodedb_types::Value::String(attacker.to_owned()).to_sql_literal(); let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 0),
        ("spoofed helper is not canonical", "fn f() { let t = canonical_trigger_template_sql(attacker); let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("shadowed quote helper is not canonical", "fn f() { let quote_ident = |value| value; let sql = format!(\"SELECT {}\", quote_ident(attacker)); plan_sql(&sql); }", 1),
        ("shadowed crate alias is not canonical", "use crate::unsafe_sql as nodedb_types;\nfn f() { let sql = format!(\"SELECT {}\", nodedb_types::quote_ident(attacker)); plan_sql(&sql); }", 1),
        ("canonical collection", "fn f() { let values = xs.iter().map(::nodedb_types::Value::to_sql_literal).collect::<Vec<_>>().join(\",\"); let sql = format!(\"SELECT {values}\"); plan_sql(&sql); }", 0),
        ("collection suffix append is unsafe", "fn f() { let values = xs.iter().map(::nodedb_types::Value::to_sql_literal).collect::<Vec<_>>().join(\",\") + attacker; let sql = format!(\"SELECT {values}\"); plan_sql(&sql); }", 1),
        ("collection prefix append is unsafe", "fn f() { let values = attacker + &xs.iter().map(::nodedb_types::Value::to_sql_literal).collect::<Vec<_>>().join(\",\"); let sql = format!(\"SELECT {values}\"); plan_sql(&sql); }", 1),
        ("empty static join is canonical", "fn f() { let values = xs.iter().map(::nodedb_types::Value::to_sql_literal).collect::<Vec<_>>().join(\"\"); let sql = format!(\"SELECT {values}\"); plan_sql(&sql); }", 0),
        ("reassignment invalidates canonical", "fn f() { let t = ::nodedb_types::quote_ident(table); t = input; let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("inner shadow does not bless outer", "fn f() { let t = input; { let t = ::nodedb_types::quote_ident(table); let safe = format!(\"SELECT {t}\"); plan_sql(&safe); } let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("multi hop alias reaches sink", "fn f() { let sql = format!(\"SELECT {table}\"); let one = sql; let two = one; plan_sql(&two); }", 1),
        ("transformed alias reaches sink", "fn f() { let sql = format!(\"SELECT {table}\"); let forwarded = sql.to_string(); plan_sql(&forwarded); }", 1),
        ("borrowed alias reaches sink", "fn f() { let sql = format!(\"SELECT {table}\"); let forwarded = sql.as_str(); plan_sql(&forwarded); }", 1),
        ("dynamic write fragment reaches sink", "fn f() { let mut sql = String::from(\"SELECT * FROM \" ); write!(&mut sql, \"{table}\"); plan_sql(&sql); }", 1),
        ("replace surgery reaches sink", "fn f() { let sql = template.replace(\"$id\", id); plan_sql(&sql); }", 1),
        ("inner shadow restores tainted outer", "fn f() { let sql = format!(\"SELECT {table}\"); { let sql = \"safe\"; log(sql); } plan_sql(&sql); }", 1),
        ("unrelated execution does not waive parser marker", "fn f() { let sql = format!(\"SELECT {value}\"); let safe = ::nodedb_types::quote_ident(table); plan_sql(&safe); Parser::parse_sql(sql); }", 1),
        ("hand escaped sibling", "fn f() { let t = format!(\"{}{}\", quote_ident(table), escaped); let sql = format!(\"SELECT {t}\"); plan_sql(&sql); }", 1),
        ("comments strings chars braces", "fn f() { // { } plan_sql(format!(\"SELECT {x}\"))\n /* outer { /* inner } */ } */ let s = \"{ }\"; let bs = b\"{ }\"; let r = r#\"{ }\"#; let br = br##\"{ }\"##; let c = '{'; let bc = b'}'; }", 0),
        ("cfg all test", "#[cfg(all(test, feature = \"x\"))] mod tests { fn t() { let sql = format!(\"SELECT {x}\"); plan_sql(&sql); } }", 0),
        ("reasoned parser", "fn f() { let sql = format!(\"SELECT {value}\");\n // reconstructed-sql: parser-only validates generated expression\n Parser::parse_sql(sql); }", 0),
        ("string fake marker", "fn f() { let sql = format!(\"SELECT {value}\"); let note = \"// reconstructed-sql: parser-only validates generated expression\"; Parser::parse_sql(sql); }", 1),
        ("short marker", "fn f() { let sql = format!(\"SELECT {value}\"); // reconstructed-sql: parser-only short\n Parser::parse_sql(sql); }", 1),
    ]
    failed = False
    for label, source, expected in cases:
        actual = len(scan_text(Path(f"<self-test:{label}>"), source))
        if actual != expected:
            print(f"self-test failed: {label}: expected {expected}, got {actual}", file=sys.stderr)
            failed = True
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        parent = root / "module.rs"
        external = root / "tests.rs"
        production = root / "production.rs"
        parent.write_text("#[cfg(any(feature = \"x\", test))]\n#[path = \"fixtures.rs\"]\nmod tests;\n", encoding="utf-8")
        external = root / "fixtures.rs"
        external.write_text("fn t() { let sql = format!(\"SELECT {x}\"); plan_sql(&sql); }\n", encoding="utf-8")
        production.write_text("fn p() { let sql = format!(\"SELECT {x}\"); plan_sql(&sql); }\n", encoding="utf-8")
        external_findings = scan_paths([parent, external])
        production_findings = scan_paths([production])
        if external_findings or len(production_findings) != 1:
            print("self-test failed: external cfg(test) module exclusion", file=sys.stderr)
            failed = True
    if not failed:
        print("OK: reconstructed-SQL gate self-tests passed.")
    return 1 if failed else 0


def stale_canonical_helpers() -> list[str]:
    """Report PATH_CANONICAL_HELPERS entries that no longer name live code.

    Each entry widens the gate for one file: the named helpers are read as
    canonical there, so a bare call to one stops counting as unquoted
    interpolation. The key is a path. Move the file and the entry keeps
    widening nothing, while the helpers at their new home silently lose
    their canonical status and the file starts reporting false violations —
    which is how this gate came to fail on every pull request. A dead entry
    is therefore a gate failure, not tidiness.
    """
    stale: list[str] = []
    for rel, helpers in sorted(PATH_CANONICAL_HELPERS.items()):
        path = RUST_ROOT / rel
        if not path.is_file():
            stale.append(f"{rel!r} names a file that does not exist")
            continue
        source = path.read_text(encoding="utf-8")
        for helper in sorted(helpers):
            if not re.search(rf"\b{re.escape(helper)}\b", source):
                stale.append(f"{rel!r} exempts {helper!r}, which does not appear in that file")
    return stale


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    stale = stale_canonical_helpers()
    if stale:
        print("FAIL: stale canonical-helper exemptions — the gate is misaimed where they point:")
        for entry in stale:
            print(f"  {entry}")
        print("\nRepoint each entry at the code's current location, or drop it if the helper is gone.")
        return 1
    findings = scan_paths(path for root in SCAN_ROOTS for path in root.rglob("*.rs"))
    if findings:
        print(f"FAIL: {len(findings)} reconstructed-SQL gate violation(s):")
        for finding in findings:
            print(f"  {finding}")
        print("\nUse quote_ident, quote_literal, or Value::to_sql_literal for every dynamic SQL name/value.")
        print("Parser-only parse_sql needs an exact-site '// reconstructed-sql: parser-only <reason>' comment.")
        return 1
    print("OK: reconstructed-SQL gate clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
