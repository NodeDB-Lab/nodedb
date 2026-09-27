#!/usr/bin/env python3
"""Check that `main` has a scheduled probe and that the wasm job runs tests.

Both halves of this are invisible in review and easy to undo by accident:

  * a workflow that only fires on `pull_request` never reports on `main`, so a
    red `main` waits for the next labelled PR to surface;
  * a wasm job that runs `cargo check` never catches a target-specific runtime
    panic, and `cargo check` without `--all-targets` never even builds a test
    target -- so dev-dependency and test-only wasm breakage stays hidden.

This gate asserts the shape that closes both, not the prose around it. Run
`--self-test` to prove the assertions still bite: it feeds each check a
configuration built to violate it and fails if the check accepts it.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CI_WORKFLOW = Path(".github/workflows/ci.yml")
TEST_WORKFLOW = Path(".github/workflows/test.yml")

# The crates NodeDB-Lite compiles into its wasm build. Keep in sync with the
# `-p` list in test.yml's `wasm` job; the check below fails on drift either way.
SHARED_CRATES = frozenset({
    "nodedb-types",
    "nodedb-codec",
    "nodedb-physical",
    "nodedb-mem",
    "nodedb-crdt",
    "nodedb-columnar",
    "nodedb-fts",
    "nodedb-graph",
    "nodedb-spatial",
    "nodedb-strict",
    "nodedb-sql",
    "nodedb-query",
    "nodedb-array",
    "nodedb-vector",
    "nodedb-client",
    "nodedb-wal",
})

WASM_TARGET = "wasm32-wasip1"
RUNNER_VAR = "CARGO_TARGET_WASM32_WASIP1_RUNNER"

# The one nodedb-codec test skipped by name in the job: an approximate-recall
# floor that this target misses on float behaviour alone. It is listed here so
# that dropping the skip is a visible change rather than a silent one. The zstd
# *encoder* tests are NOT here -- those are ignored in-source by
# `#[cfg_attr(target_arch = "wasm32", ignore = ...)]`, because there the reason
# is categorical (the target has no encoder) rather than a moving threshold.
EXPECTED_SKIPS = ("top1_recall",)

# Crates whose wasm test targets are built and executed by the job. All sixteen
# shared crates are in it, and the job's `-p` list must match this set exactly:
# a crate silently dropped from the run loses its coverage, and a crate added
# that cannot actually start on wasm fails the job.
WASM_TESTED_CRATES = frozenset({
    "nodedb-types",
    "nodedb-codec",
    "nodedb-physical",
    "nodedb-mem",
    "nodedb-crdt",
    "nodedb-columnar",
    "nodedb-fts",
    "nodedb-graph",
    "nodedb-spatial",
    "nodedb-strict",
    "nodedb-sql",
    "nodedb-query",
    "nodedb-array",
    "nodedb-vector",
    "nodedb-client",
    "nodedb-wal",
})


class Failure(Exception):
    """One gate assertion did not hold."""


def _load_yaml(path: Path) -> dict:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - environment guard
        raise Failure(f"PyYAML is required to parse {path}: {exc}") from exc
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise Failure(f"{path} is missing") from exc
    except yaml.YAMLError as exc:
        raise Failure(f"{path} is not valid YAML: {exc}") from exc


def _workflow_on(document: dict, path: Path) -> dict:
    # PyYAML resolves the bare `on:` key to the boolean True.
    trigger = document.get("on", document.get(True))
    if not isinstance(trigger, dict):
        raise Failure(f"{path}: `on:` must be a mapping, got {trigger!r}")
    return trigger


def check_scheduled_main(root: Path) -> None:
    """ci.yml must run on a timer as well as on a labelled PR."""
    document = _load_yaml(root / CI_WORKFLOW)
    trigger = _workflow_on(document, CI_WORKFLOW)
    if "workflow_dispatch" not in trigger:
        raise Failure(f"{CI_WORKFLOW}: `workflow_dispatch` trigger was removed")
    schedule = trigger.get("schedule")
    if not schedule:
        raise Failure(
            f"{CI_WORKFLOW}: no `schedule` trigger -- nothing reports a red `main`"
        )
    crons = [
        entry.get("cron")
        for entry in schedule
        if isinstance(entry, dict) and entry.get("cron")
    ]
    if not crons:
        raise Failure(f"{CI_WORKFLOW}: `schedule` has no `cron` expression")


def _wasm_job(root: Path) -> tuple[dict, dict]:
    document = _load_yaml(root / TEST_WORKFLOW)
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        raise Failure(f"{TEST_WORKFLOW}: `jobs` must be a mapping")
    job = jobs.get("wasm")
    if not isinstance(job, dict):
        raise Failure(f"{TEST_WORKFLOW}: no `wasm` job")
    return document, job


def _run_steps(job: dict) -> list[str]:
    steps: list[str] = []
    for step in job.get("steps", []) or []:
        if not isinstance(step, dict):
            continue
        # PyYAML resolves the bare `run:` key to the boolean True.
        command = step.get("run", step.get(True))
        if isinstance(command, str):
            steps.append(command)
    return steps


def _command_lines(command: str) -> list[str]:
    return [
        line.strip()
        for line in command.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _test_step_commands(job: dict) -> list[str]:
    """Commands of the steps that actually execute a suite."""
    return [
        command
        for command in _run_steps(job)
        if any(
            re.search(r"\bcargo (nextest run|test)\b", line)
            for line in _command_lines(command)
        )
    ]


def check_wasm_job_runs_tests(root: Path) -> None:
    """The wasm job must execute the suites, not merely compile them.

    The `cargo check` shape is rejected only inside the step that is meant to
    run tests. A separate compile-only step would be deliberate: it keeps crates
    whose test targets cannot start on this target from dropping out of the job
    entirely.
    """
    _, job = _wasm_job(root)
    if not _run_steps(job):
        raise Failure(f"{TEST_WORKFLOW}: `wasm` job has no `run` steps")

    test_steps = _test_step_commands(job)
    if not test_steps:
        raise Failure(
            f"{TEST_WORKFLOW}: `wasm` job never runs `cargo test`/`cargo nextest run`"
        )
    for command in test_steps:
        for line in _command_lines(command):
            if "cargo check" in line:
                raise Failure(
                    f"{TEST_WORKFLOW}: the test step of the `wasm` job still runs "
                    f"`cargo check` ({line!r}) -- a check cannot observe a runtime "
                    "panic"
                )


def check_wasm_target_and_runner(root: Path) -> None:
    """The test run must target wasip1 and name a runtime that can execute it."""
    _, job = _wasm_job(root)
    commands = "\n".join(_test_step_commands(job))
    if f"--target {WASM_TARGET}" not in commands:
        raise Failure(f"{TEST_WORKFLOW}: `wasm` job does not target {WASM_TARGET}")

    env = job.get("env") or {}
    runner = env.get(RUNNER_VAR, "")
    if not isinstance(runner, str) or "wasmtime" not in runner:
        raise Failure(
            f"{TEST_WORKFLOW}: {RUNNER_VAR} must name a wasm runtime; got {runner!r}"
        )
    if "max-wasm-stack" not in runner:
        raise Failure(
            f"{TEST_WORKFLOW}: {RUNNER_VAR} must raise the wasm stack "
            "(unoptimized test binaries overflow the runtime default)"
        )


def check_wasm_job_covers_shared_crates(root: Path) -> None:
    """The job's package lists must match what can run, plus a compile fallback.

    Checked in both directions. A crate silently dropped from the test step
    loses its coverage; a crate added back that cannot start on this target
    would fail the job; and every shared crate must appear in one of the two
    steps, so nothing is dropped from the job entirely.
    """
    _, job = _wasm_job(root)
    test_commands = "\n".join(_test_step_commands(job))
    tested = set(re.findall(r"-p\s+([A-Za-z0-9_-]+)", test_commands))

    missing = sorted(WASM_TESTED_CRATES - tested)
    if missing:
        raise Failure(
            f"{TEST_WORKFLOW}: `wasm` job does not run tests for "
            f"{', '.join(missing)}"
        )
    unexpected = sorted(tested - WASM_TESTED_CRATES)
    if unexpected:
        raise Failure(
            f"{TEST_WORKFLOW}: `wasm` job runs tests for {', '.join(unexpected)}, "
            "which is not in WASM_TESTED_CRATES -- either its test target cannot "
            "start on wasm, or this list needs updating with the evidence"
        )

    all_commands = "\n".join(_run_steps(job))
    every_package = set(re.findall(r"-p\s+([A-Za-z0-9_-]+)", all_commands))
    uncovered = sorted(SHARED_CRATES - every_package)
    if uncovered:
        raise Failure(
            f"{TEST_WORKFLOW}: `wasm` job never mentions {', '.join(uncovered)}; "
            "a shared crate that cannot run its tests still has to compile"
        )


def check_codec_skips_present(root: Path) -> None:
    """The one native-only codec test must stay skipped by name."""
    _, job = _wasm_job(root)
    commands = "\n".join(_test_step_commands(job))
    for name in EXPECTED_SKIPS:
        if f"--skip {name}" not in commands:
            raise Failure(
                f"{TEST_WORKFLOW}: `wasm` job no longer skips {name!r}; if that "
                "test was made to pass on wasm, drop it from EXPECTED_SKIPS here"
            )


CHECKS = (
    ("scheduled-main", check_scheduled_main),
    ("wasm-job-runs-tests", check_wasm_job_runs_tests),
    ("wasm-target-and-runner", check_wasm_target_and_runner),
    ("wasm-job-covers-shared-crates", check_wasm_job_covers_shared_crates),
    ("codec-skips-present", check_codec_skips_present),
)


def run_checks(root: Path) -> list[str]:
    failures: list[str] = []
    for name, check in CHECKS:
        try:
            check(root)
        except Failure as exc:
            failures.append(f"[{name}] {exc}")
    return failures


def self_test(root: Path) -> int:
    """Feed every check a configuration that violates exactly it.

    A gate nobody has seen fail is a gate that may already be vacuous: these
    mutations prove each assertion still rejects the shape it exists to catch.
    """
    import tempfile

    # name -> (file to mutate, mutation, description, checks the mutation is
    # expected to trip). The expected set is declared rather than assumed to be
    # exactly one: some mutations cascade by design -- downgrading the test run
    # to a check also removes the only test step, so the target and coverage
    # checks necessarily see it too. Declaring the cascade keeps the self-test
    # honest about both an under-reaction and an over-reaction.
    mutations = {
        "scheduled-main": (
            CI_WORKFLOW,
            lambda text: re.sub(
                r"^  schedule:\n(?:    .*\n)+", "", text, flags=re.MULTILINE
            ),
            "remove the schedule",
            ("scheduled-main",),
        ),
        "wasm-job-runs-tests": (
            TEST_WORKFLOW,
            lambda text: text.replace(
                "cargo test --profile ci --target wasm32-wasip1",
                "cargo check --profile ci --target wasm32-wasip1",
            ),
            "downgrade the test run to a check",
            (
                "wasm-job-runs-tests",
                "wasm-target-and-runner",
                "wasm-job-covers-shared-crates",
                "codec-skips-present",
            ),
        ),
        "wasm-target-and-runner": (
            TEST_WORKFLOW,
            lambda text: text.replace(
                "CARGO_TARGET_WASM32_WASIP1_RUNNER: wasmtime",
                "CARGO_TARGET_WASM32_WASIP1_RUNNER: echo",
            ),
            "replace the runtime with a no-op",
            ("wasm-target-and-runner",),
        ),
        "wasm-job-covers-shared-crates": (
            TEST_WORKFLOW,
            lambda text: text.replace(
                "-p nodedb-crdt -p nodedb-columnar -p nodedb-fts -p nodedb-graph \\\n",
                "-p nodedb-crdt -p nodedb-columnar -p nodedb-graph \\\n",
            ),
            "drop a crate from the package list",
            ("wasm-job-covers-shared-crates",),
        ),
        "codec-skips-present": (
            TEST_WORKFLOW,
            lambda text: text.replace("--skip top1_recall", ""),
            "un-skip a native-only test",
            ("codec-skips-present",),
        ),
    }

    failures: list[str] = []
    for name, (relative, mutate, description, expected) in mutations.items():
        with tempfile.TemporaryDirectory() as tmp:
            mutated_root = Path(tmp)
            for workflow in (CI_WORKFLOW, TEST_WORKFLOW):
                destination = mutated_root / workflow
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(
                    (root / workflow).read_text(encoding="utf-8"), encoding="utf-8"
                )
            target = mutated_root / relative
            before = target.read_text(encoding="utf-8")
            after = mutate(before)
            if after == before:
                failures.append(
                    f"[{name}] the self-test mutation changed nothing "
                    f"({description}) -- it no longer models the violation"
                )
                continue
            target.write_text(after, encoding="utf-8")

            tripped = {
                check_name
                for failure in run_checks(mutated_root)
                for check_name, _ in CHECKS
                if failure.startswith(f"[{check_name}]")
            }
            if name not in tripped:
                failures.append(
                    f"[{name}] accepted a configuration that {description}"
                )
            unexpected = sorted(tripped - set(expected))
            if unexpected:
                failures.append(
                    f"[{name}] mutation {description} also tripped "
                    f"{unexpected}, which the self-test did not expect"
                )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="prove each check still rejects the configuration it exists to catch",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="repository root (defaults to this script's repository)",
    )
    args = parser.parse_args()
    root = args.root.resolve()

    if args.self_test:
        failures = self_test(root)
        if failures:
            for failure in failures:
                print(f"FAIL {failure}", file=sys.stderr)
            return 1
        print(f"self-test: all {len(CHECKS)} checks reject their violation")
        return 0

    failures = run_checks(root)
    if failures:
        for failure in failures:
            print(f"FAIL {failure}", file=sys.stderr)
        return 1
    print(
        f"wasm probe: ci.yml is scheduled on main; the wasm job executes "
        f"{len(WASM_TESTED_CRATES)} shared crates on {WASM_TARGET}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
