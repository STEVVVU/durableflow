#!/usr/bin/env python
"""Assert a floor on the number of tests that actually PASSED.

A green pytest exit code proves only that nothing failed, not that anything
ran. Total collection loss is caught by pytest's own exit code 5, but *partial*
loss is silent: deleting one test file still reports "27 passed" and exits 0.
Skips are just as invisible -- the PostgreSQL tests self-skip when
``DURABLEFLOW_TEST_POSTGRES_URL`` is unset, so a renamed ``env:`` block would
turn a job named "including Postgres-marked tests" green while verifying
nothing about PostgreSQL.

So the floor counts *passing* tests (collected minus skipped, failed, and
errored), which makes a mass-``.skip`` fail the build exactly like a deletion.
``--min-passed-in`` applies the same floor to a single class prefix, which is
how the PostgreSQL job proves its five tests ran rather than skipped.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

# A <testcase> carrying one of these child tags did not pass.
NON_PASSING_TAGS = ("skipped", "failure", "error")


class TestCountError(Exception):
    """A floor was not met, or the report could not be interpreted."""


def _outcome(case: ElementTree.Element) -> str:
    """Return this test case's outcome: ``passed`` or why it did not pass."""
    for tag in NON_PASSING_TAGS:
        if case.find(tag) is not None:
            return tag
    return "passed"


def count_passed(report: Path, *, class_prefix: str | None = None) -> int:
    """Count passing test cases in a JUnit XML report.

    ``class_prefix`` restricts the count to cases whose ``classname`` starts
    with it (for example ``tests.test_postgres``).
    """
    try:
        tree = ElementTree.parse(report)
    except ElementTree.ParseError as error:  # pragma: no cover - malformed report
        raise TestCountError(f"{report} is not parseable JUnit XML: {error}") from error

    passed = 0
    for case in tree.iter("testcase"):
        if class_prefix and not case.get("classname", "").startswith(class_prefix):
            continue
        if _outcome(case) == "passed":
            passed += 1
    return passed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_test_counts",
        description="Fail if fewer tests passed than the expected floor.",
    )
    parser.add_argument("report", type=Path, help="Path to a pytest --junitxml report.")
    parser.add_argument(
        "--min-passed",
        type=int,
        required=True,
        help="Minimum number of tests that must have passed overall.",
    )
    parser.add_argument(
        "--min-passed-in",
        default=None,
        metavar="CLASS_PREFIX=COUNT",
        help=(
            "Also require COUNT passing tests whose classname starts with "
            "CLASS_PREFIX, so self-skipping tests cannot pass silently."
        ),
    )
    arguments = parser.parse_args(argv)

    if not arguments.report.exists():
        # No report at all means pytest never got far enough to write one.
        print(f"FAIL: no JUnit report at {arguments.report}", file=sys.stderr)
        return 1

    failures: list[str] = []

    total_passed = count_passed(arguments.report)
    if total_passed < arguments.min_passed:
        failures.append(
            f"expected at least {arguments.min_passed} passing tests, got {total_passed}"
        )
    print(f"passing tests: {total_passed} (floor {arguments.min_passed})")

    if arguments.min_passed_in:
        prefix, separator, raw_count = arguments.min_passed_in.partition("=")
        if not separator or not prefix or not raw_count.isdigit():
            print(
                f"FAIL: --min-passed-in expects CLASS_PREFIX=COUNT, got "
                f"{arguments.min_passed_in!r}",
                file=sys.stderr,
            )
            return 1
        expected = int(raw_count)
        scoped_passed = count_passed(arguments.report, class_prefix=prefix)
        if scoped_passed < expected:
            failures.append(
                f"expected at least {expected} passing tests in {prefix}, "
                f"got {scoped_passed} (skipped tests do not count)"
            )
        print(f"passing tests in {prefix}: {scoped_passed} (floor {expected})")

    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
