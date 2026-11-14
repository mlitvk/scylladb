#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The result of one fuzz run, and the signature of a failure.

A run writes its result as a JSON file, so that the gating procedure can
classify the failures of many runs (see ``gating``).  Two failures with the
same signature are the same failure: the signature is the kind of the failure
plus its message with the variable parts (numbers, keys, addresses, paths,
times) replaced.  One function here makes the signature, and both the test and
the gating classifier use it.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

# The kinds of a failure:
#   violation    -- a read that the model rejected, or a corrupted value
#   final_check  -- a check after the run: a quorum loss, a cache that differs
#                   from the log, a replica that changed over the restart
#   hang         -- the watchdog, or an operation timeout
#   op_error     -- an operation that raised
#   client_error -- a client error that no exception policy accepted
#   no_progress  -- too few writes or reads
#   node_log     -- the teardown found an ERROR line, an assertion or an abort
#                   in a node log; only the gating classifier sees it
#   error        -- any other exception of the test
KINDS = ("violation", "final_check", "hang", "op_error", "client_error", "no_progress", "node_log", "error")

# The parts of a message that change from run to run, in the order in which
# they are replaced.
_VARIABLE_PARTS = (
    (re.compile(r"history=\[.*?\](?=\s|$)"), "history=[...]"),
    (re.compile(r"test_\d+_\w+"), "<ks>"),
    # The tables of the workload are t0, t1, ...
    (re.compile(r"\bt\d+\b"), "<table>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"), "<uuid>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"), "<ip>"),
    (re.compile(r"\b\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:[,.]\d+)?\b"), "<time>"),
    (re.compile(r"0x[0-9a-fA-F]+"), "<hex>"),
    (re.compile(r"(?:/[\w.+\-]+){2,}"), "<path>"),
    # A number with a unit of time, for example a limit that depends on the build mode.
    (re.compile(r"\b\d+(?:\.\d+)?(ms|s)\b"), r"<n>\1"),
    (re.compile(r"-?\b\d+(?:\.\d+)?\b"), "<n>"),
    (re.compile(r"\s+"), " "),
)

# A signature keeps at most this many characters of the message.
SIGNATURE_LENGTH = 200


def normalize(message: str) -> str:
    """Returns ``message`` with the variable parts replaced."""
    text = message.strip().splitlines()[0] if message.strip() else ""
    for pattern, replacement in _VARIABLE_PARTS:
        text = pattern.sub(replacement, text)
    return text.strip()[:SIGNATURE_LENGTH]


def signature(kind: str, message: str) -> str:
    assert kind in KINDS, kind
    return f"{kind}: {normalize(message)}"


class FuzzFailure(AssertionError):
    """A failure of a fuzz run, with its kind.

    It is an AssertionError, so pytest shows it as a test failure.
    """

    def __init__(self, kind: str, message: str, details: str = "") -> None:
        assert kind in KINDS, kind
        super().__init__(f"{kind}: {message}" + (f"\n{details}" if details else ""))
        self.kind = kind
        self.message = message


class HangError(FuzzFailure):
    """A part of the run that did not finish in its time limit."""

    def __init__(self, reason: str) -> None:
        super().__init__("hang", reason)


@dataclass
class RunResult:
    # The pytest node name of the run, for example "test_gating[operations-s3-dev].dev.2".
    # The JUnit XML of test.py names the run in the same way.
    run_id: str
    case_id: str
    profile: str
    seed: int
    mode: str
    outcome: str = "pass"
    kind: Optional[str] = None
    message: Optional[str] = None
    signature: Optional[str] = None
    event_log: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def fail(self, kind: str, message: str) -> None:
        self.outcome = "fail"
        self.kind = kind
        self.message = message
        self.signature = signature(kind, message)

    def file_stem(self) -> str:
        """Returns the name of the files of the run, without the suffix."""
        return re.sub(r"[^\w.\-\[\]]", "_", self.run_id)

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (self.file_stem() + ".json")
        path.write_text(json.dumps(asdict(self), indent=1) + "\n")
        return path

    @staticmethod
    def read(path: Path) -> "RunResult":
        return RunResult(**json.loads(path.read_text()))


def failure_of(exc: BaseException) -> tuple[str, str]:
    """Returns the kind and the message of the exception that ended a run."""
    if isinstance(exc, FuzzFailure):
        return exc.kind, exc.message
    return "error", f"{type(exc).__name__}: {exc}"


# The teardown of test/cluster/conftest.py reports the errors of a node log as a
# line "Server N: found ..." and then the error lines, indented.
_TEARDOWN_SERVER_LINE = re.compile(r"Server \d+: found ")


def node_log_failure(text: str) -> Optional[str]:
    """Returns the first error line of a teardown report of node log errors, or None."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if _TEARDOWN_SERVER_LINE.search(line):
            for detail in lines[i + 1:]:
                if detail.startswith("  ") and detail.strip():
                    return detail.strip()
            return line.strip()
    return None
