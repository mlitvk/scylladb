#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The gating cases of the logstor fuzz test, and the classifier of their failures.

The ``/logstor-gating <branch>`` procedure (``~/.claude/commands/logstor-gating.md``,
plan ``~/.claude/plans/logstor-gating.md``) runs the cases of this module on a
merge of the branch with ``logstor_fuzz``, and classifies the failures with the
``classify`` command below.  The rules of the decisions are here, in code: the
cases, the coverage of the unskipped items, and the matching of a failure to a
known issue or to the record of the base.

The env vars:

  * ``LOGSTOR_FUZZ_GATING`` is ``full`` (or ``1``) or ``quick``.  When it is not
    set, ``test_gating`` does not exist, so CI does not run the cases;
  * ``LOGSTOR_FUZZ_GATING_ONLY=<id>[,<id>]`` keeps only these cases, to run
    them again (with ``--repeat``) on the branch or on the base;
  * ``LOGSTOR_FUZZ_GATING_EXPLORE=N`` adds N cases with random seeds, which
    come from ``LOGSTOR_FUZZ_GATING_EXPLORE_SEED``, so that each pytest worker
    makes the same cases;
  * ``LOGSTOR_FUZZ_INCLUDE_KNOWN`` (see ``params``) also adds the profiles that
    hold the items of the unskipped issues;
  * ``LOGSTOR_FUZZ_RESULT_DIR`` is where each case writes its result file.

The command line::

    python3 -m test.cluster.logstor_fuzz.gating cases
    python3 -m test.cluster.logstor_fuzz.gating unskip < text
    python3 -m test.cluster.logstor_fuzz.gating classify --results DIR [--junit FILE] [--base-note FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional

from test.cluster.logstor_fuzz.known_issues import KnownIssues, fixed_by
from test.cluster.logstor_fuzz.params import ENV_PREFIX, PROFILES, Profile, workload_items
from test.cluster.logstor_fuzz.results import RunResult, node_log_failure, signature

# The fixed seeds of the gating cases.  Do not change them: a case id names its
# seed, and the base records of earlier gating runs name the case ids.
SEEDS = (
    7301911871, 5194052207, 8832469051, 1609384417, 4417702263,
    2963184529, 6620573311, 9147285503, 3388146091, 5876013449,
)

# The cases of the full set: (profile, number of seeds, mode).  The profile
# ``failures`` is not a gating case yet: its failure points still find known
# issues of the separator, so a run of it does not tell a regression from them.
FULL = (
    ("workload_only", 2, "dev"),
    ("operations", 6, "dev"),
    ("operations_skewed", 4, "dev"),
    ("multi_column", 2, "dev"),
    ("operations", 1, "debug"),
    ("operations_skewed", 1, "debug"),
)
QUICK = (
    ("workload_only", 1, "dev"),
    ("operations", 1, "dev"),
    ("operations_skewed", 1, "dev"),
)
# The number of seeds of a profile that the cases add for an unskipped item.
EXTRA_SEEDS = 2


@dataclass(frozen=True)
class GatingCase:
    profile: str
    seed: int
    mode: str
    id: str


def _case(profile: str, index: int, mode: str) -> GatingCase:
    return GatingCase(profile, SEEDS[index], mode, f"{profile}-s{index}-{mode}")


_CASE_ID = re.compile(r"^(\w+)-s(\d+)-(dev|debug|release)$")


def case_from_id(case_id: str) -> Optional[GatingCase]:
    """Returns the fixed-seed case that ``case_id`` names, or None."""
    m = _CASE_ID.match(case_id)
    if not m or m.group(1) not in PROFILES or int(m.group(2)) >= len(SEEDS):
        return None
    return _case(m.group(1), int(m.group(2)), m.group(3))


def items_of(profile: Profile) -> set[str]:
    """Returns the known-issue items that a run of ``profile`` does when nothing disables them."""
    items = ({f"profile:{profile.name}"} | {f"op:{op}" for op in profile.nemesis.ops}
             | {f"failure:{f}" for f in profile.nemesis.failures})
    if profile.checks.replica_check:
        items.add("check:repair_and_compare_replicas")
    return items | workload_items(profile)


def gating_cases(env: Mapping[str, str] | None = None, modes: Iterable[str] | None = None) -> list[GatingCase]:
    """Returns the gating cases that the env vars select, for the build modes ``modes``."""
    return gating_plan(env, modes)[0]


def gating_plan(env: Mapping[str, str] | None = None,
                modes: Iterable[str] | None = None) -> tuple[list[GatingCase], list[str]]:
    """Returns the gating cases that the env vars select, and the unskipped items that no profile does.

    An item that no profile does (for example an operation that the fuzz test
    does not have yet) cannot show that the unskipped issue is fixed, so the
    gating report must list it.
    """
    env = os.environ if env is None else env
    level = env.get(ENV_PREFIX + "GATING", "")
    if not level:
        return [], []
    if level not in ("1", "full", "quick"):
        raise ValueError(f"{ENV_PREFIX}GATING={level!r} is not one of 1, full, quick")
    plan = QUICK if level == "quick" else FULL
    cases = [_case(profile, i, mode) for profile, n, mode in plan for i in range(n)]

    # The unskipped issues need cases that do their items.
    include = [x for x in env.get(ENV_PREFIX + "INCLUDE_KNOWN", "").split(",") if x]
    known = KnownIssues(include)
    covered = set().union(*(items_of(PROFILES[c.profile]) for c in cases))
    uncovered = []
    for issue in known.issues:
        if issue.id not in known.included:
            continue
        for item in issue.disables:
            if item in covered:
                continue
            profiles = [p for p in PROFILES.values() if item in items_of(p)]
            if not profiles:
                uncovered.append(f"{item} ({issue.id})")
                continue
            cases += [_case(profiles[0].name, i, "dev") for i in range(EXTRA_SEEDS)]
            covered |= items_of(profiles[0])

    explore = int(env.get(ENV_PREFIX + "GATING_EXPLORE", "0"))
    if explore:
        rng = random.Random(int(env[ENV_PREFIX + "GATING_EXPLORE_SEED"]))
        names = [p for p in PROFILES if p != "workload_only"] or list(PROFILES)
        for i in range(explore):
            seed = rng.randrange(sys.maxsize)
            profile = rng.choice(names)
            cases.append(GatingCase(profile, seed, "dev", f"explore-{profile}-{seed}"))

    unique = {c.id: c for c in cases}
    only = [x for x in env.get(ENV_PREFIX + "GATING_ONLY", "").split(",") if x]
    if only:
        # A rerun on the base runs without the unskips of the branch, so a case
        # that an unskip added is not in the plan there; make it from its id.
        selected = {}
        for case_id in only:
            case = unique.get(case_id) or case_from_id(case_id)
            if case is None:
                raise ValueError(f"{ENV_PREFIX}GATING_ONLY names unknown cases: {case_id}")
            selected[case_id] = case
        unique = selected
    return [c for c in unique.values() if modes is None or c.mode in modes], uncovered


# A base record is the note that a gating run of the base wrote; each failure
# group of it is a line "sig=<signature>".
_BASE_LINE = re.compile(r"^sig=(.*)$", re.MULTILINE)


def base_signatures(note: str) -> set[str]:
    return set(_BASE_LINE.findall(note))


_RUN_ID = re.compile(r"test_gating\[(.+?)\]")


def case_of(run_id: str) -> str:
    """Returns the case id of a gating run id, or the run id itself for another test."""
    m = _RUN_ID.search(run_id)
    return m.group(1) if m else run_id


def junit_failures(path: Path) -> dict[str, tuple[str, str]]:
    """Returns the failures of the gating runs in a JUnit XML file of test.py.

    The result is the run id mapped to the kind and the message.  It holds
    the failures that a result file cannot hold: the node log errors that the
    teardown found, and a test that ended before it wrote its result.
    """
    failures = {}
    for testcase in ET.parse(path).getroot().iter("testcase"):
        run_id = testcase.get("name", "")
        if not _RUN_ID.search(run_id):
            continue
        for element in list(testcase.findall("failure")) + list(testcase.findall("error")):
            text = (element.get("message") or "") + "\n" + (element.text or "")
            line = node_log_failure(text)
            if line is not None:
                failures[run_id] = ("node_log", line)
            else:
                failures.setdefault(run_id, ("error", text.strip().splitlines()[0] if text.strip() else ""))
    return failures


@dataclass
class FailureGroup:
    signature: str
    kind: str
    message: str
    # The run ids of the failed runs.
    runs: list[str]
    # "known (<id>)", "fix-not-effective (<id>)" (the failure of an issue that
    # the run unskipped), "pre-existing", or "unclassified" (needs a rerun on the base).
    classification: str


def classify(results_dir: Path, junits: Iterable[Path], base_note: str,
             known: KnownIssues) -> tuple[list[str], list[FailureGroup]]:
    """Returns the run ids that passed, and the failures grouped by signature and classified."""
    failures: dict[str, tuple[str, str]] = {}
    passed = []
    for path in sorted(results_dir.glob("*.json")):
        r = RunResult.read(path)
        if r.outcome == "fail":
            failures[r.run_id] = (r.kind, r.message)
        else:
            passed.append(r.run_id)
    for junit in junits:
        for run_id, (kind, message) in junit_failures(junit).items():
            # A node log error of a run that passed its own checks, or a run with no result file.
            if kind == "node_log" or run_id not in failures:
                failures[run_id] = (kind, message)
                if run_id in passed:
                    passed.remove(run_id)

    base = base_signatures(base_note)
    groups: dict[str, FailureGroup] = {}
    for run_id, (kind, message) in sorted(failures.items()):
        sig = signature(kind, message)
        group = groups.get(sig)
        if group is None:
            issue = known.match(sig)
            unskipped = known.match(sig, included=True)
            if unskipped is not None:
                classification = f"fix-not-effective ({unskipped.id})"
            elif issue is not None:
                classification = f"known ({issue.id})"
            elif sig in base:
                classification = "pre-existing"
            else:
                classification = "unclassified"
            group = groups[sig] = FailureGroup(sig, kind, message, [], classification)
        group.runs.append(run_id)
    return passed, list(groups.values())


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python3 -m test.cluster.logstor_fuzz.gating")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("cases", help="print the gating cases that the env vars select")
    sub.add_parser("unskip", help="print the known issues whose trackers the text on stdin names")
    c = sub.add_parser("classify", help="group and classify the failures of a gating run")
    c.add_argument("--results", type=Path, required=True)
    c.add_argument("--junit", type=Path, action="append", default=[])
    c.add_argument("--base-note", type=Path)
    c.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.cmd == "cases":
        cases, uncovered = gating_plan()
        for case in cases:
            print(f"{case.id}\t{case.profile}\t{case.seed}\t{case.mode}")
        for item in uncovered:
            print(f"uncovered\t{item}", file=sys.stderr)
    elif args.cmd == "unskip":
        for issue_id, tracker in fixed_by(sys.stdin.read()).items():
            print(f"{issue_id}\t{tracker}")
    elif args.cmd == "classify":
        include = [x for x in os.environ.get(ENV_PREFIX + "INCLUDE_KNOWN", "").split(",") if x]
        note = args.base_note.read_text() if args.base_note and args.base_note.exists() else ""
        passed, groups = classify(args.results, [j for j in args.junit if j.exists()], note, KnownIssues(include))
        if args.json:
            print(json.dumps({"passed": passed, "groups": [g.__dict__ for g in groups]}, indent=1))
        else:
            print(f"passed: {len(passed)}")
            for g in groups:
                print(f"{g.classification}\t{len(g.runs)}\t{g.signature}\t{','.join(g.runs)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
