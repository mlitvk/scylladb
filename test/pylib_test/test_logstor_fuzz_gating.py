#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests of the failure signatures, the gating cases and the gating classifier of the logstor fuzz test.

The gating procedure decides from these whether a failure is new, so a wrong
signature or a wrong match would blame a branch for a known failure, or the
other way around.
"""

import pytest

from test.cluster.logstor_fuzz.gating import base_signatures, case_of, classify, gating_plan, junit_failures
from test.cluster.logstor_fuzz.known_issues import KnownIssue, KnownIssues
from test.cluster.logstor_fuzz.results import RunResult, node_log_failure, signature


def test_signature_drops_the_variable_parts():
    a = ("test_1794660417078_wvzhr.t0: read returned seq 1234, which is older than a write acknowledged "
         "before the read: key=7 read=[10..20] floor=1240 pruned_below=0 history=[write:1240:ok[1..2], write:1241:pending[3..?]]")
    b = ("test_1794660999999_abcde.t0: read returned seq 99, which is older than a write acknowledged "
         "before the read: key=512 read=[30..40] floor=100 pruned_below=8 history=[write:100:ok[5..6]]")
    assert signature("violation", a) == signature("violation", b)
    assert signature("violation", a) != signature("final_check", a)
    log_a = "ERROR 2026-11-14 13:47:48,072 [shard 0:main] logstor - write failed on 127.31.255.2: /a/b/ls_0-1-Data.db"
    log_b = "ERROR 2026-11-15 01:02:03,004 [shard 1:main] logstor - write failed on 127.31.255.9: /c/d/ls_1-7-Data.db"
    assert signature("node_log", log_a) == signature("node_log", log_b)


def test_node_log_failure_takes_the_first_error_line():
    text = ("Server 3: found 2 error(s) (log: /x/scylla-3.log)\n"
            "  ERROR 2026-11-14 13:47:48,072 [shard 0:main] logstor - first\n"
            "  ERROR 2026-11-14 13:47:49,072 [shard 0:main] logstor - second\n")
    assert node_log_failure(text).endswith("logstor - first")
    assert node_log_failure("assert 1 == 2") is None


TOLERATED = KnownIssue(id="rare", kind="bug", trackers=("SCYLLADB-1",), summary="s", disables=(),
                       remove_when="fixed", signatures=(r"^hang: no write of <table> was acknowledged",))


def test_tolerate_entry_and_its_validation():
    known = KnownIssues(issues=(TOLERATED,))
    assert known.match("hang: no write of <table> was acknowledged for <n>s").id == "rare"
    assert known.match("violation: other") is None
    # An unskipped issue does not count as known: the branch says that it fixes it.
    unskipped = KnownIssues(["rare"], issues=(TOLERATED,))
    assert unskipped.match("hang: no write of <table> was acknowledged for <n>s") is None
    assert unskipped.match("hang: no write of <table> was acknowledged for <n>s", included=True).id == "rare"
    with pytest.raises(ValueError, match="disables, signatures, or both"):
        KnownIssues(issues=(KnownIssue(id="x", kind="bug", trackers=("t",), summary="s", disables=(),
                                       remove_when="w"),))


def test_gating_cases():
    assert gating_plan({}) == ([], [])
    cases, _ = gating_plan({"LOGSTOR_FUZZ_GATING": "full"})
    ids = [c.id for c in cases]
    assert len(ids) == len(set(ids))
    assert {c.mode for c in cases} == {"dev", "debug"}
    assert all(c.mode == "dev" for c in gating_plan({"LOGSTOR_FUZZ_GATING": "full"}, modes=["dev"])[0])
    explore = {"LOGSTOR_FUZZ_GATING": "quick", "LOGSTOR_FUZZ_GATING_EXPLORE": "3", "LOGSTOR_FUZZ_GATING_EXPLORE_SEED": "5"}
    # Each pytest worker must make the same cases.
    assert gating_plan(explore) == gating_plan(explore)
    assert len(gating_plan(explore)[0]) == len(gating_plan({"LOGSTOR_FUZZ_GATING": "quick"})[0]) + 3
    _, uncovered = gating_plan({"LOGSTOR_FUZZ_GATING": "quick", "LOGSTOR_FUZZ_INCLUDE_KNOWN": "logstor-repair"})
    assert "op:replace_node (logstor-repair)" in uncovered
    only, _ = gating_plan({"LOGSTOR_FUZZ_GATING": "full", "LOGSTOR_FUZZ_GATING_ONLY": "operations-s3-dev"})
    assert [c.id for c in only] == ["operations-s3-dev"]
    # A case that is not in the plan, as on a rerun on the base.
    extra, _ = gating_plan({"LOGSTOR_FUZZ_GATING": "quick", "LOGSTOR_FUZZ_GATING_ONLY": "operations-s5-dev"})
    assert [(c.profile, c.mode) for c in extra] == [("operations", "dev")]
    with pytest.raises(ValueError, match="unknown cases"):
        gating_plan({"LOGSTOR_FUZZ_GATING": "full", "LOGSTOR_FUZZ_GATING_ONLY": "nope"})


def write_result(directory, case_id, kind=None, message=None):
    r = RunResult(run_id=f"test_gating[{case_id}].dev.1", case_id=case_id, profile="operations", seed=1, mode="dev")
    if kind:
        r.fail(kind, message)
    r.write(directory)


JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest">
<testcase classname="x" name="test_gating[operations-s1-dev].dev.1">
<error message="failed on teardown">Server 2: found 1 error(s) (log: /x/scylla-2.log)
  ERROR 2026-11-14 13:47:48,072 [shard 0:main] logstor - boom 17
</error></testcase>
<testcase classname="x" name="test_gating[operations-s2-dev].dev.1">
<failure message="AssertionError">Timeout before the result</failure></testcase>
</testsuite></testsuites>
"""


def test_classify(tmp_path):
    results = tmp_path / "results"
    write_result(results, "operations-s0-dev")
    write_result(results, "operations-s1-dev")
    write_result(results, "operations-s3-dev", "hang", "no write of t0 was acknowledged for 91s")
    write_result(results, "operations-s4-dev", "violation", "t1: read returned seq 5, which no write of this key has: key=3")
    write_result(results, "operations-s5-dev", "violation", "t0: read returned seq 8, which no write of this key has: key=9")
    junit = tmp_path / "junit.xml"
    junit.write_text(JUNIT)
    assert junit_failures(junit)["test_gating[operations-s1-dev].dev.1"][0] == "node_log"

    note = "result=fail\nsig=" + signature("node_log", "ERROR 2026-11-15 01:02:03,004 [shard 1:main] logstor - boom 99") + "\n"
    assert len(base_signatures(note)) == 1
    passed, groups = classify(results, [junit], note, KnownIssues(issues=(TOLERATED,)))
    assert [case_of(r) for r in passed] == ["operations-s0-dev"]
    found = sorted((g.classification, [case_of(r) for r in g.runs]) for g in groups)
    assert found == [
        ("known (rare)", ["operations-s3-dev"]),
        ("pre-existing", ["operations-s1-dev"]),
        # A test that ended before it wrote a result is still a failure.
        ("unclassified", ["operations-s2-dev"]),
        # The two violations have one signature, and nothing knows it.
        ("unclassified", ["operations-s4-dev", "operations-s5-dev"]),
    ]
