#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests of the read checker, the value codec, the parameters and the known issues of the logstor fuzz test.

The fuzz test trusts the model to accept every correct read and to reject the
wrong ones, so these tests give the model histories of both kinds.
"""

import pytest

from test.cluster.logstor_fuzz.known_issues import KNOWN_ISSUES, KnownIssue, KnownIssues
from test.cluster.logstor_fuzz.model import Kind, Model, Status
from test.cluster.logstor_fuzz.params import load_profile
from test.cluster.logstor_fuzz.workload import CorruptValue, decode_value, encode_value

KEY = 7


def write(model: Model, seq: int, t_call: int, t_return: int, status: Status = Status.OK, kind: Kind = Kind.WRITE):
    entry = model.write_started(KEY, seq, kind, t_call)
    model.write_finished(KEY, entry, status, t_return)
    return entry


def test_read_of_a_key_with_no_writes():
    model = Model()
    assert model.check_read(KEY, 10, 20, None) is None
    assert model.check_read(KEY, 10, 20, 1) is not None


def test_read_after_an_acknowledged_write():
    model = Model()
    write(model, 1, 10, 20)
    assert model.check_read(KEY, 30, 40, 1) is None
    assert "no row" in model.check_read(KEY, 30, 40, None)


def test_read_concurrent_with_a_write_returns_either_value():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 50)
    assert model.check_read(KEY, 25, 40, 1) is None
    assert model.check_read(KEY, 25, 40, 2) is None


def test_read_older_than_the_floor():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40)
    assert "older" in model.check_read(KEY, 50, 60, 1)


def test_read_of_a_write_that_started_after_the_read():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 70, 80)
    assert "started after the read ended" in model.check_read(KEY, 30, 60, 2)


def test_read_of_a_failed_write():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40, Status.FAIL)
    assert "failed" in model.check_read(KEY, 50, 60, 2)


def test_read_of_an_unknown_write():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40, Status.UNKNOWN)
    # The unknown write can be applied at any time, or never.
    assert model.check_read(KEY, 50, 60, 2) is None
    assert model.check_read(KEY, 50, 60, 1) is None
    # Once a newer write is acknowledged, neither older value can be read.
    write(model, 3, 70, 80)
    assert model.check_read(KEY, 90, 100, 2) is not None
    assert model.check_read(KEY, 90, 100, 1) is not None
    assert model.check_read(KEY, 90, 100, 3) is None


def test_read_of_a_pending_write():
    model = Model()
    write(model, 1, 10, 20)
    model.write_started(KEY, 2, Kind.WRITE, 30)
    assert model.check_read(KEY, 40, 50, 2) is None
    assert model.check_read(KEY, 40, 50, 1) is None


def test_read_of_a_seq_that_was_never_written():
    model = Model()
    write(model, 1, 10, 20)
    assert "no write" in model.check_read(KEY, 30, 40, 5)


def test_deletes():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40, kind=Kind.DELETE)
    assert model.check_read(KEY, 50, 60, None) is None
    assert model.check_read(KEY, 50, 60, 1) is not None
    # The seq of a delete is not a row.
    assert "delete" in model.check_read(KEY, 50, 60, 2)
    # A delete that is in flight can hide the row.
    write(model, 3, 70, 80)
    model.write_started(KEY, 4, Kind.DELETE, 90)
    assert model.check_read(KEY, 95, 100, None) is None
    assert model.check_read(KEY, 95, 100, 3) is None


def test_failed_delete_does_not_hide_the_row():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40, Status.FAIL, Kind.DELETE)
    assert model.check_read(KEY, 50, 60, None) is not None


def test_prune_keeps_what_an_open_read_can_return():
    model = Model()
    read_id = model.read_started(5)
    for seq in range(1, 3 * Model.PRUNE_AT):
        write(model, seq, 10 * seq, 10 * seq + 5)
    last = 3 * Model.PRUNE_AT - 1
    # The read started before any write was acknowledged, so it can return any of them.
    assert model.check_read(KEY, 5, 10 * last + 10, 1) is None
    model.read_finished(read_id)
    # One more write prunes the history, and an old value is now an error.
    write(model, last + 1, 10 * (last + 1), 10 * (last + 1) + 5)
    assert len(model.history(KEY).entries) < Model.PRUNE_AT
    assert "older" in model.check_read(KEY, 10 * (last + 2), 10 * (last + 2) + 5, 1)
    assert model.check_read(KEY, 10 * (last + 2), 10 * (last + 2) + 5, last + 1) is None


def test_check_final():
    model = Model()
    write(model, 1, 10, 20)
    write(model, 2, 30, 40, Status.UNKNOWN)
    assert model.check_final(KEY, 100, 1) is None
    assert model.check_final(KEY, 100, 2) is None
    assert model.check_final(KEY, 100, None) is not None


def test_value_codec():
    value = encode_value(42, 1, KEY, 9, 100)
    assert decode_value(42, 1, KEY, value) == 9
    with pytest.raises(CorruptValue, match="key"):
        decode_value(42, 1, KEY + 1, value)
    with pytest.raises(CorruptValue, match="payload"):
        decode_value(42, 1, KEY, value[:-1] + bytes([value[-1] ^ 1]))
    with pytest.raises(CorruptValue, match="header"):
        decode_value(42, 1, KEY, value[:5])


def test_profile_overrides():
    profile = load_profile("workload_only", {"LOGSTOR_FUZZ_SEED": "5", "LOGSTOR_FUZZ_KEYS": "10",
                                             "LOGSTOR_FUZZ_NODES_PER_RACK": "2,1,1",
                                             "LOGSTOR_FUZZ_REPLICA_CHECK": "false"})
    assert profile.seed == 5
    assert profile.workload.keys == 10
    assert profile.cluster.nodes_per_rack == (2, 1, 1)
    assert profile.checks.replica_check is False
    with pytest.raises(ValueError, match="unknown"):
        load_profile("workload_only", {"LOGSTOR_FUZZ_KEYZ": "10"})


def test_profile_validation():
    with pytest.raises(ValueError, match="overlap"):
        load_profile("workload_only", {"LOGSTOR_FUZZ_READ_CL": "ONE"})
    with pytest.raises(ValueError, match="store"):
        load_profile("workload_only", {"LOGSTOR_FUZZ_KEYS": "1000000"})


def test_known_issues_are_well_formed():
    # KnownIssues() validates the entries, so a bad entry fails here and not in a cluster run.
    known = KnownIssues()
    for issue in KNOWN_ISSUES:
        for item in issue.disables:
            assert known.blocking(item) is not None


def test_known_issues_include():
    issue = KnownIssue(id="x", kind="bug", trackers=("SCYLLADB-1",), summary="s",
                       disables=("op:remove_node",), remove_when="fixed")
    assert not KnownIssues(issues=(issue,)).enabled("op:remove_node")
    assert KnownIssues(issues=(issue,)).enabled("op:add_node")
    assert KnownIssues(["x"], issues=(issue,)).enabled("op:remove_node")
    assert KnownIssues(["all"], issues=(issue,)).enabled("op:remove_node")
    with pytest.raises(ValueError, match="unknown entries"):
        KnownIssues(["y"], issues=(issue,))
    with pytest.raises(ValueError, match="category"):
        KnownIssues().enabled("remove_node")
    with pytest.raises(ValueError, match="kind"):
        KnownIssues(issues=(KnownIssue(id="x", kind="flaky", trackers=("t",), summary="s",
                                       disables=("op:a",), remove_when="w"),))


def test_profile_include_known():
    profile = load_profile("workload_only", {"LOGSTOR_FUZZ_INCLUDE_KNOWN": "logstor-repair"})
    assert profile.include_known == ("logstor-repair",)
    assert KnownIssues(profile.include_known).enabled("check:repair_and_compare_replicas")


def test_schedule_is_stable_for_a_seed():
    from test.cluster.logstor_fuzz.nemesis import make_schedule
    from test.cluster.logstor_fuzz.operations import OPERATIONS
    names = list(OPERATIONS)
    a = make_schedule(11, names, 300, (5, 20))
    assert a == make_schedule(11, names, 300, (5, 20))
    assert a != make_schedule(12, names, 300, (5, 20))
    assert sum(op.pause_s for op in a) < 300
    assert make_schedule(11, [], 300, (5, 20)) == []


def test_known_issues_filter_operations():
    from test.cluster.logstor_fuzz.nemesis import enabled_operations
    from test.cluster.logstor_fuzz.operations import FuzzContext
    profile = load_profile("operations", {"LOGSTOR_FUZZ_SEED": "1"})
    ctx = FuzzContext(manager=None, profile=profile, workloads=[], known=KnownIssues())
    names = enabled_operations(ctx)
    assert "remove_node" not in names and "repair" not in names
    assert "restart_node" in names
    ctx = FuzzContext(manager=None, profile=profile, workloads=[], known=KnownIssues(["logstor-repair"]))
    assert "remove_node" in enabled_operations(ctx)
    bad = load_profile("operations", {"LOGSTOR_FUZZ_SEED": "1", "LOGSTOR_FUZZ_OPS": "no_such_op"})
    with pytest.raises(ValueError, match="unknown operation"):
        enabled_operations(FuzzContext(manager=None, profile=bad, workloads=[], known=KnownIssues()))
