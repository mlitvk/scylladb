#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The logstor fuzz test.

Each test runs one profile of ``params.PROFILES``.  The log prints the seed at
the start, and ``LOGSTOR_FUZZ_SEED=<seed>`` runs the same choices again.  The
interleaving of the requests is not the same in a second run, so a second run
repeats a timing bug only by chance.

The tests have the mark ``check_nodes_for_errors``, so the teardown fails a
run whose node logs have an ERROR line that no pattern in
``manager.ignore_log_patterns`` allows.

Each run writes a result file (see ``results``) to ``LOGSTOR_FUZZ_RESULT_DIR``,
or to its ``tmp_path``.  ``test_gating`` runs the cases of ``gating``, and it
exists only when ``LOGSTOR_FUZZ_GATING`` is set.
"""

import asyncio
import dataclasses
import logging
import os
import time
from pathlib import Path

import pytest

from test.cluster.logstor_fuzz.checks import final_checks
from test.cluster.logstor_fuzz.failures import (ALLOWED_LOG_PATTERNS, enabled_failures, failure_task,
                                                make_failure_schedule)
from test.cluster.logstor_fuzz.gating import gating_cases
from test.cluster.logstor_fuzz.known_issues import KnownIssues
from test.cluster.logstor_fuzz.nemesis import (
    EventLog,
    enabled_operations,
    make_schedule,
    nemesis_task,
    time_limit,
    watchdog_task,
)
from test.cluster.logstor_fuzz.operations import EXCEPTION_POLICY, FuzzContext
from test.cluster.logstor_fuzz.params import ENV_PREFIX, Profile, load_profile, utilization_estimate
from test.cluster.logstor_fuzz.results import FuzzFailure, RunResult, failure_of
from test.cluster.logstor_fuzz.workload import LogstorWorkload, SegmentPeaks, preload, run_workloads
from test.cluster.strong_consistency.outcomes import strict
from test.cluster.strong_consistency.register_workload import check_linearizable
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


def node_config(profile: Profile) -> tuple[dict, list[str]]:
    c = profile.cluster
    config = {
        "logstor_disk_size_in_mb": c.logstor_disk_size_in_mb,
        "logstor_file_size_in_mb": c.logstor_file_size_in_mb,
        # A resize needs fresh tablet sizes.
        "tablet_load_stats_refresh_interval_in_seconds": 5,
    }
    return config, ["--smp", str(c.smp)]


async def boot_cluster(manager: ScyllaClusterManager, profile: Profile) -> dict[int, str]:
    """Starts the nodes of the profile, and returns the rack of each server."""
    config, cmdline = node_config(profile)
    racks = [f"rack{r + 1}" for r, n in enumerate(profile.cluster.nodes_per_rack) for _ in range(n)]
    servers = await manager.servers_add(len(racks), config=config, cmdline=cmdline,
                                        property_file=[{"dc": "dc1", "rack": r} for r in racks])
    await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)
    return {s.server_id: rack for s, rack in zip(servers, racks)}


def primary_failure(task_errors: list[BaseException]) -> FuzzFailure:
    """Returns the failure of a run from the exceptions of its tasks.

    A failure of the fuzz test itself (a hang, an operation error) comes first,
    then a client error that no exception policy accepted.
    """
    for e in task_errors:
        if isinstance(e, FuzzFailure):
            return e
    for e in task_errors:
        if isinstance(e, AssertionError) and "not accepted by the exception policy" in str(e):
            return FuzzFailure("client_error", str(e).splitlines()[0])
    kind, message = failure_of(task_errors[0])
    return FuzzFailure(kind, message)


async def run_case(manager: ScyllaClusterManager, profile: Profile, request, tmp_path, case_id: str, mode: str) -> None:
    """Runs ``profile`` and writes its result file, also when the run fails."""
    result = RunResult(run_id=request.node.name, case_id=case_id, profile=profile.name, seed=profile.seed, mode=mode)
    # The event log goes next to the result file, so that it outlives the run:
    # tmp_path of a test.py run is inside the dbuild container.
    result_dir = Path(os.environ.get(ENV_PREFIX + "RESULT_DIR") or tmp_path)
    result_dir.mkdir(parents=True, exist_ok=True)
    event_log = result_dir / (result.file_stem() + ".events.jsonl")
    result.event_log = str(event_log)
    try:
        await run_profile(manager, profile, tmp_path, event_log, mode)
    except BaseException as e:
        result.fail(*failure_of(e))
        raise
    finally:
        logger.info("Result: %s", result.write(result_dir))


async def run_profile(manager: ScyllaClusterManager, profile: Profile, tmp_path, event_log: Path, mode: str) -> None:
    logger.info("Logstor fuzz run: %s", profile.describe())
    limits = profile.limits.for_mode(mode)
    logger.info("To run the same choices again, set LOGSTOR_FUZZ_SEED=%d", profile.seed)
    logger.info("Planned utilization of a full-copy shard: %.2f (%d keys per table)",
                utilization_estimate(profile), profile.workload.keys)
    known = KnownIssues(profile.include_known)
    logger.info("Known issues (test/cluster/logstor_fuzz/known_issues.py):\n%s", known.summary())
    async with time_limit("the start of the cluster", limits.boot_s):
        rack_of = await boot_cluster(manager, profile)
    cql = manager.get_cql()
    c, w = profile.cluster, profile.workload
    policy = EXCEPTION_POLICY if profile.nemesis.ops or profile.nemesis.failures else strict

    ks_opts = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {c.rf}}} "
               f"AND tablets = {{'initial': {c.initial_tablets}}}")
    async with new_test_keyspace(manager, ks_opts) as ks:
        stop = asyncio.Event()
        ts_base = time.time_ns() // 1000
        workloads = []
        for i in range(w.tables):
            wl = LogstorWorkload(ks=ks, table_name=f"t{i}", name=f"t{i}", seed=profile.seed, params=w,
                                 table_index=i, ts_base=ts_base, stop_event=stop, exception_policy=policy)
            await wl.create_table(cql)
            workloads.append(wl)

        peaks = SegmentPeaks(store_bytes=c.logstor_disk_size_in_mb * 2**20)
        if w.preload:
            async with time_limit("the preload", limits.preload_s):
                await asyncio.gather(*(preload(wl, cql) for wl in workloads))
            await peaks.sample(manager)
            logger.info("Store use after the preload: %s", peaks.summary())

        config, cmdline = node_config(profile)
        ctx = FuzzContext(manager=manager, profile=profile, workloads=workloads, known=known, limits=limits,
                          node_config=config, node_cmdline=cmdline, rack_of=rack_of, peaks=peaks)
        schedule = make_schedule(profile.seed, enabled_operations(ctx), profile.duration_s,
                                 profile.nemesis.op_interval)
        events = EventLog(event_log)
        logger.info("Event log: %s; schedule: %s", events.path, [s.name for s in schedule])
        extra = []
        if schedule:
            extra.append(("nemesis", lambda: nemesis_task(ctx, schedule, stop, events)))
        failure_schedule = make_failure_schedule(profile.seed, enabled_failures(ctx), profile.duration_s,
                                                 profile.nemesis.failure_interval)
        if failure_schedule:
            logger.info("Failure schedule: %s", [f.name for f in failure_schedule])
            # The injected failures log errors that the teardown scan must accept.
            manager.ignore_log_patterns.extend(ALLOWED_LOG_PATTERNS)
            extra.append(("failures", lambda: failure_task(ctx, failure_schedule, stop, events)))
        extra.append(("watchdog", lambda: watchdog_task(ctx, stop, events)))

        # The tasks stop at ``duration_s``, and the nemesis waits for the operation that runs.
        run_limit = profile.duration_s + limits.data_op_s + limits.stop_s
        try:
            async with time_limit("the run", run_limit):
                task_errors = await run_workloads(manager, cql, workloads, profile.duration_s, peaks, extra)
        finally:
            events.close()
        logger.info("Store use during the run: %s", peaks.summary())
        violations = [v for wl in workloads for v in wl.violations]
        if violations:
            raise FuzzFailure("violation", violations[0],
                              f"seed={profile.seed}: {len(violations)} reads failed the check:\n" + "\n".join(violations[:20]))
        if task_errors:
            raise primary_failure(task_errors)
        for wl in workloads:
            try:
                wl.assert_progress(min_writes=100, min_reads=100)
            except AssertionError as e:
                raise FuzzFailure("no_progress", f"{wl.table_name}: {e}") from e

        write_quorum = {"ONE": 1, "QUORUM": c.rf // 2 + 1, "ALL": c.rf}[w.write_cl]
        async with time_limit("the checks after the run", limits.checks_s):
            cql = await final_checks(manager, manager.get_cql(), workloads, write_quorum,
                                     profile.checks.replica_check, profile.checks.final_restart, known,
                                     limits.node_start_s)

        if profile.checks.porcupine:
            for wl in workloads:
                await check_linearizable(wl, output_dir=tmp_path / "porcupine" / wl.table_name)


@pytest.mark.check_nodes_for_errors
async def test_workload_only(manager: ScyllaClusterManager, build_mode, request, tmp_path):
    """Runs the checked workload with no cluster operations and no failures.

    This profile checks the model and the checks themselves: a run with no
    disruption must pass, so any failure here is a bug in logstor or in the
    test.
    """
    profile = load_profile("workload_only")
    await run_case(manager, profile, request, tmp_path, f"workload_only-{profile.seed}-{build_mode}", build_mode)


@pytest.mark.check_nodes_for_errors
async def test_operations(manager: ScyllaClusterManager, build_mode, request, tmp_path):
    """Runs the checked workload under random cluster operations, with no injected failures."""
    profile = load_profile("operations")
    await run_case(manager, profile, request, tmp_path, f"operations-{profile.seed}-{build_mode}", build_mode)


@pytest.mark.check_nodes_for_errors
async def test_operations_skewed(manager: ScyllaClusterManager, build_mode, request, tmp_path):
    """Runs the operations profile with a zipf key distribution: a few hot keys take most of the load."""
    profile = load_profile("operations_skewed")
    await run_case(manager, profile, request, tmp_path, f"operations_skewed-{profile.seed}-{build_mode}", build_mode)


@pytest.mark.check_nodes_for_errors
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_failures(manager: ScyllaClusterManager, build_mode, request, tmp_path):
    """Runs the operations profile with injected delays, errors, pauses and a fatal disk error in logstor."""
    profile = load_profile("failures")
    await run_case(manager, profile, request, tmp_path, f"failures-{profile.seed}-{build_mode}", build_mode)


if os.environ.get(ENV_PREFIX + "GATING"):
    def pytest_generate_tests(metafunc):
        if metafunc.function.__name__ == "test_gating":
            cases = gating_cases(modes=metafunc.config.build_modes)
            metafunc.parametrize("case", cases, ids=[c.id for c in cases])

    @pytest.mark.check_nodes_for_errors
    async def test_gating(manager: ScyllaClusterManager, build_mode, request, tmp_path, case):
        """Runs one gating case: a profile with a fixed seed (see ``gating``)."""
        assert case.mode == build_mode, f"case {case.id} is for {case.mode}, the run is {build_mode}"
        env = dict(os.environ, **{ENV_PREFIX + "SEED": str(case.seed)})
        await run_case(manager, load_profile(case.profile, env), request, tmp_path, case.id, build_mode)
