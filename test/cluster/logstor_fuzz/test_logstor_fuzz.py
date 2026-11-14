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
"""

import asyncio
import logging
import time

import pytest

from test.cluster.logstor_fuzz.checks import final_checks
from test.cluster.logstor_fuzz.known_issues import KnownIssues
from test.cluster.logstor_fuzz.nemesis import EventLog, enabled_operations, make_schedule, nemesis_task, watchdog_task
from test.cluster.logstor_fuzz.operations import EXCEPTION_POLICY, FuzzContext
from test.cluster.logstor_fuzz.params import Profile, load_profile, live_bytes_per_shard
from test.cluster.logstor_fuzz.workload import LogstorWorkload, SegmentPeaks, run_workloads
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


async def run_profile(manager: ScyllaClusterManager, profile: Profile, tmp_path) -> None:
    logger.info("Logstor fuzz run: %s", profile.describe())
    logger.info("To run the same choices again, set LOGSTOR_FUZZ_SEED=%d", profile.seed)
    logger.info("Live data bound: %.1f MB per shard", live_bytes_per_shard(profile) / 2**20)
    known = KnownIssues(profile.include_known)
    logger.info("Known issues (test/cluster/logstor_fuzz/known_issues.py):\n%s", known.summary())
    rack_of = await boot_cluster(manager, profile)
    cql = manager.get_cql()
    c, w = profile.cluster, profile.workload
    policy = EXCEPTION_POLICY if profile.nemesis.ops else strict

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

        config, cmdline = node_config(profile)
        ctx = FuzzContext(manager=manager, profile=profile, workloads=workloads, known=known,
                          node_config=config, node_cmdline=cmdline, rack_of=rack_of)
        schedule = make_schedule(profile.seed, enabled_operations(ctx), profile.duration_s,
                                 profile.nemesis.op_interval)
        events = EventLog(tmp_path / "events.jsonl")
        logger.info("Event log: %s; schedule: %s", events.path, [s.name for s in schedule])
        extra = []
        if schedule:
            extra.append(("nemesis", lambda: nemesis_task(ctx, schedule, stop, events)))
        extra.append(("watchdog", lambda: watchdog_task(ctx, stop, events)))

        peaks = SegmentPeaks()
        try:
            task_errors = await run_workloads(manager, cql, workloads, profile.duration_s, peaks, extra)
        finally:
            events.close()
        logger.info("Store use during the run: %s", peaks.summary())
        violations = [v for wl in workloads for v in wl.violations]
        assert not violations, f"seed={profile.seed}: {len(violations)} reads failed the check:\n" + "\n".join(violations[:20])
        assert not task_errors, f"seed={profile.seed}: task(s) failed: {task_errors}"
        for wl in workloads:
            wl.assert_progress(min_writes=100, min_reads=100)

        write_quorum = {"ONE": 1, "QUORUM": c.rf // 2 + 1, "ALL": c.rf}[w.write_cl]
        cql = await final_checks(manager, manager.get_cql(), workloads, write_quorum,
                                 profile.checks.replica_check, profile.checks.final_restart, known)

        if profile.checks.porcupine:
            for wl in workloads:
                await check_linearizable(wl, output_dir=tmp_path / "porcupine" / wl.table_name)


@pytest.mark.check_nodes_for_errors
async def test_workload_only(manager: ScyllaClusterManager, tmp_path):
    """Runs the checked workload with no cluster operations and no failures.

    This profile checks the model and the checks themselves: a run with no
    disruption must pass, so any failure here is a bug in logstor or in the
    test.
    """
    await run_profile(manager, load_profile("workload_only"), tmp_path)


@pytest.mark.check_nodes_for_errors
async def test_operations(manager: ScyllaClusterManager, tmp_path):
    """Runs the checked workload under random cluster operations, with no injected failures."""
    await run_profile(manager, load_profile("operations"), tmp_path)
