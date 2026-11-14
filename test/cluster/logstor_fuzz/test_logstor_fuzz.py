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
"""

import asyncio
import logging
import time

from test.cluster.logstor_fuzz.checks import final_checks
from test.cluster.logstor_fuzz.known_issues import KnownIssues
from test.cluster.logstor_fuzz.params import Profile, load_profile, live_bytes_per_shard
from test.cluster.logstor_fuzz.workload import LogstorWorkload, SegmentPeaks, run_workloads
from test.cluster.strong_consistency.outcomes import strict
from test.cluster.strong_consistency.register_workload import check_linearizable
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def boot_cluster(manager: ScyllaClusterManager, profile: Profile):
    c = profile.cluster
    config = {
        "logstor_disk_size_in_mb": c.logstor_disk_size_in_mb,
        "logstor_file_size_in_mb": c.logstor_file_size_in_mb,
    }
    property_file = [{"dc": "dc1", "rack": f"rack{r + 1}"}
                     for r, n in enumerate(c.nodes_per_rack) for _ in range(n)]
    servers = await manager.servers_add(len(property_file), config=config, cmdline=["--smp", str(c.smp)],
                                        property_file=property_file)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    return servers, cql


async def run_profile(manager: ScyllaClusterManager, profile: Profile, tmp_path) -> None:
    logger.info("Logstor fuzz run: %s", profile.describe())
    logger.info("To run the same choices again, set LOGSTOR_FUZZ_SEED=%d", profile.seed)
    logger.info("Live data bound: %.1f MB per shard", live_bytes_per_shard(profile) / 2**20)
    known = KnownIssues(profile.include_known)
    logger.info("Known issues (test/cluster/logstor_fuzz/known_issues.py):\n%s", known.summary())
    _, cql = await boot_cluster(manager, profile)
    c, w = profile.cluster, profile.workload

    ks_opts = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {c.rf}}} "
               f"AND tablets = {{'initial': {c.initial_tablets}}}")
    async with new_test_keyspace(manager, ks_opts) as ks:
        stop = asyncio.Event()
        ts_base = time.time_ns() // 1000
        workloads = []
        for i in range(w.tables):
            wl = LogstorWorkload(ks=ks, table_name=f"t{i}", name=f"t{i}", seed=profile.seed, params=w,
                                 table_index=i, ts_base=ts_base, stop_event=stop, exception_policy=strict)
            await wl.create_table(cql)
            workloads.append(wl)

        peaks = SegmentPeaks()
        task_errors = await run_workloads(manager, cql, workloads, profile.duration_s, peaks)
        logger.info("Store use during the run: %s", peaks.summary())
        violations = [v for wl in workloads for v in wl.violations]
        assert not violations, f"seed={profile.seed}: {len(violations)} reads failed the check:\n" + "\n".join(violations[:20])
        assert not task_errors, f"seed={profile.seed}: task(s) failed: {task_errors}"
        for wl in workloads:
            wl.assert_progress(min_writes=100, min_reads=100)

        write_quorum = {"ONE": 1, "QUORUM": c.rf // 2 + 1, "ALL": c.rf}[w.write_cl]
        cql = await final_checks(manager, cql, workloads, write_quorum, profile.checks.replica_check,
                                 profile.checks.final_restart, known)

        if profile.checks.porcupine:
            for wl in workloads:
                await check_linearizable(wl, output_dir=tmp_path / "porcupine" / wl.table_name)


async def test_workload_only(manager: ScyllaClusterManager, tmp_path):
    """Runs the checked workload with no cluster operations and no failures.

    This profile checks the model and the checks themselves: a run with no
    disruption must pass, so any failure here is a bug in logstor or in the
    test.
    """
    await run_profile(manager, load_profile("workload_only"), tmp_path)
