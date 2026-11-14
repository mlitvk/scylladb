#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The cluster operations that the nemesis of the logstor fuzz test runs.

Each operation keeps this invariant of the model: each acknowledged write
stays on at least a write quorum of its replicas.  An operation runs only when
all nodes are up, it takes down at most one node, and it brings the node back
(or removes it from the cluster) before it returns.  So at most one replica of
a tablet is down at a time.

These operations keep the data of each replica:

  * a restart, a kill and a pause, because logstor acknowledges a write only
    after the write is in a segment of a file that is opened with O_DSYNC;
  * a tablet migration and a decommission, because they stream the logstor
    segments from the leaving replica.

A removenode rebuilds a replica from one other replica and then repairs it, so
it needs logstor repair, and a known issue disables it until then.

An operation that can make a client request fail runs inside a disruption
window of each workload (see ``RegisterWorkload.disruption_window``), and the
exception policy of the run accepts the errors of the window:

  * ``node`` accepts what a stopped, killed, paused or leaving node causes
    (``tolerate_reboot``);
  * ``tablets`` accepts timeouts (``tolerate_unavailability``), because a
    migration or a resize slows the requests of its tablet, in debug mode most.

An operation returns None when it ran, or a reason when it did not run because
the cluster state does not allow it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Sequence

from test.cluster.logstor_fuzz.known_issues import KnownIssues
from test.cluster.logstor_fuzz.params import Profile
from test.cluster.logstor_fuzz.workload import LogstorWorkload, SegmentPeaks
from test.cluster.strong_consistency.outcomes import (
    ExceptionPolicy,
    first_match,
    only_during,
    tolerate_reboot,
    tolerate_resets,
    tolerate_unavailability,
)
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_count
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

NODE_WINDOW = "node"
TABLETS_WINDOW = "tablets"

# The errors that the operations can cause, and when the run accepts them.
EXCEPTION_POLICY: ExceptionPolicy = first_match(
    tolerate_resets,
    only_during(NODE_WINDOW, tolerate_reboot),
    only_during(TABLETS_WINDOW, tolerate_unavailability),
)

# A move request that names a tablet which changed after the operation read the
# tablet map: the balancer or a resize moved it first.  The operation skips it.
# These are the messages of storage_service::move_tablet() and transit_tablet().
STALE_MOVE = re.compile(r"is in transition|has no replica on|has replica on")


@dataclass
class FuzzContext:
    """What the operations need: the cluster, the run and its state."""

    manager: object
    profile: Profile
    workloads: Sequence[LogstorWorkload]
    known: KnownIssues
    # The config and the command line of a new node.
    node_config: dict = field(default_factory=dict)
    node_cmdline: list[str] = field(default_factory=list)
    # The rack of each server that is in the cluster.
    rack_of: dict[int, str] = field(default_factory=dict)
    balancing_enabled: bool = True
    # The use of the store of each shard, which the monitor samples.
    peaks: SegmentPeaks = field(default_factory=SegmentPeaks)

    def too_full(self) -> Optional[str]:
        """Returns a reason when a shard is too full for an operation that moves data, or None."""
        u = self.peaks.max_utilization()
        if u > self.profile.nemesis.max_shard_utilization:
            return f"a shard is at {u:.2f} of its store, above {self.profile.nemesis.max_shard_utilization}"
        return None

    @property
    def ks(self) -> str:
        return self.workloads[0].ks

    async def servers(self) -> list[ServerInfo]:
        return [s for s in await self.manager.running_servers() if s.server_id in self.rack_of]

    async def rack_sizes(self) -> dict[str, int]:
        sizes = {f"rack{r + 1}": 0 for r in range(len(self.profile.cluster.nodes_per_rack))}
        for s in await self.servers():
            sizes[self.rack_of[s.server_id]] += 1
        return sizes

    @contextlib.asynccontextmanager
    async def window(self, name: Optional[str]):
        """Opens the disruption window ``name`` in each workload, or nothing for None."""
        async with contextlib.AsyncExitStack() as stack:
            if name is not None:
                for wl in self.workloads:
                    await stack.enter_async_context(wl.disruption_window(name))
            yield

    async def wait_healthy(self) -> None:
        """Waits until all nodes see each other as up and the driver can use each of them."""
        servers = await self.servers()
        await self.manager.servers_see_each_other(servers)
        await wait_for_cql_and_get_hosts(self.manager.get_cql(), servers, time.time() + 120)

    async def set_balancing(self, enabled: bool) -> None:
        server = (await self.servers())[0]
        if enabled:
            await self.manager.api.enable_tablet_balancing(server.ip_addr)
        else:
            await self.manager.api.disable_tablet_balancing(server.ip_addr)
        self.balancing_enabled = enabled


OpFn = Callable[[FuzzContext, random.Random], Awaitable[Optional[str]]]


@dataclass(frozen=True)
class Operation:
    name: str
    weight: float
    # The disruption window that the operation runs in, or None when no client
    # request may fail because of it.
    window: Optional[str]
    fn: OpFn


async def restart_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server = rng.choice(await ctx.servers())
    await ctx.manager.server_stop_gracefully(server.server_id)
    await ctx.manager.server_start(server.server_id)
    await ctx.wait_healthy()
    return None


async def kill_restart_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server = rng.choice(await ctx.servers())
    await ctx.manager.server_stop(server.server_id, convict=False)
    await ctx.manager.server_start(server.server_id)
    await ctx.wait_healthy()
    return None


async def pause_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server = rng.choice(await ctx.servers())
    await ctx.manager.server_pause(server.server_id)
    try:
        await asyncio.sleep(rng.uniform(2, 20))
    finally:
        await ctx.manager.server_unpause(server.server_id)
    await ctx.wait_healthy()
    return None


async def add_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    sizes = await ctx.rack_sizes()
    rack = min(sizes, key=lambda r: (sizes[r], r))
    if sizes[rack] >= ctx.profile.cluster.max_nodes_per_rack:
        return f"each rack has {ctx.profile.cluster.max_nodes_per_rack} nodes"
    server = await ctx.manager.server_add(config=ctx.node_config, cmdline=ctx.node_cmdline,
                                          property_file={"dc": "dc1", "rack": rack})
    ctx.rack_of[server.server_id] = rack
    await ctx.wait_healthy()
    return None


async def _pick_leaving_node(ctx: FuzzContext, rng: random.Random) -> tuple[Optional[ServerInfo], Optional[str]]:
    sizes = await ctx.rack_sizes()
    candidates = [s for s in await ctx.servers() if sizes[ctx.rack_of[s.server_id]] > 1]
    if not candidates:
        return None, "no rack has a second node"
    return rng.choice(candidates), None


async def decommission_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server, reason = await _pick_leaving_node(ctx, rng)
    if server is None:
        return reason
    # The drain moves the tablets of the node with the balancer.
    if not ctx.balancing_enabled:
        await ctx.set_balancing(True)
    await ctx.manager.decommission_node(server.server_id)
    del ctx.rack_of[server.server_id]
    await ctx.wait_healthy()
    return None


async def remove_node(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server, reason = await _pick_leaving_node(ctx, rng)
    if server is None:
        return reason
    if not ctx.balancing_enabled:
        await ctx.set_balancing(True)
    initiator = rng.choice([s for s in await ctx.servers() if s.server_id != server.server_id])
    await ctx.manager.server_stop_gracefully(server.server_id)
    await ctx.manager.remove_node(initiator.server_id, server.server_id)
    del ctx.rack_of[server.server_id]
    await ctx.wait_healthy()
    return None


async def _host_ids(ctx: FuzzContext) -> dict[str, ServerInfo]:
    return {str(await ctx.manager.get_host_id(s.server_id)): s for s in await ctx.servers()}


async def _move(ctx: FuzzContext, rng: random.Random, intranode: bool) -> Optional[str]:
    if reason := ctx.too_full():
        return reason
    wl = rng.choice(ctx.workloads)
    servers = await ctx.servers()
    tablets = await get_all_tablet_replicas(ctx.manager, servers[0], wl.ks, wl.table_name)
    tablet = rng.choice(tablets)
    hosts = await _host_ids(ctx)
    smp = ctx.profile.cluster.smp
    if intranode:
        if smp < 2:
            return "smp is 1"
        src_host, src_shard = rng.choice(tablet.replicas)
        dst_host, dst_shard = src_host, rng.choice([sh for sh in range(smp) if sh != src_shard])
    else:
        replica_hosts = {str(h) for h, _ in tablet.replicas}

        # A rack-valid keyspace keeps one replica in each rack, so the new
        # replica must be in the rack of the replica that it replaces.
        def targets(src_host) -> list[str]:
            rack = ctx.rack_of[hosts[str(src_host)].server_id]
            return [h for h, s in hosts.items() if ctx.rack_of[s.server_id] == rack and h not in replica_hosts]

        sources = [(h, sh) for h, sh in tablet.replicas if targets(h)]
        if not sources:
            return "no rack of the tablet has a node that is not its replica"
        src_host, src_shard = rng.choice(sources)
        dst_host, dst_shard = rng.choice(targets(src_host)), rng.randrange(smp)
    logger.info("Moving tablet %s of %s from %s:%d to %s:%d", tablet.last_token, wl.table_name,
                src_host, src_shard, dst_host, dst_shard)
    try:
        await ctx.manager.api.move_tablet(servers[0].ip_addr, wl.ks, wl.table_name, src_host, src_shard,
                                          dst_host, dst_shard, tablet.last_token)
    except Exception as e:
        if STALE_MOVE.search(str(e)):
            return f"the tablet changed before the move: {e}"
        raise
    return None


async def move_tablet(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    return await _move(ctx, rng, intranode=False)


async def intranode_move(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    return await _move(ctx, rng, intranode=True)


async def split_merge(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    if reason := ctx.too_full():
        return reason
    wl = rng.choice(ctx.workloads)
    # Not 1: one tablet of a table puts all its data on one shard of a node,
    # which can fill the shard at the planned utilization.
    count = rng.choice([2, 4, 8, 16, 32])
    before = await get_tablet_count(ctx.manager, (await ctx.servers())[0], wl.ks, wl.table_name)
    logger.info("Setting min_tablet_count of %s to %d (%d tablets now)", wl.table_name, count, before)
    await ctx.manager.get_cql().run_async(f"ALTER TABLE {wl.fqtn} WITH tablets = {{'min_tablet_count': {count}}}")
    return None


async def balancing_toggle(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    await ctx.set_balancing(not ctx.balancing_enabled)
    logger.info("Tablet balancing is %s", "on" if ctx.balancing_enabled else "off")
    return None


async def flush(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server = rng.choice(await ctx.servers())
    if rng.random() < 0.5:
        await ctx.manager.api.keyspace_flush(server.ip_addr, ctx.ks)
    else:
        await ctx.manager.api.flush_all_keyspaces(server.ip_addr)
    return None


async def compact(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    server = rng.choice(await ctx.servers())
    await ctx.manager.api.logstor_compaction(server.ip_addr, major=rng.random() < 0.5)
    return None


async def alter_caching(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    wl = rng.choice(ctx.workloads)
    enabled = rng.choice(["true", "false"])
    await ctx.manager.get_cql().run_async(f"ALTER TABLE {wl.fqtn} WITH caching = {{'enabled': '{enabled}'}}")
    return None


async def repair(ctx: FuzzContext, rng: random.Random) -> Optional[str]:
    wl = rng.choice(ctx.workloads)
    server = rng.choice(await ctx.servers())
    await ctx.manager.api.repair(server.ip_addr, wl.ks, wl.table_name)
    return None


OPERATIONS: dict[str, Operation] = {op.name: op for op in (
    Operation("restart_node", 3, NODE_WINDOW, restart_node),
    Operation("kill_restart_node", 3, NODE_WINDOW, kill_restart_node),
    Operation("pause_node", 2, NODE_WINDOW, pause_node),
    Operation("add_node", 1, NODE_WINDOW, add_node),
    Operation("decommission_node", 1, NODE_WINDOW, decommission_node),
    Operation("remove_node", 1, NODE_WINDOW, remove_node),
    Operation("move_tablet", 4, TABLETS_WINDOW, move_tablet),
    Operation("intranode_move", 3, TABLETS_WINDOW, intranode_move),
    Operation("split_merge", 2, TABLETS_WINDOW, split_merge),
    Operation("balancing_toggle", 1, TABLETS_WINDOW, balancing_toggle),
    Operation("flush", 2, None, flush),
    Operation("compact", 2, None, compact),
    Operation("alter_caching", 1, None, alter_caching),
    Operation("repair", 1, TABLETS_WINDOW, repair),
)}
