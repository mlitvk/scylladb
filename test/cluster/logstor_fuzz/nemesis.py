#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The nemesis of the logstor fuzz test: a schedule of cluster operations, and the loop that runs it.

The schedule comes from the seed before the run starts, so a second run with
the same seed picks the same operations in the same order, with the same
random choices inside each operation.  The cluster state is not the same in a
second run, so an operation can skip in one run and run in the other, and the
event log records each skip.

Two more tasks watch the run for hangs:

  * the watchdog fails the run when a table has no acknowledged write for
    ``stall_timeout_s``, which the operations never cause, because at most one
    replica of a tablet is down at a time;
  * the nemesis fails the run when an operation runs longer than
    ``op_timeout_s``.

Before either fails the run, it logs the diagnostics of the cluster.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from test.cluster.logstor_fuzz.operations import OPERATIONS, FuzzContext
from test.cluster.logstor_fuzz.results import FuzzFailure
from test.pylib.rest_client import ScyllaMetricsLine

logger = logging.getLogger(__name__)

# The logstor metrics that the diagnostics show for each shard.
DIAGNOSTIC_METRICS = (
    "scylla_logstor_sm_segments_in_use",
    "scylla_logstor_sm_free_segments",
    "scylla_logstor_sm_segment_pool_size",
    "scylla_logstor_queued_write_count",
)


class HangError(FuzzFailure):
    def __init__(self, reason: str) -> None:
        super().__init__("hang", reason)


@dataclass(frozen=True)
class ScheduledOp:
    index: int
    name: str
    # The pause before the operation, in seconds.
    pause_s: float


class EventLog:
    """Writes the events of a run as JSON lines, and to the test log."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.start_ns = time.monotonic_ns()
        self.recent: list[dict] = []
        self._file = open(path, "w")

    def add(self, event: str, **fields) -> None:
        record = {"t": round((time.monotonic_ns() - self.start_ns) / 1e9, 3), "event": event, **fields}
        self._file.write(json.dumps(record, default=str) + "\n")
        self._file.flush()
        self.recent = (self.recent + [record])[-50:]
        logger.info("Event: %s", record)

    def close(self) -> None:
        self._file.close()


def enabled_operations(ctx: FuzzContext) -> list[str]:
    """Returns the operations of the profile that no known issue disables."""
    names = []
    for name in ctx.profile.nemesis.ops:
        if name not in OPERATIONS:
            raise ValueError(f"unknown operation {name!r}; the operations are {sorted(OPERATIONS)}")
        if ctx.known.enabled(f"op:{name}"):
            names.append(name)
    return names


def make_schedule(seed: int, names: Sequence[str], duration_s: float,
                  interval: tuple[float, float]) -> list[ScheduledOp]:
    """Returns the operations of a run, with a pause before each one.

    The pauses add up to ``duration_s`` at most.  The operations take time too,
    so a run does not reach the end of the schedule.
    """
    if not names:
        return []
    rng = random.Random(f"{seed}/nemesis/schedule")
    weights = [OPERATIONS[n].weight for n in names]
    schedule = []
    total = 0.0
    while True:
        pause = rng.uniform(*interval)
        total += pause
        if total >= duration_s:
            return schedule
        schedule.append(ScheduledOp(len(schedule), rng.choices(names, weights)[0], round(pause, 3)))


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Waits ``seconds`` and returns False, or returns True as soon as ``stop`` is set."""
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)
    return stop.is_set()


async def diagnostics(ctx: FuzzContext, events: EventLog, reason: str) -> None:
    """Logs the state of the cluster for a hang."""
    logger.error("Hang: %s. Diagnostics follow.", reason)
    for server in await ctx.manager.running_servers():
        try:
            metrics = await ctx.manager.metrics.query(server.ip_addr)
            values = {}
            for line in metrics.lines_by_prefix("scylla_logstor_"):
                m = ScyllaMetricsLine.from_string(line)
                if m is not None and m.name in DIAGNOSTIC_METRICS:
                    values.setdefault(m.name, {})[m.labels.get("shard", "?")] = m.value
            logger.error("Logstor metrics of %s: %s", server.ip_addr, values)
        except Exception as e:
            logger.error("Metrics of %s are not available: %s", server.ip_addr, e)
    try:
        rows = await ctx.manager.get_cql().run_async(
            "SELECT table_id, last_token, stage, transition, replicas, new_replicas FROM system.tablets")
        moving = [r for r in rows if r.stage is not None]
        logger.error("Tablets in transition (%d): %s", len(moving), moving[:20])
    except Exception as e:
        logger.error("system.tablets is not available: %s", e)
    for record in events.recent[-20:]:
        logger.error("Recent event: %s", record)
    events.add("hang", reason=reason)


async def nemesis_task(ctx: FuzzContext, schedule: Sequence[ScheduledOp], stop: asyncio.Event,
                       events: EventLog) -> None:
    timeout = ctx.profile.nemesis.op_timeout_s
    for item in schedule:
        if await _wait_or_stop(stop, item.pause_s):
            return
        op = OPERATIONS[item.name]
        rng = random.Random(f"{ctx.profile.seed}/nemesis/op/{item.index}")
        events.add("op_start", index=item.index, op=op.name)
        start = time.monotonic()
        async with ctx.window(op.window):
            try:
                skipped = await asyncio.wait_for(op.fn(ctx, rng), timeout)
            except asyncio.TimeoutError:
                reason = f"operation {item.index} {op.name} did not finish in {timeout:.0f}s"
                await diagnostics(ctx, events, reason)
                raise HangError(reason) from None
            except Exception as e:
                events.add("op_error", index=item.index, op=op.name, error=repr(e))
                stop.set()
                raise FuzzFailure("op_error", f"{op.name}: {type(e).__name__}: {e}") from e
        if skipped is not None:
            events.add("op_skip", index=item.index, op=op.name, reason=skipped)
        else:
            events.add("op_end", index=item.index, op=op.name, duration_s=round(time.monotonic() - start, 1))


async def watchdog_task(ctx: FuzzContext, stop: asyncio.Event, events: EventLog) -> None:
    """Fails the run when a table has no acknowledged write for ``stall_timeout_s``."""
    stall_ns = ctx.profile.nemesis.stall_timeout_s * 1e9
    start_ns = time.monotonic_ns()
    while not await _wait_or_stop(stop, 5):
        now = time.monotonic_ns()
        for wl in ctx.workloads:
            last = max(wl.last_write_ok_ns, start_ns)
            if now - last > stall_ns:
                reason = (f"no write of {wl.table_name} was acknowledged for "
                          f"{(now - last) / 1e9:.0f}s")
                await diagnostics(ctx, events, reason)
                stop.set()
                raise HangError(reason)
