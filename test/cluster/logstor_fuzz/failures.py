#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The injected failures of the logstor fuzz test, and the loop that injects them.

A failure is an error injection point of Scylla, which the loop enables on
one node for a while.  The kinds of failure:

  * ``delay``: a point of ``replica/logstor/injected_delay.hh`` that waits
    ``delay_ms`` (see docs/dev/logstor.md).  It changes the order of the steps
    that run at the same time, so it finds races;
  * ``error``: a point that throws, so a write, a compaction or a separator
    flush fails on that node;
  * ``pause``: a ``wait_for_message`` point.  The loop disables it before its
    60 s timeout, which releases the waiters;
  * ``fatal``: a point that makes the node stop serving (the disk error
    handler isolates it).  The loop restarts the node at the end.

A failure keeps the invariant of the operations (see ``operations``): a fatal
failure takes the node lock, as a node operation does, so that at most one
node is down at a time.  The other failures do not lose an acknowledged write
on a quorum: a failed write on one replica is not acknowledged by it.

Each failure runs inside the disruption window ``failure`` (or ``node`` for a
fatal one), and the exception policy accepts the timeouts and the write
failures of the window.  The injections exist in debug and dev builds only.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from test.cluster.logstor_fuzz.operations import FAILURE_WINDOW, NODE_WINDOW, FuzzContext, start_node
from test.cluster.logstor_fuzz.results import FuzzFailure, HangError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Failure:
    name: str
    # The error injection point.
    injection: str
    kind: str
    weight: float
    # The time that the point stays enabled, in seconds.
    duration: tuple[float, float] = (5.0, 20.0)
    # The parameters of the injection, from a random stream.
    params: Callable[[random.Random], dict] = lambda rng: {}
    one_shot: bool = False

    @property
    def window(self) -> str:
        return NODE_WINDOW if self.kind == "fatal" else FAILURE_WINDOW


def _delay(low_ms: int, high_ms: int) -> Callable[[random.Random], dict]:
    return lambda rng: {"delay_ms": rng.randint(low_ms, high_ms)}


FAILURES: dict[str, Failure] = {f.name: f for f in (
    # Delays.  A delay on the write path slows the writes of the node, so it is short.
    Failure("delay_segment_write", "logstor_delay_segment_write", "delay", 3, params=_delay(5, 200)),
    Failure("delay_index_insert", "logstor_delay_index_insert", "delay", 3, params=_delay(5, 200)),
    Failure("delay_segment_read", "logstor_delay_segment_read", "delay", 3, params=_delay(5, 200)),
    Failure("delay_separator_flush", "logstor_delay_separator_flush", "delay", 3, params=_delay(50, 2000)),
    Failure("delay_compaction", "logstor_delay_compaction", "delay", 3, params=_delay(50, 2000)),
    Failure("delay_stream_snapshot", "logstor_delay_stream_snapshot", "delay", 2, params=_delay(500, 5000)),
    Failure("delay_stream_adopt", "logstor_delay_stream_adopt", "delay", 2, params=_delay(500, 5000)),
    # Errors.  A failed segment write retires its segment and fails the writes of its buffer on
    # this replica; the other replicas still make a quorum.
    Failure("fail_segment_write", "logstor_fail_segment_write", "error", 2, duration=(1.0, 5.0)),
    Failure("fail_compaction_index_update", "logstor_compaction_fail_index_update", "error", 2, one_shot=True),
    Failure("fail_compaction_after_rewrite", "logstor_compaction_fail_after_rewrite", "error", 2, one_shot=True),
    Failure("fail_separator_flush", "fail_flush_separator_buffer", "error", 2, one_shot=True),
    # Pauses.  The point waits for a message for at most 60 s; the loop disables it before.
    Failure("pause_before_compaction_free", "logstor_compaction_wait_before_remove_segments", "pause", 2,
            duration=(2.0, 30.0)),
    Failure("pause_separator_buffer_allocated", "logstor_separator_buffer_allocated", "pause", 2,
            duration=(2.0, 30.0)),
    # Fatal.  The disk error handler isolates the node.
    Failure("segment_write_io_error", "logstor_segment_write_io_error", "fatal", 1, one_shot=True,
            duration=(2.0, 10.0)),
)}

# The node log lines that the failures cause and that the teardown scan must accept.
ALLOWED_LOG_PATTERNS = (
    r"by injection",
    r"Shutting down communications due to I/O errors",
    r"Disk error",
    r"storage_io_error",
)


@dataclass(frozen=True)
class ScheduledFailure:
    index: int
    name: str
    pause_s: float


def make_failure_schedule(seed: int, names: Sequence[str], duration_s: float,
                          interval: tuple[float, float]) -> list[ScheduledFailure]:
    """Returns the failures of a run, with a pause before each one, as make_schedule() of nemesis does."""
    if not names:
        return []
    rng = random.Random(f"{seed}/failures/schedule")
    weights = [FAILURES[n].weight for n in names]
    schedule = []
    total = 0.0
    while True:
        pause = rng.uniform(*interval)
        total += pause
        if total >= duration_s:
            return schedule
        schedule.append(ScheduledFailure(len(schedule), rng.choices(names, weights)[0], round(pause, 3)))


def enabled_failures(ctx: FuzzContext) -> list[str]:
    """Returns the failures of the profile that no known issue disables."""
    names = []
    for name in ctx.profile.nemesis.failures:
        if name not in FAILURES:
            raise ValueError(f"unknown failure {name!r}; the failures are {sorted(FAILURES)}")
        if ctx.known.enabled(f"failure:{name}"):
            names.append(name)
    return names


async def _disable(ctx: FuzzContext, ip: str, injection: str) -> None:
    # The node can be gone (a decommission) or restarted (the injection is gone with it).
    try:
        await ctx.manager.api.disable_injection(ip, injection)
    except Exception as e:
        logger.info("Cannot disable %s on %s: %s", injection, ip, e)


async def inject(ctx: FuzzContext, failure: Failure, rng: random.Random, events) -> None:
    """Enables ``failure`` on a random node for its duration, then disables it or restarts the node."""
    server = rng.choice(await ctx.servers())
    params = failure.params(rng)
    duration = rng.uniform(*failure.duration)
    events.add("failure_start", failure=failure.name, node=server.ip_addr, params=params,
               duration_s=round(duration, 1))
    await ctx.manager.api.enable_injection(server.ip_addr, failure.injection, failure.one_shot, params)
    try:
        await asyncio.sleep(duration)
    finally:
        await _disable(ctx, server.ip_addr, failure.injection)
    if failure.kind == "fatal":
        # The node may have isolated itself; a kill and a start bring it back in any case.
        await ctx.manager.server_stop(server.server_id, convict=False)
        await start_node(ctx.manager, server, ctx.limits.node_start_s)
        await ctx.wait_healthy()


async def failure_task(ctx: FuzzContext, schedule: Sequence[ScheduledFailure], stop: asyncio.Event,
                       events) -> None:
    from test.cluster.logstor_fuzz.nemesis import _wait_or_stop, diagnostics

    for item in schedule:
        if await _wait_or_stop(stop, item.pause_s):
            return
        failure = FAILURES[item.name]
        rng = random.Random(f"{ctx.profile.seed}/failures/{item.index}")
        start = time.monotonic()
        # The longest time that the point stays enabled, and then a quick request
        # (disable) or a restart of the node.
        timeout = failure.duration[1] + ctx.limits.op_s("node" if failure.kind == "fatal" else "quick")
        lock = ctx.node_lock if failure.kind == "fatal" else contextlib.nullcontext()
        async with lock, ctx.window(failure.window):
            try:
                await asyncio.wait_for(inject(ctx, failure, rng, events), timeout)
            except asyncio.TimeoutError:
                reason = f"failure {item.index} {failure.name} did not finish in {timeout:.0f}s"
                await diagnostics(ctx, events, reason)
                raise HangError(reason) from None
            except HangError as e:
                await diagnostics(ctx, events, f"failure {item.index} {failure.name}: {e.message}")
                raise
            except Exception as e:
                events.add("failure_error", index=item.index, failure=failure.name, error=repr(e))
                stop.set()
                raise FuzzFailure("op_error", f"failure {failure.name}: {type(e).__name__}: {e}") from e
        events.add("failure_end", index=item.index, failure=failure.name,
                   duration_s=round(time.monotonic() - start, 1))
