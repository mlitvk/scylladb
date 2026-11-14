#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The read and write workload of the logstor fuzz test.

Each table of the run has its own :class:`LogstorWorkload`.  The workload
builds on :class:`~test.cluster.strong_consistency.register_workload.RegisterWorkload`,
which gives it the history for the Porcupine checker, the exception policies,
the disruption windows and the random streams of the seed.  The workload adds
what logstor needs:

  * the writes are whole-row INSERTs and partition DELETEs, because logstor
    replaces the row on each write and rejects a write of only some cells;
  * each write has the timestamp ``ts_base + seq``, so that the write with the
    higher seq wins on each replica;
  * each key belongs to one writer, so that one key has one write in flight
    at a time;
  * a value carries a header (table, key, seq) and a payload that the reader
    makes again from the seed, so a read finds a corrupted value without a
    copy of the payload in memory;
  * each read is checked by :class:`~test.cluster.logstor_fuzz.model.Model`
    when it returns.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import struct
import time
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import accumulate
from typing import Awaitable, Callable, Optional, Sequence

from cassandra import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import PreparedStatement

from test.cluster.logstor_fuzz.model import Kind, Model, Status
from test.cluster.logstor_fuzz.params import ValueSize, WorkloadParams
from test.cluster.strong_consistency.outcomes import Outcome
from test.cluster.strong_consistency.register_workload import RegisterWorkload
from test.pylib.rest_client import ScyllaMetricsLine

logger = logging.getLogger(__name__)

TABLE_SCHEMA = "(pk bigint PRIMARY KEY, v blob) WITH storage_engine = 'logstor'"

_MAGIC = b"LSFZ"
_HEADER = struct.Struct(">4sHqqI")

_STATUS_OF_OUTCOME = {
    Outcome.OK: Status.OK,
    Outcome.FAIL: Status.FAIL,
    Outcome.UNKNOWN: Status.UNKNOWN,
}


class CorruptValue(Exception):
    pass


def make_payload(seed: int, table: int, pk: int, seq: int, size: int) -> bytes:
    return random.Random(f"{seed}/{table}/{pk}/{seq}").randbytes(size)


def encode_value(seed: int, table: int, pk: int, seq: int, size: int) -> bytes:
    return _HEADER.pack(_MAGIC, table, pk, seq, size) + make_payload(seed, table, pk, seq, size)


def decode_value(seed: int, table: int, pk: int, value: bytes) -> int:
    """Returns the seq of ``value``, and checks the value against the key and the seed.

    Raises CorruptValue when the header or the payload is wrong.
    """
    if len(value) < _HEADER.size:
        raise CorruptValue(f"value of {len(value)} bytes is shorter than the header")
    magic, v_table, v_pk, seq, size = _HEADER.unpack_from(value)
    if magic != _MAGIC:
        raise CorruptValue(f"bad magic {magic!r}")
    if (v_table, v_pk) != (table, pk):
        raise CorruptValue(f"value of table {v_table} key {v_pk} (seq {seq}) is in table {table} key {pk}")
    if len(value) != _HEADER.size + size:
        raise CorruptValue(f"seq {seq}: payload is {len(value) - _HEADER.size} bytes, the header says {size}")
    if value[_HEADER.size:] != make_payload(seed, table, pk, seq, size):
        raise CorruptValue(f"seq {seq}: payload differs from the payload that was written")
    return seq


class KeyChooser:
    """Picks keys from a list with the distribution of the parameters.

    With "zipf", the rank of a key comes from a permutation of the whole key
    space that the seed gives, so that the hot keys are spread over the
    tablets, and the weight of rank r is 1 / (r + 1) ** s.
    """

    def __init__(self, keys: Sequence[int], params: WorkloadParams, rank: dict[int, int]) -> None:
        self.keys = list(keys)
        self.cum_weights: Optional[list[float]] = None
        if params.key_dist == "zipf":
            self.cum_weights = list(accumulate(1.0 / (rank[k] + 1) ** params.zipf_s for k in self.keys))

    def pick(self, rng: random.Random) -> int:
        if self.cum_weights is None:
            return rng.choice(self.keys)
        return rng.choices(self.keys, cum_weights=self.cum_weights)[0]


@dataclass
class LogstorWorkload(RegisterWorkload):
    """The workload of one logstor table."""

    params: WorkloadParams = field(default_factory=WorkloadParams)
    # The index of the table in the run; the value header carries it.
    table_index: int = 0
    # The write timestamp is ts_base + seq.  All tables of a run use the same base.
    ts_base: int = 0
    model: Model = field(default_factory=Model)
    # The reads that the model did not accept, as messages.
    violations: list[str] = field(default_factory=list)
    # The time of the last acknowledged write, for the progress report.
    last_write_ok_ns: int = 0

    insert_stmt: Optional[PreparedStatement] = field(default=None, repr=False)
    delete_stmt: Optional[PreparedStatement] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.num_keys = self.params.keys
        self.num_writers = self.params.writers
        self.num_readers = self.params.readers
        self.dml_pause_s = self.params.op_pause
        self._value_size = ValueSize(self.params.value_size)
        rank = list(range(self.num_keys))
        self.rng_for("key-rank").shuffle(rank)
        self._rank = {k: r for r, k in enumerate(rank)}

    async def create_table(self, cql) -> None:
        await cql.run_async(f"CREATE TABLE {self.fqtn} {TABLE_SCHEMA}")
        self.prepare(cql)

    def prepare(self, cql) -> None:
        """Prepares the statements, each with FallthroughRetryPolicy.

        See RegisterWorkload.prepare() for why a retry would make the history
        wrong.
        """
        write_cl = getattr(ConsistencyLevel, self.params.write_cl)
        read_cl = getattr(ConsistencyLevel, self.params.read_cl)
        self.insert_stmt = cql.prepare(f"INSERT INTO {self.fqtn} (pk, v) VALUES (?, ?) USING TIMESTAMP ?")
        self.delete_stmt = cql.prepare(f"DELETE FROM {self.fqtn} USING TIMESTAMP ? WHERE pk = ?")
        self.read_stmt = cql.prepare(f"SELECT v, writetime(v) FROM {self.fqtn} WHERE pk = ?")
        self.write_stmt = self.insert_stmt
        for stmt in (self.insert_stmt, self.delete_stmt):
            stmt.retry_policy = FallthroughRetryPolicy()
            stmt.consistency_level = write_cl
        self.read_stmt.retry_policy = FallthroughRetryPolicy()
        self.read_stmt.consistency_level = read_cl

    def owned_keys(self, writer_id: int) -> list[int]:
        return [k for k in range(self.num_keys) if k % self.num_writers == writer_id]

    def chooser(self, keys: Sequence[int]) -> KeyChooser:
        return KeyChooser(keys, self.params, self._rank)

    def decode_row(self, pk: int, value: bytes, writetime: int) -> int:
        """Returns the seq of a row that a read returned, and checks it.

        Raises CorruptValue for a wrong value, or for a write time that is not
        the write time of the seq in the value.
        """
        seq = decode_value(self.seed, self.table_index, pk, value)
        if writetime != self.ts_base + seq:
            raise CorruptValue(f"seq {seq}: write time {writetime} is not {self.ts_base + seq}")
        return seq

    def violation(self, message: str) -> None:
        message = f"{self.fqtn}: {message}"
        logger.error("Check failed: %s", message)
        self.violations.append(message)
        # Stop the run at the first violation, so that the node logs end near it.
        self.stop_event.set()

    def progress_line(self) -> str:
        return f"{self.table_name}: {self.stats()} violations={len(self.violations)}"


async def writer_task(wl: LogstorWorkload, cql, writer_id: int) -> None:
    rng = wl.rng_for(f"writer/{writer_id}")
    chooser = wl.chooser(wl.owned_keys(writer_id))

    while not wl.stop_event.is_set():
        pk = chooser.pick(rng)
        seq = wl.next_write_value()
        ts = wl.ts_base + seq
        if rng.random() < wl.params.delete_fraction:
            kind = Kind.DELETE
            bound = wl.delete_stmt.bind([ts, pk])
            history_value = 0
        else:
            kind = Kind.WRITE
            value = encode_value(wl.seed, wl.table_index, pk, seq, wl._value_size.sample(rng))
            bound = wl.insert_stmt.bind([pk, value, ts])
            history_value = seq

        op_id, t_call_ns = wl.history.record_call(writer_id, "write", pk, history_value)
        entry = wl.model.write_started(pk, seq, kind, t_call_ns)
        try:
            await cql.run_async(bound)
        except Exception as exc:
            outcome = wl.record_failure(exc, op_id=op_id, client_id=writer_id, op="write", key=pk,
                                        value=history_value, t_call_ns=t_call_ns)
            wl.model.write_finished(pk, entry, _STATUS_OF_OUTCOME[outcome], time.monotonic_ns())
        else:
            t_return_ns = time.monotonic_ns()
            wl.history.record_return(op_id, writer_id, "write", pk, history_value, Outcome.OK, t_return_ns)
            wl.model.write_finished(pk, entry, Status.OK, t_return_ns)
            wl.last_write_ok_ns = t_return_ns

        # Paced on the failure path too, as in register_workload.writer_task().
        await asyncio.sleep(rng.uniform(*wl.dml_pause_s))


async def preload(wl: LogstorWorkload, cql) -> None:
    """Writes each key of ``wl`` once, with ``preload_concurrency`` requests in flight.

    Each request slot is a client of its own in the history, with one request
    in flight at a time, as the Porcupine history needs.  The writes go into
    the model like the writes of the run.
    """
    keys = iter(range(wl.num_keys))
    rng = wl.rng_for("preload")
    first_client = wl.reset_client_id + 1

    async def slot(client_id: int) -> None:
        for pk in keys:
            seq = wl.next_write_value()
            value = encode_value(wl.seed, wl.table_index, pk, seq, wl._value_size.sample(rng))
            op_id, t_call_ns = wl.history.record_call(client_id, "write", pk, seq)
            entry = wl.model.write_started(pk, seq, Kind.WRITE, t_call_ns)
            try:
                await cql.run_async(wl.insert_stmt.bind([pk, value, wl.ts_base + seq]))
            except Exception as exc:
                outcome = wl.record_failure(exc, op_id=op_id, client_id=client_id, op="write", key=pk,
                                            value=seq, t_call_ns=t_call_ns)
                wl.model.write_finished(pk, entry, _STATUS_OF_OUTCOME[outcome], time.monotonic_ns())
            else:
                t_return_ns = time.monotonic_ns()
                wl.history.record_return(op_id, client_id, "write", pk, seq, Outcome.OK, t_return_ns)
                wl.model.write_finished(pk, entry, Status.OK, t_return_ns)

    start = time.monotonic()
    await asyncio.gather(*(slot(first_client + i) for i in range(wl.params.preload_concurrency)))
    logger.info("Preloaded %d keys of %s in %.1fs", wl.num_keys, wl.fqtn, time.monotonic() - start)


async def reader_task(wl: LogstorWorkload, cql, reader_id: int) -> None:
    rng = wl.rng_for(f"reader/{reader_id}")
    client_id = wl.num_writers + reader_id
    chooser = wl.chooser(range(wl.num_keys))

    while not wl.stop_event.is_set():
        pk = chooser.pick(rng)
        check = rng.random() < wl.params.read_sample
        op_id, t_call_ns = wl.history.record_call(client_id, "read", pk)
        read_id = wl.model.read_started(t_call_ns)
        try:
            rows = await cql.run_async(wl.read_stmt.bind([pk]))
        except Exception as exc:
            wl.model.read_finished(read_id)
            wl.record_failure(exc, op_id=op_id, client_id=client_id, op="read", key=pk, value=0,
                              t_call_ns=t_call_ns)
        else:
            t_return_ns = time.monotonic_ns()
            seq = None
            try:
                if rows:
                    seq = wl.decode_row(pk, rows[0].v, rows[0].writetime_v)
            except CorruptValue as e:
                wl.violation(f"key={pk} read=[{t_call_ns}..{t_return_ns}]: {e}")
            else:
                if check:
                    error = wl.model.check_read(pk, t_call_ns, t_return_ns, seq)
                    if error:
                        wl.violation(error)
            wl.model.read_finished(read_id)
            wl.history.record_return(op_id, client_id, "read", pk, seq or 0, Outcome.OK, t_return_ns)

        await asyncio.sleep(rng.uniform(*wl.dml_pause_s))


@dataclass
class SegmentPeaks:
    """The use of the logstor store of each shard during a run: the last sample and the peaks."""

    # The size of the store of a shard, in bytes.
    store_bytes: float = 1.0
    # (node ip, shard) -> the highest segments_in_use seen.
    in_use: dict[tuple[str, str], float] = field(default_factory=lambda: defaultdict(float))
    # (node ip, shard) -> the lowest free_segments seen.
    free: dict[tuple[str, str], float] = field(default_factory=dict)
    # (node ip, shard) -> live_record_bytes / store_bytes, at the last sample of the node.
    utilization: dict[tuple[str, str], float] = field(default_factory=dict)
    # The highest utilization of a shard seen.
    peak_utilization: float = 0.0

    def add(self, ip: str, lines: list[str]) -> None:
        for line in lines:
            m = ScyllaMetricsLine.from_string(line)
            if m is None:
                continue
            key = (ip, m.labels.get("shard", "?"))
            if m.name == "scylla_logstor_sm_segments_in_use":
                self.in_use[key] = max(self.in_use[key], m.value)
            elif m.name == "scylla_logstor_sm_free_segments":
                self.free[key] = min(self.free.get(key, m.value), m.value)
            elif m.name == "scylla_logstor_sm_live_record_bytes":
                self.utilization[key] = m.value / self.store_bytes
                self.peak_utilization = max(self.peak_utilization, self.utilization[key])

    def max_utilization(self) -> float:
        """Returns the highest utilization of a shard at the last samples."""
        return max(self.utilization.values(), default=0.0)

    def summary(self) -> str:
        if not self.in_use:
            return "no samples"
        worst = max(self.in_use, key=self.in_use.get)
        values = sorted(self.utilization.values())
        return (f"utilization now min/max {values[0]:.2f}/{values[-1]:.2f} peak {self.peak_utilization:.2f}, "
                f"peak segments_in_use={self.in_use[worst]:.0f} on {worst[0]} shard {worst[1]}, "
                f"lowest free_segments={min(self.free.values(), default=float('nan')):.0f}")

    async def sample(self, manager) -> None:
        for server in await manager.running_servers():
            try:
                metrics = await manager.metrics.query(server.ip_addr)
            except Exception as e:
                logger.info("Metrics of %s are not available: %s", server.ip_addr, e)
                continue
            self.add(server.ip_addr, metrics.lines_by_prefix("scylla_logstor_sm_"))


async def monitor_task(manager, workloads: Sequence[LogstorWorkload], stop: asyncio.Event,
                       peaks: SegmentPeaks, interval_s: float = 5) -> None:
    """Logs the progress of the run and samples the segment use of each shard."""
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), interval_s)
        await peaks.sample(manager)
        logger.info("Progress: %s | %s", " | ".join(wl.progress_line() for wl in workloads), peaks.summary())


async def run_workloads(manager, cql, workloads: Sequence[LogstorWorkload], duration_s: float,
                        peaks: SegmentPeaks,
                        extra_tasks: Sequence[tuple[str, Callable[[], Awaitable[None]]]] = ()) -> list[BaseException]:
    """Runs the clients of all workloads for ``duration_s``, or until a check fails.

    ``extra_tasks`` run beside the clients, and each one must return soon after
    the shared stop_event is set.  All workloads must share one stop_event.
    Returns the exceptions that the tasks raised, as run_workload() of
    register_workload does.
    """
    stop = workloads[0].stop_event
    assert all(wl.stop_event is stop for wl in workloads)
    for wl in workloads:
        logger.info("Workload %s: seed=%d writers=%d readers=%d keys=%d", wl.fqtn, wl.seed,
                    wl.num_writers, wl.num_readers, wl.num_keys)

    task_errors: list[BaseException] = []
    try:
        async with asyncio.TaskGroup() as tg:
            async def stop_timer():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), duration_s)
                stop.set()

            tg.create_task(stop_timer(), name="stop-timer")
            tg.create_task(monitor_task(manager, workloads, stop, peaks), name="monitor")
            for name, factory in extra_tasks:
                tg.create_task(factory(), name=name)
            for wl in workloads:
                for i in range(wl.num_writers):
                    tg.create_task(writer_task(wl, cql, i), name=f"{wl.table_name}-writer-{i}")
                for i in range(wl.num_readers):
                    tg.create_task(reader_task(wl, cql, i), name=f"{wl.table_name}-reader-{i}")
    except* Exception as eg:
        task_errors = list(eg.exceptions)
        logger.error("Task(s) failed: %s", task_errors)

    for wl in workloads:
        wl.totals = wl.stats()
        logger.info("Workload %s done: %s", wl.fqtn, wl.progress_line())
    return task_errors
