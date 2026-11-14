#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The checks that the logstor fuzz test runs after the workload stops.

The reads during the run use QUORUM, so they do not see a value that one
replica lost: the other replicas hide the loss.  The checks here also read each
replica on its own, with ``MUTATION_FRAGMENTS()``, which reads only the data
of the node that gets the query.  The checks run in this order:

  1. A scan at CL=ALL checks each key with the model.  The scan also does a
     read repair of each key whose replicas differ.
  2. A dump of each replica checks that a quorum of the replicas of each key
     holds the newest acknowledged write or a newer one, that a node that is
     not a replica does not hold the key, and that the cache of a node holds
     the same row as its log.
  3. A repair of each table, and a dump that requires all replicas of each key
     to be equal.  Logstor has no repair yet, so the known issue
     ``logstor-repair`` skips this step, and the replicas of a key can differ.
  4. A restart of all nodes, a dump that requires each replica to hold the
     same state as before the restart, and a second scan at CL=ALL.  A
     difference on one replica is a loss in the recovery of logstor.

When the first dump of a replica has no record of a key that another replica
holds, a single-key dump of that replica tells whether the full dump missed a
record that the node has.  The failure details of a later difference show the
result, with the segment of each record.
"""

from __future__ import annotations

import bisect
import json
import logging
import struct
import time
from dataclasses import dataclass
from typing import Optional, Sequence

from cassandra import ConsistencyLevel
from cassandra.murmur3 import murmur3
from cassandra.query import SimpleStatement

from test.cluster.logstor_fuzz.known_issues import KnownIssues
from test.cluster.logstor_fuzz.model import Kind, Status
from test.cluster.logstor_fuzz.operations import start_node
from test.cluster.logstor_fuzz.results import FuzzFailure
from test.cluster.logstor_fuzz.workload import CorruptValue, LogstorWorkload
from test.cluster.util import reconnect_driver
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# The number of failed checks that a report lists.
MAX_REPORTED = 20

# The number of keys that the single-key dumps check after the first dump.
MAX_PROBES = 10


@dataclass(frozen=True)
class KeyState:
    """The state of one key on one replica: a row or a partition tombstone."""

    kind: Kind
    seq: int
    timestamp: int


def token_of(pk: int) -> int:
    return murmur3(struct.pack(">q", pk))


class CheckErrors:
    def __init__(self, what: str) -> None:
        self.what = what
        self.errors: list[str] = []
        # The facts that explain the errors, for the failure details.
        self.notes: list[str] = []

    def add(self, message: str) -> None:
        if len(self.errors) < MAX_REPORTED:
            logger.error("%s: %s", self.what, message)
        self.errors.append(message)

    def note(self, message: str) -> None:
        logger.info("%s: %s", self.what, message)
        self.notes.append(message)

    def raise_if_any(self) -> None:
        if self.errors:
            shown = "\n  ".join(self.errors[:MAX_REPORTED])
            more = f"\n  ... and {len(self.errors) - MAX_REPORTED} more" if len(self.errors) > MAX_REPORTED else ""
            notes = "".join(f"\n  note: {n}" for n in self.notes[:MAX_REPORTED])
            raise FuzzFailure("final_check", self.errors[0],
                              f"{self.what}: {len(self.errors)} errors:\n  {shown}{more}{notes}")


class ReplicaDump(dict):
    """The state of each key that one node holds, with the log source of each record."""

    def __init__(self) -> None:
        super().__init__()
        # The ``logstor-log:`` mutation source of the record of each key.
        self.sources: dict[int, str] = {}


def _state_of_source(wl: LogstorWorkload, pk: int, rows: list, errors: CheckErrors, where: str) -> Optional[KeyState]:
    """Returns the state that the fragments of one mutation source give for a key."""
    tombstone_ts = None
    row = None
    for r in rows:
        if r.mutation_fragment_kind == "partition start":
            tombstone = json.loads(r.metadata).get("tombstone") or {}
            tombstone_ts = tombstone.get("timestamp")
        elif r.mutation_fragment_kind == "clustering row":
            column = json.loads(r.metadata).get("columns", {}).get("v")
            if column and column.get("is_live"):
                row = (column["timestamp"], bytes.fromhex(json.loads(r.value)["v"]))
    if row is not None and (tombstone_ts is None or row[0] > tombstone_ts):
        ts, value = row
        try:
            seq = wl.decode_row(pk, value, ts)
        except CorruptValue as e:
            errors.add(f"{where} key={pk}: {e}")
            return None
        return KeyState(Kind.WRITE, seq, ts)
    if tombstone_ts is not None:
        return KeyState(Kind.DELETE, tombstone_ts - wl.ts_base, tombstone_ts)
    return None


_DUMP_COLUMNS = "pk, mutation_source, mutation_fragment_kind, metadata, value"


async def dump_replica(cql, host, wl: LogstorWorkload, errors: CheckErrors, pk: Optional[int] = None) -> ReplicaDump:
    """Returns the state of each key that one node holds for the table of ``wl``.

    With ``pk``, the dump reads only that key.  Also checks that the cached
    state of a key equals the state in the log.
    """
    where_key = "" if pk is None else f" WHERE pk = {pk}"
    stmt = SimpleStatement(f"SELECT {_DUMP_COLUMNS} FROM MUTATION_FRAGMENTS({wl.fqtn}){where_key}", fetch_size=1000)
    rows = await cql.run_async(stmt, host=host)
    by_key: dict[int, dict[str, list]] = {}
    for r in rows:
        by_key.setdefault(r.pk, {}).setdefault(r.mutation_source, []).append(r)

    states = ReplicaDump()
    for pk, sources in by_key.items():
        where = f"{host.address} {wl.table_name}"
        log_sources = [s for s in sources if s.startswith("logstor-log:")]
        if len(log_sources) != 1:
            errors.add(f"{where} key={pk}: expected one log source, found {sorted(sources)}")
            continue
        state = _state_of_source(wl, pk, sources[log_sources[0]], errors, where)
        if "logstor-cache" in sources:
            cached = _state_of_source(wl, pk, sources["logstor-cache"], errors, f"{where} cache")
            if cached != state:
                errors.add(f"{where} key={pk}: the cache holds {cached}, the log holds {state}")
        if state is not None:
            states[pk] = state
            states.sources[pk] = log_sources[0]
    return states


async def key_replicas(manager, server, wl: LogstorWorkload) -> dict[int, set[str]]:
    """Returns the IPs of the replicas of each key of ``wl``, from the tablet map."""
    tablets = sorted(await get_all_tablet_replicas(manager, server, wl.ks, wl.table_name), key=lambda t: t.last_token)
    last_tokens = [t.last_token for t in tablets]
    ip_of_host = {}
    for s in await manager.running_servers():
        ip_of_host[str(await manager.get_host_id(s.server_id))] = s.ip_addr
    replicas = {}
    for pk in range(wl.num_keys):
        tablet = tablets[bisect.bisect_left(last_tokens, token_of(pk))]
        replicas[pk] = {ip_of_host[str(host)] for host, _ in tablet.replicas}
    return replicas


async def dump_all(manager, cql, wl: LogstorWorkload, errors: CheckErrors) -> dict[str, ReplicaDump]:
    """Returns the states that each running node holds for the table of ``wl``."""
    dumps = {}
    for server in await manager.running_servers():
        host = cql.cluster.metadata.get_host(server.ip_addr)
        dumps[server.ip_addr] = await dump_replica(cql, host, wl, errors)
    logger.info("Dump of %s: %s", wl.fqtn, ", ".join(f"{ip} holds {len(d)} keys" for ip, d in dumps.items()))
    return dumps


def check_entry_exists(wl: LogstorWorkload, pk: int, state: KeyState, errors: CheckErrors, where: str) -> None:
    """Checks that a replica state comes from a write or a delete that the workload sent."""
    h = wl.model.history(pk)
    if state.seq < h.pruned_below:
        return
    entry = h.find(state.seq)
    if entry is None or entry.kind is not state.kind or entry.status is Status.FAIL:
        errors.add(f"{where} key={pk}: holds {state}, which no {state.kind.value} that may be applied has "
                   f"(entry {entry})")


def check_quorum(wl: LogstorWorkload, dumps: dict[str, dict[int, KeyState]], replicas: dict[int, set[str]],
                 write_quorum: int, errors: CheckErrors) -> None:
    """Checks that a quorum of the replicas of each key holds the newest acknowledged write or a newer one.

    Also checks that a node that is not a replica of a key does not hold it.
    """
    for pk in range(wl.num_keys):
        for ip, dump in dumps.items():
            if pk in dump:
                if ip not in replicas[pk]:
                    errors.add(f"{ip} {wl.table_name} key={pk}: holds {dump[pk]}, but it is not a replica "
                               f"(replicas {sorted(replicas[pk])})")
                check_entry_exists(wl, pk, dump[pk], errors, f"{ip} {wl.table_name}")
        acked = wl.model.newest_acked(pk)
        if acked is None:
            continue
        holders = [ip for ip in replicas[pk] if ip in dumps and pk in dumps[ip] and dumps[ip][pk].seq >= acked.seq]
        if len(holders) < write_quorum:
            states = {ip: dumps.get(ip, {}).get(pk) for ip in sorted(replicas[pk])}
            errors.add(f"{wl.table_name} key={pk}: {len(holders)} replicas hold seq {acked.seq} "
                       f"({acked.kind.value}) or newer, expected {write_quorum}: {states}")


def check_replicas_equal(wl: LogstorWorkload, dumps: dict[str, dict[int, KeyState]], replicas: dict[int, set[str]],
                         errors: CheckErrors) -> None:
    """Checks that all replicas of each key hold the same state, and that the state passes the final check."""
    now = time.monotonic_ns()
    for pk in range(wl.num_keys):
        states = {ip: dumps.get(ip, {}).get(pk) for ip in sorted(replicas[pk])}
        distinct = set(states.values())
        if len(distinct) != 1:
            errors.add(f"{wl.table_name} key={pk}: the replicas differ after the repair: {states}")
            continue
        state = distinct.pop()
        if state is None:
            seq = None
        else:
            seq = state.seq if state.kind is Kind.WRITE else None
        error = wl.model.check_final(pk, now, seq)
        if error:
            errors.add(f"{wl.table_name} after the repair: {error}")


async def probe_missing_keys(cql, wl: LogstorWorkload, dumps: dict[str, ReplicaDump],
                             replicas: dict[int, set[str]], errors: CheckErrors) -> dict[tuple[str, int], str]:
    """Dumps again, one key at a time, the keys that a replica has no record of and another replica holds.

    A replica that was down during the writes of a key can have no record of
    it, so a missing key is not an error here.  The single-key dump tells
    whether the full dump missed a record that the node has.  When it did, a
    second full dump of the node tells whether the miss repeats.  Returns a
    note for each key that it checked, by the IP and the key.
    """
    probes: dict[tuple[str, int], str] = {}
    candidates = [(ip, pk) for pk in range(wl.num_keys) for ip in sorted(replicas[pk])
                  if ip in dumps and pk not in dumps[ip]
                  and any(pk in dumps[other] for other in replicas[pk] if other in dumps)]
    redumped: dict[str, ReplicaDump] = {}
    for ip, pk in candidates[:MAX_PROBES]:
        host = cql.cluster.metadata.get_host(ip)
        single = await dump_replica(cql, host, wl, errors, pk)
        if pk not in single:
            probes[(ip, pk)] = "a single-key dump at the first dump found no record either"
        else:
            if ip not in redumped:
                redumped[ip] = await dump_replica(cql, host, wl, errors)
            again = "has it" if pk in redumped[ip] else "misses it again"
            probes[(ip, pk)] = (f"the full dump missed {single[pk]} in {single.sources[pk]}, which a single-key "
                                f"dump at the first dump found; a second full dump {again}")
        errors.note(f"{ip} {wl.table_name} key={pk}: {probes[(ip, pk)]}")
    if len(candidates) > MAX_PROBES:
        errors.note(f"{wl.table_name}: {len(candidates)} keys are missing on a replica, the first {MAX_PROBES} "
                    f"were checked")
    return probes


def check_same_dumps(wl: LogstorWorkload, before: dict[str, ReplicaDump], after: dict[str, ReplicaDump],
                     errors: CheckErrors, probes: dict[tuple[str, int], str]) -> None:
    """Checks that each node holds the same state of each key after the restart as before it."""
    for ip in sorted(before):
        if ip not in after:
            errors.add(f"{ip} {wl.table_name}: no dump after the restart")
            continue
        for pk in sorted(set(before[ip]) | set(after[ip])):
            if before[ip].get(pk) != after[ip].get(pk):
                errors.add(f"{ip} {wl.table_name} key={pk}: holds {after[ip].get(pk)} after the restart, "
                           f"{before[ip].get(pk)} before it")
                errors.note(f"{ip} {wl.table_name} key={pk}: the record before the restart is in "
                            f"{before[ip].sources.get(pk)}, after it in {after[ip].sources.get(pk)}; "
                            f"{probes.get((ip, pk), 'no single-key dump')}")


async def scan_all(cql, wl: LogstorWorkload, errors: CheckErrors, what: str) -> None:
    """Reads all keys at CL=ALL and checks each one with the model."""
    stmt = SimpleStatement(f"SELECT pk, v, writetime(v) FROM {wl.fqtn}", consistency_level=ConsistencyLevel.ALL,
                           fetch_size=1000)
    rows = await cql.run_async(stmt)
    now = time.monotonic_ns()
    found: dict[int, int] = {}
    for r in rows:
        if not 0 <= r.pk < wl.num_keys:
            errors.add(f"{what} {wl.table_name}: key {r.pk} is outside the key space")
            continue
        try:
            found[r.pk] = wl.decode_row(r.pk, r.v, r.writetime_v)
        except CorruptValue as e:
            errors.add(f"{what} {wl.table_name} key={r.pk}: {e}")
            found[r.pk] = -1
    for pk in range(wl.num_keys):
        if found.get(pk) == -1:
            continue
        error = wl.model.check_final(pk, now, found.get(pk))
        if error:
            errors.add(f"{what} {wl.table_name}: {error}")
    logger.info("%s: scanned %s at CL=ALL, %d rows", what, wl.fqtn, len(rows))


async def final_checks(manager, cql, workloads: Sequence[LogstorWorkload], write_quorum: int,
                       replica_check: bool, final_restart: str, known: KnownIssues, node_start_s: float):
    """Runs the checks of the module docstring, and returns the driver session to use after them.

    Raises AssertionError with the list of the failed checks.
    """
    servers = await manager.running_servers()
    for s in servers:
        await manager.api.disable_tablet_balancing(s.ip_addr)

    errors = CheckErrors("final checks")
    for wl in workloads:
        await scan_all(cql, wl, errors, "before the restart")

    last_dumps = {}
    probes: dict[str, dict[tuple[str, int], str]] = {}
    if replica_check:
        for wl in workloads:
            replicas = await key_replicas(manager, servers[0], wl)
            dumps = await dump_all(manager, cql, wl, errors)
            probes[wl.table_name] = await probe_missing_keys(cql, wl, dumps, replicas, errors)
            check_quorum(wl, dumps, replicas, write_quorum, errors)
            if known.enabled("check:repair_and_compare_replicas"):
                await manager.api.repair(servers[0].ip_addr, wl.ks, wl.table_name)
                dumps = await dump_all(manager, cql, wl, errors)
                check_replicas_equal(wl, dumps, replicas, errors)
            last_dumps[wl.table_name] = dumps
            logger.info("Replica checks of %s: %d errors so far", wl.fqtn, len(errors.errors))
    errors.raise_if_any()

    if final_restart == "full":
        logger.info("Restarting all nodes")
        for s in servers:
            await manager.server_stop_gracefully(s.server_id)
        for s in servers:
            await start_node(manager, s, node_start_s)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + node_start_s)
        errors = CheckErrors("checks after the restart")
        for wl in workloads:
            if replica_check:
                dumps = await dump_all(manager, cql, wl, errors)
                check_same_dumps(wl, last_dumps[wl.table_name], dumps, errors, probes[wl.table_name])
            await scan_all(cql, wl, errors, "after the restart")
        errors.raise_if_any()
    return cql
