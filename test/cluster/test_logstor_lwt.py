#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Conditional statements (LWT) on logstor tables, on a three-node cluster with
replication factor 3 and tablets.
"""

import asyncio
import json
import logging
import random
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}"
TABLE_DEF = "(id int PRIMARY KEY, a text, b text, version int) WITH storage_engine = 'logstor'"


async def start_cluster(manager: ScyllaClusterManager):
    cfg = {
        'experimental_features': ['logstor'],
        'logstor_disk_size_in_mb': 8,
        'logstor_file_size_in_mb': 4,
        'logstor_sparse_files': True,
        # A replica that is down across a round must stay behind until the test looks at it.
        'hinted_handoff_enabled': False,
    }
    servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc1')
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    return servers, cql


def read_at(table: str, key: int, cl) -> SimpleStatement:
    return SimpleStatement(f"SELECT id, a, b, version FROM {table} WHERE id = {key}", consistency_level=cl)


def serial_read(table: str, key: int) -> SimpleStatement:
    return read_at(table, key, ConsistencyLevel.SERIAL)


def as_tuples(rows):
    return [(r.id, r.a, r.b, r.version) for r in rows]


def host_of(hosts, server):
    return next(h for h in hosts if h.address == server.ip_addr)


async def read_local(cql, hosts, server, table: str, key: int):
    """The row as one node holds it: a read at ONE through that node, which is a replica."""
    rows = await cql.run_async(read_at(table, key, ConsistencyLevel.ONE), host=host_of(hosts, server))
    return as_tuples(rows)


async def stop_and_wait(manager, cql, servers, server):
    """Stops server and waits until the others see it down, so the next rounds cannot reach it."""
    await manager.server_stop_gracefully(server.server_id)
    others = [s for s in servers if s.server_id != server.server_id]
    await asyncio.gather(*(manager.server_not_sees_other_server(s.ip_addr, server.ip_addr) for s in others))
    return await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)


async def restart_and_wait(manager, cql, servers, server):
    await manager.server_start(server.server_id)
    await manager.servers_see_each_other(servers)
    return await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)



async def test_update_condition_not_met(manager: ScyllaClusterManager):
    """
    A failed condition returns [applied] = false with the current values and
    leaves the row unchanged, and the next conditional round on the key still
    works. Paxos learns an empty mutation when a condition fails, so logstor
    must store an empty mutation as a no-op; before that fix the learn failed
    on every replica and no later round on the key could complete.
    """
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")

        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        assert rows[0].applied

        rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = 1 IF version = 2")
        assert not rows[0].applied
        assert rows[0].version == 1

        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert [(r.id, r.a, r.b, r.version) for r in rows] == [(1, 'a1', 'b1', 1)]

        rows = await cql.run_async(f"DELETE FROM {table} WHERE id = 1 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a3', 'b3', 3) IF NOT EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert [(r.id, r.a, r.b, r.version) for r in rows] == [(1, 'a3', 'b3', 3)]


async def test_serial_read(manager: ScyllaClusterManager):
    """
    A SELECT at SERIAL returns the row, and the next conditional round on the
    key still works. A SERIAL read runs a Paxos round that learns an empty
    mutation, the same no-op write as a failed condition.
    """
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")

        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        assert rows[0].applied

        rows = await cql.run_async(serial_read(table, 1))
        assert [(r.id, r.a, r.b, r.version) for r in rows] == [(1, 'a1', 'b1', 1)]
        rows = await cql.run_async(serial_read(table, 2))
        assert rows == []

        rows = await cql.run_async(f"DELETE FROM {table} WHERE id = 1 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(serial_read(table, 1))
        assert rows == []


async def test_insert_if_not_exists(manager: ScyllaClusterManager):
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a2', 'b2', 2) IF NOT EXISTS")
        assert not rows[0].applied
        assert (rows[0].a, rows[0].b, rows[0].version) == ('a1', 'b1', 1)
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, 'a1', 'b1', 1)]


async def test_update_keeps_other_columns(manager: ScyllaClusterManager):
    """
    A conditional UPDATE of one column keeps the other columns, with their write
    times: the coordinator reads the partition, applies the update and proposes
    the whole partition.
    """
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (100, 'a1', 'b1', 1) IF NOT EXISTS")
        before = (await cql.run_async(f"SELECT writetime(a) AS wa, writetime(b) AS wb FROM {table} WHERE id = 100"))[0]

        rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = 100 IF version = 1")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 100")
        assert as_tuples(rows) == [(100, 'a2', 'b1', 1)]
        after = (await cql.run_async(f"SELECT writetime(a) AS wa, writetime(b) AS wb FROM {table} WHERE id = 100"))[0]
        assert after.wb == before.wb
        assert after.wa > before.wa


async def test_update_creates_row(manager: ScyllaClusterManager):
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        rows = await cql.run_async(f"UPDATE {table} SET a = 'a1' WHERE id = 5 IF b = null")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 5")
        assert as_tuples(rows) == [(5, 'a1', None, None)]


async def test_update_with_ttl_creates_expiring_row(manager: ScyllaClusterManager):
    """
    A row that a conditional UPDATE with a TTL creates expires with its cells,
    as on an sstable table: the row marker logstor needs takes the TTL of the
    longest-living cell.
    """
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        rows = await cql.run_async(f"UPDATE {table} USING TTL 1 SET a = 'a1' WHERE id = 7 IF b = null")
        assert rows[0].applied

        async def row_gone():
            rows = await cql.run_async(f"SELECT id FROM {table} WHERE id = 7")
            return True if not rows else None
        await wait_for(row_gone, time.time() + 30)


async def test_delete_column(manager: ScyllaClusterManager):
    """
    A conditional DELETE of one column leaves a cell tombstone in the stored
    partition, and every later image carries it. With one node stopped across
    the delete and the next update, a read at ALL that includes the restarted
    node must not bring the deleted column back from its stale row.
    """
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        rows = await cql.run_async(f"DELETE a FROM {table} WHERE id = 1 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, None, 'b1', 1)]

        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (2, 'a1', 'b1', 1) IF NOT EXISTS")
        await stop_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(f"DELETE a FROM {table} WHERE id = 2 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"UPDATE {table} SET b = 'b2' WHERE id = 2 IF version = 1")
        assert rows[0].applied
        hosts = await restart_and_wait(manager, cql, servers, servers[0])
        assert await read_local(cql, hosts, servers[0], table, 2) == [(2, 'a1', 'b1', 1)]
        rows = await cql.run_async(read_at(table, 2, ConsistencyLevel.ALL))
        assert as_tuples(rows) == [(2, None, 'b2', 1)]


async def test_delete_partition(manager: ScyllaClusterManager):
    """
    A conditional DELETE of the partition stores the tombstone alone, and an
    INSERT after it keeps the tombstone under the new row, so a replica that
    was down across both does not bring the old cells back into a read at ALL.
    Hints are off, so the replica stays behind until that read.
    """
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        rows = await cql.run_async(f"DELETE FROM {table} WHERE id = 1 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a2', 'b2', 2) IF NOT EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, 'a2', 'b2', 2)]

        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (2, 'a1', 'b1', 1) IF NOT EXISTS")
        await stop_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(f"DELETE FROM {table} WHERE id = 2 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"INSERT INTO {table} (id, a) VALUES (2, 'a3') IF NOT EXISTS")
        assert rows[0].applied
        hosts = await restart_and_wait(manager, cql, servers, servers[0])
        assert await read_local(cql, hosts, servers[0], table, 2) == [(2, 'a1', 'b1', 1)]
        assert await read_local(cql, hosts, servers[1], table, 2) == [(2, 'a3', None, None)]
        # The record on a replica that took both rounds holds the tombstone under the new row.
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE id = 2",
                                                   consistency_level=ConsistencyLevel.ONE), host=host_of(hosts, servers[1]))
        starts = [r for r in rows if r.mutation_fragment_kind == 'partition start']
        assert starts and all(json.loads(r.metadata)['tombstone']['timestamp'] > 0 for r in starts)
        assert any(r.mutation_fragment_kind == 'clustering row' for r in rows)


@pytest.mark.xfail(strict=False, reason="the merge of the replica versions keeps the stale cells under the tombstone when the stale "
                   "version is merged last (host id order), and read repair then sends the up-to-date replicas those "
                   "cells alone, which a logstor replica rejects; whole-partition read repair is on branch logstor_repair1")
async def test_delete_partition_read_with_lagging_replica(manager: ScyllaClusterManager):
    """
    The merged read of test_delete_partition: the result keeps the stale cells
    of the restarted node shadowed, and read repair gives that node the whole
    new partition. It fails on master in about half the runs, see the reason.
    """
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (2, 'a1', 'b1', 1) IF NOT EXISTS")
        await stop_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(f"DELETE FROM {table} WHERE id = 2 IF EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"INSERT INTO {table} (id, a) VALUES (2, 'a3') IF NOT EXISTS")
        assert rows[0].applied
        hosts = await restart_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(read_at(table, 2, ConsistencyLevel.ALL))
        assert as_tuples(rows) == [(2, 'a3', None, None)]
        assert await read_local(cql, hosts, servers[0], table, 2) == [(2, 'a3', None, None)]


async def test_insert_ttl_kept(manager: ScyllaClusterManager):
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS USING TTL 3600")
        rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = 1 IF version = 1")
        assert rows[0].applied
        row = (await cql.run_async(f"SELECT ttl(a) AS ta, ttl(b) AS tb, a, b FROM {table} WHERE id = 1"))[0]
        assert row.ta is None
        assert 0 < row.tb <= 3600
        assert (row.a, row.b) == ('a2', 'b1')


async def test_conditional_batch(manager: ScyllaClusterManager):
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        rows = await cql.run_async(f"BEGIN BATCH "
                                   f"UPDATE {table} SET a = 'a9' WHERE id = 1 IF version = 1; "
                                   f"UPDATE {table} SET b = 'b9' WHERE id = 1; "
                                   f"APPLY BATCH")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, 'a9', 'b9', 1)]
        rows = await cql.run_async(f"BEGIN BATCH "
                                   f"UPDATE {table} SET a = 'a0' WHERE id = 1 IF version = 2; "
                                   f"UPDATE {table} SET b = 'b0' WHERE id = 1; "
                                   f"APPLY BATCH")
        assert not rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, 'a9', 'b9', 1)]


async def test_concurrent_increments(manager: ScyllaClusterManager):
    """
    N clients each increment the version with a conditional UPDATE until one
    of their attempts is applied. No increment is lost, and no increment lands
    that no client sent: an attempt that ended in an error (a timeout under
    contention) may or may not have been applied, so the final version is at
    least N and at most N plus the number of such attempts.
    """
    _, cql = await start_cluster(manager)
    n = 16
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 0) IF NOT EXISTS")
        unknown = 0

        async def increment(i: int):
            nonlocal unknown
            for _ in range(200):
                rows = await cql.run_async(serial_read(table, 1))
                v = rows[0].version
                try:
                    rows = await cql.run_async(f"UPDATE {table} SET version = {v + 1}, a = 'c{i}' WHERE id = 1 IF version = {v}")
                except Exception as e:
                    logger.info("client %d: outcome unknown: %s", i, e)
                    unknown += 1
                    await asyncio.sleep(random.uniform(0, 0.05))
                    continue
                if rows[0].applied:
                    return
            raise AssertionError(f"client {i} did not get its increment in")

        await asyncio.gather(*(increment(i) for i in range(n)))
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert n <= rows[0].version <= n + unknown
        assert rows[0].b == 'b1'


async def test_replica_missed_learn(manager: ScyllaClusterManager):
    """
    A replica that missed two rounds gets the most recent image from the next
    round's catch-up and then the new image: the late image has the older
    record timestamp, so the newer one is what the replica keeps.
    """
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        await stop_and_wait(manager, cql, servers, servers[0])
        for v in (2, 3):
            rows = await cql.run_async(f"UPDATE {table} SET a = 'a{v}', version = {v} WHERE id = 1 IF version = {v - 1}")
            assert rows[0].applied
        hosts = await restart_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(f"UPDATE {table} SET b = 'b4', version = 4 WHERE id = 1 IF version = 3")
        assert rows[0].applied
        rows = await cql.run_async(read_at(table, 1, ConsistencyLevel.ALL))
        assert as_tuples(rows) == [(1, 'a3', 'b4', 4)]
        assert await read_local(cql, hosts, servers[0], table, 1) == [(1, 'a3', 'b4', 4)]


async def test_lwt_after_plain_insert(manager: ScyllaClusterManager):
    _, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1)")
        rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = 1 IF version = 1")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT id, a, b, version FROM {table} WHERE id = 1")
        assert as_tuples(rows) == [(1, 'a2', 'b1', 1)]


async def test_restart_keeps_lwt_rows(manager: ScyllaClusterManager):
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES ({i}, 'a1', 'b1', 1) IF NOT EXISTS")
            rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = {i} IF version = 1")
            assert rows[0].applied
        for s in servers:
            await manager.server_stop_gracefully(s.server_id)
            await manager.server_start(s.server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        for i in range(10):
            rows = await cql.run_async(read_at(table, i, ConsistencyLevel.ALL))
            assert as_tuples(rows) == [(i, 'a2', 'b1', 1)]
            rows = await cql.run_async(f"UPDATE {table} SET b = 'b3' WHERE id = {i} IF version = 1")
            assert rows[0].applied
            rows = await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES ({i}, 'x', 'x', 9) IF NOT EXISTS")
            assert not rows[0].applied


@pytest.mark.xfail(strict=True, reason="read repair sends a logstor replica the per-replica diff, a partial row that replaces its record; "
                   "whole-partition read repair for logstor tables is on branch logstor_repair1")
async def test_plain_read_repair_keeps_untouched_cells(manager: ScyllaClusterManager):
    """
    A plain read at ALL that includes a replica that missed an LWT round
    repairs that replica. The image's untouched cells keep their old
    timestamps, so the diff for the lagging replica is partial, and a
    logstor replica stores the diff as its whole row.
    """
    servers, cql = await start_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} {TABLE_DEF}")
        await cql.run_async(f"INSERT INTO {table} (id, a, b, version) VALUES (1, 'a1', 'b1', 1) IF NOT EXISTS")
        await stop_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(f"UPDATE {table} SET a = 'a2' WHERE id = 1 IF version = 1")
        assert rows[0].applied
        hosts = await restart_and_wait(manager, cql, servers, servers[0])
        rows = await cql.run_async(read_at(table, 1, ConsistencyLevel.ALL))
        assert as_tuples(rows) == [(1, 'a2', 'b1', 1)]
        assert await read_local(cql, hosts, servers[0], table, 1) == [(1, 'a2', 'b1', 1)]
