#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Conditional statements (LWT) on logstor tables, on a three-node cluster with
replication factor 3 and tablets.
"""

import logging
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}"
TABLE_DEF = "(id int PRIMARY KEY, a text, b text, version int) WITH storage_engine = 'logstor'"


async def start_cluster(manager: ScyllaClusterManager):
    cfg = {
        'experimental_features': ['logstor'],
        'logstor_disk_size_in_mb': 8,
        'logstor_file_size_in_mb': 4,
        'logstor_sparse_files': True,
    }
    servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc1')
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    return servers, cql


def serial_read(table: str, key: int) -> SimpleStatement:
    return SimpleStatement(f"SELECT id, a, b, version FROM {table} WHERE id = {key}", consistency_level=ConsistencyLevel.SERIAL)


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
