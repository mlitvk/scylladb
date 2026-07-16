#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time
from typing import Any, cast

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts


async def _create_repair_cluster(manager: ManagerClient, *, smp: int = 1) -> tuple[list[Any], Any]:
    cmdline = ['--logger-log-level', 'logstor=debug', '--hinted-handoff-enabled', '0']
    if smp != 1:
        cmdline.append(f'--smp={smp}')
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(2, cmdline=cmdline, config=cfg, auto_rack_dc="dc1")
    cql = cast(Any, manager.get_cql())
    return servers, cql


async def test_repair_missing_partition(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 10)", consistency_level=ConsistencyLevel.ONE), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)

        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=node2)
        assert len(rows) == 0
        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1])
        assert len(rows) == 1
        assert rows[0].pk == 1
        assert rows[0].v == 10


async def test_repair_same_timestamp_conflict(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 100) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 200) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node2)

        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        before = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        assert before[0][0].v == 100
        assert before[1][0].v == 200

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        after = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        for rows in after:
            assert len(rows) == 1
            assert rows[0].pk == 1
            assert rows[0].v == 200


async def test_repair_same_timestamp_tombstone_wins(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 100) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"DELETE FROM {ks}.test USING TIMESTAMP 1000 WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=node2)

        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        before = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        assert len(before[0]) == 1
        assert len(before[1]) == 0

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        after = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        assert len(after[0]) == 0
        assert len(after[1]) == 0


async def test_repair_multishard_routing(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager, smp=2)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        for pk in range(16):
            await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk + 100})", consistency_level=ConsistencyLevel.ONE), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)

        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        for pk in range(16, 32):
            await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk + 100})", consistency_level=ConsistencyLevel.ONE), host=node2)

        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        for host in hosts:
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test", host=host)
            assert len(rows) == 32
            assert sorted((row.pk, row.v) for row in rows) == [(pk, pk + 100) for pk in range(32)]


async def test_repair_mixed_outcomes(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        for pk, v in [(1, 10), (2, 100), (3, 300), (4, 400), (5, 500)]:
            await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {v}) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (2, 200) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node2)
        await cql.run_async(SimpleStatement(f"DELETE FROM {ks}.test USING TIMESTAMP 1000 WHERE pk = 3", consistency_level=ConsistencyLevel.ONE), host=node2)
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (4, 450) USING TIMESTAMP 900", consistency_level=ConsistencyLevel.ONE), host=node2)
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (6, 600) USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ONE), host=node2)

        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        expected = {
            1: 10,
            2: 200,
            4: 400,
            5: 500,
            6: 600,
        }
        for host in hosts:
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test", host=host)
            assert sorted((row.pk, row.v) for row in rows) == sorted(expected.items())
            rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test WHERE pk = 3", consistency_level=ConsistencyLevel.ONE), host=host)
            assert len(rows) == 0


async def test_repair_mutation_merge_same_timestamp(manager: ManagerClient):
    servers, cql = await _create_repair_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, a text, b text) WITH storage_engine = 'logstor'")

        node1 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        node2 = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.driver_connect(servers[0])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(
            f"INSERT INTO {ks}.test (pk, a, b) VALUES (1, 'a', 'b') USING TIMESTAMP 1000",
            consistency_level=ConsistencyLevel.ONE,
        ), host=node1)

        await manager.server_start(servers[1].server_id, wait_others=1)
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.driver_connect(servers[1])
        cql = cast(Any, manager.get_cql())
        await cql.run_async(SimpleStatement(
            f"INSERT INTO {ks}.test (pk, a, b) VALUES (1, 'b', 'a') USING TIMESTAMP 1000",
            consistency_level=ConsistencyLevel.ONE,
        ), host=node2)

        await manager.server_start(servers[0].server_id, wait_others=1)

        await manager.driver_connect()
        cql = cast(Any, manager.get_cql())

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        before = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT a, b FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT a, b FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        assert (before[0][0].a, before[0][0].b) == ('a', 'b')
        assert (before[1][0].a, before[1][0].b) == ('b', 'a')

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        after = await asyncio.gather(
            cql.run_async(SimpleStatement(f"SELECT a, b FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[0]),
            cql.run_async(SimpleStatement(f"SELECT a, b FROM {ks}.test WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=hosts[1]),
        )
        for rows in after:
            assert len(rows) == 1
            assert rows[0].a == 'b'
            assert rows[0].b == 'b'
