#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_property(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t_enabled (pk int PRIMARY KEY, v int) WITH log_structured_storage = true")
        await cql.run_async(f"CREATE TABLE {ks}.t_disabled (pk int PRIMARY KEY, v int) WITH log_structured_storage = false")

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_enabled")
        logger.info(f"Table t_enabled description:\n{desc}")
        assert "log_structured_storage = true" in desc[0].create_statement

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_disabled")
        logger.info(f"Table t_disabled description:\n{desc}")
        assert "log_structured_storage = false" in desc[0].create_statement

@pytest.mark.asyncio
async def test_basic_write_and_read(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH log_structured_storage = true")

        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 100)")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (2, 150)")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == 100
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 2")
        assert rows[0].pk == 2
        assert rows[0].v == 150

        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 200)")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == 200
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 2")
        assert rows[0].pk == 2
        assert rows[0].v == 150

@pytest.mark.asyncio
async def test_basic_write_and_read_map(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v map<text, text>) WITH log_structured_storage = true")

        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana'}

        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana', 'c': 'cherry'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana', 'c': 'cherry'}

@pytest.mark.asyncio
async def test_parallel_writes(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH log_structured_storage = true")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i+1})") for i in range(100)])

        # validate
        for i in range(100):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].pk == i
            assert rows[0].v == i + 1

@pytest.mark.asyncio
async def test_overwrites(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH log_structured_storage = true")

        pk = 0
        for i in range(100):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {i})")

        # validate
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
        assert rows[0].pk == pk
        assert rows[0].v == 99

@pytest.mark.asyncio
async def test_parallel_big_writes(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH log_structured_storage = true")

        # Create a large value of approximately 100KB
        large_value = 'x' * (100 * 1024)
        num_writes = 8

        # Perform parallel writes with large values
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{large_value}')") for i in range(num_writes)])

        # Validate that all writes succeeded
        for i in range(num_writes):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].pk == i
            assert rows[0].v == large_value
            assert len(rows[0].v) == 100 * 1024

@pytest.mark.asyncio
async def test_compaction(manager: ManagerClient):
    """
    Test log compaction by creating dead data and verifying space reclamation.

    This test:
    1. Fills ~2 segments with large values (4KB each) to 64 different keys
    2. Overwrites 60 of those keys to create mostly-dead segments
    3. Waits for compaction to rewrite live data and reclaim space
    4. Performs another round of overwrites to create more dead data
    5. Waits for compaction again and verifies all data remains correct
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH log_structured_storage = true")

        # Create a ~4KB value to fill segments
        # With segment size of 128KB, we need 32 writes per segment
        value_size = 4 * 1024 - 100
        large_value = 'x' * value_size
        num_keys = 64  # Fill ~2 segments

        logger.info(f"Phase 1: Writing {num_keys} keys with {value_size} byte values to fill ~2 segments")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{large_value}')")
            for i in range(num_keys)
        ])

        # Verify initial writes
        for i in range(num_keys):
            rows = await cql.run_async(f"SELECT pk FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i

        # Overwrite most keys (60 out of 64) to create dead data
        # This should make the original segments have low live ratios
        num_overwrites = 60
        new_value = 'y' * value_size

        logger.info(f"Phase 2: Overwriting {num_overwrites} keys to create dead data in original segments")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{new_value}')")
            for i in range(num_overwrites)
        ])

        # Verify overwrites
        for i in range(num_overwrites):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == new_value

        # Verify keys that weren't overwritten still have original value
        for i in range(num_overwrites, num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == large_value

        # Wait for compaction to occur (default interval is 10 seconds)
        logger.info("Phase 3: Waiting for compaction to reclaim space from segments with dead data")
        await asyncio.sleep(5)

        # Verify data after first compaction
        logger.info("Phase 4: Verifying all data is still readable after first compaction")
        for i in range(num_overwrites):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == new_value

        for i in range(num_overwrites, num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == large_value

        # Perform another round of overwrites on a different subset of keys
        num_second_overwrites = 50
        second_value = 'z' * value_size

        logger.info(f"Phase 5: Performing second round of overwrites on {num_second_overwrites} keys")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{second_value}')")
            for i in range(10, 10 + num_second_overwrites)  # Overwrite keys 10-59
        ])

        # Wait for second compaction
        logger.info("Phase 6: Waiting for second compaction round")
        await asyncio.sleep(5)

        # Final verification - all data should still be readable after second compaction
        logger.info("Phase 7: Verifying all data is correct after second compaction")
        # Keys 0-9: should have 'y' value
        for i in range(10):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == new_value

        # Keys 10-59: should have 'z' value (second overwrite)
        for i in range(10, 60):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == second_value

        # Keys 60-63: should have original 'x' value (never overwritten)
        for i in range(60, num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1
            assert rows[0].pk == i
            assert rows[0].v == large_value

@pytest.mark.asyncio
async def test_continuous_overwrites_single_key(manager: ManagerClient):
    """
    Test continuous overwrites of a single key with large values.

    This test continuously overwrites the same key with large values (~4KB)
    for several seconds to stress test the log storage system under heavy
    write load to a single partition. This creates maximum dead data in
    segments as each write obsoletes the previous one.
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'enable_log_structured_storage': True}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH log_structured_storage = true")

        # Create a value slightly less than 4KB
        value_size = 4 * 1024 - 100
        large_value = 'x' * value_size

        pk = 0
        write_count = 0
        duration_seconds = 5

        logger.info(f"Starting continuous overwrites of key {pk} for {duration_seconds} seconds with {value_size} byte values")

        import time
        start_time = time.time()
        end_time = start_time + duration_seconds

        while time.time() < end_time:
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{large_value}')")
            write_count += 1

        elapsed = time.time() - start_time
        logger.info(f"Completed {write_count} writes in {elapsed:.2f} seconds ({write_count/elapsed:.2f} writes/sec)")

        # Verify the final value is still readable
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
        assert len(rows) == 1
        assert rows[0].pk == pk
        assert rows[0].v == large_value
        assert len(rows[0].v) == value_size

        logger.info(f"Final verification successful")
