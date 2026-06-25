# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import logging
import random
import re
import time

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_tablet_count, get_tablet_replicas
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

NO_REPLICA_RE = re.compile(r"has no replica on", re.IGNORECASE)
DST_REPLICA_RE = re.compile(r"has replica on", re.IGNORECASE)

MIN_TABLETS = 1
MAX_TABLETS = 8
RESIZE_TIMEOUT_S = 60
MIGRATE_ONE_TIMEOUT_S = 60
TEST_RUNTIME_S = 120
NUM_WORKERS = 12
KEYS_PER_WORKER = 50
MIGRATION_TARGET = 32
RESIZE_TARGET = 4
VALUE_PADDING = 2 * 1024
MIGRATION_SLEEP_RANGE_S = (1.0, 2.5)


def _err_code(e: Exception):
    return getattr(e, "code", None)


def _err_text(e: Exception):
    return getattr(e, "text", "") or str(e)


def _is_tablet_in_transition_http_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500 and "in transition" in _err_text(e).lower()


def _is_no_replica_on_src_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500 and NO_REPLICA_RE.search(_err_text(e)) is not None


def _is_dst_already_replica_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500 and DST_REPLICA_RE.search(_err_text(e)) is not None


def powers_of_two_in_range(lo: int, hi: int) -> list[int]:
    if lo > hi or hi < 1:
        return []
    lo = max(1, lo)
    start_e = (lo - 1).bit_length()
    end_e = hi.bit_length()
    return [1 << e for e in range(start_e, end_e + 1) if (1 << e) <= hi]


async def _move_tablet_with_retry(manager, src_server, ks, tbl,
                                  src_host_id, src_shard, dst_host_id, dst_shard, token,
                                  *, timeout_s=MIGRATE_ONE_TIMEOUT_S, base_sleep=0.1, max_sleep=2.0):
    deadline = time.time() + timeout_s
    sleep = base_sleep
    attempt = 1
    while True:
        try:
            logger.info(
                "Migration attempt %s for %s.%s token=%s src=%s/%s dst=%s/%s via %s",
                attempt, ks, tbl, token, src_host_id, src_shard, dst_host_id, dst_shard, src_server.ip_addr,
            )
            await manager.api.move_tablet(
                src_server.ip_addr, ks, tbl,
                src_host_id, src_shard, dst_host_id, dst_shard, token
            )
            logger.info(
                "Migration succeeded for %s.%s token=%s src=%s/%s dst=%s/%s on attempt %s",
                ks, tbl, token, src_host_id, src_shard, dst_host_id, dst_shard, attempt,
            )
            return
        except Exception as e:
            if _is_tablet_in_transition_http_error(e) and time.time() + sleep < deadline:
                logger.info(
                    "Migration retry for %s.%s token=%s due to transition: %s",
                    ks, tbl, token, e,
                )
                await asyncio.sleep(sleep + random.uniform(0, sleep))
                sleep = min(sleep * 1.7, max_sleep)
                attempt += 1
                continue
            raise


async def _writer_worker(cql, table: str, worker_id: int, keys: list[int], expected_values: dict[int, int], stop_event: asyncio.Event, started_event: asyncio.Event):
    select_stmt = cql.prepare(f"SELECT v FROM {table} WHERE pk = ?")
    insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, v, padding) VALUES (?, ?, ?)")
    padding = f"worker{worker_id}_" + ("x" * VALUE_PADDING)

    logger.info("Writer %s starting with %s keys: %s", worker_id, len(keys), keys)
    started_event.set()
    iterations = 0
    while not stop_event.is_set():
        pk = random.choice(keys)
        expected = expected_values[pk]

        rows = await cql.run_async(select_stmt, [pk])
        if expected == 0:
            assert len(rows) in (0, 1), f"Unexpected row count for new key {pk}: {len(rows)}"
            if rows:
                assert rows[0].v == expected, f"Key {pk} expected {expected} but read {rows[0].v}"
        else:
            assert len(rows) == 1, f"Key {pk} expected a row with value {expected}, got {len(rows)} rows"
            assert rows[0].v == expected, f"Key {pk} expected {expected} but read {rows[0].v}"

        new_value = expected + 1
        await cql.run_async(insert_stmt, [pk, new_value, padding])
        expected_values[pk] = new_value
        iterations += 1

        if iterations % 50 == 0:
            logger.info(
                "Writer %s completed %s iterations; last key=%s new_value=%s",
                worker_id, iterations, pk, new_value,
            )
            await asyncio.sleep(0)

    logger.info("Writer %s stopping after %s iterations", worker_id, iterations)


async def _run_random_resizes(stop_event: asyncio.Event, manager: ManagerClient, servers, cql, ks: str, table: str, target_steps: int):
    resize_count = 0
    split_count = 0
    merge_count = 0
    pow2_targets = powers_of_two_in_range(MIN_TABLETS, MAX_TABLETS)

    while not stop_event.is_set() and resize_count < target_steps:
        current_count = await get_tablet_count(manager, servers[0], ks, table)
        target_count = random.choice([cnt for cnt in pow2_targets if cnt != current_count])
        direction = "split" if target_count > current_count else "merge"

        logger.info(
            "Resize step %s/%s for %s.%s: current tablets=%s target=%s direction=%s",
            resize_count + 1, target_steps, ks, table, current_count, target_count, direction,
        )

        await cql.run_async(f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {target_count}}}")

        if direction == "split":
            async def resize_finished(tgt=target_count):
                count = await get_tablet_count(manager, servers[0], ks, table)
                return count if count >= tgt else None
        else:
            async def resize_finished(tgt=target_count):
                count = await get_tablet_count(manager, servers[0], ks, table)
                return count if count <= tgt else None

        await wait_for(resize_finished, time.time() + RESIZE_TIMEOUT_S)
        new_count = await get_tablet_count(manager, servers[0], ks, table)
        logger.info(
            "Resize step %s for %s.%s finished: direction=%s target=%s observed tablets=%s",
            resize_count + 1, ks, table, direction, target_count, new_count,
        )

        resize_count += 1
        if direction == "split":
            split_count += 1
        else:
            merge_count += 1

        await asyncio.sleep(random.uniform(0.2, 1.0))

    return {
        "steps_done": resize_count,
        "seen_split": split_count,
        "seen_merge": merge_count,
    }


async def _tablet_migration_ops(stop_event: asyncio.Event, manager: ManagerClient, servers, ks: str, table: str, tokens: list[int], *, server_properties):
    migration_count = 0
    intranode_ratio = 0.5

    server_id_to_rack = {s.server_id: prop["rack"] for s, prop in zip(servers, server_properties)}
    host_ids = await asyncio.gather(*(manager.get_host_id(s.server_id) for s in servers))
    server_id_to_host_id = {s.server_id: hid for s, hid in zip(servers, host_ids)}
    host_id_to_server = {hid: s for s, hid in zip(servers, host_ids)}

    while not stop_event.is_set() and migration_count < MIGRATION_TARGET:
        token = random.choice(tokens)
        replicas = await get_tablet_replicas(manager, servers[0], ks, table, token)
        src_host_id, src_shard = random.choice(replicas)
        src_server = host_id_to_server[src_host_id]

        if random.random() < intranode_ratio and len(replicas) == 1:
            dst_host_id = src_host_id
            dst_server = src_server
            dst_shard = 1 - src_shard
        else:
            replica_hids = {host_id for host_id, _shard in replicas}
            src_rack = server_id_to_rack[src_server.server_id]
            same_rack_candidates = [
                s for s in servers
                if server_id_to_rack[s.server_id] == src_rack and server_id_to_host_id[s.server_id] not in replica_hids
            ]

            if same_rack_candidates:
                dst_server = random.choice(same_rack_candidates)
                dst_host_id = server_id_to_host_id[dst_server.server_id]
                dst_shard = 0
            else:
                dst_host_id = src_host_id
                dst_server = src_server
                dst_shard = 1 - src_shard

        logger.info(
            "Migration op %s/%s for %s.%s token=%s replicas=%s chosen src=%s/%s dst=%s/%s",
            migration_count + 1, MIGRATION_TARGET, ks, table, token, replicas,
            src_host_id, src_shard, dst_host_id, dst_shard,
        )

        try:
            await _move_tablet_with_retry(
                manager, src_server, ks, table,
                src_host_id, src_shard, dst_host_id, dst_shard, token,
            )
            migration_count += 1
            logger.info(
                "Migration op %s/%s completed for %s.%s token=%s",
                migration_count, MIGRATION_TARGET, ks, table, token,
            )
        except Exception as e:
            if _is_tablet_in_transition_http_error(e) or _is_no_replica_on_src_error(e) or _is_dst_already_replica_error(e):
                logger.info(
                    "Migration op skipped for %s.%s token=%s due to transient topology state: %s",
                    ks, table, token, e,
                )
                continue
            raise

        sleep_s = random.uniform(*MIGRATION_SLEEP_RANGE_S)
        logger.info("Sleeping %.2fs before next migration op for %s.%s", sleep_s, ks, table)
        await asyncio.sleep(sleep_s)

    return migration_count


async def _periodic_flush(stop_event: asyncio.Event, manager: ManagerClient, servers):
    iteration = 0
    while not stop_event.is_set():
        iteration += 1
        logger.info("Starting flush cycle %s on %s servers", iteration, len(servers))
        await asyncio.gather(*(manager.api.logstor_flush(server.ip_addr) for server in servers))
        logger.info("Finished flush cycle %s", iteration)
        await asyncio.sleep(1.0)


async def _periodic_compaction(stop_event: asyncio.Event, manager: ManagerClient, servers):
    iteration = 0
    while not stop_event.is_set():
        iteration += 1
        logger.info("Starting forced compaction cycle %s on %s servers", iteration, len(servers))
        await asyncio.gather(*(manager.api.logstor_compaction(server.ip_addr) for server in servers))
        logger.info("Finished forced compaction cycle %s", iteration)
        await asyncio.sleep(2.0)


async def _verify_all_keys(cql, table: str, expected_values: dict[int, int]):
    select_stmt = cql.prepare(f"SELECT v FROM {table} WHERE pk = ?")
    logger.info("Starting final verification of %s keys in %s", len(expected_values), table)
    for pk, expected in expected_values.items():
        rows = await cql.run_async(select_stmt, [pk])
        assert len(rows) == 1, f"Key {pk} not found during final validation"
        assert rows[0].v == expected, f"Key {pk} expected {expected} but read {rows[0].v} during final validation"
    logger.info("Final verification of %s completed", table)


@pytest.mark.skip_mode(mode="debug", reason="debug mode is too slow for this stress test")
async def test_logstor_writes_during_tablet_migrations_splits_and_merges(manager: ManagerClient):
    """
    Stress logstor with overwrite-heavy single-writer keys while tablet migrations,
    splits, and merges run concurrently.

    The test validates every write by reading the current value first and ensures the
    final state matches the workers' expected values.
    """
    cfg = {
        "enable_tablets": True,
        "tablet_load_stats_refresh_interval_in_seconds": 1,
        "target-tablet-size-in-bytes": 1024 * 16,
        "experimental_features": ["logstor"],
        "logstor_disk_size_in_mb": 32,
        "logstor_file_size_in_mb": 8,
    }
    server_properties = [
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r2"},
    ]
    cmdline = [
        "--logger-log-level", "logstor=debug",
        "--logger-log-level", "stream_blob=debug",
        "--logger-log-level", "table=debug",
        "--logger-log-level", "load_balancer=debug",
        "--logger-log-level", "raft_topology=debug",
        "--smp=2",
    ]

    logger.info("Starting logstor stress test cluster with cfg=%s server_properties=%s", cfg, server_properties)
    servers = await manager.servers_add(4, config=cfg, property_file=server_properties, cmdline=cmdline)
    logger.info("Started servers: %s", [(s.server_id, s.ip_addr) for s in servers])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2} AND tablets = {'initial': 1}") as ks:
        logger.info("Created keyspace %s", ks)
        await cql.run_async(
            f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v bigint, padding text) WITH storage_engine = 'logstor' AND caching={{'enabled': false}}"
        )

        table = f"{ks}.test"
        logger.info("Created logstor table %s", table)
        token_stmt = cql.prepare(f"SELECT token(pk) AS tok FROM {table} WHERE pk = ?")
        seed_stmt = cql.prepare(f"INSERT INTO {table} (pk, v, padding) VALUES (?, ?, ?)")
        expected_values = {}
        tokens = []
        for worker_id in range(NUM_WORKERS):
            for key_idx in range(KEYS_PER_WORKER):
                pk = worker_id * 10_000 + key_idx
                expected_values[pk] = 0
                await cql.run_async(seed_stmt, [pk, 0, "seed"])
                token_rows = await cql.run_async(token_stmt, [pk])
                assert len(token_rows) == 1, f"Failed to fetch token for key {pk}"
                tokens.append(token_rows[0].tok)
        logger.info("Seeded %s keys and collected %s tokens", len(expected_values), len(tokens))

        stop_event = asyncio.Event()
        started_events = [asyncio.Event() for _ in range(NUM_WORKERS)]

        writer_tasks = []
        for worker_id in range(NUM_WORKERS):
            keys = [worker_id * 10_000 + key_idx for key_idx in range(KEYS_PER_WORKER)]
            writer_tasks.append(asyncio.create_task(
                _writer_worker(cql, table, worker_id, keys, expected_values, stop_event, started_events[worker_id])
            ))

        await asyncio.gather(*(event.wait() for event in started_events))
        logger.info("All %s writer workers started", NUM_WORKERS)

        migration_task = asyncio.create_task(
            _tablet_migration_ops(stop_event, manager, servers, ks, "test", tokens, server_properties=server_properties)
        )
        resize_task = asyncio.create_task(
            _run_random_resizes(stop_event, manager, servers, cql, ks, "test", RESIZE_TARGET)
        )
        flush_task = asyncio.create_task(_periodic_flush(stop_event, manager, servers))
        compaction_task = asyncio.create_task(_periodic_compaction(stop_event, manager, servers))
        logger.info(
            "Background tasks started: migrations target=%s, resizes target=%s, runtime=%ss",
            MIGRATION_TARGET, RESIZE_TARGET, TEST_RUNTIME_S,
        )

        deadline = time.time() + TEST_RUNTIME_S
        try:
            while time.time() < deadline:
                await asyncio.sleep(1.0)
        finally:
            logger.info("Stopping background tasks and writer workers")
            stop_event.set()

            resize_stats, migration_count = await asyncio.gather(resize_task, migration_task)
            await asyncio.gather(flush_task, compaction_task)
            await asyncio.gather(*writer_tasks)
            logger.info("Background tasks finished: resize_stats=%s migration_count=%s", resize_stats, migration_count)

        if RESIZE_TARGET > 0:
            assert resize_stats["steps_done"] > 0, "No tablet resize completed"
            assert resize_stats["seen_split"] > 0, "No tablet split completed"
            assert resize_stats["seen_merge"] > 0, "No tablet merge completed"
        if MIGRATION_TARGET > 0:
            assert migration_count > 0, "No tablet migration completed"

        logger.info("Running final flush before verification")
        await asyncio.gather(*(manager.api.logstor_flush(server.ip_addr) for server in servers))
        await _verify_all_keys(cql, table, expected_values)
