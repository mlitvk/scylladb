# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Checks that perf_logstor runs each of its tests and that its options do what they say.

perf_logstor is the micro benchmark of the logstor hot path, in test/perf/perf_logstor.cc. No other
test runs it, so a logstor change can break one of its tests and nobody sees it until the next
measurement. These tests run it with a small dataset and a small segment pool. They check that each
test completes and that the counters of each test show the path it measures: a read from a segment
reads the disk, a step that uses only the CPU does not, and a write writes.

The tests do not check the performance numbers. Use perf_logstor itself to measure them.
"""

import asyncio
import json
import logging
import re
import subprocess
from pathlib import Path

import pytest

from test import path_to

logger = logging.getLogger(__name__)

# All the tests of perf_logstor, in the order of `--test all`. test_all_tests_run finds a test that
# perf_logstor has and this list does not.
TESTS = [
    "index-lookup",
    "index-insert",
    "deserialize",
    "decode",
    "build-mutation",
    "encode",
    "record-header",
    "append",
    "serialize",
    "cache-lookup",
    "cache-populate",
    "raw-read",
    "query-cached",
    "query-disk",
    "segment-read",
    "write",
    "write-frozen",
]

# The tests that read a record from a segment, which is one disk read per operation.
DISK_READ_TESTS = {"raw-read", "query-disk", "segment-read"}
# The tests that write a record.
WRITE_TESTS = {"write", "write-frozen"}
# The tests that draw only from keys in the cache.
CACHE_HIT_TESTS = {"cache-lookup", "query-cached"}

PARTITIONS = 1000

# A small dataset, and a segment pool that the writes of one test do not fill. A fixed number of
# operations, rather than a duration, keeps a debug run short.
SMALL_RUN = {
    "partitions": PARTITIONS,
    "operations-per-shard": 2000,
    "segment-size-in-kb": 128,
    "file-size-in-mb": 4,
    "disk-size-in-mb": 16,
}

# A run that does not complete in this time is stuck. A write into a full segment pool waits for a
# free segment and does not fail.
TIMEOUT = 300


def options(base: dict, **overrides) -> list[str]:
    """Returns the options in base, with the values in overrides, as command line arguments.

    An option can occur only one time on the command line of perf_logstor. Give an option with a
    dash in its name as a keyword argument with an underscore.
    """
    merged = base | {name.replace("_", "-"): value for name, value in overrides.items()}
    return [arg for name, value in merged.items() for arg in (f"--{name}", str(value))]


@pytest.fixture(scope="module")
def perf_logstor_path(build_mode):
    return path_to(build_mode, "test/perf/perf_logstor")


async def run_perf_logstor(perf_logstor_path: str, tmp_path: Path, args: list[str], smp: int = 1) -> subprocess.CompletedProcess:
    """Runs perf_logstor with its logstor files in tmp_path and returns the result, also when it fails."""
    cmd = [
        perf_logstor_path,
        "--smp", str(smp),
        "--memory", f"{smp}G",
        "--overprovisioned",
        "--idle-poll-time-us", "0",
        "--dir", str(tmp_path / "logstor"),
        *args,
    ]
    logger.info("Running %s", " ".join(cmd))
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, lambda: subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=TIMEOUT))
    logger.info("perf_logstor exited with %d, output:\n%s", result.returncode, result.stdout)
    return result


async def run_tests(perf_logstor_path: str, tmp_path: Path, tests: list[str], args: list[str], smp: int = 1) -> tuple[str, dict[str, dict]]:
    """Runs the tests in one perf_logstor process, and expects it to succeed.

    Returns the output of the run and the JSON result of each test, by the name of the test.
    """
    json_prefix = tmp_path / "result"
    result = await run_perf_logstor(perf_logstor_path, tmp_path,
            [*args, "--test", ",".join(tests), "--json-result", str(json_prefix)], smp)
    assert result.returncode == 0, f"perf_logstor exited with {result.returncode}, see the log of the test"
    results = {}
    for test in tests:
        with open(f"{json_prefix}.{test}") as f:
            results[test] = json.load(f)
    return result.stdout, results


def check_counters(test: str, stats: dict) -> None:
    """Checks that the counters of a test show the path the test measures."""
    assert stats["median tps"] > 0, f"{test}: no operation completed"
    assert stats["frame_size"] > stats["payload_bytes"], f"{test}: a record frame is smaller than the row it holds"
    if test in WRITE_TESTS:
        # A write test can also read: compaction reads the segments that the overwrites leave
        # partly dead.
        assert stats["write_bytes_per_op"] > 0, f"{test}: an operation did not write"
        return
    if test in DISK_READ_TESTS:
        assert stats["reads_per_op"] >= 1, f"{test}: an operation did not read the disk"
    else:
        assert stats["reads_per_op"] == 0, f"{test}: an operation read the disk"
    assert stats["writes_per_op"] == 0, f"{test}: an operation wrote to the disk"


def check_cache_hits(output: str, tests: list[str], partitions: int) -> None:
    """Checks that each cache test found all the keys of the dataset in the cache, and that no lookup missed."""
    resident = re.findall(r"^(\d+) of the (\d+) keys of the dataset are in the cache$", output, re.MULTILINE)
    expected = [(str(partitions), str(partitions))] * len([t for t in tests if t in CACHE_HIT_TESTS])
    assert resident == expected, "the cache did not hold the whole dataset"
    assert "lookups found no cached mutation" not in output


@pytest.mark.parametrize("test", TESTS)
async def test_one_test(perf_logstor_path, tmp_path, test):
    """Runs one test of perf_logstor in its own process, so that a failure shows the test that failed."""
    output, results = await run_tests(perf_logstor_path, tmp_path, [test], options(SMALL_RUN))
    check_counters(test, results[test]["stats"])
    check_cache_hits(output, [test], PARTITIONS)


async def test_all_tests_run(perf_logstor_path, tmp_path):
    """Runs `--test all`, which runs the tests one after the other on one dataset.

    The run must write a result for each test in TESTS, and for no other test.
    """
    output, results = await run_tests(perf_logstor_path, tmp_path, TESTS, options(SMALL_RUN))
    results_written = sorted(p.name.removeprefix("result.") for p in tmp_path.glob("result.*"))
    assert results_written == sorted(TESTS)
    for test in TESTS:
        check_counters(test, results[test]["stats"])
    check_cache_hits(output, TESTS, PARTITIONS)


async def test_multiple_shards(perf_logstor_path, tmp_path):
    """Runs on two shards. Each shard has its own logstor and its own dataset."""
    tests = ["cache-lookup", "query-disk", "write"]
    output, results = await run_tests(perf_logstor_path, tmp_path, tests, options(SMALL_RUN), smp=2)
    for test in tests:
        assert results[test]["parameters"]["cpus"] == 2
        check_counters(test, results[test]["stats"])
    check_cache_hits(output, tests, 2 * PARTITIONS)


async def test_concurrency_one(perf_logstor_path, tmp_path):
    """Runs the tests that wait for the disk with one operation in flight."""
    tests = ["query-disk", "segment-read", "write", "write-frozen"]
    _, results = await run_tests(perf_logstor_path, tmp_path, tests, options(SMALL_RUN, concurrency=1))
    for test in tests:
        assert results[test]["parameters"]["concurrency"] == 1
        check_counters(test, results[test]["stats"])


async def test_wide_rows_and_column_selection(perf_logstor_path, tmp_path):
    """Runs the whole reads and the writes with 30 value columns, and reads only 5 of them."""
    tests = ["query-cached", "query-disk", "write", "write-frozen"]
    args = options(SMALL_RUN, columns=30, value_size=20, select_columns=5, disk_size_in_mb=64)
    output, results = await run_tests(perf_logstor_path, tmp_path, tests, args)
    for test in tests:
        assert results[test]["parameters"]["columns"] == 30
        assert results[test]["parameters"]["select_columns"] == 5
        check_counters(test, results[test]["stats"])
    check_cache_hits(output, tests, PARTITIONS)


async def test_writes_fill_the_pool(perf_logstor_path, tmp_path):
    """Writes more than the segment pool holds, so that the writes continue only if compaction frees segments.

    The pool holds 8 MB. 20000 writes of a 493 byte record frame write about 10 MB of records.
    """
    args = options(SMALL_RUN, operations_per_shard=20000, file_size_in_mb=1, disk_size_in_mb=8)
    _, results = await run_tests(perf_logstor_path, tmp_path, ["write"], args)
    stats = results["write"]["stats"]
    assert stats["median tps"] > 0
    assert stats["read_bytes_per_op"] > 0, "compaction did not read a segment"


async def test_record_size_report(perf_logstor_path, tmp_path):
    """Prints the record size report, which builds no logstor, for a matrix of 2 x 2 x 2 row shapes."""
    result = await run_perf_logstor(perf_logstor_path, tmp_path, [
        "--record-size-report", "--report-key-sizes", "8,64", "--report-columns", "1,30", "--report-value-sizes", "20,300"])
    assert result.returncode == 0
    rows = re.findall(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+) \|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+) \|", result.stdout, re.MULTILINE)
    assert len(rows) == 8
    for key, columns, value, payload, header, record_value, frame, padding in (map(int, row) for row in rows):
        assert payload == key + columns * value
        assert frame > header + record_value
        assert frame > payload


@pytest.mark.parametrize("overrides, error", [
    ({"partitions": 0}, "--partitions must be at least one"),
    ({"test": "no-such-test"}, "unknown test 'no-such-test'"),
    ({"columns": 1, "select-columns": 2}, "--select-columns 2 asks for more value columns than the 1 the table has"),
    ({"disk-size-in-mb": 0}, "Disk size 0 must be greater than or equal to file size"),
    ({"segment-size-in-kb": 2048, "file-size-in-mb": 1}, "Segment size 2097152 must be less than or equal to file size 1048576"),
], ids=["no-partitions", "unknown-test", "too-many-selected-columns", "no-disk", "segment-larger-than-file"])
async def test_bad_configuration(perf_logstor_path, tmp_path, overrides, error):
    """Gives perf_logstor a configuration it cannot run. It must exit with an error that says why, and not abort."""
    result = await run_perf_logstor(perf_logstor_path, tmp_path, options(SMALL_RUN | {"test": "index-lookup"} | overrides))
    assert result.returncode == 1
    assert error in result.stdout
