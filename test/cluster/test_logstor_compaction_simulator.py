# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Checks that the logstor compaction simulator runs and that the results of a run agree with each other.

logstor_compaction_sim, in test/manual/logstor_compaction_sim.cc, runs the engine's own compaction
selection over a modeled segment pool. It links the engine's code, so a logstor change can break it,
and no other test runs it. These tests run it with a small disk. They check that each kind of run
completes, that compaction runs, and that the numbers a run reports are consistent: the utilization is
the one that was asked for, the histograms add up to the totals, and a sweep or a time series has the
rows that its parameters give.

The tests do not check the write amplification against fixed values. Use the simulator itself to
measure it.
"""

import asyncio
import csv
import io
import logging
import re
import subprocess
from pathlib import Path

import pytest

from test import path_to

logger = logging.getLogger(__name__)

# A small disk keeps each run short, also in a debug build: 256 segments of 128 KiB.
SMALL_DISK = ["--disk-size", "32M"]
SEGMENTS = 256

# The user data bytes per second of a timed run, and the period the time series is sampled at.
WRITE_RATE = 2 * 1024 * 1024
SAMPLE_PERIOD = 0.25

TIMEOUT = 300

OUT_OF_SPACE = "the disk ran out of segments"


@pytest.fixture(scope="module")
def simulator_path(build_mode):
    return path_to(build_mode, "test/manual/logstor_compaction_sim")


async def run_simulator(simulator_path: str, args: list[str]) -> subprocess.CompletedProcess:
    """Runs the simulator on one shard and returns the result, also when it fails."""
    cmd = [
        simulator_path,
        "--smp", "1",
        "--memory", "1G",
        "--overprovisioned",
        "--idle-poll-time-us", "0",
        *args,
    ]
    logger.info("Running %s", " ".join(cmd))
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, lambda: subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=TIMEOUT))
    logger.info("The simulator exited with %d, output:\n%s\nerrors:\n%s", result.returncode, result.stdout, result.stderr)
    return result


async def run_ok(simulator_path: str, args: list[str]) -> str:
    """Runs the simulator, expects it to succeed, and returns what it printed."""
    result = await run_simulator(simulator_path, args)
    assert result.returncode == 0, f"the simulator exited with {result.returncode}, see the log of the test"
    return result.stdout


def report_value(report: str, pattern: str) -> tuple[str, ...]:
    """Returns the groups of the one line of the report that matches pattern."""
    matches = re.findall(rf"^\s*{pattern}\s*$", report, re.MULTILINE)
    assert len(matches) == 1, f"the report has {len(matches)} lines that match '{pattern}'"
    match = matches[0]
    return match if isinstance(match, tuple) else (match,)


def histogram(report: str, title: str, row: str) -> list[tuple[str, ...]]:
    """Returns the rows of the report's histogram with this title, as the groups of the row pattern."""
    lines = report.split("\n")
    start = next(i for i, line in enumerate(lines) if line.strip() == title) + 1
    rows = []
    for line in lines[start:]:
        match = re.fullmatch(rf"\s*{row}\s*", line)
        if not match:
            break
        rows.append(match.groups())
    return rows


def check_report(report: str, utilization: float) -> None:
    """Checks that a run completed, that compaction ran, and that the numbers of the report agree."""
    assert OUT_OF_SPACE not in report

    mean_utilization = float(report_value(report, r"mean utilization U\s+([\d.]+)")[0])
    assert mean_utilization == pytest.approx(utilization, abs=0.01)
    effective_utilization = float(report_value(report, r"effective utilization U_eff\s+([\d.]+)")[0])
    # The free segments are not occupied, so the occupied ones are fuller than the disk.
    assert mean_utilization < effective_utilization < 1

    wa_gc = float(report_value(report, r"WA_gc .*?([\d.]+)")[0])
    device_wa = float(report_value(report, r"device WA .*?([\d.]+)")[0])
    assert wa_gc > 0
    assert device_wa > 1 + wa_gc * 0.9

    jobs, segments_in, segments_out, reclaimed = map(int, report_value(
        report, r"jobs (\d+), segments in (\d+), out (\d+), reclaimed (\d+)"))
    assert jobs > 0, "compaction did not run"
    assert segments_out < segments_in
    assert reclaimed > 0

    batch_sizes = histogram(report, "executed batch size", r"(\d+) segments\s+(\d+) jobs")
    assert sum(int(n) for _, n in batch_sizes) == jobs
    assert sum(int(size) * int(n) for size, n in batch_sizes) == segments_in
    efficiency = histogram(report, "executed batch efficiency (segments reclaimed per segment copied, 1/marginal WA)",
            r"(?:[\d.]+ - [\d.]+|>= [\d.]+)\s+(\d+) jobs")
    assert sum(int(n) for (n,) in efficiency) == jobs


def parse_csv(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


async def test_self_test(simulator_path):
    """Runs --self-test, which checks the accounting invariants of the model over a few short runs."""
    output = await run_ok(simulator_path, ["--self-test"])
    lines = [line for line in output.split("\n") if re.match(r"^(ok|FAIL) ", line)]
    assert lines, "the self test ran no case"
    assert all(line.startswith("ok ") for line in lines), "a case of the self test failed"
    assert output.rstrip().endswith("self test passed")


async def test_report(simulator_path):
    """Runs one simulation and checks its report."""
    report = await run_ok(simulator_path, [*SMALL_DISK, "--utilization", "0.6"])
    assert report_value(report, r"disk\s+32 MiB in (\d+) segments of 128 KiB") == (str(SEGMENTS),)
    check_report(report, 0.6)


@pytest.mark.parametrize("args", [
    ["--workload", "zipf"],
    ["--workload", "hot-cold", "--value-size-dist", "lognormal"],
    ["--value-size-dist", "uniform", "--value-size-min", "100", "--value-size-max", "3000"],
    ["--strategy", "absolute"],
    ["--strategy", "random"],
    ["--groups", "8", "--group-skew", "1.5"],
    ["--groups", "8", "--group-skew", "1.5", "--jobs-per-group", "4"],
    ["--free-band", "1", "--batch-cap", "8", "--parallelism", "1"],
    ["--segment-size", "64K", "--file-size", "1M"],
], ids=["zipf", "hot-cold-lognormal", "uniform-values", "absolute", "random", "skewed-groups",
        "jobs-per-group", "pinned-band", "small-segments"])
async def test_configurations(simulator_path, args):
    """Runs the workloads, the selection rules and the compaction limits the simulator models."""
    report = await run_ok(simulator_path, [*SMALL_DISK, "--utilization", "0.6", *args])
    check_report(report, 0.6)


async def test_sweep(simulator_path):
    """Runs a sweep of two parameters as csv. It runs every combination, the first parameter the outer one."""
    output = await run_ok(simulator_path, [*SMALL_DISK, "--csv",
            "--sweep", "utilization=0.5,0.7", "--sweep", "groups=1,4"])
    rows = parse_csv(output)
    assert [(r["utilization"], r["groups"]) for r in rows] == [("0.5", "1"), ("0.5", "4"), ("0.7", "1"), ("0.7", "4")]
    for r in rows:
        assert float(r["U"]) == pytest.approx(float(r["utilization"]), abs=0.01)
        assert float(r["U"]) < float(r["U_eff"]) < 1
        assert int(r["jobs"]) > 0
    # A fuller disk costs more write amplification.
    by_config = {(r["utilization"], r["groups"]): float(r["WA_gc"]) for r in rows}
    for groups in ("1", "4"):
        assert by_config[("0.5", groups)] < by_config[("0.7", groups)]
    # Only a timed run has dynamics.
    assert "level sd" not in rows[0]


async def test_sweep_table(simulator_path):
    """Prints a sweep as a table: a header and a row for each value."""
    output = await run_ok(simulator_path, [*SMALL_DISK, "--sweep", "utilization=0.5,0.6,0.7"])
    lines = output.strip().split("\n")
    assert lines[0].split()[:3] == ["utilization", "U", "U_eff"]
    assert [line.split()[0] for line in lines[1:]] == ["0.5", "0.6", "0.7"]


async def test_timed_run(simulator_path, tmp_path):
    """Runs with a clock and a time series. The free level must hold near the target without a stall."""
    trace = tmp_path / "trace.csv"
    report = await run_ok(simulator_path, [*SMALL_DISK, "--utilization", "0.6",
            "--write-rate", str(WRITE_RATE), "--trace", str(trace)])
    check_report(report, 0.6)
    elapsed = float(report_value(report, r"Dynamics over (\d+)s")[0])
    assert elapsed > 0
    assert "write path stalled" not in report
    mean_level, level_min, level_max = report_value(
        report, r"free segments\s+mean ([\d.]+), sd [\d.]+ \(.*\), min (\d+) max (\d+)")
    assert int(level_min) <= float(mean_level) <= int(level_max)

    rows = parse_csv(trace.read_text())
    # The trace samples the measured part of the run, one row per period.
    assert len(rows) == pytest.approx(elapsed / SAMPLE_PERIOD, rel=0.1)
    # A row has the time of the record write that ended the period, which is a little after it.
    times = [float(r["time"]) for r in rows]
    assert times == sorted(times)
    assert (times[-1] - times[0]) / (len(times) - 1) == pytest.approx(SAMPLE_PERIOD, rel=0.01)
    for r in rows:
        assert 0 <= int(r["free_segments"]) <= SEGMENTS
        assert 0 <= float(r["shares"]) <= 1
        assert float(r["write_rate"]) == WRITE_RATE
    # The time series is the samples the level is reported from.
    levels = [int(r["free_segments"]) for r in rows]
    assert (min(levels), max(levels)) == (int(level_min), int(level_max))


async def test_write_rate_step(simulator_path, tmp_path):
    """Doubles the write rate half way through the measured run."""
    trace = tmp_path / "trace.csv"
    report = await run_ok(simulator_path, [*SMALL_DISK, "--utilization", "0.6",
            "--write-rate", str(WRITE_RATE), "--write-rate-step", "2", "--trace", str(trace)])
    assert "write rate x2 half way" in report
    rates = [float(r["write_rate"]) for r in parse_csv(trace.read_text())]
    assert rates[0] == WRITE_RATE and rates[-1] == 2 * WRITE_RATE
    assert rates == sorted(rates), "the write rate went back down"


async def test_timed_sweep(simulator_path):
    """Runs a timed sweep, which adds the dynamics to the table, with compaction that follows the shares."""
    output = await run_ok(simulator_path, [*SMALL_DISK, "--csv", "--write-rate", str(WRITE_RATE),
            "--sweep", "shares-authority=0,1"])
    rows = parse_csv(output)
    assert [r["shares-authority"] for r in rows] == ["0", "1"]
    for r in rows:
        assert float(r["stall s"]) == 0
        assert 0 <= float(r["level min"]) <= float(r["level"]) <= float(r["level max"]) <= SEGMENTS
        assert float(r["level sd"]) >= 0
        assert 0 <= float(r["shares"]) <= 1


async def test_out_of_space(simulator_path):
    """Asks for a utilization the free-segment target cannot hold. The run must say so and not fail."""
    report = await run_ok(simulator_path, [*SMALL_DISK, "--utilization", "0.99"])
    assert OUT_OF_SPACE in report


async def test_extension_tolerance(simulator_path):
    """Sweeps the batch extension tolerance, which the engine takes as a constant."""
    output = await run_ok(simulator_path, [*SMALL_DISK, "--csv", "--sweep", "extension-tolerance=0.5,0.8,1"])
    rows = parse_csv(output)
    assert [r["extension-tolerance"] for r in rows] == ["0.5", "0.8", "1"]
    for r in rows:
        assert float(r["U"]) == pytest.approx(0.75, abs=0.01)
        assert int(r["jobs"]) > 0
    report = await run_ok(simulator_path, [*SMALL_DISK, "--extension-tolerance", "0.5"])
    assert "tolerance 0.5," in report
    check_report(report, 0.75)


@pytest.mark.parametrize("args, error", [
    (["--groups", "0"], "a shard has at least one compaction group"),
    (["--jobs-per-group", "0"], "a group runs at least one compaction job at a time"),
    (["--segment-size", "4M"], "a segment of 4194304 is above the 131072 the free-space histogram is built for"),
    (["--disk-size", "1M"], "the disk holds whole files and a file holds whole segments"),
    (["--disk-size", "2M", "--file-size", "1M"], "a disk of 16 segments is too small to compact"),
    (["--strategy", "no-such-strategy"], "unknown value 'no-such-strategy'"),
    (["--disk-size", "3X"], "unknown size suffix 'X'"),
    (["--sweep", "no-such-parameter=1"], "unknown parameter 'no-such-parameter'"),
    (["--sweep", "groups"], "a sweep is 'parameter=value,value,...', got 'groups'"),
], ids=["no-groups", "no-jobs-per-group", "segment-above-histogram", "disk-smaller-than-file",
        "disk-too-small", "unknown-strategy", "unknown-size-suffix", "unknown-sweep-parameter", "sweep-without-values"])
async def test_bad_configuration(simulator_path, args, error):
    """Gives the simulator a configuration it cannot run. It must exit with an error that says why."""
    result = await run_simulator(simulator_path, args)
    assert result.returncode == 1
    assert error in result.stdout + result.stderr
