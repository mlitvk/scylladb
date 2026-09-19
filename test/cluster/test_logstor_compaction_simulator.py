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

# A shard whose garbage is in a few of its groups, where a job beyond the first can only rewrite a
# group that holds little garbage. This is the case the marginal admission gate exists for.
SKEWED_GROUPS = ["--groups", "16", "--group-skew", "2"]

THRESHOLD = ["--controller", "threshold"]


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
    [*SKEWED_GROUPS, "--marginal-admission-efficiency", "1"],
    ["--segment-size", "64K", "--file-size", "1M"],
], ids=["zipf", "hot-cold-lognormal", "uniform-values", "absolute", "random", "skewed-groups",
        "jobs-per-group", "pinned-band", "small-segments", "admission-floor"])
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


async def test_marginal_admission_gate(simulator_path):
    """Runs skewed groups with the gate off and at the engine's ratio.

    The gate must refuse batches, and it must save write amplification.
    """
    output = await run_ok(simulator_path, [*SMALL_DISK, *SKEWED_GROUPS, "--csv",
            "--sweep", "utilization=0.4,0.5", "--sweep", "marginal-admission-ratio=0,0.75"])
    wa_gc = {(r["utilization"], r["marginal-admission-ratio"]): float(r["WA_gc"]) for r in parse_csv(output)}
    for utilization in ("0.4", "0.5"):
        assert wa_gc[(utilization, "0.75")] < wa_gc[(utilization, "0")]

    # The default is the engine's ratio.
    report = await run_ok(simulator_path, [*SMALL_DISK, *SKEWED_GROUPS, "--utilization", "0.5"])
    check_report(report, 0.5)
    assert "ratio 0.75" in report
    refused = int(report_value(report, r"batches refused a second job slot by the admission floor: (\d+)")[0])
    assert refused > 0


async def test_threshold_policy(simulator_path):
    """Runs the threshold policy without a clock, where every batch under the threshold is compacted when it appears."""
    report = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.4"])
    check_report(report, 0.4)
    threshold, anchor, space_floor, ceiling = report_value(
        report, r"policy\s+utilization threshold ([\d.]+) at (\d+) free, 1 at the space floor of (\d+), (.*)")
    assert threshold == "0.5"
    assert int(anchor) == 2 * int(space_floor)
    assert ceiling == "constant above the anchor"

    report = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.4",
            "--threshold-anchor", "0.15", "--threshold-ceiling", "0.3"])
    check_report(report, 0.4)
    assert report_value(report, r"policy\s+utilization threshold 0.5 at (\d+) free, .*, 0 at (\d+)") == (
            str(round(0.15 * SEGMENTS)), str(round(0.3 * SEGMENTS)))


async def test_utilization_threshold(simulator_path):
    """Sweeps the threshold on a disk with room to spare, where space pressure does not relax it.

    A segment is compacted only while less of it than the threshold is live. A lower threshold
    compacts emptier segments, for less write amplification, and leaves more garbage in the
    segments it does not compact.
    """
    output = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--threshold-ceiling", "0",
            "--utilization", "0.4", "--csv", "--sweep", "utilization-threshold=0.3,0.5,0.7"])
    rows = parse_csv(output)
    assert [r["utilization-threshold"] for r in rows] == ["0.3", "0.5", "0.7"]
    for r in rows:
        assert float(r["victim u"]) <= float(r["utilization-threshold"]) + 0.01
    wa_gc = [float(r["WA_gc"]) for r in rows]
    effective_utilization = [float(r["U_eff"]) for r in rows]
    assert wa_gc == sorted(wa_gc)
    assert effective_utilization == sorted(effective_utilization)


async def test_min_batch(simulator_path):
    """Without a minimum batch, a job takes the few segments that just crossed the threshold."""
    output = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--threshold-ceiling", "0",
            "--utilization", "0.5", "--csv", "--sweep", "min-batch=1,0"])
    segments_per_job = {r["min-batch"]: float(r["seg/job"]) for r in parse_csv(output)}
    assert segments_per_job["1"] < 4
    assert segments_per_job["0"] > 6


async def test_threshold_ceiling(simulator_path):
    """A ceiling lowers the threshold to zero as the free level rises to it, so a lower ceiling holds less free space."""
    output = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.4",
            "--csv", "--sweep", "threshold-ceiling=0.25,0.5,0"])
    free = [float(r["free%"]) for r in parse_csv(output)]
    assert free == sorted(free)


async def test_rate_controller(simulator_path, tmp_path):
    """Runs the threshold policy with a clock. The engine's rate controller paces it.

    In steady state compaction must deliver the rate the controller commands, and the time series
    must show the controller's state.
    """
    trace = tmp_path / "trace.csv"
    report = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.6",
            "--write-rate", str(WRITE_RATE), "--trace", str(trace)])
    check_report(report, 0.6)
    assert "controller threshold" in report
    assert "write path stalled" not in report
    delivered, commanded = map(float, report_value(
        report, r"reclaim rate\s+([\d.]+) segments/s delivered, ([\d.]+) commanded"))
    assert delivered == pytest.approx(commanded, rel=0.2)
    throttled = float(report_value(report, r"periods throttled\s+([\d.]+)%")[0])
    assert 0 <= throttled <= 100

    rows = parse_csv(trace.read_text())
    assert rows
    space_floor = int(rows[0]["space_floor"])
    assert sum(int(r["free_segments"]) for r in rows) / len(rows) > space_floor
    for r in rows:
        assert int(r["space_floor"]) == space_floor
        assert 0 <= float(r["threshold"]) <= 1
        assert float(r["backlog"]) >= 0
        assert float(r["commanded_rate"]) >= 0
        assert float(r["allocation_rate"]) >= 0


async def test_rate_controller_through_a_step(simulator_path, tmp_path):
    """Doubles the write rate half way. The commanded rate must follow the allocation rate up, without a stall."""
    trace = tmp_path / "trace.csv"
    report = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.6",
            "--write-rate", str(WRITE_RATE), "--write-rate-step", "2", "--trace", str(trace)])
    assert "write path stalled" not in report
    rows = parse_csv(trace.read_text())
    quarter = len(rows) // 4
    mean = lambda rows: sum(float(r["commanded_rate"]) for r in rows) / len(rows)
    assert mean(rows[-quarter:]) > 1.5 * mean(rows[:quarter])


async def test_rate_controller_parameters(simulator_path):
    """Sweeps the controller's time constants, the contended shard and the old allocation count."""
    args = [*SMALL_DISK, *THRESHOLD, "--utilization", "0.6", "--write-rate", str(WRITE_RATE), "--csv"]
    for sweep in (["--sweep", "drain-time=5,60"],
                  ["--sweep", "shares-rise-time=1,10", "--sweep", "shares-decay-time=5,40"],
                  ["--sweep", "mixed-allocations=0,1"]):
        for r in parse_csv(await run_ok(simulator_path, [*args, *sweep])):
            assert float(r["stall s"]) == 0
            assert int(r["jobs"]) > 0
            assert 0 <= float(r["shares"]) <= 1

    # On a contended shard the bandwidth follows the shares, so the controller asks for more of them.
    rows = parse_csv(await run_ok(simulator_path, [*args, "--sweep", "shares-authority=0,1"]))
    shares = {r["shares-authority"]: float(r["shares"]) for r in rows}
    assert shares["1"] > shares["0"]
    assert all(float(r["stall s"]) == 0 for r in rows)

    report = await run_ok(simulator_path, [*SMALL_DISK, *THRESHOLD, "--utilization", "0.6",
            "--write-rate", str(WRITE_RATE), "--mixed-allocations", "1"])
    assert "mixed allocations counted" in report


async def test_controllers_compared(simulator_path):
    """Runs both controllers with a clock. The relay holds its free-segment target, which the threshold does not need."""
    output = await run_ok(simulator_path, [*SMALL_DISK, "--write-rate", str(WRITE_RATE), "--utilization", "0.4",
            "--csv", "--sweep", "controller=relay,threshold"])
    free = {r["controller"]: float(r["free%"]) for r in parse_csv(output)}
    assert free["threshold"] > free["relay"]


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
    (["--controller", "no-such-controller"], "unknown value 'no-such-controller'"),
    (["--controller", "threshold", "--utilization-threshold", "1"], "the utilization threshold is a ratio in [0, 1)"),
    (["--controller", "threshold", "--strategy", "absolute"], "the threshold policy runs the engine's own selection rule"),
], ids=["no-groups", "no-jobs-per-group", "segment-above-histogram", "disk-smaller-than-file",
        "disk-too-small", "unknown-strategy", "unknown-size-suffix", "unknown-sweep-parameter", "sweep-without-values",
        "unknown-controller", "no-threshold", "threshold-with-other-rule"])
async def test_bad_configuration(simulator_path, args, error):
    """Gives the simulator a configuration it cannot run. It must exit with an error that says why."""
    result = await run_simulator(simulator_path, args)
    assert result.returncode == 1
    assert error in result.stdout + result.stderr
