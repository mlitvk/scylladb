#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The parameters and the profiles of the logstor fuzz test.

A profile is a named set of parameters.  An env var can override each
parameter of the profile, so a run can change one value without a code change:

  * ``LOGSTOR_FUZZ_SEED`` sets the seed of the run;
  * ``LOGSTOR_FUZZ_DURATION`` sets the run time in seconds;
  * ``LOGSTOR_FUZZ_INCLUDE_KNOWN`` runs the items that a known issue disables
    (see ``known_issues``): ``all``, or a list of entry ids with commas;
  * ``LOGSTOR_FUZZ_<NAME>`` sets the field ``<name>`` of any of the parameter
    classes below, for example ``LOGSTOR_FUZZ_KEYS=500`` or
    ``LOGSTOR_FUZZ_VALUE_SIZE=fixed:100``.  A tuple field takes a value with
    commas, for example ``LOGSTOR_FUZZ_NODES_PER_RACK=2,1,1``.
"""

from __future__ import annotations

import dataclasses
import os
import random
import sys
from dataclasses import dataclass, field
from typing import Any

ENV_PREFIX = "LOGSTOR_FUZZ_"

# The env vars with the prefix that are not parameters of a profile: the
# gating procedure reads them (see ``gating``).
NON_PARAMETER_ENV = {ENV_PREFIX + name for name in (
    "GATING", "GATING_ONLY", "GATING_EXPLORE", "GATING_EXPLORE_SEED", "RESULT_DIR",
)}


@dataclass(frozen=True)
class ValueSize:
    """The distribution of the payload size of a value.

    The text forms are ``fixed:N``, ``uniform:A-B``, and ``bimodal:A-B:C-D:P``,
    in which a value takes a size from ``C-D`` with probability ``P`` and from
    ``A-B`` otherwise.
    """

    spec: str

    def __post_init__(self) -> None:
        self.max()

    def _ranges(self) -> list[tuple[int, int, float]]:
        kind, _, rest = self.spec.partition(":")
        if kind == "fixed":
            n = int(rest)
            return [(n, n, 1.0)]
        if kind == "uniform":
            a, b = (int(x) for x in rest.split("-"))
            return [(a, b, 1.0)]
        if kind == "bimodal":
            small, large, p = rest.split(":")
            a, b = (int(x) for x in small.split("-"))
            c, d = (int(x) for x in large.split("-"))
            return [(a, b, 1.0 - float(p)), (c, d, float(p))]
        raise ValueError(f"unknown value size spec {self.spec!r}")

    def max(self) -> int:
        return max(b for _, b, _ in self._ranges())

    def mean(self) -> float:
        return sum((a + b) / 2 * w for a, b, w in self._ranges())

    def sample(self, rng: random.Random) -> int:
        ranges = self._ranges()
        a, b, _ = rng.choices(ranges, weights=[w for _, _, w in ranges])[0]
        return rng.randint(a, b)


@dataclass(frozen=True)
class WorkloadParams:
    # The number of logstor tables.  More than one table gives more than one
    # group on a shard, which the separator and compaction see.
    tables: int = 2
    # The number of keys in each table.  The key space is fixed and the writes
    # overwrite it, so the live data stays bounded.  0 derives it from
    # target_utilization (see keys_for()).
    keys: int = 0
    # The fraction of the logstor store of a shard that the live data takes
    # after the preload, on the nodes that hold a full copy of the data.  The
    # deletes keep about a tenth of the keys dead during the run, so the run
    # settles near 0.45.  Logstor compaction works around 0.5, so this keeps
    # it busy.
    target_utilization: float = 0.5
    # Writes each key once before the timed run, so that the run starts at the
    # target utilization.
    preload: bool = True
    preload_concurrency: int = 64
    # "uniform", or "zipf" with the exponent zipf_s.
    key_dist: str = "uniform"
    zipf_s: float = 1.1
    value_size: str = "uniform:32-2048"
    # The writer and reader tasks of each table.
    writers: int = 4
    readers: int = 2
    # The fraction of the writes that are partition deletes.
    delete_fraction: float = 0.1
    # The fraction of the reads that the model checks.
    read_sample: float = 1.0
    # The fraction of the reads that use BYPASS CACHE, so that the reads test
    # both the logstor cache and the read from the segments.
    bypass_cache_fraction: float = 0.3
    # The pause of a client between two requests, in seconds.
    op_pause: tuple[float, float] = (0.005, 0.02)
    write_cl: str = "QUORUM"
    read_cl: str = "QUORUM"


@dataclass(frozen=True)
class ClusterParams:
    # The number of nodes in each rack at the start; the number of racks is the length.
    nodes_per_rack: tuple[int, ...] = (1, 1, 1)
    # The add_node operation does not grow a rack above this.
    max_nodes_per_rack: int = 2
    rf: int = 3
    smp: int = 2
    initial_tablets: int = 8
    # Per shard.  A small store makes the writes fill the free space of a shard
    # in tens of seconds, so compaction and the separator run all the time.
    logstor_disk_size_in_mb: int = 16
    logstor_file_size_in_mb: int = 4


@dataclass(frozen=True)
class NemesisParams:
    # The cluster operations that the nemesis picks from (see operations.OPERATIONS).
    # A known issue can disable some of them.
    ops: tuple[str, ...] = ()
    # The pause between two operations, in seconds.
    op_interval: tuple[float, float] = (5.0, 20.0)
    # The run fails as a hang when no write of a table is acknowledged for this long.
    stall_timeout_s: float = 90.0
    # An operation that moves data between shards (a tablet move, a resize) does
    # not run when a shard holds more live data than this fraction of its store,
    # so that the operations cannot fill a shard (see the known issue
    # logstor-full-store-hang).
    max_shard_utilization: float = 0.65


@dataclass(frozen=True)
class TimeLimits:
    """The time limits of the parts of a run, in seconds, for a dev or a release build.

    A part that runs longer than its limit fails the run as a hang, so that a
    stuck node or a stuck check ends the test in minutes and not at the timeout
    of pytest.  A debug build is slower, so ``for_mode()`` multiplies each limit
    by ``MODE_TIME_SCALE``.  Each limit is three or more times the longest
    time that the part took in a dev run in Nov 2026.
    """

    # The start of the cluster, and the preload of the tables.
    boot_s: float = 120.0
    preload_s: float = 120.0
    # The time that the tasks of the run get to stop after ``duration_s``,
    # above the limit of the longest operation.
    stop_s: float = 120.0
    # The checks after the run, with the restart of all nodes.
    checks_s: float = 300.0
    # The start of one node, until it serves CQL.
    node_start_s: float = 60.0
    # The operations of the nemesis, by the kind of the operation (see
    # ``operations.Operation.kind``).
    quick_op_s: float = 60.0
    node_op_s: float = 120.0
    data_op_s: float = 240.0

    def op_s(self, kind: str) -> float:
        """Returns the limit of an operation of ``kind``: ``quick``, ``node`` or ``data``."""
        return getattr(self, f"{kind}_op_s")

    def for_mode(self, mode: str) -> TimeLimits:
        """Returns the limits for a build of ``mode``."""
        scale = MODE_TIME_SCALE.get(mode, 1.0)
        return dataclasses.replace(self, **{f.name: getattr(self, f.name) * scale for f in dataclasses.fields(self)})


# A debug build runs the operations about one and a half to three times slower
# than a dev build.
MODE_TIME_SCALE = {"debug": 3.0}


@dataclass(frozen=True)
class CheckParams:
    # Compares the replicas of each key with MUTATION_FRAGMENTS after the run.
    replica_check: bool = True
    # "full" restarts all nodes at once after the run and checks again;
    # "none" does not restart.
    final_restart: str = "full"
    # Runs the Porcupine linearizability checker over each table's history.
    # It needs a container runtime, so it is off by default.
    porcupine: bool = False


@dataclass(frozen=True)
class Profile:
    name: str
    duration_s: float
    workload: WorkloadParams = field(default_factory=WorkloadParams)
    cluster: ClusterParams = field(default_factory=ClusterParams)
    nemesis: NemesisParams = field(default_factory=NemesisParams)
    checks: CheckParams = field(default_factory=CheckParams)
    limits: TimeLimits = field(default_factory=TimeLimits)
    seed: int = 0
    # The ids of the known issues whose items this run includes anyway.
    include_known: tuple[str, ...] = ()

    def describe(self) -> str:
        return (f"profile={self.name} seed={self.seed} duration={self.duration_s}s "
                f"workload={self.workload} cluster={self.cluster} nemesis={self.nemesis} checks={self.checks} "
                f"limits={self.limits} "
                f"include_known={self.include_known}")


PROFILES: dict[str, Profile] = {
    # The workload and the checks, with no cluster operations and no failures.
    "workload_only": Profile(name="workload_only", duration_s=60),
    # The workload under random cluster operations, with no injected failures.
    "operations": Profile(
        name="operations", duration_s=300,
        cluster=ClusterParams(nodes_per_rack=(2, 1, 1)),
        nemesis=NemesisParams(ops=(
            "restart_node", "kill_restart_node", "pause_node",
            "add_node", "decommission_node", "remove_node",
            "move_tablet", "intranode_move", "split_merge", "balancing_toggle",
            "flush", "compact", "alter_caching", "repair",
        )),
    ),
}

# The operations profile with a skewed load: the keys of a zipf distribution,
# so that a few hot keys take most of the writes and the reads.  The hot keys
# make most of the dead records, in a few tablets, while the cold keys stay
# live, which is the case that the separator and compaction must handle.
PROFILES["operations_skewed"] = dataclasses.replace(
    PROFILES["operations"], name="operations_skewed",
    workload=WorkloadParams(key_dist="zipf", zipf_s=1.1),
)


def _convert(value: str, current: Any, name: str) -> Any:
    if isinstance(current, bool):
        if value.lower() in ("1", "true", "yes", "on"):
            return True
        if value.lower() in ("0", "false", "no", "off"):
            return False
        raise ValueError(f"{ENV_PREFIX}{name.upper()}={value!r} is not a bool")
    if isinstance(current, tuple):
        return tuple(type(current[0])(x) if current else x for x in value.split(","))
    return type(current)(value)


def _override(params: Any, env: dict[str, str], used: set[str]) -> Any:
    changes = {}
    for f in dataclasses.fields(params):
        key = ENV_PREFIX + f.name.upper()
        if key in env:
            changes[f.name] = _convert(env[key], getattr(params, f.name), f.name)
            used.add(key)
    return dataclasses.replace(params, **changes)


def load_profile(name: str, env: dict[str, str] | None = None) -> Profile:
    """Returns the profile ``name`` with the overrides from the env vars, and a seed.

    Raises ValueError for an env var with the prefix that names no parameter,
    so that a typo does not make a run with the default value.
    """
    env = dict(os.environ if env is None else env)
    profile = PROFILES[name]
    used: set[str] = set()
    workload = _override(profile.workload, env, used)
    cluster = _override(profile.cluster, env, used)
    nemesis = _override(profile.nemesis, env, used)
    checks = _override(profile.checks, env, used)
    limits = _override(profile.limits, env, used)
    duration_s = profile.duration_s
    if ENV_PREFIX + "DURATION" in env:
        duration_s = float(env[ENV_PREFIX + "DURATION"])
        used.add(ENV_PREFIX + "DURATION")
    if ENV_PREFIX + "SEED" in env:
        seed = int(env[ENV_PREFIX + "SEED"])
        used.add(ENV_PREFIX + "SEED")
    else:
        seed = random.randrange(sys.maxsize)
    include_known: tuple[str, ...] = ()
    if ENV_PREFIX + "INCLUDE_KNOWN" in env:
        include_known = tuple(x for x in env[ENV_PREFIX + "INCLUDE_KNOWN"].split(",") if x)
        used.add(ENV_PREFIX + "INCLUDE_KNOWN")
    unknown = {k for k in env if k.startswith(ENV_PREFIX)} - used - NON_PARAMETER_ENV
    if unknown:
        raise ValueError(f"unknown logstor fuzz parameters: {sorted(unknown)}")
    if workload.keys == 0:
        workload = dataclasses.replace(workload, keys=keys_for(workload, cluster))
    profile = dataclasses.replace(profile, workload=workload, cluster=cluster, nemesis=nemesis, checks=checks,
                                  limits=limits, duration_s=duration_s, seed=seed, include_known=include_known)
    validate(profile)
    return profile


# The bytes of a record above the payload of its value: the value header of the
# workload (26 B) and the logstor record (the header, the key and the mutation
# framing, about 330 B for this one-column schema on master in Nov 2026).
# Calibrated against scylla_logstor_sm_live_record_bytes after a preload.
RECORD_OVERHEAD = 356

# A profile may not plan for more live data than this fraction of the store of
# a shard: above it, an uneven spread of the tablets can fill a shard.
MAX_UTILIZATION = 0.6


def record_bytes(w: WorkloadParams) -> float:
    """Returns the mean size of a record of the workload in the logstor store."""
    return ValueSize(w.value_size).mean() + RECORD_OVERHEAD


def keys_for(w: WorkloadParams, c: ClusterParams) -> int:
    """Returns the keys of a table that give target_utilization.

    Each rack holds one replica of each tablet (rf equals the number of
    racks), so a node that is alone in its rack holds all the data once, over
    its smp shards.  The size is for such a node, which is the fullest one.
    """
    store = c.logstor_disk_size_in_mb * 2**20 * c.smp
    return int(store * w.target_utilization / (w.tables * record_bytes(w)))


def utilization_estimate(profile: Profile) -> float:
    """Returns the planned live data of a shard of a full-copy node, as a fraction of its store."""
    w, c = profile.workload, profile.cluster
    return w.tables * w.keys * record_bytes(w) / (c.logstor_disk_size_in_mb * 2**20 * c.smp)


def validate(profile: Profile) -> None:
    """Raises ValueError for a profile that the checks cannot support."""
    w, c = profile.workload, profile.cluster
    replicas = {"ONE": 1, "QUORUM": c.rf // 2 + 1, "ALL": c.rf}
    if w.write_cl not in replicas or w.read_cl not in replicas:
        raise ValueError(f"write_cl and read_cl must be one of {sorted(replicas)}")
    if replicas[w.write_cl] + replicas[w.read_cl] <= c.rf:
        raise ValueError(f"write_cl={w.write_cl} and read_cl={w.read_cl} do not overlap at rf={c.rf}, "
                         f"so the model cannot check the reads")
    if c.rf > len(c.nodes_per_rack):
        raise ValueError(f"rf={c.rf} needs {c.rf} racks, and the profile has {len(c.nodes_per_rack)}")
    if w.key_dist not in ("uniform", "zipf"):
        raise ValueError(f"unknown key_dist {w.key_dist!r}")
    ValueSize(w.value_size)
    if w.keys < w.writers:
        raise ValueError(f"keys={w.keys} is fewer than writers={w.writers}")
    if c.rf != len(c.nodes_per_rack):
        raise ValueError(f"the store sizing needs rf ({c.rf}) equal to the number of racks ({len(c.nodes_per_rack)})")
    utilization = utilization_estimate(profile)
    if utilization > MAX_UTILIZATION:
        raise ValueError(f"the live data of a shard is planned at {utilization:.2f} of its "
                         f"{c.logstor_disk_size_in_mb} MB store, above {MAX_UTILIZATION}; "
                         f"use fewer keys or smaller values, or a larger store")
    if max(c.nodes_per_rack) > c.max_nodes_per_rack:
        raise ValueError(f"nodes_per_rack={c.nodes_per_rack} is above max_nodes_per_rack={c.max_nodes_per_rack}")
    if profile.checks.final_restart not in ("full", "none"):
        raise ValueError(f"unknown final_restart {profile.checks.final_restart!r}")
