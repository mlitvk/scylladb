#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The model that checks each read of the logstor fuzz workload.

The model keeps a history of the writes of each key, and it checks each read
result against two bounds:

  * the floor is the highest seq of a write that was acknowledged before the
    read started, so a read must not return an older value;
  * the ceiling is the highest seq of a write that started before the read
    ended, so a read must not return a value from the future.

These bounds hold for QUORUM writes and QUORUM reads when each acknowledged
write stays on a quorum of replicas, and they hold for a strongly consistent
keyspace too.  The check is weaker than a linearizability check (the Porcupine
checker in ``test.cluster.tools.porcupine`` does that one), but it needs no
search, so it runs on each read and it reports the read that broke the rule.

The workload gives each key to one writer and each write a seq from a counter
that only increases, and the write timestamp is a fixed base plus the seq.  So
the write with the higher seq wins on each replica, and the seq of a value
tells which write made it.

A delete has a seq too, and the model checks a read that returns no row
against the deletes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Hashable, Optional


class Kind(Enum):
    WRITE = "write"
    DELETE = "delete"


class Status(Enum):
    # The client did not get a result yet.
    PENDING = "pending"
    # The client got a success.
    OK = "ok"
    # The client got an error that says that the write did not happen.
    FAIL = "fail"
    # The client got an error that says nothing about the write, so the write
    # can be applied later, or never.
    UNKNOWN = "unknown"


@dataclass(slots=True)
class Entry:
    seq: int
    kind: Kind
    t_call_ns: int
    t_return_ns: Optional[int] = None
    status: Status = Status.PENDING


class KeyHistory:
    """The writes of one key, in seq order.

    One writer owns the key and it has one write in flight at a time, so the
    writes arrive in seq order and ``entries`` stays sorted.
    """

    __slots__ = ("entries", "pruned_below")

    def __init__(self) -> None:
        self.entries: list[Entry] = []
        # The seq of each entry that the model dropped is lower than this.
        self.pruned_below: int = 0

    def find(self, seq: int) -> Optional[Entry]:
        lo, hi = 0, len(self.entries)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.entries[mid].seq < seq:
                lo = mid + 1
            else:
                hi = mid
        if lo < len(self.entries) and self.entries[lo].seq == seq:
            return self.entries[lo]
        return None

    def floor(self, t_ns: int) -> Optional[Entry]:
        """Returns the entry with the highest seq that was acknowledged before ``t_ns``."""
        best = None
        for e in self.entries:
            if e.status is Status.OK and e.t_return_ns < t_ns:
                best = e
        return best


class Model:
    """The writes of all keys, and the reads that are in flight.

    The times are ``time.monotonic_ns()`` values, which the caller takes right
    before it sends a request and right after the result comes back.
    """

    # The model drops the old entries of a key when the key has more entries
    # than this.
    PRUNE_AT = 16

    def __init__(self) -> None:
        self._keys: dict[Hashable, KeyHistory] = {}
        self._open_reads: dict[int, int] = {}
        self._next_read_id = 0

    def history(self, key: Hashable) -> KeyHistory:
        h = self._keys.get(key)
        if h is None:
            h = self._keys[key] = KeyHistory()
        return h

    def keys(self):
        return self._keys.keys()

    def write_started(self, key: Hashable, seq: int, kind: Kind, t_call_ns: int) -> Entry:
        h = self.history(key)
        assert not h.entries or h.entries[-1].seq < seq, (
            f"key {key}: write seq {seq} is not above the last seq {h.entries[-1].seq}")
        entry = Entry(seq, kind, t_call_ns)
        h.entries.append(entry)
        return entry

    def write_finished(self, key: Hashable, entry: Entry, status: Status, t_return_ns: int) -> None:
        assert status is not Status.PENDING
        entry.status = status
        entry.t_return_ns = t_return_ns
        h = self._keys[key]
        if len(h.entries) > self.PRUNE_AT:
            self._prune(h)

    def read_started(self, t_call_ns: int) -> int:
        read_id = self._next_read_id
        self._next_read_id += 1
        self._open_reads[read_id] = t_call_ns
        return read_id

    def read_finished(self, read_id: int) -> None:
        del self._open_reads[read_id]

    def _prune(self, h: KeyHistory) -> None:
        # A read can return an entry older than the newest acknowledged entry
        # only if the read started before that entry was acknowledged.  So the
        # model keeps the newest entry that was acknowledged before the oldest
        # open read started, and all entries after it.
        horizon = min(self._open_reads.values(), default=None)
        keep_from = None
        for i, e in enumerate(h.entries):
            if e.status is Status.OK and (horizon is None or e.t_return_ns < horizon):
                keep_from = i
        if keep_from:
            h.pruned_below = h.entries[keep_from].seq
            del h.entries[:keep_from]

    def check_read(self, key: Hashable, t_call_ns: int, t_return_ns: int, seq: Optional[int]) -> Optional[str]:
        """Checks the result of one read of ``key``.

        ``seq`` is the seq of the value that the read returned, or None when
        the read returned no row.  Returns None when the result is allowed,
        or a message that says why it is not.
        """
        h = self._keys.get(key)
        entries = h.entries if h else []
        floor = h.floor(t_call_ns) if h else None
        floor_seq = floor.seq if floor else None

        def describe() -> str:
            shown = ", ".join(
                f"{e.kind.value}:{e.seq}:{e.status.value}"
                f"[{e.t_call_ns}..{e.t_return_ns if e.t_return_ns is not None else '?'}]"
                for e in entries[-8:])
            return (f"key={key} read=[{t_call_ns}..{t_return_ns}] floor={floor_seq} "
                    f"pruned_below={h.pruned_below if h else 0} history=[{shown}]")

        if seq is None:
            if floor is None:
                return None
            for e in reversed(entries):
                if e.seq < floor.seq:
                    break
                if e.kind is Kind.DELETE and e.status is not Status.FAIL and e.t_call_ns < t_return_ns:
                    return None
            return f"read returned no row, but a write that is not deleted was acknowledged before the read: {describe()}"

        if h is not None and seq < h.pruned_below:
            return f"read returned seq {seq}, which is older than a write acknowledged before the read: {describe()}"
        entry = h.find(seq) if h else None
        if entry is None:
            return f"read returned seq {seq}, which no write of this key has: {describe()}"
        if entry.kind is not Kind.WRITE:
            return f"read returned a row with seq {seq}, which is the seq of a delete: {describe()}"
        if entry.status is Status.FAIL:
            return f"read returned seq {seq}, whose write failed with an error that says it did not happen: {describe()}"
        if entry.t_call_ns >= t_return_ns:
            return f"read returned seq {seq}, whose write started after the read ended: {describe()}"
        if floor is not None and seq < floor.seq:
            return f"read returned seq {seq}, which is older than a write acknowledged before the read: {describe()}"
        return None

    def check_final(self, key: Hashable, t_ns: int, seq: Optional[int]) -> Optional[str]:
        """Checks a read that started after all writes ended, at ``t_ns``."""
        return self.check_read(key, t_ns, t_ns + 1, seq)

    def newest_acked(self, key: Hashable) -> Optional[Entry]:
        """Returns the acknowledged entry with the highest seq of ``key``."""
        h = self._keys.get(key)
        if h is None:
            return None
        for e in reversed(h.entries):
            if e.status is Status.OK:
                return e
        return None
