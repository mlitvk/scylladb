#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The known issues and the missing features that disable parts of the logstor fuzz test.

This module is the only place that turns off a part of the fuzz test, or
documents a failure of it, because of a bug or a missing feature.  Each entry
names the tracker (a Jira issue or a pull request), and the condition to remove
the entry.  When the fix lands, delete the entry.

An entry has two ways to act, and it must use at least one:

  * ``disables`` lists the items that the run does not do (a skip).  Use it
    for a failure that is frequent, or that ends the run early (a hang, a
    crash) and so hides the rest of the run.
  * ``signatures`` lists regexes that match the signature of a failure (see
    ``results.signature``).  A failure that matches still fails a run, so CI
    and a developer see it, but the gating procedure counts it as known and
    does not blame the branch under test (a tolerate).  Use it for a failure
    that is rare, when the run still checks everything else.

An item is a string ``<category>:<name>``:

  * ``op:<name>`` is a cluster operation of the nemesis;
  * ``failure:<name>`` is an injected failure;
  * ``workload:<name>`` is a workload option;
  * ``schema:<name>`` is a table schema option;
  * ``check:<name>`` is a step of the checks after the run;
  * ``profile:<name>`` is a whole profile.

The code asks :meth:`KnownIssues.enabled` before it runs an item, and it logs
the issue that disables the item.  ``LOGSTOR_FUZZ_INCLUDE_KNOWN`` runs the
disabled items anyway: ``all``, or a list of entry ids with commas.  This is
how to check a fix before its entry is deleted.

To list the entries with their trackers::

    python3 -m test.cluster.logstor_fuzz.known_issues
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

CATEGORIES = ("op", "failure", "workload", "schema", "check", "profile")
KINDS = ("bug", "missing-feature")


@dataclass(frozen=True)
class KnownIssue:
    # A short stable name, for the logs and for LOGSTOR_FUZZ_INCLUDE_KNOWN.
    id: str
    kind: str
    # The Jira issues and the pull requests that track the fix.
    trackers: tuple[str, ...]
    summary: str
    # The items that the issue disables.
    disables: tuple[str, ...]
    # The condition to delete the entry.
    remove_when: str
    # Regexes that match the signatures of the failures that the issue causes.
    signatures: tuple[str, ...] = ()

    @property
    def action(self) -> str:
        return "+".join(a for a, on in (("skip", self.disables), ("tolerate", self.signatures)) if on)


KNOWN_ISSUES: tuple[KnownIssue, ...] = (
    KnownIssue(
        id="logstor-repair",
        kind="missing-feature",
        trackers=("SCYLLADB-2722", "SCYLLADB-3593", "https://github.com/scylladb/scylladb/pull/31126"),
        summary="Logstor tables have no repair.  A removenode or a replace rebuilds a tablet replica "
                "from one replica and then repairs it (rebuild_v2), so the new replica can miss "
                "acknowledged writes, and a quorum can lose a write.  The end check cannot make the "
                "replicas equal, so it does not require it.",
        disables=("op:remove_node", "op:replace_node", "op:repair", "check:repair_and_compare_replicas"),
        remove_when="Full tablet repair of logstor tables is merged (SCYLLADB-2722).",
    ),
    KnownIssue(
        id="logstor-read-repair",
        kind="missing-feature",
        trackers=("SCYLLADB-3493",),
        summary="Read repair sends a diff of the row to a stale replica, and logstor applies only whole "
                "rows.  The default schema has one regular column and each write sets it, so a diff is "
                "a whole row; a schema with more columns can make read repair write part of a row.",
        disables=("schema:multi_column",),
        remove_when="Read repair sends whole rows to logstor replicas (SCYLLADB-3493).",
    ),
    KnownIssue(
        id="logstor-full-store-hang",
        kind="bug",
        trackers=("SCYLLADB-6165",),
        summary="A write that waits for a segment on a full store waits for ever, holds a token "
                "metadata version, and so blocks the topology barriers.  A run that fills the store "
                "hangs instead of failing the writes.",
        disables=("profile:disk_pressure",),
        remove_when="A write on a full store fails with an out-of-space error (SCYLLADB-6165, the "
                    "out-of-space plan).",
    ),
    KnownIssue(
        id="logstor-start-hang",
        kind="bug",
        trackers=("no ticket yet: the logstor re-separation plan",),
        summary="After a kill, a shard whose recovery leaves only 8 of its 128 segments free logs "
                "'Starting logstor' and nothing more, so the node does not start.  The shard stops "
                "before the separation of the recovered segments, probably in the wait for a free "
                "segment.  Found in 2 of 100 dev runs of the operations profiles in Nov 2026.",
        disables=(),
        signatures=(r"^hang: <ip> did not start in <n>s: logstor did not finish its start on shard",),
        remove_when="A shard that recovery leaves with only the reserved segments free finishes its "
                    "start (the logstor re-separation plan).",
    ),
    KnownIssue(
        id="logstor-partial-insert",
        kind="missing-feature",
        trackers=("no ticket yet: the logstor-cql plan (~/.claude/plans/logstor-cql.md)",),
        summary="An INSERT that sets only some columns replaces the whole row on a logstor table, so "
                "the other columns become null, which is not what the CQL layer and CDC expect.",
        disables=("workload:partial_writes",),
        remove_when="The CQL layer checks or completes a partial INSERT on a logstor table.",
    ),
)


def _validate(issues: Iterable[KnownIssue]) -> None:
    ids = set()
    for issue in issues:
        if issue.id in ids:
            raise ValueError(f"known issue {issue.id} is listed twice")
        ids.add(issue.id)
        if issue.kind not in KINDS:
            raise ValueError(f"known issue {issue.id}: kind {issue.kind!r} is not one of {KINDS}")
        if not issue.trackers or not issue.remove_when:
            raise ValueError(f"known issue {issue.id}: trackers and remove_when must not be empty")
        if not issue.disables and not issue.signatures:
            raise ValueError(f"known issue {issue.id}: it must have disables, signatures, or both")
        for item in issue.disables:
            check_item(item)
        for pattern in issue.signatures:
            re.compile(pattern)


def check_item(item: str) -> None:
    category, _, name = item.partition(":")
    if category not in CATEGORIES or not name:
        raise ValueError(f"item {item!r} is not '<category>:<name>' with a category of {CATEGORIES}")


class KnownIssues:
    """The known issues of one run, with the entries that the run includes anyway."""

    def __init__(self, include: Iterable[str] = (), issues: tuple[KnownIssue, ...] = KNOWN_ISSUES) -> None:
        _validate(issues)
        self.issues = issues
        include = set(include)
        if "all" in include:
            include = {i.id for i in issues}
        unknown = include - {i.id for i in issues}
        if unknown:
            raise ValueError(f"LOGSTOR_FUZZ_INCLUDE_KNOWN names unknown entries: {sorted(unknown)}")
        self.included = include

    def blocking(self, item: str) -> Optional[KnownIssue]:
        """Returns the known issue that disables ``item`` in this run, or None."""
        check_item(item)
        for issue in self.issues:
            if item in issue.disables and issue.id not in self.included:
                return issue
        return None

    def enabled(self, item: str) -> bool:
        """Returns True when no known issue disables ``item``, and logs the issue when one does."""
        issue = self.blocking(item)
        if issue is not None:
            logger.info("Skipping %s: known issue %s (%s)", item, issue.id, ", ".join(issue.trackers))
            return False
        return True

    def match(self, signature: str, included: bool = False) -> Optional[KnownIssue]:
        """Returns the known issue whose signatures match the signature of a failure, or None.

        With ``included`` False, the match skips the issues that this run
        includes, because a run includes an issue to check its fix.  With
        ``included`` True, the match looks only at those issues.
        """
        for issue in self.issues:
            if (issue.id in self.included) == included and any(re.search(p, signature) for p in issue.signatures):
                return issue
        return None

    def summary(self) -> str:
        lines = []
        for issue in self.issues:
            state = "INCLUDED" if issue.id in self.included else "disables"
            parts = [f"{state} {', '.join(issue.disables)}"] if issue.disables else []
            if issue.signatures:
                parts.append(f"tolerates {len(issue.signatures)} signature(s)")
            lines.append(f"{issue.id} [{issue.kind}, {', '.join(issue.trackers)}]: {'; '.join(parts)}")
        return "\n".join(lines)


_JIRA_KEY = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")
_PULL_URL = re.compile(r"/pull/(\d+)")


def fixed_by(text: str, issues: tuple[KnownIssue, ...] = KNOWN_ISSUES) -> dict[str, str]:
    """Returns the known issues whose trackers ``text`` names, each with the tracker found.

    ``text`` is what a branch says about itself: its commit messages and its
    machete annotation.  A Jira key matches as a word.  A pull request matches
    as its URL, ``PR 123``, ``PR #123``, ``pull/123`` or ``#123``.
    """
    found = {}
    for issue in issues:
        for tracker in issue.trackers:
            if _JIRA_KEY.fullmatch(tracker):
                if re.search(rf"\b{re.escape(tracker)}\b", text):
                    found.setdefault(issue.id, tracker)
            elif m := _PULL_URL.search(tracker):
                n = m.group(1)
                if re.search(rf"(?:\bPR ?#?|pull/|#){n}\b", text):
                    found.setdefault(issue.id, tracker)
    return found


def main() -> None:
    _validate(KNOWN_ISSUES)
    for issue in KNOWN_ISSUES:
        print(f"{issue.id} ({issue.kind})")
        print(f"  trackers:    {', '.join(issue.trackers)}")
        print(f"  disables:    {', '.join(issue.disables)}")
        print(f"  action:      {issue.action}")
        if issue.signatures:
            print(f"  signatures:  {'; '.join(issue.signatures)}")
        print(f"  remove when: {issue.remove_when}")
        print(f"  summary:     {issue.summary}")
        print()


if __name__ == "__main__":
    main()
