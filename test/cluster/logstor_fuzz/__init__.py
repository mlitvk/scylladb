#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The logstor fuzz test: a checked read and write workload on logstor tables.

  * ``params`` — the parameters, the profiles and the env var overrides;
  * ``model`` — the per-key history and the check of each read;
  * ``workload`` — the writers, the readers and the value codec;
  * ``operations`` — the cluster operations, and the errors each one may cause;
  * ``nemesis`` — the schedule of the operations, the event log and the hang checks;
  * ``failures`` — the injected failures and the loop that injects them;
  * ``known_issues`` — the one list of the skips, with their trackers;
  * ``checks`` — the checks after the run: CL=ALL scans, replica dumps, restart;
  * ``test_logstor_fuzz`` — one test per profile.
"""
