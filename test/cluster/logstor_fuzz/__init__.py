#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The logstor fuzz test: a checked read and write workload on logstor tables.

  * ``params`` — the parameters, the profiles and the env var overrides;
  * ``model`` — the per-key history and the check of each read;
  * ``workload`` — the writers, the readers and the value codec;
  * ``checks`` — the checks after the run: CL=ALL scans, replica dumps, restart;
  * ``test_logstor_fuzz`` — one test per profile.
"""
