/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The delay injection points of logstor. A test enables a point with the error injection REST
// API and the parameter `delay_ms`, and the code at the point waits that long, so that the test
// can change the order of the steps that run at the same time. The logstor fuzz test
// (test/cluster/logstor_fuzz) enables them at random.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

#include "utils/error_injection.hh"

namespace replica::logstor {

// Returns the delay of the injection point `name`, or nullopt when the point is not enabled or
// has no `delay_ms` parameter. A release build compiles the injector out, so the result is
// always nullopt there, and the branch at the point costs nothing on the data path.
inline std::optional<std::chrono::milliseconds> injected_delay(std::string_view name) {
    auto ms = utils::get_local_injector().inject_parameter<uint32_t>(name, "delay_ms");
    if (!ms) {
        return std::nullopt;
    }
    return std::chrono::milliseconds(*ms);
}

} // namespace replica::logstor
