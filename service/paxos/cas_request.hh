/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <optional>

#include "cdc/log.hh"
#include "mutation/timestamp.hh"
#include <seastar/core/sharded.hh>

class mutation;

namespace query {

class result;
class partition_slice;

} // namespace query

namespace service {

// An instance of this class is passed as an argument to storage_proxy::cas().
// The apply() method, which must be defined by the implementation. It can
// either return a mutation that will be used as a value for paxos 'propose'
// stage or it can return an empty option in which case an empty mutation will
// be used
class cas_request {
public:
    virtual ~cas_request() = default;
    // it is safe to dereference and use the qr foreign pointer, the result was
    // created by a foreign shard but no longer used by it.
    virtual std::optional<mutation> apply(seastar::foreign_ptr<seastar::lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts, cdc::per_request_options& cdc_opts) = 0;
    // When true, the proxy reads the whole partition in mutation form after the prepare round
    // and calls apply_to_partition() instead of apply(). The prepare round then reads nothing.
    virtual bool needs_partition() const { return false; }
    // partition is reconciled across a quorum of replicas, or nullopt when no replica has it.
    virtual std::optional<mutation> apply_to_partition(std::optional<mutation> partition,
            api::timestamp_type ts, cdc::per_request_options& cdc_opts);
};

} // namespace service
