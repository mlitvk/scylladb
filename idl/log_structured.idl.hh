/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/frozen_schema.idl.hh"
#include "mutation/canonical_mutation.hh"

namespace replica {
namespace log_structured {

struct index_key {
    uint64_t key;
};

class log_structured_segment_record {
    replica::log_structured::index_key key;
    canonical_mutation mut;
};

}
}