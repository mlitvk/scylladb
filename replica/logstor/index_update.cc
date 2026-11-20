/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/index_update.hh"

#include "replica/logstor/index.hh"

namespace replica::logstor {

void index_insert::on_written(record_location location) noexcept {
    try {
        index->insert(key, index_entry{
            .location = location,
            .timestamp = timestamp,
        });
    } catch (...) {
        done.set_exception(std::current_exception());
        return;
    }
    done.set_value();
}

void index_insert::on_failed(std::exception_ptr ex) noexcept {
    done.set_exception(std::move(ex));
}

void index_relocation::on_written(record_location new_location) noexcept {
    if (index->update_record_location(key, old_location, new_location) && relocated) {
        ++*relocated;
    }
}

}
