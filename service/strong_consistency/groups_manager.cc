/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "groups_manager.hh"

#include "service/migration_manager.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "gms/feature_service.hh"
#include "service/raft/raft_rpc.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "locator/tablet_metadata_guard.hh"
#include "utils/composite_abort_source.hh"
#include "utils/exponential_backoff_retry.hh"

#include <seastar/core/abort_source.hh>

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

class groups_manager::rpc_impl: public service::raft_rpc {
    raft_group_state& _group_state;
public:
    rpc_impl(raft_state_machine& sm, netw::messaging_service& ms,
             shared_ptr<raft::failure_detector> failure_detector,
             raft::group_id gid, raft::server_id my_id,
             raft_group_state& group_state)
        : service::raft_rpc(sm, ms, std::move(failure_detector), gid, my_id)
        , _group_state(group_state)

    {
    }

    void on_configuration_change(raft::server_address_set add, raft::server_address_set del) override {
        // Wake the non-leader config-change fiber so it can re-evaluate the config.
        if (_group_state.config_change_waiting && !_group_state.config_change_waiting->abort_requested()) {
            _group_state.config_change_waiting->request_abort();
        }
    }
};

static void for_each_sc_tablet(const token_metadata& tm,
    noncopyable_function<void(global_tablet_id, raft::group_id)>&& func)
{
    const auto this_replica = locator::tablet_replica {
        .host = tm.get_my_id(),
        .shard = this_shard_id()
    };
    const auto& tablets = tm.tablets();
    for (const auto& [table_id, _]: tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(table_id);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tablet_id: tablet_map.tablet_ids()) {
            if (tablet_map.has_replica(tablet_id, this_replica)) {
                const auto group_id = tablet_map.get_tablet_raft_info(tablet_id).group_id;
                func(global_tablet_id{table_id, tablet_id}, group_id);
            }
        }
    }
}

raft_server::raft_server(groups_manager::raft_group_state& state, gate::holder holder)
    : _state(state)
    , _holder(std::move(holder))
{
}

// conditional_variable::wait doesn't have an overload taking an abort_source.
// This is a temporary workaround until we extend the interface.
// See: scylladb/seastar#3292.
static future<> wait_with_abort_source(condition_variable& cv, abort_source& as) {
    as.check();
    const auto _ = as.subscribe([&cv] noexcept { cv.broadcast(); });
    co_await cv.wait();
    as.check();
}

auto raft_server::begin_mutate(abort_source& as) -> begin_mutate_result {
    const auto leader = _state.server->current_leader();
    if (!leader) {
        return need_wait_for_leader{_state.server->wait_for_leader(&as)};
    }
    if (leader != _state.server->id()) {
        return raft::not_a_leader{leader};
    }
    const auto term = _state.server->get_current_term();
    if (!_state.leader_info || _state.leader_info->term != term) {
        // We are the leader, but the leader_info_updater fiber hasn't processed
        // the state change yet (leader_info is either empty or stale).
        //
        // We must wait for the updater to catch up. It is safe to wait on
        // leader_info_cond because the updater fiber guarantees a broadcast
        // after every state change wake-up. This ensures we will not deadlock,
        // even if the raft server state changes again (e.g., we lose leadership)
        // before the updater gets a chance to run.
        return need_wait_for_leader{wait_with_abort_source(_state.leader_info_cond, as)};
    }
    const auto new_ts = std::max(api::new_timestamp(), _state.leader_info->last_timestamp + 1);
    _state.leader_info->last_timestamp = new_ts;
    return timestamp_with_term{new_ts, term};
}

groups_manager::groups_manager(netw::messaging_service& ms, 
        raft_group_registry& raft_gr, cql3::query_processor& qp,
        replica::database& db, service::migration_manager& mm, db::system_keyspace& sys_ks, gms::feature_service& features)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
    , _db(db)
    , _mm(mm)
    , _sys_ks(sys_ks)
    , _features(features)
{
}

future<> groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm,
        raft_group_state& state)
{
    const auto my_id = to_server_id(tm->get_my_id());
    const auto this_replica = locator::tablet_replica{
        .host = tm->get_my_id(),
        .shard = this_shard_id(),
    };

    auto state_machine = make_state_machine(tablet, group_id, _db, _mm, _sys_ks);
    auto& state_machine_ref = *state_machine;
    auto rpc = std::make_unique<rpc_impl>(state_machine_ref, _ms, _raft_gr.failure_detector(), group_id, my_id, state);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_groups_storage>(_qp, group_id, my_id, this_shard_id());

    // Store the initial configuration if this is the first time we create this group
    // on this node
    const auto snapshot = co_await storage->load_snapshot_descriptor();
    if (!snapshot.id) {
        const auto& tablet_map = tm->tablets().get_tablet_map(tablet.table);
        const auto& tablet_info = tablet_map.get_tablet_info(tablet.tablet);
        const auto* trinfo = tablet_map.get_tablet_transition_info(tablet.tablet);

        const bool joining_replica = trinfo
            && locator::contains(trinfo->next, this_replica)
            && !locator::contains(tablet_info.replicas, this_replica);

        // for joining nodes use an empty configuration.
        // for now set nontrivial_snapshot to true because snapshot transfer is not implemented yet.
        raft::configuration configuration;
        bool nontrivial_snapshot = true;

        if (!joining_replica) {
            configuration.current.reserve(tablet_info.replicas.size());
            for (const auto& r: tablet_info.replicas) {
                configuration.current.emplace(raft::server_address{to_server_id(r.host), {}},
                    raft::is_voter::yes);
            }
            nontrivial_snapshot = true;
        }

        co_await storage->bootstrap(std::move(configuration), nontrivial_snapshot);
    }

    auto& persistence_ref = *storage;
    auto config = raft::server::configuration {
        // Snapshotting is not implemented yet for strong consistency,
        // so effectively disable periodic snapshotting.
        // TODO: Revert after snapshots are implemented
        .snapshot_threshold = std::numeric_limits<size_t>::max(),
        .snapshot_threshold_log_size = 10 * 1024 * 1024, // 10MB
        .max_log_size = 20 * 1024 * 1024, // 20MB
        .enable_forwarding = false,
        .on_background_error = [tablet, group_id](std::exception_ptr e) {
            on_internal_error(logger, 
                ::format("table {}, tablet {} raft group {} background error {}", 
                    tablet.table, tablet.tablet, group_id, e));
        }
    };
    auto server = raft::create_server(my_id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), config);

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });
    co_await _raft_gr.start_server_for_group(raft_server_for_group {
        .gid = group_id,
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .state_machine = state_machine_ref
    });
}

void groups_manager::schedule_raft_group_deletion(raft::group_id id, raft_group_state& state) {
    if (state.gate->is_closed()) {
        return;
    }
    logger.info("schedule_raft_group_deletion(): group id {}: scheduling", id);
    state.server_control_op = futurize_invoke([this, &state, id, g = state.gate](this auto) -> future<> {
        co_await state.server_control_op.get_future();
        logger.debug("schedule_raft_group_deletion(): group id {}: starting", id);

        co_await g->close();
        logger.debug("schedule_raft_group_deletion(): group id {}: gate closed", id);

        co_await _raft_gr.abort_server(id);
        logger.debug("schedule_raft_group_deletion(): group id {}: server aborted", id);

        co_await std::move(state.leader_info_updater);

        _raft_gr.destroy_server(id);
        logger.info("schedule_raft_group_deletion(): raft server for group id {} is destroyed", id);

        // We need to erase the raft group state only if we are still the last operation on it.
        // If another start arrived while we were stopping the raft server, a new gate
        // would have been assigned, and we should leave the state in the map.
        if (state.gate.get() == g.get() && _raft_groups.erase(id) != 1) {
            on_internal_error(logger, format("raft group {} is already deleted", id));
        }
    });
}

void groups_manager::schedule_raft_groups_deletion(bool all) {
    for (auto it = _raft_groups.begin(); it != _raft_groups.end(); ) {
        const auto next = std::next(it);
        auto& [group_id, group_state] = *it;
        if (all || !group_state.has_tablet) {
            schedule_raft_group_deletion(group_id, group_state);
        }
        it = next;
    }
}

future<> groups_manager::wait_for_groups_to_start() {
    while (true) {
        const auto it = std::ranges::find_if(_raft_groups, [](const auto& p) {
            auto& state = p.second;
            return !state.gate->is_closed() && !state.server_control_op.available();
        });
        if (it == _raft_groups.end()) {
            break;
        }

        const auto& [id, state] = *it;
        logger.info("waiting for group {} to start", id);
        co_await state.server_control_op.get_future();
    }
}

future<> groups_manager::leader_info_updater(raft_group_state& state, global_tablet_id tablet, raft::group_id gid) {
    try {
        const auto schema = _db.find_schema(tablet.table);
        const auto server_id = state.server->id();

        while (true) {
            const auto current_term = state.server->get_current_term();
            const auto current_leader = state.server->current_leader();

            if (current_leader == server_id) {
                logger.debug("leader_info_updater({}-{}): current term {}, running read_barrier()",
                    tablet, gid,
                    current_term);
                // We intentionally pass nullptr here. If the tablet is leaving this node,
                // the Raft server will be aborted and the loop will break.
                // The same will happen when the node is shutting down.
                // There's no reason to abort this operation in any other case.
                co_await state.server->read_barrier(nullptr);

                co_await utils::get_local_injector().inject("sc_leader_info_updater_wait_before_setting_leader_info",
                    utils::wait_for_message(5min));

                state.leader_info = leader_info {
                    .term = current_term,
                    .last_timestamp = schema->table().get_max_timestamp_for_tablet(tablet.tablet)
                };
                logger.debug("leader_info_updater({}-{}): read_barrier() completed, "
                    "new leader term {}, last_timestamp {}",
                    tablet, gid,
                    state.leader_info->term,
                    state.leader_info->last_timestamp);
            } else if (state.leader_info) {
                logger.debug("leader_info_updater({}-{}): this replica {} is no longer a leader, current leader {}",
                    tablet, gid, server_id, current_leader);
                state.leader_info = std::nullopt;
            }
            state.leader_info_cond.broadcast();

            // We intentionally pass nullptr here. If the tablet is leaving this node,
            // the Raft server will be aborted and the loop will break.
            // The same will happen when the node is shutting down.
            // There's no reason to abort this operation in any other case.
            co_await state.server->wait_for_state_change(nullptr);
        }
    } catch (const raft::request_aborted&) {
        // thrown from read_barrier() and wait_for_state_change when the tablet leaves this shard
        logger.debug("leader_info_updater({}-{}): got raft::request_aborted {}",
            tablet, gid, std::current_exception());
    } catch (const raft::stopped_error&) {
        // thrown from read_barrier() and wait_for_state_change when the tablet leaves this shard
        logger.debug("leader_info_updater({}-{}): got raft::stopped_error {}",
            tablet, gid, std::current_exception());
    } catch (const replica::no_such_column_family&) {
        // thrown from find_schema() and schema->table() when the table is dropped
        logger.debug("leader_info_updater({}-{}): got replica::no_such_column_family {}",
            tablet, gid, std::current_exception());
    } catch (...) {
        on_internal_error(logger, ::format("leader_info_updater({}-{}): unexpected exception: {}",
            tablet, gid, std::current_exception()));
    }
}

void groups_manager::maybe_update_group_configuration(raft_group_state& state, global_tablet_id tablet, raft::group_id id, const token_metadata& tm) {
    static constexpr auto config_change_timeout = std::chrono::seconds(60);
    static constexpr auto config_change_read_barrier_timeout = std::chrono::minutes(5);

    const auto& tablet_map = tm.tablets().get_tablet_map(tablet.table);
    const auto& tinfo = tablet_map.get_tablet_info(tablet.tablet);
    const auto* trinfo = tablet_map.get_tablet_transition_info(tablet.tablet);

    const auto current_stage = trinfo ? std::optional(trinfo->stage) : std::nullopt;
    if (state.migration_action_stage != current_stage) {
        state.migration_action_stage.reset();
    } else {
        // action is already running for this stage.
        return;
    }

    const auto this_replica = locator::tablet_replica{
        .host = tm.get_my_id(),
        .shard = this_shard_id(),
    };

    if (!trinfo) {
        return;
    }

    std::vector<raft::config_member> to_add;
    std::vector<raft::server_id> to_del;
    bool do_stepdown = false;
    bool do_read_barrier = false;

    switch (trinfo->stage) {
        case tablet_transition_stage::allow_write_both_read_old:
            // Add the pending replica as nonvoter.
            if (trinfo->pending_replica) {
                const auto pending_id = to_server_id(trinfo->pending_replica->host);
                to_add.push_back(raft::config_member{raft::server_address{pending_id, {}}, raft::is_voter::no});
            }
            break;
        case tablet_transition_stage::write_both_read_old:
            break;
        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
            break;
        case tablet_transition_stage::streaming:
            break;
        case tablet_transition_stage::rebuild_repair:
            break;
        case tablet_transition_stage::write_both_read_new:
            // The pending replica becomes a voter.
            if (trinfo->pending_replica) {
                const auto pending_id = to_server_id(trinfo->pending_replica->host);
                to_add.push_back(raft::config_member{raft::server_address{pending_id, {}}, raft::is_voter::yes});

                // The pending replica executes a read barrier to wait for it catching up before it starts
                // serving requests in use_new.
                if (*trinfo->pending_replica == this_replica) {
                    do_read_barrier = true;
                }
            }
            break;
        case tablet_transition_stage::use_new:
            // The leaving replica becomes a nonvoter and steps down as leader.
            if (auto leaving = locator::get_leaving_replica(tinfo, *trinfo)) {
                if (!locator::contains(trinfo->next, leaving->host)) {
                    auto leaving_id = to_server_id(leaving->host);
                    to_add.push_back(raft::config_member{raft::server_address{leaving_id, {}}, raft::is_voter::no});

                    if (*leaving == this_replica) {
                        do_stepdown = true;
                    }
                }
            }
            break;
        case tablet_transition_stage::raft_group_cleanup:
            // Remove the leaving replica from the raft group.
            if (auto leaving = locator::get_leaving_replica(tinfo, *trinfo)) {
                if (!locator::contains(trinfo->next, leaving->host)) {
                    auto leaving_id = to_server_id(leaving->host);
                    to_del.push_back(leaving_id);
                }
            }
            break;
        case tablet_transition_stage::cleanup:
            break;
        case tablet_transition_stage::raft_group_cleanup_target:
            // Remove the pending replica from the raft group.
            if (trinfo->pending_replica && !locator::contains(tinfo.replicas, trinfo->pending_replica->host)) {
                auto pending_id = to_server_id(trinfo->pending_replica->host);
                to_del.push_back(pending_id);
            }
            break;
        case tablet_transition_stage::cleanup_target:
            break;
        case tablet_transition_stage::revert_migration:
            break;
        case tablet_transition_stage::end_migration:
            break;
        case tablet_transition_stage::repair:
            break;
        case tablet_transition_stage::end_repair:
            break;
    }

    if (to_add.empty() && to_del.empty() && !do_read_barrier) {
        return;
    }

    state.migration_action_stage = trinfo->stage;

    logger.debug("maybe_update_group_configuration(): "
        "starting config change fiber for raft group {} tablet {}: to_add={}, to_del={}, do_read_barrier={}",
        id, tablet, to_add, to_del, do_read_barrier);

    // Every replica (not just the leader) runs this fiber. The fiber loops until the
    // configuration matches the expected state or the raft server stops:
    //  1. Recheck which changes are still needed against the current raft config.
    //  2. If nothing remains, stop reconciling config.
    //  3. Wait until a leader is known.
    //  4. If this replica is the leader, call modify_config with the pending changes.
    //  5. On transient errors loop back to step 1. On stopped_error/request_aborted exit.
    // After the config is reconciled, the pending replica may still need to run a
    // read barrier before we let the coordinator advance to use_new.
    state.server_control_op = futurize_invoke([this, &state, id, to_add = std::move(to_add), to_del = std::move(to_del), do_stepdown, do_read_barrier, tablet](this auto) -> future<> {
        locator::tablet_metadata_guard guard(_db.find_column_family(tablet.table), tablet);
        auto retry = exponential_backoff_retry(10ms, 10s);
        abort_on_expiry config_change_aoe(lowres_clock::now() + config_change_timeout);
        co_await state.server_control_op.get_future();
        while (true) {
            // Wait for a leader to be elected.
            try {
                co_await state.server->wait_for_leader(&config_change_aoe.abort_source());
            } catch (const raft::stopped_error&) {
                co_return;
            }

            // Recheck which changes are still pending against the live raft config.
            const auto& current_config = state.server->get_configuration().current;
            std::vector<raft::config_member> add;
            for (const auto& member : to_add) {
                auto it = current_config.find(member.addr.id);
                if (it == current_config.end() || it->can_vote != member.can_vote) {
                    add.push_back(member);
                }
            }
            std::vector<raft::server_id> del;
            for (const auto& host : to_del) {
                if (current_config.contains(host)) {
                    del.push_back(host);
                }
            }

            if (add.empty() && del.empty()) {
                logger.debug("maybe_update_group_configuration(): raft group {} tablet {} config change completed, current config: {}",
                    id, tablet, current_config);
                break;
            }

            logger.debug("maybe_update_group_configuration(): raft group {} tablet {} pending config change: to_add={}, to_del={}, current config: {}",
                id, tablet, add, del, current_config);

            if (!state.server->current_leader()) {
                continue;
            }

            if (state.server->current_leader() != state.server->id()) {
                // We are not the leader; wait for either a role change (which
                // might make us the leader) or a configuration change (which
                // means the current leader already applied the pending change).
                //
                // wait_for_state_change() only fires on Raft FSM role transitions
                // (follower/candidate/leader), NOT on configuration commits. We
                // therefore use an abort_source that rpc_impl::on_configuration_change
                // signals whenever a new configuration entry is received by this
                // replica, so we re-evaluate the config in either case.
                abort_source config_changed_as;
                utils::composite_abort_source as;
                as.add(config_change_aoe.abort_source());
                as.add(config_changed_as);
                state.config_change_waiting = &config_changed_as;
                auto clear_config_change_waiting = defer([&state] {
                    state.config_change_waiting = nullptr;
                });
                try {
                    co_await state.server->wait_for_state_change(&as.abort_source());
                } catch (const raft::stopped_error&) {
                    co_return;
                } catch (const raft::request_aborted&) {
                    if (config_change_aoe.abort_source().abort_requested()) {
                        throw;
                    }
                    // Woken by on_configuration_change (config entry received) or
                    // by the abort_source from the server itself.  Re-check the
                    // config at the top of the loop.
                }
                continue;
            }

            // We are the leader — apply the config change.
            try {
                logger.debug("maybe_update_group_configuration(): applying config change for group {} tablet {}: to_add={}, to_del={}", id, tablet, add, del);
                co_await state.server->modify_config(std::move(add), std::move(del), &config_change_aoe.abort_source());
                break;
            } catch (const raft::stopped_error&) {
                co_return;
            } catch (const raft::request_aborted&) {
                throw;
            } catch (...) {
                logger.warn("maybe_update_group_configuration(): "
                    "error updating config for group {} tablet {}: {}",
                    id, tablet, std::current_exception());
            }

            // Transient error: retry
            co_await retry.retry();
        }

        if (do_read_barrier) {
            abort_on_expiry read_barrier_aoe(lowres_clock::now() + config_change_read_barrier_timeout);
            co_await state.server->read_barrier(&read_barrier_aoe.abort_source());
            logger.debug("maybe_update_group_configuration(): pending replica read barrier completed for raft group {} tablet {}",
                id, tablet);
        }

        if (do_stepdown && state.server->is_leader()) {
            const auto stepdown_timeout_ticks = std::chrono::seconds(5) / raft_tick_interval;
            co_await state.server->stepdown(raft::logical_clock::duration(stepdown_timeout_ticks));
        }
    }).handle_exception([&state, id, tablet] (std::exception_ptr ep) {
        logger.warn("maybe_update_group_configuration(): action failed for group {} tablet {}: {}",
            id, tablet, ep);
        state.migration_action_stage.reset();
    });
}

void groups_manager::update(token_metadata_ptr new_tm) {
    if (!_features.strongly_consistent_tables) {
        return;
    }

    if (!_started) {
        _pending_tm = new_tm;
        return;
    }

    for (auto& [id, state]: _raft_groups) {
        state.has_tablet = false;
    }

    for_each_sc_tablet(*new_tm, [&](global_tablet_id tablet, raft::group_id id) {
        auto& state = _raft_groups[id];
        state.has_tablet = true;

        // Don't start the raft server if it is already (started or starting) and not stopping.
        if (state.gate && !state.gate->is_closed()) {
            maybe_update_group_configuration(state, tablet, id, *new_tm);
            return;
        }

        logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
        state.gate = make_lw_shared<gate>();
        state.server_control_op = futurize_invoke([&state, this, tablet, id, new_tm](this auto) -> future<> {
            co_await state.server_control_op.get_future();
            co_await start_raft_group(tablet, id, std::move(new_tm), state);
            state.server = &_raft_gr.get_server(id);
            state.leader_info_updater = leader_info_updater(state, tablet, id);
            logger.info("update(): raft server for tablet {} and group id {} is started", tablet, id);
        });
    });

    schedule_raft_groups_deletion(false);
}

future<raft_server> groups_manager::acquire_server(table_id table_id, raft::group_id group_id, abort_source& as) {
    if (!_features.strongly_consistent_tables) {
        on_internal_error(logger, "strongly consistent tables are not enabled on this shard");
    }

    // A concurrent DROP TABLE may have already removed the table from database
    // registries and erased the raft group from _raft_groups via
    // schedule_raft_group_deletion.  The schema.table() in create_operation_ctx()
    // might not fail though in this case because someone might be holding
    // lw_shared_ptr<table>, so that the table is dropped but the table object
    // is still alive.
    //
    // Check that the table still exists in the database to turn the
    // fatal on_internal_error below into a clean no_such_column_family
    // exception.
    //
    // When the table does exist, we proceed to acquire state.gate->hold().
    // This prevents schedule_raft_group_deletion (which co_awaits gate::close)
    // from erasing the group until the DML operation completes.
    _db.find_column_family(table_id);

    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        on_internal_error(logger, format("raft group {} not found", group_id));
    }
    auto& state = it->second;
    return state.server_control_op.get_future(as).then([&state, h = state.gate->hold()] mutable {
        return raft_server(state, std::move(h));
    });
}

future<> groups_manager::start() {
    _started = true;

    if (!_features.strongly_consistent_tables) {
        co_return;
    }

    if (_pending_tm) {
        update(std::move(_pending_tm));
        co_await wait_for_groups_to_start();
    }
}

future<> groups_manager::stop() {
    if (!_started) {
        co_return;
    }

    logger.info("stop() enter");

    schedule_raft_groups_deletion(true);

    while (!_raft_groups.empty()) {
        co_await _raft_groups.begin()->second.server_control_op.get_future();
    }

    logger.info("stop() completed");
}

}
