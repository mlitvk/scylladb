/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_driver.hh"
#include "replica/logstor/compaction.hh"
#include <exception>
#include <optional>
#include <ranges>
#include <utility>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

namespace replica::logstor {

extern seastar::logger logstor_logger;

compaction_driver::compaction_driver(host& h, config cfg)
    : _host(h)
    , _cfg(std::move(cfg))
    , _rate_timer([this] { tick_rate_controller(); })
{}

void compaction_driver::start(disk_geometry geometry) {
    _geometry = geometry;
    set_space_floor(_cfg.space_floor());
    _space_floor_observer.emplace(_cfg.space_floor.observe([this] (double fraction) {
        logstor_logger.info("Updating logstor compaction space floor to {}", fraction);
        set_space_floor(fraction);
    }));

    _last_rate_tick = lowres_clock::now() - compaction_rate_control_period;
    tick_rate_controller();
    _rate_timer.arm_periodic(compaction_rate_control_period);
    _completion = shared_future(with_scheduling_group(_cfg.sg, [this] {
        return run();
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
    }));
}

void compaction_driver::request_stop() noexcept {
    _stopping = true;
    _rate_timer.cancel();
    _work_wanted.broken();
}

void compaction_driver::set_space_floor(double fraction) noexcept {
    if (fraction > max_space_floor_fraction) {
        logstor_logger.warn("logstor_compaction_space_floor {} is above the maximum of {}. The maximum is used",
                fraction, max_space_floor_fraction);
    }
    _space_floor = compaction_space_floor(_geometry.segment_count, fraction);
    _limits = make_compaction_limits(_space_floor, max_segments_per_compaction);

    // The jobs in flight keep their slots. When the parallelism decreases, the semaphore goes below
    // zero, and no job starts until enough of them end. After the stop, run() waits for all the slots.
    if (!_stopping) {
        const auto held_back = max_auto_compaction_parallelism - _limits.auto_parallelism;
        if (_held_back.count() > held_back) {
            _held_back.return_units(_held_back.count() - held_back);
        } else {
            _held_back.adopt(consume_units(_sem, held_back - _held_back.count()));
        }
    }

    logstor_logger.debug("Compaction space floor {} segments, compaction parallelism {}, up to {} segments per compaction",
            _space_floor, _limits.auto_parallelism, _limits.batch_cap);
}

future<> compaction_driver::stop() {
    auto f = co_await coroutine::as_future(_completion.get_future());
    f.ignore_ready_future();
}

compaction_batch_limits compaction_driver::automatic_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
        .min_batch = _rate_controller.min_batch(),
        .max_utilization = _rate_controller.utilization_threshold(),
    };
}

compaction_batch_limits compaction_driver::explicit_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
    };
}

utilization_threshold_curve compaction_driver::threshold_curve() const noexcept {
    return make_utilization_threshold_curve(_geometry.segment_count, _space_floor,
            _cfg.utilization_threshold(), _cfg.threshold_anchor(),
            _cfg.threshold_ceiling(), _cfg.min_utilization_threshold());
}

void compaction_driver::tick_rate_controller() noexcept {
    const auto now = lowres_clock::now();
    const auto dt = std::chrono::duration<double>(now - _last_rate_tick).count();
    // A period of zero gives no rate. Its counts go to the next tick.
    if (!(dt > 0)) {
        return;
    }
    _last_rate_tick = now;

    const auto shard = _host.sample();
    const auto allocated = shard.reclaimable_segments_allocated;

    _rate_controller.tick({
        .available_segments = shard.available_segments,
        .curve = threshold_curve(),
        .utilization = shard.utilization,
        .segments_allocated = allocated - _last_segments_allocated,
        .burst_cap = double(_limits.batch_cap),
        .dt = dt,
        .candidates_empty = _no_candidates,
        .throttled = _throttled_since_tick || _wait == wait_reason::credit,
    });
    _candidate_counts = candidate_counts{
        .under_threshold = _rate_controller.backlog().candidate_segments,
        .dead = shard.dead_segments,
    };
    _last_segments_allocated = allocated;
    _throttled_since_tick = false;

    // A driver that waits for credit wakes up when the bucket holds what its candidate needs, or
    // when it may not compact. A driver that found nothing to compact wakes up when the candidates
    // of the shard differ from the candidates that the driver ranked, or when the controller is
    // switched on.
    if (_wait == wait_reason::credit) {
        if (!may_compact() || _rate_controller.can_afford(_credit_needed)) {
            _work_wanted.signal();
        }
    } else if (_wait == wait_reason::candidates && _rate_controller.enabled()
            && (!_empty_ranking || _candidate_counts != _empty_ranking->candidates)) {
        _work_wanted.signal();
    }
}

bool compaction_driver::may_compact() const noexcept {
    return _host.can_submit_compaction() && _rate_controller.enabled();
}

void compaction_driver::notify_group_idle() noexcept {
    ++_idle_notifications;
    if (_wait == wait_reason::candidates) {
        _work_wanted.signal();
    }
}

future<> compaction_driver::wait_for_work(wait_reason reason) {
    _wait = reason;
    auto f = co_await coroutine::as_future(_work_wanted.wait(wait_timeout));
    _wait = wait_reason::nothing;
    if (f.failed()) {
        // The wait ended by a timeout or because request_stop() broke the condition variable, and
        // the loop checks the conditions again in both cases.
        f.ignore_ready_future();
    }
}

future<> compaction_driver::run() {
    ranking_state ranking;

    while (!_stopping) {
        if (!may_compact()) {
            // Compaction is disabled, or the threshold disables it. The driver treats both like a
            // shard with nothing to reclaim.
            ranking.candidates.clear();
            _no_candidates = true;
            _empty_ranking.reset();
            co_await wait_for_work(wait_reason::candidates);
            continue;
        }

        auto slot = co_await get_units(_sem, 1);
        if (_stopping) {
            break;
        }

        if (ranking.candidates.empty() && !co_await rank(ranking)) {
            slot.return_all();
            co_await wait_for_candidates();
            continue;
        }

        auto c = std::move(ranking.candidates.back());
        ranking.candidates.pop_back();
        if (!_host.can_start(c.group)) {
            continue;
        }

        // A live update can disable compaction while the driver waits for a slot or for a ranking.
        // Check again before the candidate is kept for credit, so that no job starts from a ranking
        // made while compaction was disabled.
        if (!may_compact()) {
            ranking.candidates.clear();
            continue;
        }

        // The gate comes before the throttle, so that a candidate that the gate refuses does not
        // wait for credit. The bar cannot increase while the driver waits for credit, because only
        // the driver starts jobs.
        if (auto bar = admission_bar(ranking); bar && refuse_marginal(c, *bar)) {
            // The ranking is best first, so no later candidate passes either, and the bar does not
            // change until a job ends. Wait for one job to end, and then rank again.
            ranking.candidates.clear();
            slot.return_all();
            co_await wait_for_job_end();
            continue;
        }

        // The throttle makes compaction run at the commanded rate, and not as fast as it can when
        // there is a candidate. It also paces a batch of dead segments, because a job costs more
        // than its copy. At the space floor the throttle is off.
        if (!_rate_controller.can_afford(c.score.reclaimed())) {
            // This candidate is the best, and compaction_admission_credit limits its wait, so keep
            // it for when the bucket fills. If the limits changed during the wait, the ranking is
            // out of date, and the driver ranks again.
            const auto needed = c.score.reclaimed();
            ranking.candidates.push_back(std::move(c));
            slot.return_all();
            co_await wait_for_credit(needed);
            if (automatic_batch_limits() != ranking.limits) {
                ranking.candidates.clear();
            }
            continue;
        }

        start_job(std::move(c), ranking.limits, std::move(slot));
    }

    co_await wait_for_jobs_in_flight();
}

future<bool> compaction_driver::rank(ranking_state& ranking) {
    // The policy is read once per ranking, so that every group is ranked by the same rule.
    const auto limits = automatic_batch_limits();
    // The driver reads these counts before the ranking, because the ranking can yield, so that a
    // change that a tick sees during the ranking, or a group that becomes idle during the ranking,
    // wakes the driver.
    const auto before = empty_ranking{
        .candidates = _candidate_counts,
        .idle_notifications = _idle_notifications,
    };
    auto ranked = co_await _host.find_top_compaction_candidates(limits, _limits.auto_parallelism);
    if (_stopping) {
        co_return false;
    }
    if (ranked.candidates.empty()) {
        // No group has a batch under the threshold with a net gain, or each such group is
        // compacting, as on a shard with one tablet while its job runs. More CPU cannot change the
        // first case, so the controller must know which. A split or a manual compaction does not
        // use the rate, so a group that it holds counts only while an automatic job runs too.
        _no_candidates = !ranked.skipped_busy_group || _jobs_in_flight.empty();
        _empty_ranking = before;
        co_return false;
    }

    _no_candidates = false;
    ranking.candidates = std::move(ranked.candidates);
    ranking.limits = limits;
    // A batch of dead segments has an infinite efficiency, so it does not set the bar, because no
    // batch that copies data can reach a fraction of an infinite efficiency.
    ranking.best_copying.reset();
    for (const auto& c : ranking.candidates | std::views::reverse) {
        if (c.score.live_bytes != 0) {
            ranking.best_copying = c.score;
            break;
        }
    }
    co_return true;
}

future<> compaction_driver::wait_for_candidates() {
    // A group that became idle during the ranking can have a batch now.
    if (_empty_ranking && _idle_notifications != _empty_ranking->idle_notifications) {
        co_return;
    }
    co_await wait_for_work(wait_reason::candidates);
}

std::optional<compaction_candidate_score> compaction_driver::admission_bar(const ranking_state& ranking) const {
    if (_jobs_in_flight.empty()) {
        return std::nullopt;
    }
    // A job in flight that reclaims better than the best of the ranking also sets the bar. The
    // ranking skips a group that is compacting, so without the jobs in flight, a ranking made while
    // the best group compacts would admit the next best group, even a very poor one.
    auto bar = ranking.best_copying;
    for (const auto& job : _jobs_in_flight) {
        if (job.live_bytes != 0 && (!bar || *bar < job)) {
            bar = job;
        }
    }
    return bar;
}

future<> compaction_driver::wait_for_credit(size_t needed) {
    _throttled_since_tick = true;
    _credit_needed = needed;
    co_await wait_for_work(wait_reason::credit);
}

future<> compaction_driver::wait_for_job_end() {
    // The wait counts the jobs that end, and not the free slots, because a change of the space
    // floor also changes the slots.
    const auto jobs_ended = _jobs_ended;
    while (_jobs_ended == jobs_ended && !_stopping) {
        co_await wait_for_work(wait_reason::job_end);
    }
}

bool compaction_driver::refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar) {
    // A parallel job can take only a group that is not compacting. When one tablet holds the
    // garbage of the shard, the other jobs rewrite groups with almost no garbage, so a parallel job
    // must reclaim almost as well as the best batch.
    if (c.score.efficiency_at_least(admission_bar, compaction_marginal_admission_ratio)) {
        return false;
    }
    ++_stats.batches_refused;
    return true;
}

void compaction_driver::start_job(candidate c, compaction_batch_limits limits, semaphore_units<> units) {
    // The job is charged what its batch is estimated to reclaim, which is what is known now, and
    // the charge holds while the job runs. When the job ends, the difference to what it reclaimed is
    // settled. At the space floor the throttle is off, so a job that starts there is not charged.
    const auto estimate = double(c.score.reclaimed());
    const bool charged = !_rate_controller.bypassed();
    const auto generation = _rate_controller.generation();
    if (charged) {
        _rate_controller.charge(estimate);
    }

    auto job = _jobs_in_flight.insert(_jobs_in_flight.end(), c.score);
    (void)_host.submit_normal_compaction(*c.group, limits).then_wrapped(
            [this, job, estimate, charged, generation, units = std::move(units)] (future<uint64_t> f) {
        uint64_t reclaimed = 0;
        if (f.failed()) {
            logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", f.get_exception());
        } else {
            reclaimed = f.get();
        }
        if (charged && generation == _rate_controller.generation()) {
            _rate_controller.charge(double(reclaimed) - estimate);
            // A refund can pay for the candidate that waits for credit.
            if (_wait == wait_reason::credit && _rate_controller.can_afford(_credit_needed)) {
                _work_wanted.signal();
            }
        }
        on_job_end(job);
    });
}

void compaction_driver::on_job_end(std::list<compaction_candidate_score>::iterator job) noexcept {
    _jobs_in_flight.erase(job);
    ++_jobs_ended;
    if (_wait == wait_reason::job_end) {
        _work_wanted.signal();
    }
    notify_group_idle();
}

future<> compaction_driver::wait_for_jobs_in_flight() {
    _held_back.return_all();
    co_await get_units(_sem, max_auto_compaction_parallelism);
}

} // namespace replica::logstor
