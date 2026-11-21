#include "types.hpp"

#include <atomic>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace shopping {
namespace {

int compare_bytes(SV a, SV b) {
  // Linked aliases share the canonical name's backing bytes. Equal pointers
  // need no memory read; lengths still distinguish any prefix string_views.
  if (a.data() == b.data())
    return (a.size() > b.size()) - (a.size() < b.size());
  const size_t n = std::min(a.size(), b.size());
  if (n) {
    const int compared = std::memcmp(a.data(), b.data(), n);
    if (compared) return compared;
  }
  return (a.size() > b.size()) - (a.size() < b.size());
}

bool bytes_less(SV a, SV b) { return compare_bytes(a, b) < 0; }

int32_t day_of(int64_t ts) {
  int64_t day = ts / DAY;
  if (ts % DAY < 0) --day;
  if (day < INT32_MIN || day > INT32_MAX)
    throw std::runtime_error("Session date does not fit Parquet DATE");
  return static_cast<int32_t>(day);
}

struct Session {
  int64_t start, revenue;
  int32_t day;
};
struct DayBlock {
  size_t begin, end;
  int32_t day;
};
struct Interval {
  int64_t begin = 0, end = 0;
  bool active = false;
};
struct Counts {
  int64_t sessions = 0, purchasing = 0, revenue = 0, assists = 0;
};

uint64_t campaign_key(uint32_t campaign, int32_t day) {
  return (uint64_t(static_cast<uint32_t>(day)) << 32) | campaign;
}

// Typical inputs have fourteen days and 2,001 campaigns including organic.
// Keep that case in a compact array, but support arbitrary manifest windows.
class CampaignTotals {
  int32_t first_day_ = 0;
  size_t day_count_ = 0, campaign_count_;
  std::vector<Counts> dense_;
  std::unordered_map<uint64_t, Counts> sparse_;

  Counts& get(uint32_t campaign, int32_t day) {
    const int64_t offset = int64_t(day) - first_day_;
    if (!dense_.empty() && offset >= 0 && uint64_t(offset) < day_count_)
      return dense_[size_t(offset) * campaign_count_ + campaign];
    return sparse_[campaign_key(campaign, day)];
  }

public:
  explicit CampaignTotals(const Dataset& data)
      : campaign_count_(data.campaigns.size() + 1) {
    if (data.window_end > data.window_start) {
      first_day_ = day_of(data.window_start);
      const int64_t days = int64_t(day_of(data.window_end - 1)) - first_day_ + 1;
      if (days > 0 && days <= 4096 &&
          campaign_count_ <= 1000000 / static_cast<size_t>(days)) {
        day_count_ = static_cast<size_t>(days);
        dense_.resize(day_count_ * campaign_count_);
      }
    }
  }

  void credit(uint32_t campaign, int32_t day, int64_t revenue) {
    Counts& row = get(campaign, day);
    ++row.sessions;
    row.purchasing += revenue > 0;
    row.revenue += revenue;
  }

  void assist(uint32_t campaign, int32_t day, size_t count) {
    if (count) get(campaign, day).assists += static_cast<int64_t>(count);
  }

  void append(std::vector<CampaignRow>& out) const {
    for (size_t d = 0; d < day_count_; ++d) {
      for (size_t c = 0; c < campaign_count_; ++c) {
        const Counts& row = dense_[d * campaign_count_ + c];
        if (row.sessions || row.assists)
          out.push_back({static_cast<uint32_t>(c),
                         static_cast<int32_t>(int64_t(first_day_) + static_cast<int64_t>(d)),
                         row.sessions, row.purchasing, row.revenue, row.assists});
      }
    }
    for (const auto& [key, row] : sparse_) {
      if (row.sessions || row.assists)
        out.push_back({static_cast<uint32_t>(key),
                       static_cast<int32_t>(static_cast<uint32_t>(key >> 32)),
                       row.sessions, row.purchasing, row.revenue, row.assists});
    }
  }
};

class Worker {
  const Dataset& data_;
  std::vector<Session> sessions_;
  std::vector<DayBlock> days_;
  std::vector<int64_t> category_revenue_;
  std::vector<uint32_t> touched_categories_;
  std::vector<Interval> intervals_;
  std::vector<uint32_t> touched_campaigns_;

  void finish_day(UserRow row, std::vector<UserRow>& output) {
    uint32_t top = INVALID;
    for (uint32_t category : touched_categories_) {
      if (top == INVALID || category_revenue_[category] > category_revenue_[top] ||
          (category_revenue_[category] == category_revenue_[top] &&
           bytes_less(data_.categories[category], data_.categories[top])))
        top = category;
    }
    row.category = top;
    output.push_back(row);
    for (uint32_t category : touched_categories_) category_revenue_[category] = 0;
    touched_categories_.clear();
  }

  void make_sessions(const std::vector<Event>& events, size_t begin, size_t end,
                     std::vector<UserRow>& output) {
    sessions_.clear();
    days_.clear();
    UserRow row{events[begin].user, 0, 0, 0, 0, INVALID};
    int64_t previous = 0;
    for (size_t i = begin; i < end; ++i) {
      const Event& event = events[i];
      // Unsigned subtraction avoids signed overflow even across epoch zero.
      if (i == begin || uint64_t(event.ts) - uint64_t(previous) > uint64_t(GAP)) {
        const int32_t day = day_of(event.ts);
        if (row.sessions && day != row.day) {
          finish_day(row, output);
          row = {event.user, day, 0, 0, 0, INVALID};
        }
        row.day = day;
        ++row.sessions;
        if (days_.empty() || days_.back().day != day)
          days_.push_back({sessions_.size(), sessions_.size(), day});
        sessions_.push_back({event.ts, 0, day});
        days_.back().end = sessions_.size();
      }
      previous = event.ts;
      ++row.events;
      if (event.price) {
        if (event.category >= category_revenue_.size())
          throw std::runtime_error("Purchase category index is invalid");
        row.revenue += event.price;
        sessions_.back().revenue += event.price;
        int64_t& revenue = category_revenue_[event.category];
        if (!revenue) touched_categories_.push_back(event.category);
        revenue += event.price;
      }
    }
    finish_day(row, output);
  }

  // A click at c assists precisely the starts in the closed interval [c,c+W].
  // Per-campaign unions make the contribution idempotent for duplicate clicks.
  void count_interval(uint32_t campaign, const Interval& interval) {
    const auto first = std::lower_bound(
        sessions_.begin(), sessions_.end(), interval.begin,
        [](const Session& session, int64_t ts) { return session.start < ts; });
    if (first == sessions_.end() || first->start > interval.end) return;
    const auto last = std::upper_bound(
        first, sessions_.end(), interval.end,
        [](int64_t ts, const Session& session) { return ts < session.start; });
    const size_t l = static_cast<size_t>(first - sessions_.begin());
    const size_t r = static_cast<size_t>(last - sessions_.begin());
    if (first->day == (last - 1)->day) {
      totals.assist(campaign, first->day, r - l);
      return;
    }
    auto day = std::lower_bound(days_.begin(), days_.end(), l,
                               [](const DayBlock& block, size_t index) {
                                 return block.end <= index;
                               });
    for (; day != days_.end() && day->begin < r; ++day)
      totals.assist(campaign, day->day,
                    std::min(r, day->end) - std::max(l, day->begin));
  }

  void attribute(const std::vector<Click>& clicks, size_t begin, size_t end) {
    const uint32_t organic = static_cast<uint32_t>(data_.campaigns.size());
    size_t next = begin;
    const Click* latest = nullptr;
    for (const Session& session : sessions_) {
      while (next < end && clicks[next].ts <= session.start) {
        const Click& click = clicks[next++];
        if (!latest || click.ts > latest->ts ||
            (click.ts == latest->ts && bytes_less(click.id, latest->id)))
          latest = &click;
      }
      const uint32_t campaign =
          latest && uint64_t(session.start) - uint64_t(latest->ts) <= uint64_t(WEEK)
              ? latest->campaign
              : organic;
      totals.credit(campaign, session.day, session.revenue);
    }

    touched_campaigns_.clear();
    for (size_t i = begin; i < end; ++i) {
      const Click& click = clicks[i];
      if (click.campaign >= intervals_.size())
        throw std::runtime_error("Click campaign index is invalid");
      Interval& interval = intervals_[click.campaign];
      const int64_t reach = click.ts > INT64_MAX - WEEK ? INT64_MAX : click.ts + WEEK;
      if (!interval.active) {
        touched_campaigns_.push_back(click.campaign);
        interval = {click.ts, reach, true};
      } else if (click.ts <= interval.end) {
        interval.end = reach;
      } else {
        count_interval(click.campaign, interval);
        interval = {click.ts, reach, true};
      }
    }
    for (uint32_t campaign : touched_campaigns_) {
      count_interval(campaign, intervals_[campaign]);
      intervals_[campaign].active = false;
    }
  }

public:
  CampaignTotals totals;

  explicit Worker(const Dataset& data)
      : data_(data), category_revenue_(data.categories.size()),
        intervals_(data.campaigns.size()), totals(data) {
    sessions_.reserve(256);
    days_.reserve(16);
    touched_categories_.reserve(std::min<size_t>(data.categories.size(), 64));
    touched_campaigns_.reserve(std::min<size_t>(data.campaigns.size(), 256));
  }

  void run(std::vector<Event>& events, std::vector<Click>& clicks,
           std::vector<UserRow>& output) {
    if (events.empty()) {
      std::vector<Event>().swap(events);
      std::vector<Click>().swap(clicks);
      return;
    }
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
      const int person = compare_bytes(a.user, b.user);
      return person ? person < 0 : a.ts < b.ts;
    });
    std::sort(clicks.begin(), clicks.end(), [](const Click& a, const Click& b) {
      const int person = compare_bytes(a.user, b.user);
      // attribute() explicitly selects the minimum click ID at a tied time;
      // interval unions depend only on time, so sorting tied IDs is redundant.
      return person ? person < 0 : a.ts < b.ts;
    });
    size_t event_at = 0, click_at = 0;
    while (event_at < events.size()) {
      const SV user = events[event_at].user;
      size_t event_end = event_at + 1;
      while (event_end < events.size() && events[event_end].user == user) ++event_end;
      while (click_at < clicks.size() && bytes_less(clicks[click_at].user, user)) ++click_at;
      size_t click_end = click_at;
      while (click_end < clicks.size() && clicks[click_end].user == user) ++click_end;
      make_sessions(events, event_at, event_end, output);
      attribute(clicks, click_at, click_end);
      event_at = event_end;
      click_at = click_end;
    }
    // Dataset::storage remains alive; result string_views do not point at vectors.
    std::vector<Event>().swap(events);
    std::vector<Click>().swap(clicks);
  }
};

} // namespace

Results aggregate(Dataset& data, unsigned threads) {
  if (!threads) throw std::runtime_error("Aggregation needs at least one thread");
  if (data.events.size() != data.clicks.size())
    throw std::runtime_error("Event and click partition counts differ");
  if (data.campaigns.size() >= INVALID)
    throw std::runtime_error("Too many campaigns for uint32 identifiers");
  Results results;
  results.users.resize(data.events.size());
  if (data.events.empty()) return results;
  const unsigned workers = static_cast<unsigned>(
      std::min<size_t>(threads, data.events.size()));
  std::vector<std::unique_ptr<Worker>> states;
  states.reserve(workers);
  for (unsigned i = 0; i < workers; ++i) states.push_back(std::make_unique<Worker>(data));
  std::atomic<size_t> next{0};
  std::atomic<bool> failed{false};
  std::exception_ptr error;
  std::mutex error_mutex;
  std::vector<std::thread> pool;
  pool.reserve(workers);
  auto work = [&](unsigned worker) {
    try {
      while (!failed.load(std::memory_order_relaxed)) {
        const size_t bucket = next.fetch_add(1, std::memory_order_relaxed);
        if (bucket >= data.events.size()) break;
        states[worker]->run(data.events[bucket], data.clicks[bucket], results.users[bucket]);
      }
    } catch (...) {
      failed.store(true, std::memory_order_relaxed);
      std::lock_guard<std::mutex> lock(error_mutex);
      if (!error) error = std::current_exception();
    }
  };
  // Main thread does one worker's work, so the total active count stays <= threads.
  try {
    for (unsigned i = 1; i < workers; ++i) pool.emplace_back(work, i);
  } catch (...) {
    failed.store(true, std::memory_order_relaxed);
    for (auto& thread : pool) thread.join();
    throw;
  }
  work(0);
  for (auto& thread : pool) thread.join();
  if (error) std::rethrow_exception(error);

  for (const auto& state : states) state->totals.append(results.campaigns);
  std::sort(results.campaigns.begin(), results.campaigns.end(),
            [](const CampaignRow& a, const CampaignRow& b) {
              return a.campaign == b.campaign ? a.day < b.day : a.campaign < b.campaign;
            });
  size_t kept = 0;
  for (const CampaignRow row : results.campaigns) {
    if (kept && results.campaigns[kept - 1].campaign == row.campaign &&
        results.campaigns[kept - 1].day == row.day) {
      CampaignRow& target = results.campaigns[kept - 1];
      target.sessions += row.sessions;
      target.purchasing += row.purchasing;
      target.revenue += row.revenue;
      target.assists += row.assists;
    } else {
      results.campaigns[kept++] = row;
    }
  }
  results.campaigns.resize(kept);
  return results;
}

} // namespace shopping
