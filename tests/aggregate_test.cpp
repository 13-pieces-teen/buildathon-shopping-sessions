#include "types.hpp"
#include <array>
#include <cassert>
#include <deque>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <tuple>

using namespace shopping;
using UserKey = std::pair<std::string, int32_t>;
using UserValue = std::tuple<int64_t, int64_t, int64_t, uint32_t>;
using CampaignKey = std::pair<uint32_t, int32_t>;
using CampaignValue = std::array<int64_t, 4>;
struct Snapshot {
  std::map<UserKey, UserValue> users;
  std::map<CampaignKey, CampaignValue> campaigns;
  bool operator==(const Snapshot&) const = default;
};
struct Strings : Storage {
  std::deque<std::string> values;
  SV add(std::string value) { values.push_back(std::move(value)); return values.back(); }
};

Snapshot snapshot(const Results& result) {
  Snapshot out;
  for (const auto& bucket : result.users) for (const auto& row : bucket) {
    const bool fresh = out.users.emplace(UserKey{std::string(row.user), row.day},
        UserValue{row.sessions, row.events, row.revenue, row.category}).second;
    assert(fresh);
  }
  for (const auto& row : result.campaigns) {
    const bool fresh = out.campaigns.emplace(CampaignKey{row.campaign, row.day},
        CampaignValue{row.sessions, row.purchasing, row.revenue, row.assists}).second;
    assert(fresh);
  }
  return out;
}

// Deliberately simple oracle: materialize each session, scan every click for
// that person, and use a set for its assisting campaigns. No interval algorithm.
Snapshot reference(const Dataset& data) {
  std::map<std::string, std::vector<Event>> people;
  std::map<std::string, std::vector<Click>> ad_clicks;
  for (const auto& bucket : data.events) for (const Event& e : bucket)
    people[std::string(e.user)].push_back(e);
  for (const auto& bucket : data.clicks) for (const Click& c : bucket)
    ad_clicks[std::string(c.user)].push_back(c);
  Snapshot out;
  std::map<UserKey, std::map<uint32_t, int64_t>> categories;
  for (auto& [user, events] : people) {
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
      return a.ts < b.ts;
    });
    size_t begin = 0;
    while (begin < events.size()) {
      size_t end = begin + 1;
      while (end < events.size() && events[end].ts - events[end - 1].ts <= GAP) ++end;
      const int64_t ts = events[begin].ts;
      const int32_t day = int32_t(ts / DAY - (ts % DAY < 0));
      const UserKey key{user, day};
      auto [where, fresh] = out.users.try_emplace(key, 0, 0, 0, INVALID);
      auto& [sessions, count, revenue, top] = where->second;
      ++sessions;
      count += end - begin;
      int64_t session_revenue = 0;
      for (size_t i = begin; i < end; ++i) if (events[i].price) {
        session_revenue += events[i].price;
        categories[key][events[i].category] += events[i].price;
      }
      revenue += session_revenue;
      const Click* winner = nullptr;
      std::set<uint32_t> assists;
      for (const Click& click : ad_clicks[user]) {
        if (click.ts > ts || click.ts < ts - WEEK) continue;
        assists.insert(click.campaign);
        if (!winner || click.ts > winner->ts ||
            (click.ts == winner->ts && click.id < winner->id)) winner = &click;
      }
      const uint32_t credited = winner ? winner->campaign : uint32_t(data.campaigns.size());
      auto& campaign = out.campaigns[{credited, day}];
      ++campaign[0];
      campaign[1] += session_revenue > 0;
      campaign[2] += session_revenue;
      for (uint32_t assisted : assists) ++out.campaigns[{assisted, day}][3];
      begin = end;
    }
  }
  for (auto& [key, value] : out.users) {
    int64_t maximum = -1;
    auto& top = std::get<3>(value);
    for (const auto& [category, revenue] : categories[key]) {
      if (revenue > maximum ||
          (revenue == maximum && data.categories[category] < data.categories[top])) {
        maximum = revenue;
        top = category;
      }
    }
  }
  return out;
}

Dataset base(unsigned partitions) {
  Dataset data;
  data.storage.push_back(std::make_unique<Strings>());
  data.categories = {"b", "A", "a", "unknown", "\xc3\xa9"};
  for (unsigned i = 0; i < 5; ++i)
    data.campaigns.push_back({"c_" + std::to_string(i), "channel"});
  data.events.resize(partitions);
  data.clicks.resize(partitions);
  data.window_start = -40 * DAY;
  data.window_end = 40 * DAY;
  return data;
}
SV str(Dataset& data, std::string s) {
  return static_cast<Strings*>(data.storage[0].get())->add(std::move(s));
}
void event(Dataset& data, SV user, int64_t ts, uint32_t price = 0, uint32_t category = INVALID) {
  data.events[hash_sv(user) % data.events.size()].push_back({user, ts, price, category});
}
void click(Dataset& data, SV user, std::string id, int64_t ts, uint32_t campaign, bool duplicate = false) {
  Click c{user, str(data, std::move(id)), ts, campaign};
  auto& bucket = data.clicks[hash_sv(user) % data.clicks.size()];
  bucket.push_back(c);
  if (duplicate) bucket.push_back(c);
}
void verify(Dataset& data, unsigned threads) {
  const Snapshot expected = reference(data);
  const Results result = aggregate(data, threads);
  const Snapshot actual = snapshot(result);
  if (!(actual == expected)) {
    std::cerr << "Aggregation mismatch: " << actual.users.size() << "/"
              << expected.users.size() << " user rows, " << actual.campaigns.size()
              << "/" << expected.campaigns.size() << " campaign rows\n";
    std::abort();
  }
  for (const auto& bucket : data.events) assert(bucket.empty());
  for (const auto& bucket : data.clicks) assert(bucket.empty());
}

int main() {
  {
    Dataset data = base(7);
    const SV user = str(data, " u");
    event(data, user, -60000);
    event(data, user, 0, 100, 0);
    event(data, user, GAP, 100, 1);
    event(data, user, 2 * GAP + 1);
    click(data, user, "a", -60000, 0, true);
    click(data, user, "Z", -60000, 1, true);
    click(data, user, "after", -59999, 2);
    click(data, user, "seven", -60000 - WEEK, 3);
    click(data, user, "too_old", -60001 - WEEK, 4);
    const Snapshot expected = reference(data);
    assert(expected.users.at({" u", -1}) == UserValue(1, 3, 200, 1));
    assert(expected.users.at({" u", 0}) == UserValue(1, 1, 0, INVALID));
    assert(expected.campaigns.at({1, -1}) == CampaignValue({1, 1, 200, 1}));
    assert(expected.campaigns.at({3, -1}) == CampaignValue({0, 0, 0, 1}));
    assert(!expected.campaigns.contains({2, -1}));
    assert(!expected.campaigns.contains({4, -1}));
    verify(data, 4);
  }
  {
    Dataset data = base(2);
    const SV user = str(data, "u");
    // Two disjoint intervals, touching endpoints, days without sessions,
    // repeated clicks, and a campaign present only as an assist.
    for (int day : {-21, -14, -7, 0, 1, 7, 8, 14, 20, 21})
      event(data, user, day * DAY);
    click(data, user, "z", -21 * DAY, 0, true);
    click(data, user, "y", -14 * DAY, 0);
    click(data, user, "x", 0, 0);
    click(data, user, "w", 14 * DAY, 0);
    click(data, user, "a", -21 * DAY, 1);
    verify(data, 2);
  }
  std::mt19937_64 rng(0x53a95);
  for (unsigned trial = 0; trial < 300; ++trial) {
    Dataset data = base(1 + rng() % 17);
    if (trial % 3 == 0) {
      data.window_start = 0;
      data.window_end = 0; // Force sparse aggregation, including negative days.
    }
    const std::vector<std::string> names{"u", "U", " u", "u ", "\xc3\xa9", "u10", "u2"};
    for (const auto& name : names) {
      const SV user = str(data, name);
      for (unsigned i = 0, n = rng() % 140; i < n; ++i) {
        const int64_t ts = (int64_t(rng() % 61) - 30) * DAY +
                           int64_t(rng() % 8) * GAP + int64_t(rng() % 3);
        const uint32_t price = rng() % 3 == 0 ? 1 + rng() % 10000000 : 0;
        event(data, user, ts, price, price ? rng() % data.categories.size() : INVALID);
        if (rng() % 7 == 0) event(data, user, ts, 17, 2);
      }
      for (unsigned i = 0, n = rng() % 100; i < n; ++i) {
        const int64_t ts = (int64_t(rng() % 77) - 38) * DAY +
                           int64_t(rng() % 8) * GAP + int64_t(rng() % 3);
        click(data, user, "click" + std::to_string(i), ts, rng() % 5, rng() % 3 == 0);
      }
    }
    for (auto& bucket : data.events) std::shuffle(bucket.begin(), bucket.end(), rng);
    for (auto& bucket : data.clicks) std::shuffle(bucket.begin(), bucket.end(), rng);
    verify(data, 1 + rng() % 8);
  }
  {
    Dataset data = base(3);
    verify(data, 2);
  }
  {
    Dataset data = base(0);
    assert(snapshot(aggregate(data, 1)) == Snapshot{});
  }
  std::cout << "PASS: edge cases, 300 randomized oracle comparisons, and empty inputs\n";
}
