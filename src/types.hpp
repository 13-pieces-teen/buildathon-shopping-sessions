#pragma once
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace shopping {
using SV = std::string_view;
constexpr int64_t DAY = 86400000, WEEK = 7 * DAY, GAP = 1800000;
constexpr uint32_t INVALID = UINT32_MAX;
// All string_views refer to storage owned by Dataset::storage until output completes.
struct Event { SV user; int64_t ts; uint32_t price; uint32_t category; };
struct Click { SV user; SV id; int64_t ts; uint32_t campaign; };
struct Campaign { std::string id, channel; };
struct Storage { virtual ~Storage() = default; };
struct Dataset {
  std::vector<std::unique_ptr<Storage>> storage;
  std::vector<std::vector<Event>> events;
  std::vector<std::vector<Click>> clicks;
  std::vector<std::string> categories;
  std::vector<Campaign> campaigns; // organic is NOT present; aggregation adds it.
  int64_t window_start = 0, window_end = 0;
  uint64_t parsed_events = 0, surviving_events = 0;
};
struct UserRow { SV user; int32_t day; int64_t sessions, events, revenue; uint32_t category; };
struct CampaignRow { uint32_t campaign; int32_t day; int64_t sessions, purchasing, revenue, assists; };
struct Results { std::vector<std::vector<UserRow>> users; std::vector<CampaignRow> campaigns; };
inline uint64_t hash_sv(SV s) {
  // Stable exact-key hash. Callers MUST compare full values on collisions.
  uint64_t h = 14695981039346656037ULL;
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
  h ^= h >> 32; h *= 0xd6e8feb86659fd93ULL; h ^= h >> 32;
  return h;
}
struct SVHash { size_t operator()(SV s) const { return hash_sv(s); } };
inline int32_t utc_day(int64_t ts) { return static_cast<int32_t>(ts >= 0 ? ts / DAY : (ts - DAY + 1) / DAY); }
Dataset load_dataset(const std::string& path, unsigned threads, unsigned partitions);
Results aggregate(Dataset& data, unsigned threads);
void write_results(const Dataset& data, const Results& results, const std::string& path);
}
