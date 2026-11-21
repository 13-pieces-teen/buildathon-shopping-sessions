#pragma once
#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace shopping {
using SV = std::string_view;
constexpr int64_t DAY = 86400000, WEEK = 7 * DAY, GAP = 1800000;
constexpr uint32_t INVALID = UINT32_MAX;
// All string_views refer to storage owned by Dataset::storage until output completes.
struct Event { uint32_t user; int64_t ts; uint32_t price; uint32_t category; };
struct Click {
  SV id;
  int64_t ts;
  uint32_t user, campaign;
  Click() = default;
  // Preserve the logical construction order while packing both uint32 fields
  // together. The canonical user is an ID, not a pointer into source CSVs.
  Click(uint32_t person, SV click_id, int64_t stamp, uint32_t credited)
      : id(click_id), ts(stamp), user(person), campaign(credited) {}
};
static_assert(sizeof(Event)==24);
static_assert(sizeof(Click)==32);
struct Campaign { std::string id, channel; };
struct Storage { virtual ~Storage() = default; };
struct Dataset {
  std::vector<std::unique_ptr<Storage>> storage;
  std::vector<std::vector<Event>> events;
  std::vector<std::vector<Click>> clicks;
  std::vector<std::string> categories;
  std::vector<Campaign> campaigns; // organic is NOT present; aggregation adds it.
  std::vector<SV> linked_names;
  std::vector<std::vector<SV>> unlinked_names;
  unsigned unlinked_shift = 0;
  uint32_t unlinked_mask = 0;
  SV person_name(uint32_t id) const {
    constexpr uint32_t unlinked = uint32_t{1} << 31;
    if(!(id & unlinked))return linked_names.at(id);
    if(unlinked_shift>=31)throw std::logic_error("Invalid unlinked person ID layout");
    return unlinked_names.at(id & unlinked_mask).at((id & ~unlinked) >> unlinked_shift);
  }
  int64_t window_start = 0, window_end = 0;
  uint64_t parsed_events = 0, surviving_events = 0;
};
struct UserRow { SV user; int32_t day; int64_t sessions, events, revenue; uint32_t category; };
struct CampaignRow { uint32_t campaign; int32_t day; int64_t sessions, purchasing, revenue, assists; };
struct Results { std::vector<std::vector<UserRow>> users; std::vector<CampaignRow> campaigns; };
inline uint64_t hash_sv(SV s) {
  // Every byte participates, including embedded NULs. Word copies are safe for
  // unaligned keys and never read beyond the string. Equality remains exact.
  const auto word = [](const char* p) {
    uint64_t value;
    std::char_traits<char>::copy(reinterpret_cast<char*>(&value),p,sizeof(value));
    return value;
  };
  const auto half = [](const char* p) {
    uint32_t value;
    std::char_traits<char>::copy(reinterpret_cast<char*>(&value),p,sizeof(value));
    return static_cast<uint64_t>(value);
  };
  const auto mix = [](uint64_t a,uint64_t b) {
    a *= 0x9e3779b185ebca87ULL;
    a ^= (b << 23) | (b >> 41);
    a ^= a >> 32; a *= 0xc2b2ae3d27d4eb4fULL;
    return a ^ (a >> 29);
  };
  uint64_t h=0xa0761d6478bd642fULL ^ (s.size()*0xe7037ed1a0b428dbULL);
  const char* p=s.data();size_t n=s.size();
  while(n>16) {
    h=mix(h^word(p),word(p+8));p+=16;n-=16;
  }
  uint64_t a=0,b=0;
  if(n>=8) {a=word(p);b=word(p+n-8);}
  else if(n>=4) {a=half(p);b=half(p+n-4);}
  else if(n) {
    a=static_cast<unsigned char>(p[0]);
    b=(static_cast<uint64_t>(static_cast<unsigned char>(p[n/2]))<<8) |
      static_cast<unsigned char>(p[n-1]);
  }
  return mix(h^a,b^0x8ebc6af09c88c6e3ULL);
}
struct SVHash { size_t operator()(SV s) const { return hash_sv(s); } };
inline int32_t utc_day(int64_t ts) { return static_cast<int32_t>(ts >= 0 ? ts / DAY : (ts - DAY + 1) / DAY); }
Dataset load_dataset(const std::string& path, unsigned threads, unsigned partitions);
Results aggregate(Dataset& data, unsigned threads);
void write_results(const Dataset& data, const Results& results, const std::string& path);
}
