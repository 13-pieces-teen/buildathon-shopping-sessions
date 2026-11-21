#include "types.hpp"
#include <fstream>
#include <filesystem>
#include <limits>
#include <stdexcept>

namespace shopping {
namespace {
template<class T> void value(std::ofstream& f, const T& x) { f.write(reinterpret_cast<const char*>(&x), sizeof(T)); }
template<class T> void array(std::ofstream& f, const std::vector<T>& x) {
  if (!x.empty()) f.write(reinterpret_cast<const char*>(x.data()), x.size()*sizeof(T));
}
template<class Rows, class Getter> void strings(std::ofstream& f, const Rows& rows, Getter get, bool nullable = false) {
  std::vector<int32_t> offsets; offsets.reserve(rows.size()+1); offsets.push_back(0);
  std::string bytes;
  std::vector<uint8_t> valid((rows.size()+7)/8, 0);
  for (size_t i = 0; i < rows.size(); ++i) {
    auto s = get(rows[i]);
    if (s.data()) { bytes.append(s); valid[i/8] |= uint8_t(1u << (i%8)); }
    if (bytes.size() > size_t(INT32_MAX)) throw std::runtime_error("string output chunk exceeds 2 GiB");
    offsets.push_back(static_cast<int32_t>(bytes.size()));
  }
  value(f, uint64_t(bytes.size())); array(f, offsets); f.write(bytes.data(), bytes.size());
  if (nullable) array(f, valid);
}
template<class T, class Rows, class Getter> void numbers(std::ofstream& f, const Rows& rows, Getter get) {
  std::vector<T> values; values.reserve(rows.size());
  for (const auto& row : rows) values.push_back(get(row));
  array(f, values);
}
std::ofstream open(const std::string& path, size_t n) {
  std::ofstream f(path, std::ios::binary); f.exceptions(std::ios::badbit | std::ios::failbit);
  f.write("SSCOL01\0", 8); value(f, uint64_t(n)); return f;
}
}
void write_results(const Dataset& data, const Results& results, const std::string& path) {
  std::filesystem::create_directories(path);
  bool wrote = false;
  for (size_t p = 0; p < results.users.size(); ++p) {
    const auto& rows = results.users[p];
    if (rows.empty()) continue;
    wrote = true;
    auto f = open(path + "/user-" + std::to_string(p) + ".bin", rows.size());
    strings(f, rows, [](const UserRow& r) { return r.user; });
    numbers<int32_t>(f, rows, [](const UserRow& r) { return r.day; });
    numbers<int64_t>(f, rows, [](const UserRow& r) { return r.sessions; });
    numbers<int64_t>(f, rows, [](const UserRow& r) { return r.events; });
    numbers<int64_t>(f, rows, [](const UserRow& r) { return r.revenue; });
    strings(f, rows, [&](const UserRow& r) -> SV { return r.category == INVALID ? SV{} : SV(data.categories.at(r.category)); }, true);
  }
  if (!wrote) {
    std::vector<UserRow> empty;
    auto f = open(path + "/user-0.bin", 0);
    strings(f, empty, [](const UserRow& r) { return r.user; });
    strings(f, empty, [](const UserRow&) { return SV{}; }, true);
  }
  const auto& rows = results.campaigns;
  auto f = open(path + "/campaign-0.bin", rows.size());
  strings(f, rows, [&](const CampaignRow& r) -> SV { return r.campaign == data.campaigns.size() ? SV("organic") : SV(data.campaigns.at(r.campaign).id); });
  strings(f, rows, [&](const CampaignRow& r) -> SV { return r.campaign == data.campaigns.size() ? SV("organic") : SV(data.campaigns.at(r.campaign).channel); });
  numbers<int32_t>(f, rows, [](const CampaignRow& r) { return r.day; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.sessions; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.purchasing; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.revenue; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.assists; });
}
}
