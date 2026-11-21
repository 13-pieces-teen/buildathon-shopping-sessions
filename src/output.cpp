#include "types.hpp"
#include <atomic>
#include <fstream>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace shopping {
namespace {
template<class T> void value(std::ofstream& f, const T& x) { f.write(reinterpret_cast<const char*>(&x), sizeof(T)); }
template<class T> void array(std::ofstream& f, const std::vector<T>& x) {
  if (!x.empty()) f.write(reinterpret_cast<const char*>(x.data()), x.size()*sizeof(T));
}
class StringColumn {
  std::vector<int32_t> offsets;
  std::string bytes;
  std::vector<uint8_t> valid;
  bool nullable;
public:
  StringColumn(size_t count, bool nulls = false) : nullable(nulls) {
    offsets.reserve(count+1); offsets.push_back(0);
    if(nullable) valid.resize((count+7)/8, 0);
  }
  void append(SV s) {
    if(s.size()>size_t(INT32_MAX)-bytes.size())
      throw std::runtime_error("string output chunk exceeds 2 GiB");
    if(s.data()) {
      bytes.append(s);
      if(nullable) {
        const size_t index=offsets.size()-1;
        valid[index/8] |= uint8_t(1u << (index%8));
      }
    }
    offsets.push_back(static_cast<int32_t>(bytes.size()));
  }
  void write(std::ofstream& f) const {
    value(f, uint64_t(bytes.size())); array(f, offsets); f.write(bytes.data(), bytes.size());
    if(nullable) array(f, valid);
  }
};
template<class Rows, class Getter> void strings(std::ofstream& f, const Rows& rows, Getter get, bool nullable = false) {
  StringColumn column(rows.size(), nullable);
  for(const auto& row:rows) column.append(get(row));
  column.write(f);
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
void write_user_part(const Dataset& data, const Results& results, const std::string& path,
                     size_t part, size_t stride, size_t row_count) {
  StringColumn users(row_count), categories(row_count, true);
  std::vector<int32_t> days; days.reserve(row_count);
  std::vector<int64_t> sessions, events, revenue;
  sessions.reserve(row_count); events.reserve(row_count); revenue.reserve(row_count);
  // Visit the existing result partitions directly. A single pass fills every
  // column, avoiding both a merged UserRow copy and six scans of that copy.
  for(size_t p=part; p<results.users.size(); p+=stride) {
    for(const UserRow& row:results.users[p]) {
      users.append(row.user);
      days.push_back(row.day); sessions.push_back(row.sessions);
      events.push_back(row.events); revenue.push_back(row.revenue);
      categories.append(row.category==INVALID ? SV{} : SV(data.categories.at(row.category)));
    }
  }
  auto f=open(path+"/user-"+std::to_string(part)+".bin", row_count);
  users.write(f); array(f,days); array(f,sessions); array(f,events); array(f,revenue); categories.write(f);
  f.close(); // propagate delayed write failures through the worker exception path
}
void write_campaigns(const Dataset& data, const Results& results, const std::string& path) {
  const auto& rows = results.campaigns;
  auto f = open(path + "/campaign-0.bin", rows.size());
  strings(f, rows, [&](const CampaignRow& r) -> SV { return r.campaign == data.campaigns.size() ? SV("organic") : SV(data.campaigns.at(r.campaign).id); });
  strings(f, rows, [&](const CampaignRow& r) -> SV { return r.campaign == data.campaigns.size() ? SV("organic") : SV(data.campaigns.at(r.campaign).channel); });
  numbers<int32_t>(f, rows, [](const CampaignRow& r) { return r.day; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.sessions; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.purchasing; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.revenue; });
  numbers<int64_t>(f, rows, [](const CampaignRow& r) { return r.assists; });
  f.close();
}
}
void write_results(const Dataset& data, const Results& results, const std::string& path) {
  std::filesystem::create_directories(path);
  const auto count=std::getenv("NCPU");
  const size_t concurrency=size_t(std::max(1,count ? std::atoi(count) : 8));
  const size_t output_parts=std::min(results.users.size(),concurrency);
  struct Job {size_t part, rows; bool campaign;};
  std::vector<Job> jobs;
  for(size_t p=0;p<output_parts;++p) {
    size_t rows=0;
    for(size_t i=p;i<results.users.size();i+=output_parts)rows+=results.users[i].size();
    if(rows)jobs.push_back({p,rows,false});
  }
  if(jobs.empty())jobs.push_back({0,0,false}); // valid empty user schema, including offsets
  jobs.push_back({0,results.campaigns.size(),true});

  const size_t workers=std::min(concurrency,jobs.size());
  std::atomic<size_t> next{0};
  std::atomic<bool> failed{false};
  std::exception_ptr error;
  std::mutex error_mutex;
  auto work=[&] {
    try {
      while(!failed.load(std::memory_order_relaxed)) {
        const size_t index=next.fetch_add(1,std::memory_order_relaxed);
        if(index>=jobs.size())break;
        const Job& job=jobs[index];
        if(job.campaign)write_campaigns(data,results,path);
        else write_user_part(data,results,path,job.part,std::max(size_t(1),output_parts),job.rows);
      }
    }catch(...) {
      failed.store(true,std::memory_order_relaxed);
      std::lock_guard lock(error_mutex);
      if(!error)error=std::current_exception();
    }
  };
  std::vector<std::thread> pool;
  pool.reserve(workers-1);
  try {for(size_t i=1;i<workers;++i)pool.emplace_back(work);}
  catch(...) {failed=true;for(auto& thread:pool)thread.join();throw;}
  work();
  for(auto& thread:pool)thread.join();
  if(error)std::rethrow_exception(error);
}
}
