#include "types.hpp"
#include "parse.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace shopping {
namespace {
using Clock=std::chrono::steady_clock;
struct Mapped final:Storage {
  const char* data=nullptr; size_t size=0;
  explicit Mapped(const std::filesystem::path& path) {
    int fd=::open(path.c_str(),O_RDONLY);
    if(fd<0) throw std::runtime_error("cannot open "+path.string());
    struct stat st{};
    if(fstat(fd,&st)) {close(fd);throw std::runtime_error("cannot stat "+path.string());}
    size=static_cast<size_t>(st.st_size);
    if(size) {
      void* p=mmap(nullptr,size,PROT_READ,MAP_PRIVATE,fd,0);
      if(p==MAP_FAILED) {close(fd);throw std::runtime_error("cannot mmap "+path.string());}
      data=static_cast<const char*>(p);
    }
    close(fd);
  }
  ~Mapped() override {if(data)munmap(const_cast<char*>(data),size);}
};
struct Chunk {const char* begin;const char* end;};
std::vector<Chunk> map_file(Dataset& data,const std::filesystem::path& path) {
  auto file=std::make_unique<Mapped>(path);
  std::vector<Chunk> tasks;
  if(file->size) {
    const char* end=file->data+file->size;
    const char* p=static_cast<const char*>(memchr(file->data,'\n',file->size));
    if(p) ++p; else p=end; // exactly the first physical line is the header
    constexpr size_t chunk_bytes=8*1024*1024;
    while(p<end) {
      const char* q=end;
      if(static_cast<size_t>(end-p)>chunk_bytes) {
        const char* split=static_cast<const char*>(memchr(p+chunk_bytes,'\n',end-(p+chunk_bytes)));
        if(split)q=split+1;
      }
      tasks.push_back({p,q});p=q;
    }
  }
  data.storage.push_back(std::move(file));return tasks;
}
std::vector<Chunk> map_directory(Dataset& data,const std::filesystem::path& path) {
  std::vector<std::filesystem::path> files;
  for(const auto& entry:std::filesystem::directory_iterator(path))
    if(entry.is_regular_file()&&entry.path().extension()==".csv")files.push_back(entry.path());
  std::sort(files.begin(),files.end());
  std::vector<Chunk> chunks;
  for(const auto& file:files) {auto more=map_file(data,file);chunks.insert(chunks.end(),more.begin(),more.end());}
  return chunks;
}
template<class Fn> void lines(Chunk chunk,Fn&& fn) {
  const char* p=chunk.begin;
  while(p<chunk.end) {
    const char* q=static_cast<const char*>(memchr(p,'\n',chunk.end-p));
    if(!q)q=chunk.end;
    fn(SV(p,q-p));
    p=q==chunk.end?q:q+1;
  }
}
template<class Fn> void parallel(size_t count,unsigned threads,Fn&& fn) {
  std::atomic<size_t> next{0};std::atomic<bool> failed{false};
  std::exception_ptr error;std::mutex error_mutex;
  std::vector<std::thread> pool;
  unsigned n=std::min<size_t>(std::max(1u,threads),count);
  for(unsigned t=0;t<n;++t)pool.emplace_back([&,t] {
    try {while(!failed) {size_t i=next.fetch_add(1,std::memory_order_relaxed);if(i>=count)break;fn(i,t);}}
    catch(...) {std::lock_guard lock(error_mutex);if(!error)error=std::current_exception();failed=true;}
  });
  for(auto& t:pool)t.join();
  if(error)std::rethrow_exception(error);
}
template<class T> void append(std::vector<T>& dst,const std::vector<T>& src) {
  if(src.empty())return;
  size_t needed=dst.size()+src.size();
  if(needed>dst.capacity())dst.reserve(std::max(needed,dst.capacity()+dst.capacity()/8+1024));
  dst.insert(dst.end(),src.begin(),src.end());
}
struct Identities {
  std::unordered_map<SV,uint32_t,SVHash> index;
  std::vector<uint32_t> parent,weight;
  std::vector<SV> names;
  uint32_t add(SV name) {
    auto [it,inserted]=index.try_emplace(name,static_cast<uint32_t>(names.size()));
    if(inserted){parent.push_back(it->second);weight.push_back(1);names.push_back(name);}
    return it->second;
  }
  uint32_t root(uint32_t v) {
    while(parent[v]!=v){parent[v]=parent[parent[v]];v=parent[v];}return v;
  }
  void join(SV a,SV b) {
    uint32_t x=root(add(a)),y=root(add(b));if(x==y)return;
    if(weight[x]<weight[y])std::swap(x,y);
    parent[y]=x;weight[x]+=weight[y];if(names[y]<names[x])names[x]=names[y];
  }
  void freeze() {
    for(uint32_t i=0;i<parent.size();++i)parent[i]=root(i);
    for(uint32_t i=0;i<parent.size();++i)names[i]=names[parent[i]];
    std::vector<uint32_t>().swap(parent);std::vector<uint32_t>().swap(weight);
  }
  SV canonical(SV user)const {auto it=index.find(user);return it==index.end()?user:names[it->second];}
};
// All references remain valid because Dataset owns every source mmap.
struct RawEvent {
  const char* line;
  uint64_t hash;
  int64_t ingest;
  uint32_t length,id_length;
  SV id()const{return SV(line+(*line=='"'),id_length);}
};
SV json_string(SV input,SV key) {
  std::string needle;
  needle.reserve(key.size()+2);needle.push_back('"');needle.append(key);needle.push_back('"');
  size_t pos=input.find(needle);if(pos==SV::npos)throw std::runtime_error("manifest missing "+std::string(key));
  pos=input.find(':',pos+needle.size());if(pos==SV::npos)throw std::runtime_error("invalid manifest");
  pos=input.find('"',pos+1);if(pos==SV::npos)throw std::runtime_error("invalid manifest");
  size_t end=input.find('"',pos+1);if(end==SV::npos)throw std::runtime_error("invalid manifest");
  return input.substr(pos+1,end-pos-1);
}
}

Dataset load_dataset(const std::string& path,unsigned threads,unsigned partitions) {
  Dataset data;threads=std::max(1u,threads);partitions=std::max(1u,partitions);
  data.events.resize(partitions);data.clicks.resize(partitions);
  auto start=Clock::now();
  auto progress=[&](const char* label){std::cerr<<label<<" elapsed="<<std::chrono::duration<double>(Clock::now()-start).count()<<"s\n";};
  const std::filesystem::path base(path);
  std::ifstream mf(base/"manifest.json",std::ios::binary);
  std::string manifest((std::istreambuf_iterator<char>(mf)),{});
  if(!parse::timestamp(json_string(manifest,"window_start"),data.window_start)||
     !parse::timestamp(json_string(manifest,"window_end"),data.window_end)||data.window_start>=data.window_end)
    throw std::runtime_error("invalid manifest window");

  std::unordered_map<SV,uint32_t,SVHash> products,campaigns;
  std::unordered_map<std::string,uint32_t> categories;
  auto category=[&](SV name) {
    auto [it,inserted]=categories.try_emplace(std::string(name),data.categories.size());
    if(inserted)data.categories.emplace_back(name);
    return it->second;
  };
  uint32_t unknown=category("unknown");
  for(auto chunk:map_file(data,base/"products.csv"))lines(chunk,[&](SV line){
    std::array<SV,2> f;if(parse::csv(line,f))products.emplace(f[0],category(f[1]));
  });
  for(auto chunk:map_file(data,base/"campaigns.csv"))lines(chunk,[&](SV line){
    std::array<SV,2> f;if(parse::csv(line,f)){
      campaigns.emplace(f[0],data.campaigns.size());data.campaigns.push_back({std::string(f[0]),std::string(f[1])});
    }
  });
  Identities identities;
  for(auto chunk:map_directory(data,base/"links"))lines(chunk,[&](SV line){
    std::array<SV,2> f;if(parse::csv(line,f)&&!f[0].empty()&&!f[1].empty())identities.join(f[0],f[1]);
  });
  identities.freeze();progress("links");

  // Bucket globally by event_id BEFORE applying user, time, type or price filters.
  // A later valid copy may not rescue an invalid earliest copy.
  unsigned buckets=std::max(64u,threads*16);
  std::vector<std::vector<RawEvent>> raw(buckets);
  std::vector<std::mutex> raw_locks(buckets),output_locks(partitions);
  auto event_chunks=map_directory(data,base/"events");
  std::atomic<uint64_t> parsed{0},surviving{0};
  parallel(event_chunks.size(),threads,[&](size_t task,unsigned){
    std::vector<std::vector<RawEvent>> local(buckets);uint64_t count=0;
    lines(event_chunks[task],[&](SV line){
      std::array<SV,7> f;
      if(!parse::csv(line,f)||f[0].empty())return;
      int64_t ingest;
      if(!parse::timestamp(f[3],ingest))return; // ingest_ts is guaranteed valid F1.
      if(line.size()>UINT32_MAX||f[0].size()>UINT32_MAX)throw std::runtime_error("CSV row exceeds 4 GiB");
      uint64_t h=hash_sv(f[0]);
      local[h%buckets].push_back({line.data(),h,ingest,static_cast<uint32_t>(line.size()),static_cast<uint32_t>(f[0].size())});
      ++count;
    });
    for(unsigned b=0;b<buckets;++b){std::lock_guard lock(raw_locks[b]);append(raw[b],local[b]);}
    parsed.fetch_add(count,std::memory_order_relaxed);
  });
  data.parsed_events=parsed;progress("events parsed");

  parallel(buckets,threads,[&](size_t bucket,unsigned){
    auto& entries=raw[bucket];
    if(entries.empty())return;
    if(entries.size()>=UINT32_MAX)throw std::runtime_error("event bucket exceeds index capacity");
    size_t table_size=1;while(table_size<entries.size()*3/2+1)table_size*=2;
    std::vector<uint32_t> table(table_size,INVALID);
    for(uint32_t i=0;i<entries.size();++i){
      const auto& candidate=entries[i];size_t slot=(candidate.hash>>32)&(table_size-1);
      while(table[slot]!=INVALID){
        auto& old=entries[table[slot]];
        if(old.hash==candidate.hash&&old.id()==candidate.id()){
          if(candidate.ingest<old.ingest)table[slot]=i;
          break;
        }
        slot=(slot+1)&(table_size-1);
      }
      if(table[slot]==INVALID)table[slot]=i;
    }
    std::vector<std::vector<Event>> local(partitions);uint64_t count=0;
    for(uint32_t i:table){
      if(i==INVALID)continue;
      const auto& winner=entries[i];std::array<SV,7> f;
      parse::csv(SV(winner.line,winner.length),f);
      if(f[1].empty()||f[5].empty())continue;
      int64_t ts;
      if(!parse::timestamp(f[2],ts)||ts<data.window_start||ts>=data.window_end)continue;
      if(winner.ingest-ts>DAY)continue;
      uint32_t price=0,cat=INVALID;
      if(f[4]=="purchase"){
        uint64_t value;if(!parse::number(f[6],10000000,value)||value==0)continue;
        price=static_cast<uint32_t>(value);auto it=products.find(f[5]);cat=it==products.end()?unknown:it->second;
      }else if(f[4]!="view"&&f[4]!="cart")continue;
      SV user=identities.canonical(f[1]);
      local[hash_sv(user)%partitions].push_back({user,ts,price,cat});++count;
    }
    std::vector<RawEvent>().swap(entries);std::vector<uint32_t>().swap(table);
    for(unsigned p=0;p<partitions;++p){std::lock_guard lock(output_locks[p]);append(data.events[p],local[p]);}
    surviving.fetch_add(count,std::memory_order_relaxed);
  });
  data.surviving_events=surviving;progress("events deduped and validated");

  auto click_chunks=map_directory(data,base/"clicks");
  parallel(click_chunks.size(),threads,[&](size_t task,unsigned){
    std::vector<std::vector<Click>> local(partitions);
    lines(click_chunks[task],[&](SV line){
      std::array<SV,4> f;
      if(!parse::csv(line,f)||f[0].empty()||f[1].empty())return;
      auto campaign=campaigns.find(f[2]);if(campaign==campaigns.end())return;
      int64_t ts;if(!parse::timestamp(f[3],ts)||ts<data.window_start-WEEK||ts>=data.window_end)return;
      SV user=identities.canonical(f[1]);
      local[hash_sv(user)%partitions].push_back({user,f[0],ts,campaign->second});
    });
    // Identical click duplicates are intentionally retained: selecting the latest
    // click and unioning each campaign's eligible intervals are both idempotent.
    for(unsigned p=0;p<partitions;++p){std::lock_guard lock(output_locks[p]);append(data.clicks[p],local[p]);}
  });
  progress("clicks parsed");
  std::cerr<<"events parsed="<<data.parsed_events<<" surviving="<<data.surviving_events<<" linked_ids="<<identities.index.size()<<'\n';
  return data;
}
}
