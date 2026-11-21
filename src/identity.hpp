#pragma once
#include "types.hpp"
#include <atomic>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace shopping {

struct HashedId { SV value; uint64_t hash; };
struct IdentityHash {
  using is_transparent=void;
  size_t operator()(SV value)const {return hash_sv(value);}
  size_t operator()(HashedId value)const {return value.hash;}
};
struct IdentityEqual {
  using is_transparent=void;
  bool operator()(SV a,SV b)const {return a==b;}
  bool operator()(SV a,HashedId b)const {return a==b.value;}
  bool operator()(HashedId a,SV b)const {return a.value==b;}
};

// Store complete keys up to eleven bytes directly in each sixteen-byte slot.
// Length is encoded separately, so embedded/trailing NULs remain significant.
// Longer keys retain a hash fingerprint and always compare the original bytes.
class FlatIdentityIndex {
  struct Slot {uint64_t low=0; uint32_t high=0,id=INVALID;};
  std::vector<Slot> slots_;
  std::vector<SV> keys_;
  std::vector<uint64_t> hashes_;
  static std::pair<uint64_t,uint32_t> packed(SV key,uint64_t hash) {
    if(key.size()>11)return {hash,0xff000000u};
    uint64_t low=0;uint32_t high=static_cast<uint32_t>(key.size())<<24;
    if(key.size()>=8) {
      std::memcpy(&low,key.data(),8);
      for(size_t i=8;i<key.size();++i)
        high|=static_cast<uint32_t>(static_cast<unsigned char>(key[i]))<<((i-8)*8);
    } else if(!key.empty()) std::memcpy(&low,key.data(),key.size());
    return {low,high};
  }
  void rehash(size_t count) {
    std::vector<Slot> next(count);
    const size_t mask=count-1;
    for(const auto& old:slots_)if(old.id!=INVALID) {
      size_t pos=(hashes_[old.id]>>32)&mask;
      while(next[pos].id!=INVALID)pos=(pos+1)&mask;
      next[pos]=old;
    }
    slots_.swap(next);
  }
public:
  size_t size()const noexcept {return keys_.size();}
  SV key(uint32_t id)const noexcept {return keys_[id];}
  void reserve(size_t count) {
    size_t capacity=slots_.empty()?16:slots_.size();
    while(capacity/10*7+(capacity%10)*7/10<count) {
      if(capacity>std::numeric_limits<size_t>::max()/2)
        throw std::length_error("identity table too large");
      capacity*=2;
    }
    if(capacity>slots_.size())rehash(capacity);
    keys_.reserve(count);
    hashes_.reserve(count);
  }
  uint32_t find(SV value,uint64_t hash)const noexcept {
    if(slots_.empty())return INVALID;
    const size_t mask=slots_.size()-1;
    const auto [low,high]=packed(value,hash);
    // Intern shards select low hash bits. Use high bits here to avoid making
    // every key in a shard begin probing at the same few slots.
    size_t pos=(hash>>32)&mask;
    for(;;) {
      const auto& slot=slots_[pos];
      if(slot.id==INVALID)return INVALID;
      if(slot.low==low&&slot.high==high&&
         (value.size()<=11||keys_[slot.id]==value))return slot.id;
      pos=(pos+1)&mask;
    }
  }
  uint32_t insert_absent(SV value,uint64_t hash) {
    if(keys_.size()>=INVALID)throw std::length_error("too many identity keys");
    if(slots_.empty()||(keys_.size()+1)*10>slots_.size()*7)
      rehash(slots_.empty()?16:slots_.size()*2);
    const auto id=static_cast<uint32_t>(keys_.size());
    keys_.push_back(value);
    hashes_.push_back(hash);
    const size_t mask=slots_.size()-1;
    size_t pos=(hash>>32)&mask;
    while(slots_[pos].id!=INVALID)pos=(pos+1)&mask;
    const auto [low,high]=packed(value,hash);
    slots_[pos]={low,high,id};
    return id;
  }
  std::pair<uint32_t,bool> add(SV value,uint64_t hash) {
    auto id=find(value,hash);
    return id==INVALID?std::pair{insert_absent(value,hash),true}:std::pair{id,false};
  }
  const std::vector<SV>& keys()const noexcept {return keys_;}
};

class IdentityArena {
  std::vector<std::unique_ptr<char[]>> blocks_;
  char* next_=nullptr;
  size_t remaining_=0;
public:
  SV copy(SV value) {
    // Allocate one byte even for an empty key; no null-pointer arithmetic.
    const size_t size=std::max<size_t>(1,value.size());
    if(remaining_<size) {
      const size_t capacity=std::max<size_t>(65536,size);
      auto block=std::make_unique<char[]>(capacity);
      next_=block.get();remaining_=capacity;blocks_.push_back(std::move(block));
    }
    char* result=next_;
    if(!value.empty())std::memcpy(result,value.data(),value.size());
    next_+=size;remaining_-=size;
    return SV(result,value.size());
  }
};

struct InternPool final:Storage {
  static constexpr uint32_t UNLINKED=uint32_t{1}<<31;
private:
  struct alignas(64) Shard {
    std::mutex mutex;
    IdentityArena arena;
    FlatIdentityIndex index;
  };
  unsigned shard_bits_=0;
  uint32_t shard_mask_=0;
  std::vector<Shard> shards_;
  static unsigned shard_count(unsigned count) {
    unsigned result=1;
    while(result<std::max(1u,count)) {
      if(result>=UNLINKED/2)throw std::length_error("too many intern shards");
      result*=2;
    }
    return result;
  }
public:
  explicit InternPool(unsigned count):shards_(shard_count(count)) {
    shard_mask_=static_cast<uint32_t>(shards_.size()-1);
    for(size_t count=shards_.size();count>1;count>>=1)++shard_bits_;
  }
  // Return the stable view while holding the shard lock. Reading index.key()
  // outside that lock would race with another thread growing its key vector.
  std::pair<SV,uint32_t> intern_record(SV value,uint64_t hash) {
    const uint32_t shard_id=static_cast<uint32_t>(hash)&shard_mask_;
    auto& shard=shards_[shard_id];
    std::lock_guard lock(shard.mutex);
    auto id=shard.index.find(value,hash);
    if(id==INVALID) {
      if(shard.index.size()>=(UNLINKED>>shard_bits_))
        throw std::length_error("too many identities in an intern shard");
      id=shard.index.insert_absent(shard.arena.copy(value),hash);
    }
    return {shard.index.key(id),UNLINKED|(id<<shard_bits_)|shard_id};
  }
  SV intern(SV value,uint64_t hash) {return intern_record(value,hash).first;}
  // Only after all interning threads have joined. The returned character
  // storage itself remains stable even when later calls add more strings.
  SV name_for_id(uint32_t id)const noexcept {
    const uint32_t shard_id=id&shard_mask_;
    return shards_[shard_id].index.key((id&~UNLINKED)>>shard_bits_);
  }
  unsigned shard_bits()const noexcept {return shard_bits_;}
  uint32_t shard_mask()const noexcept {return shard_mask_;}
  std::vector<std::vector<SV>> names_snapshot()const {
    std::vector<std::vector<SV>> names;names.reserve(shards_.size());
    for(const auto& shard:shards_)names.push_back(shard.index.keys());
    return names;
  }
};

namespace identity_detail {
inline std::atomic<uint64_t> next_epoch{1};
struct CacheEntry {
  uint64_t epoch=0,hash=0;
  SV key,name;
  uint32_t id=INVALID;
};
// Thread-local entries never retain a view into event/click mmap storage.
// Epochs prevent stale cache hits when an Identities address is reused.
inline thread_local CacheEntry cache[128];
}

struct Identities {
  FlatIdentityIndex index;
  std::vector<uint32_t> parent,weight;
  std::vector<SV> names;
  unsigned partitions=1;
  InternPool* unlinked=nullptr;
private:
  const uint64_t epoch_=identity_detail::next_epoch.fetch_add(1,std::memory_order_relaxed);
public:
  Identities()=default;
  Identities(const Identities&)=delete;
  Identities& operator=(const Identities&)=delete;
  uint32_t add(SV name) {
    const uint64_t hash=hash_sv(name);
    const auto [id,inserted]=index.add(name,hash);
    if(inserted) {
      if(id>=InternPool::UNLINKED)throw std::length_error("too many linked IDs");
      parent.push_back(id);weight.push_back(1);names.push_back(name);
    }
    return id;
  }
  uint32_t root(uint32_t id) {
    while(parent[id]!=id) {parent[id]=parent[parent[id]];id=parent[id];}
    return id;
  }
  void join(SV a,SV b) {
    uint32_t x=root(add(a)),y=root(add(b));
    if(x==y)return;
    if(weight[x]<weight[y])std::swap(x,y);
    parent[y]=x;weight[x]+=weight[y];
    if(names[y]<names[x])names[x]=names[y];
  }
  // All additions/joins must precede freeze; all concurrent lookups follow it.
  void freeze(unsigned count) {
    partitions=std::max(1u,count);
    for(uint32_t i=0;i<parent.size();++i)parent[i]=root(i);
    for(uint32_t i=0;i<parent.size();++i)names[i]=names[parent[i]];
    std::vector<uint32_t>().swap(weight);
  }
  std::pair<SV,uint32_t> canonical_record(SV user)const {
    const uint64_t hash=hash_sv(user);
    auto& cached=identity_detail::cache[hash&127];
    if(cached.epoch==epoch_&&cached.hash==hash&&cached.key==user)
      return {cached.name,cached.id};
    const uint32_t original=index.find(user,hash);
    SV key,name;uint32_t id;
    if(original!=INVALID) {
      id=parent[original];name=names[id];key=index.key(original);
    } else {
      const auto result=unlinked->intern_record(user,hash);
      name=result.first;id=result.second;key=name;
    }
    cached={epoch_,hash,key,name,id};
    return {name,id};
  }
  uint32_t canonical_id(SV user)const {return canonical_record(user).second;}
  SV name_for_id(uint32_t id)const noexcept {
    return (id&InternPool::UNLINKED)?unlinked->name_for_id(id):names[id];
  }
  std::pair<SV,uint32_t> canonical(SV user)const {
    const auto [name,id]=canonical_record(user);
    return {name,id%partitions};
  }
};

}
