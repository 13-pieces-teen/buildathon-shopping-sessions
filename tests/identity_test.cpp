#include "identity.hpp"
#include <array>
#include <cassert>
#include <iostream>
#include <random>
#include <new>
#include <utility>
#include <set>
#include <thread>
#include <unordered_set>
#include <sys/mman.h>
#include <unistd.h>

using namespace shopping;

void hash_checks() {
  std::mt19937_64 rng(71931);
  for(size_t length=0;length<257;++length) {
    std::string value(length,'x');
    for(char& byte:value)byte=static_cast<char>(rng());
    const auto baseline=hash_sv(value);
    std::string padded="prefix"+value+"suffix";
    assert(baseline==hash_sv(SV(padded.data()+6,length)));
    for(size_t i=0;i<length;++i) {
      value[i]^=1;assert(hash_sv(value)!=baseline);value[i]^=1;
    }
  }
  // Deliberately exercise overlapping loads at exactly 4/8/16 bytes.
  for(size_t length:{4,8,16}) {
    std::unordered_set<uint64_t> hashes;
    for(unsigned i=0;i<10000;++i) {
      std::string value(length,'\0');
      std::memcpy(value.data(),&i,sizeof(i));hashes.insert(hash_sv(value));
    }
    assert(hashes.size()==10000);
  }
  const size_t page=static_cast<size_t>(sysconf(_SC_PAGESIZE));
  auto* mapping=static_cast<char*>(mmap(nullptr,2*page,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0));
  assert(mapping!=MAP_FAILED);
  assert(mprotect(mapping+page,page,PROT_NONE)==0);
  std::memset(mapping,'z',page);
  for(size_t length=0;length<257;++length)
    assert(hash_sv(SV(mapping+page-length,length))==hash_sv(std::string(length,'z')));
  assert(munmap(mapping,2*page)==0);
}

void flat_checks() {
  FlatIdentityIndex table;
  std::vector<std::string> keys;
  keys.reserve(6000);
  // Equal-prefix, differing-length and trailing NUL keys must remain distinct.
  // These all deliberately share one hash, so hashing cannot hide an equality bug.
  for(size_t length=0;length<=20;++length) {
    keys.emplace_back(length,'\0');
    if(length) {
      keys.emplace_back(length,'p');
      std::string tail(length,'p');tail.back()='\0';keys.push_back(tail);
      std::string head(length,'p');head.front()='\0';
      if(head!=tail)keys.push_back(head);
    }
  }
  for(size_t length:{7,8,9,10,11,12,13,15,16,17,31,32,33,255,256,257}) {
    keys.push_back("key"+std::string(length-3,'x'));
    keys.push_back("key"+std::string(length-4,'x')+'A');
    keys.push_back("key"+std::string(length-4,'x')+'B');
  }
  // Exercise every packed high-byte position with arbitrary unsigned bytes.
  for(size_t position=8;position<11;++position)for(unsigned byte=0;byte<256;++byte) {
    std::string key(11,'Q');key[position]=static_cast<char>(byte);
    if(byte!='Q')keys.push_back(key);
  }
  keys.push_back(std::string(11,'Q'));
  for(unsigned i=0;i<5000;++i)keys.emplace_back(std::to_string(i)+std::string("\0key",4));
  // Remove only duplicate fixture spellings, before any string_view is retained.
  std::sort(keys.begin(),keys.end());keys.erase(std::unique(keys.begin(),keys.end()),keys.end());
  // Hash zero is legal. Every key deliberately collides, including duplicates.
  for(unsigned i=0;i<keys.size();++i) {
    auto [id,added]=table.add(keys[i],0);assert(added&&id==i);
  }
  assert(table.size()==keys.size());
  table.reserve(keys.size()*3); // force another rehash after all insertions
  for(unsigned i=0;i<keys.size();++i) {
    assert(table.find(keys[i],0)==i);
    const std::string separate_backing=keys[i];
    auto [id,added]=table.add(separate_backing,0);assert(!added&&id==i);
  }
  assert(table.find("missing",0)==INVALID);
  FlatIdentityIndex natural;
  for(unsigned i=0;i<keys.size();++i)assert(natural.add(keys[i],hash_sv(keys[i])).first==i);
  natural.reserve(keys.size()*2);
  for(unsigned i=0;i<keys.size();++i)assert(natural.find(keys[i],hash_sv(keys[i]))==i);
}

void identity_checks() {
  InternPool pool(3);Identities ids;ids.unlinked=&pool;
  std::vector<std::string> stable{"z","a","A"," a","a ",std::string("\x80",1),std::string("\x7f",1),std::string("q\0r",3)};
  ids.join(stable[0],stable[1]);ids.join(stable[1],stable[2]);
  ids.join(stable[2],stable[0]);ids.join(stable[0],stable[0]);
  ids.join(stable[3],stable[4]);ids.join(stable[5],stable[6]);
  ids.join(stable[7],stable[7]);ids.freeze(127);
  assert(ids.index.size()==8);
  assert(ids.canonical_record("a").first=="A");
  assert(ids.canonical_id("a")==ids.canonical_id("z"));
  assert(ids.canonical_record(" a").first==" a");
  assert(ids.canonical_record("a ").first==" a");
  assert(ids.canonical_record(stable[5]).first==stable[6]);
  assert(ids.canonical_record(stable[7]).first==stable[7]);
  assert(ids.canonical_id("A")!=ids.canonical_id(" a"));
  for(const auto& value:stable) {
    auto [name,id]=ids.canonical_record(value);
    assert(ids.name_for_id(id)==name);
    assert(ids.canonical(value).second==id%127);
  }
  std::string ephemeral="unlinked";
  auto [owned,number]=ids.canonical_record(ephemeral);
  ephemeral.assign(100000,'q');
  assert(owned=="unlinked"&&(number&InternPool::UNLINKED));
  assert(ids.canonical_id("unlinked")==number);
  assert(ids.name_for_id(number)==owned);
  // Arena growth must not invalidate short, long, NUL or empty stored views.
  std::vector<std::pair<SV,std::string>> expected;
  for(size_t length:{0,1,15,16,17,65535,65536,65537,200000}) {
    std::string value(length,'L');if(length)value[length/2]='\0';
    expected.push_back({pool.intern(value,hash_sv(value)),value});
  }
  for(const auto& [view,value]:expected)assert(view==value);
  assert(owned=="unlinked");
}

void threaded_checks() {
  InternPool pool(64);Identities ids;ids.unlinked=&pool;
  const std::string alias="alias",canonical="CANON";
  ids.join(alias,canonical);ids.freeze(128);
  constexpr unsigned count=5000,threads=8;
  std::vector<std::string> values;
  values.reserve(count);
  for(unsigned i=0;i<count;++i)values.push_back("raw-user-"+std::to_string(i));
  std::array<std::vector<uint32_t>,threads> observed;
  std::vector<std::thread> workers;
  for(unsigned t=0;t<threads;++t)workers.emplace_back([&,t] {
    observed[t].resize(count);
    for(unsigned step=0;step<count;++step) {
      const unsigned i=(step+t*997)%count;
      std::string temporary=values[i];
      auto [view,id]=ids.canonical_record(temporary);
      observed[t][i]=id;assert(view==values[i]);
      assert(ids.canonical(temporary).second==id%128);
      assert(ids.canonical_record(alias).first==canonical);
    }
  });
  for(auto& worker:workers)worker.join();
  for(unsigned t=1;t<threads;++t)assert(observed[t]==observed[0]);
  std::set<uint32_t> unique(observed[0].begin(),observed[0].end());
  assert(unique.size()==count);
  for(unsigned i=0;i<count;++i)assert(ids.name_for_id(observed[0][i])==values[i]);
}

void snapshot_checks() {
  // Export after all lookups, destroy Identities, then decode through Dataset.
  // Its numeric order is intentionally unrelated to the canonical byte order.
  for(unsigned shards:{1,3,64,127}) {
    Dataset data;
    auto pool=std::make_unique<InternPool>(shards);
    InternPool* raw=pool.get();data.storage.push_back(std::move(pool));
    std::vector<std::string> linked={"linked-z","linked-a",std::string("a\0tail",6)};
    std::vector<std::pair<uint32_t,std::string>> expected;
    {
      Identities ids;ids.unlinked=raw;
      ids.join(linked[0],linked[1]);ids.join(linked[1],linked[2]);ids.freeze(7);
      const uint32_t linked_id=ids.canonical_id(linked[0]);
      for(const auto& alias:linked) {
        assert(ids.canonical_id(alias)==linked_id);
        expected.push_back({linked_id,linked[2]});
      }
      for(unsigned i=0;i<500;++i) {
        std::string temporary="user-"+std::to_string(i)+std::string("\0tail",5);
        auto [name,id]=ids.canonical_record(temporary);
        expected.push_back({id,temporary});
        temporary.assign(200,'x');
        assert(name==expected.back().second);
      }
      data.linked_names=std::move(ids.names);
      data.unlinked_names=raw->names_snapshot();
      data.unlinked_shift=raw->shard_bits();data.unlinked_mask=raw->shard_mask();
    }
    for(const auto& [id,name]:expected)assert(data.person_name(id)==name);
  }
}

void epoch_checks() {
  // Reuse exactly the same object address; the prior cached key is destroyed.
  alignas(Identities) std::byte memory[sizeof(Identities)];
  for(unsigned i=0;i<100;++i) {
    InternPool pool(16);
    auto* ids=new(memory)Identities;ids->unlinked=&pool;
    const std::string name="a"+std::to_string(i),alias="repeat-alias";
    ids->join(name,alias);ids->freeze(17);
    assert(ids->canonical_record(alias).first==name);
    assert(ids->canonical_record("unlinked-hot").first=="unlinked-hot");
    ids->~Identities();
  }
}

int main() {
  std::cerr<<"hash checks\n";hash_checks();
  std::cerr<<"packed flat-index checks\n";flat_checks();
  std::cerr<<"identity/arena checks\n";identity_checks();
  std::cerr<<"threaded identity checks\n";threaded_checks();
  std::cerr<<"Dataset snapshot checks\n";snapshot_checks();
  std::cerr<<"identity epoch checks\n";epoch_checks();
  std::cout<<"identity/hash/packed-key/snapshot checks PASS\n";
}
