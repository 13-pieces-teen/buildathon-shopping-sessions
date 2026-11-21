#include "types.hpp"
#include "parse.hpp"
#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <unistd.h>

using namespace shopping;
void require(bool condition,const std::string& what) {
  if(!condition)throw std::runtime_error(what);
}
void id_storage_checks() {
  char pattern[]="/tmp/shopping-ingest-ids-XXXXXX";
  char* directory=mkdtemp(pattern);require(directory!=nullptr,"create temporary ID fixture");
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup(){std::error_code ec;std::filesystem::remove_all(path,ec);}
  } cleanup{directory};
  const auto& root=cleanup.path;
  for(const char* folder:{"events","clicks","links"})std::filesystem::create_directory(root/folder);
  auto write=[&](const std::filesystem::path& path,const std::string& value){
    std::ofstream file(root/path,std::ios::binary);file<<value;require(bool(file),"write temporary ID fixture");
  };
  write("manifest.json","{\"window_start\":\"2026-10-01T00:00:00Z\",\"window_end\":\"2026-10-15T00:00:00Z\"}");
  write("products.csv","product_id,category\np1,shoes\n");
  write("campaigns.csv","campaign_id,channel\nc_1,search\n");
  write("links/part.csv","user_id_a,user_id_b\n");
  write("clicks/part.csv","click_id,user_id,campaign_id,ts\n");
  const std::string header="event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n";
  std::string later=header,earlier=header;
  std::vector<std::string> ids={
    "short", "0123456789abcde", "0123456789abcdef",
    "0123456789abcdefA", "0123456789abcdefB",
    std::string(256,'x')+"A", std::string(256,'x')+"B",
    "0123456789abcde\r", std::string("id\0suffix",9)
  };
  std::map<std::string,std::pair<uint64_t,uint64_t>> expected;
  for(size_t i=0;i<ids.size();++i){
    std::string user="u";user+=std::to_string(i);
    const std::string price=std::to_string(101+i);
    // Quoting changes only syntax, including the 16/17-byte storage boundary.
    later+=ids[i]+",wrong_user,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,purchase,p1,999\n";
    earlier+='"'+ids[i]+"\","+user+",2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,"+price+"\n";
    expected[user]={1,101+i};
  }
  const std::string invalid_id=std::string(256,'x')+"invalid";
  later+=invalid_id+",rescue_forbidden,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,purchase,p1,999\n";
  earlier+='"'+invalid_id+"\",dropped_user,bad_ts,2026-10-03T10:01:00Z,purchase,p1,999\n";
  // A malformed earlier copy cannot suppress a later well-formed long ID.
  const std::string malformed_id=std::string(256,'m');
  earlier+=malformed_id+"\",wrong_user,2026-10-03T10:00:00Z,2026-10-03T10:00:00Z,purchase,p1,999\n";
  later+=malformed_id+",u_malformed,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,purchase,p1,250\n";
  expected["u_malformed"]={1,250};
  earlier+="intern_a,shared_unlinked,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,123\n";
  later+="intern_b,shared_unlinked,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,purchase,p1,321\n";
  expected["shared_unlinked"]={2,444};
  write("events/part-a.csv",later);write("events/part-b.csv",earlier);
  for(unsigned threads:{1u,4u}){
    auto dataset=load_dataset(root.string(),threads,7);
    std::map<std::string,std::pair<uint64_t,uint64_t>> actual;
    const char* shared_address=nullptr;
    for(const auto& partition:dataset.events)for(const auto& event:partition){
      const SV user=dataset.person_name(event.user);
      auto& totals=actual[std::string(user)];++totals.first;totals.second+=event.price;
      if(user=="shared_unlinked"){
        if(shared_address)require(shared_address==user.data(),"unlinked user is interned exactly once");
        shared_address=user.data();
      }
    }
    require(actual==expected,"short/long/quoted event ID dedup regression");
  }
}
int main(int argc,char** argv) {
  try {
    int64_t t=0,other=0;
    require(parse::timestamp("2028-02-29T00:00:00Z",t),"leap day");
    require(parse::timestamp("2028-02-29T23:59:00+23:59",other)&&t==other,"maximum offset");
    require(parse::timestamp("2028-02-29T00:00:00-00:00",other)&&t==other,"negative zero offset");
    require(parse::timestamp("0000000000000000000000000000000000000000000000000000000000086400000",t)&&t==86400000,"arbitrary leading zeros");
    for(SV bad:{"2026-02-29T00:00:00Z","2028-02-30T00:00:00Z","2026-13-01T00:00:00Z",
                "2026-10-01T24:00:00Z","2026-10-01T00:00:60Z","2026-10-01t00:00:00Z",
                "2026-10-01T00:00:00z","2026-10-01T00:00:00Z\r","2026-10-01T00:00:00+24:00",
                "2026-10-01T00:00:00+05:60","2026-10-01T00:00:00+0200","1.79e12","-5","+5",
                "999999999999999999999999999999999999999999999999999999999999999999"})
      require(!parse::timestamp(bad,t),"accepted invalid timestamp "+std::string(bad));
    std::array<SV,2> pair;
    require(parse::csv("\" u1\",u1 \r",pair)&&pair[0]==" u1"&&pair[1]=="u1 \r","preserve whitespace and CR");
    require(parse::csv("\"\",",pair)&&pair[0].empty()&&pair[1].empty(),"quoted empty");
    require(parse::csv("user_id_a,user_id_b",pair),"header-like data row");
    for(SV bad:{"","a","a,b,c","\"a,b\"","a,b\"","a,\"b\"\r","a,\"\"\""})
      require(!parse::csv(bad,pair),"accepted malformed CSV "+std::string(bad));
    uint64_t price=0;
    require(parse::number("000000250",10000000,price)&&price==250,"leading zero price");
    require(!parse::number("10000001",10000000,price),"price limit");
    require(!parse::number("250\r",10000000,price),"price CR");
    id_storage_checks();
    require(argc==2,"usage: ingest_checks FIXTURES");
    size_t checked=0;
    for(const auto& entry:std::filesystem::directory_iterator(argv[1])) {
      if(!entry.is_directory())continue;
      auto dataset=load_dataset((entry.path()/"input").string(),4,7);
      using Totals=std::pair<uint64_t,uint64_t>;
      std::map<std::string,Totals> actual,expected;
      for(const auto& partition:dataset.events)for(const auto& event:partition){
        auto& v=actual[std::string(dataset.person_name(event.user))];++v.first;v.second+=event.price;
        require(event.price ? event.category<dataset.categories.size() : event.category==INVALID,"category contract");
      }
      // Partition choice is an implementation detail (string hash or compact
      // person ID). The semantic requirement is that all events AND clicks for
      // one canonical person are co-located, so attribution can see both.
      std::map<std::string,std::pair<uint32_t,size_t>> identity_by_person;
      auto check_person_partition=[&](uint32_t user_id,size_t partition) {
        const SV user=dataset.person_name(user_id);
        auto [it,inserted]=identity_by_person.try_emplace(std::string(user),user_id,partition);
        require(inserted||it->second.first==user_id,"canonical person has one encoded identity");
        require(inserted||it->second.second==partition,"canonical person event/click co-location");
      };
      for(size_t p=0;p<dataset.events.size();++p)for(const auto& event:dataset.events[p])
        check_person_partition(event.user,p);
      for(size_t p=0;p<dataset.clicks.size();++p)for(const auto& click:dataset.clicks[p])
        check_person_partition(click.user,p);
      std::ifstream input(entry.path()/"expected"/"user_daily.csv",std::ios::binary);
      std::string line;std::getline(input,line);
      while(std::getline(input,line)){
        std::array<SV,6> f;require(parse::csv(line,f),"expected CSV parse");
        auto& v=expected[std::string(f[0])];
        v.first+=std::stoull(std::string(f[3]));v.second+=std::stoull(std::string(f[4]));
      }
      require(actual==expected,"ingest mismatch "+entry.path().filename().string());
      ++checked;
    }
    std::cout<<"parser boundaries, short/long/quoted IDs and "<<checked<<" fixture ingest totals passed\n";
  }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
