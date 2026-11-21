#include "types.hpp"
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <tuple>

using namespace shopping;
void require(bool condition,const char* message) {if(!condition)throw std::runtime_error(message);}
struct Reader {
  std::vector<char> bytes;
  size_t at=0;
  explicit Reader(const std::filesystem::path& path) {
    std::ifstream file(path,std::ios::binary);
    bytes.assign(std::istreambuf_iterator<char>(file),{});
    require(bytes.size()>=16 && std::memcmp(bytes.data(),"SSCOL01\0",8)==0,"bad binary header");
    at=8;
  }
  template<class T>T take() {
    require(at<=bytes.size() && sizeof(T)<=bytes.size()-at,"truncated scalar");
    T value;std::memcpy(&value,bytes.data()+at,sizeof(T));at+=sizeof(T);return value;
  }
  template<class T>std::vector<T> numbers(size_t n) {
    std::vector<T> result;result.reserve(n);
    for(size_t i=0;i<n;++i)result.push_back(take<T>());
    return result;
  }
  std::vector<std::optional<std::string>> strings(size_t n,bool nullable=false) {
    const uint64_t size=take<uint64_t>();
    auto offsets=numbers<int32_t>(n+1);
    require(size<=bytes.size()-at,"truncated string data");
    const char* data=bytes.data()+at;at+=size;
    std::vector<uint8_t> valid;
    if(nullable)valid=numbers<uint8_t>((n+7)/8);
    require(offsets.front()==0 && offsets.back()==static_cast<int64_t>(size),"bad offsets extent");
    std::vector<std::optional<std::string>> result;
    for(size_t i=0;i<n;++i){
      require(offsets[i]>=0 && offsets[i]<=offsets[i+1],"bad offsets order");
      if(nullable && !(valid[i/8]&(1u<<(i%8))))result.emplace_back(std::nullopt);
      else result.emplace_back(std::string(data+offsets[i],offsets[i+1]-offsets[i]));
    }
    return result;
  }
  void finish(){require(at==bytes.size(),"unexpected trailing bytes");}
};
struct ExpectedUser {
  std::string user;int32_t day;int64_t sessions,events,revenue;
  std::optional<std::string> category;
  bool operator==(const ExpectedUser&)const=default;
};
struct ExpectedCampaign {
  std::string campaign,channel;int32_t day;int64_t sessions,purchasing,revenue,assists;
  bool operator==(const ExpectedCampaign&)const=default;
};
void check(const Dataset& data,const Results& results,const std::filesystem::path& folder,unsigned cpus) {
  setenv("NCPU",std::to_string(cpus).c_str(),1);
  write_results(data,results,folder.string());
  std::vector<ExpectedUser> expected_users,actual_users;
  std::vector<ExpectedCampaign> expected_campaigns,actual_campaigns;
  for(const auto& partition:results.users)for(const auto& row:partition)
    expected_users.push_back({std::string(row.user),row.day,row.sessions,row.events,row.revenue,
      row.category==INVALID ? std::nullopt : std::optional<std::string>(data.categories.at(row.category))});
  for(const auto& row:results.campaigns){
    const bool organic=row.campaign==data.campaigns.size();
    expected_campaigns.push_back({organic?"organic":data.campaigns.at(row.campaign).id,
      organic?"organic":data.campaigns.at(row.campaign).channel,row.day,row.sessions,row.purchasing,row.revenue,row.assists});
  }
  size_t user_files=0,campaign_files=0;
  for(const auto& entry:std::filesystem::directory_iterator(folder)){
    Reader reader(entry.path());size_t n=reader.take<uint64_t>();
    if(entry.path().filename().string().starts_with("user-")){
      ++user_files;
      auto user=reader.strings(n);auto days=reader.numbers<int32_t>(n);
      auto sessions=reader.numbers<int64_t>(n),events=reader.numbers<int64_t>(n),revenue=reader.numbers<int64_t>(n);
      auto category=reader.strings(n,true);
      for(size_t i=0;i<n;++i)actual_users.push_back({user[i].value(),days[i],sessions[i],events[i],revenue[i],category[i]});
    }else{
      ++campaign_files;
      auto campaign=reader.strings(n),channel=reader.strings(n);auto days=reader.numbers<int32_t>(n);
      auto sessions=reader.numbers<int64_t>(n),purchasing=reader.numbers<int64_t>(n);
      auto revenue=reader.numbers<int64_t>(n),assists=reader.numbers<int64_t>(n);
      for(size_t i=0;i<n;++i)actual_campaigns.push_back({campaign[i].value(),channel[i].value(),days[i],sessions[i],purchasing[i],revenue[i],assists[i]});
    }
    reader.finish();
  }
  auto user_less=[](const ExpectedUser& a,const ExpectedUser& b){return std::tie(a.user,a.day)<std::tie(b.user,b.day);};
  auto campaign_less=[](const ExpectedCampaign& a,const ExpectedCampaign& b){return std::tie(a.campaign,a.day)<std::tie(b.campaign,b.day);};
  std::sort(expected_users.begin(),expected_users.end(),user_less);std::sort(actual_users.begin(),actual_users.end(),user_less);
  std::sort(expected_campaigns.begin(),expected_campaigns.end(),campaign_less);std::sort(actual_campaigns.begin(),actual_campaigns.end(),campaign_less);
  require(expected_users==actual_users,"user output mismatch");
  require(expected_campaigns==actual_campaigns,"campaign output mismatch");
  require(user_files>=1 && user_files<=cpus && campaign_files==1,"incorrect file count");
}
int main(int argc,char** argv) {
  try {
    require(argc==2,"usage: output_test OUTPUT_ROOT");
    const std::filesystem::path root(argv[1]);
    Dataset data;data.categories={"books","",std::string("cat\0tail",8),"鞋\t类\r"};
    data.campaigns={{"c_1","search"},{"广告\t二","社会"}};
    const std::vector<std::string> names={"u1"," leading\t\r",std::string("id\0tail",7),"用户"};
    Results rich;rich.users.resize(11);
    for(size_t i=0;i<11;++i){
      if(i==2 || i==6)continue;
      rich.users[i].push_back({names[i%names.size()],static_cast<int32_t>(20000+i),
                              static_cast<int64_t>(i+1),static_cast<int64_t>(i+3),
                              (int64_t(1)<<40)+static_cast<int64_t>(i),
                              i%5==0 && i<10?INVALID:static_cast<uint32_t>(i%4)});
    }
    rich.campaigns={{0,20000,10,4,int64_t(1)<<42,21},{1,20001,0,0,0,3},{2,-1,1,0,0,0}};
    for(unsigned cpus:{1u,2u,8u})check(data,rich,root/("rich-"+std::to_string(cpus)),cpus);
    Results all_null;all_null.users.resize(5);
    all_null.users[0].push_back({names[0],-1,1,1,0,INVALID});
    all_null.users[4].push_back({names[1],20000,1,2,0,INVALID});
    all_null.campaigns={{2,-1,1,0,0,0},{2,20000,1,0,0,0}};
    check(data,all_null,root/"all-null",8);
    Results empty;check(data,empty,root/"empty-zero-partitions",1);
    empty.users.resize(11);check(data,empty,root/"empty-many-partitions",8);
    Results bad;bad.users.resize(3);bad.users[2].push_back({names[0],0,1,1,1,999});
    bool threw=false;try{write_results(data,bad,(root/"expected-error").string());}catch(const std::out_of_range&){threw=true;}
    require(threw,"worker exception was not propagated");
    std::cout<<"6 output protocol cases and worker exception propagation passed\n";
  }catch(const std::exception& error){std::cerr<<error.what()<<'\n';return 1;}
}
