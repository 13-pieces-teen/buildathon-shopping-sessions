#pragma once
#include "types.hpp"
#include <array>
#include <cstring>
#include <limits>

namespace shopping::parse {
// Only LF terminates a record. CR and every other whitespace byte are data.
template<size_t N> inline bool csv(SV line, std::array<SV,N>& fields) {
  if (line.empty()) return false;
  const char* p = line.data();
  const char* end = p + line.size();
  for (size_t i=0;i<N;++i) {
    const char* comma = static_cast<const char*>(std::memchr(p, ',', end-p));
    if ((i+1<N) != (comma!=nullptr)) return false;
    const char* q = comma ? comma : end;
    SV value(p, q-p);
    if (!value.empty() && value.front()=='"') {
      if (value.size()<2 || value.back()!='"') return false;
      value.remove_prefix(1); value.remove_suffix(1);
    }
    if (std::memchr(value.data(), '"', value.size())) return false;
    fields[i]=value;
    p = comma ? comma+1 : end;
  }
  return true;
}

inline bool number(SV s, uint64_t limit, uint64_t& result) {
  if (s.empty()) return false;
  uint64_t n=0;
  for (unsigned char ch:s) {
    if (ch<'0'||ch>'9') return false;
    unsigned d=ch-'0';
    if (n>limit/10 || (n==limit/10 && d>limit%10)) return false;
    n=n*10+d;
  }
  result=n; return true;
}

inline int64_t civil_days(int year, unsigned month, unsigned day) {
  year -= month<=2;
  int era = (year>=0 ? year : year-399)/400;
  unsigned yoe=static_cast<unsigned>(year-era*400);
  unsigned doy=(153*(month+(month>2 ? -3 : 9))+2)/5+day-1;
  unsigned doe=yoe*365+yoe/4-yoe/100+doy;
  return static_cast<int64_t>(era)*146097+doe-719468;
}

inline bool timestamp(SV s, int64_t& out) {
  if (s.empty()) return false;
  if (s.size()!=20 && s.size()!=25) {
    uint64_t n;
    if (!number(s, static_cast<uint64_t>(INT64_MAX), n)) return false;
    out=static_cast<int64_t>(n); return true;
  }
  // Numeric forms may have any number of leading zeroes, including length 20/25.
  if (s[4]!='-') {
    uint64_t n;
    if (!number(s, static_cast<uint64_t>(INT64_MAX), n)) return false;
    out=static_cast<int64_t>(n); return true;
  }
  if (s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':') return false;
  constexpr unsigned positions[]={0,1,2,3,5,6,8,9,11,12,14,15,17,18};
  for (unsigned pos:positions) if(s[pos]<'0'||s[pos]>'9') return false;
  auto two=[&](unsigned p) { return (s[p]-'0')*10+s[p+1]-'0'; };
  int year=(s[0]-'0')*1000+(s[1]-'0')*100+two(2);
  int month=two(5), day=two(8), hour=two(11), minute=two(14), second=two(17);
  if (month<1||month>12||hour>23||minute>59||second>59) return false;
  constexpr int days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  int maxday=days[month]+(month==2 && year%4==0 && (year%100!=0||year%400==0));
  if(day<1||day>maxday) return false;
  int offset=0;
  if(s.size()==20) { if(s[19]!='Z') return false; }
  else {
    if((s[19]!='+'&&s[19]!='-')||s[22]!=':') return false;
    for(unsigned p:{20u,21u,23u,24u}) if(s[p]<'0'||s[p]>'9') return false;
    int oh=two(20),om=two(23);
    if(oh>23||om>59) return false;
    offset=(oh*60+om)*(s[19]=='+' ? 1 : -1);
  }
  out=civil_days(year,month,day)*DAY+(hour*3600+minute*60+second)*1000LL-offset*60000LL;
  return true;
}
}
