#pragma once
#include "types.hpp"
#include <array>
#include <cstring>
#include <limits>
#if defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace shopping::parse {
// Only LF terminates a record. CR and every other whitespace byte are data.
template<size_t N> inline bool csv_scalar(SV line, std::array<SV,N>& fields) {
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

inline bool number_scalar(SV s, uint64_t limit, uint64_t& result) {
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

template<size_t N> inline bool csv(SV line,std::array<SV,N>& fields) {
  if constexpr(N<4)return csv_scalar(line,fields);
#if defined(__SSE2__)
  // Each lane counts at most 16 quotes at this bound, so byte counters cannot
  // wrap. Arbitrarily long IDs/rows retain the exact scalar implementation.
  if(line.size()<16||line.size()>256)return csv_scalar(line,fields);
  const char* begin=line.data();const size_t size=line.size();
  size_t field=0,start=0;unsigned expected_quotes=0;
  const __m128i comma=_mm_set1_epi8(','),quote=_mm_set1_epi8('"');
  __m128i quote_counts=_mm_setzero_si128();
  const auto append=[&](size_t end) {
    SV value(begin+start,end-start);
    if(!value.empty()&&value.front()=='"') {
      if(value.size()<2||value.back()!='"')return false;
      expected_quotes+=2;value.remove_prefix(1);value.remove_suffix(1);
    }
    fields[field++]=value;start=end+1;return true;
  };
  const auto block=[&](unsigned delimiters,size_t offset) {
    while(delimiters) {
      const unsigned bit=static_cast<unsigned>(__builtin_ctz(delimiters));
      if(field>=N-1||!append(offset+bit))return false;
      delimiters&=delimiters-1;
    }
    return true;
  };
  size_t offset=0;
  for(;offset+16<=size;offset+=16) {
    const __m128i input=_mm_loadu_si128(reinterpret_cast<const __m128i*>(begin+offset));
    quote_counts=_mm_sub_epi8(quote_counts,_mm_cmpeq_epi8(input,quote));
    if(!block(static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(input,comma))),offset))return false;
  }
  if(offset<size) {
    // The final load stays within the line; discard its already scanned prefix.
    const __m128i input=_mm_loadu_si128(reinterpret_cast<const __m128i*>(begin+size-16));
    const unsigned shift=16-static_cast<unsigned>(size-offset);
    const __m128i positions=_mm_setr_epi8(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
    const __m128i tail=_mm_cmpgt_epi8(positions,_mm_set1_epi8(static_cast<char>(shift-1)));
    quote_counts=_mm_sub_epi8(quote_counts,_mm_and_si128(_mm_cmpeq_epi8(input,quote),tail));
    if(!block(static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(input,comma)))>>shift,offset))return false;
  }
  const __m128i counts=_mm_sad_epu8(quote_counts,_mm_setzero_si128());
  const unsigned quotes=static_cast<unsigned>(_mm_cvtsi128_si64(counts)+_mm_cvtsi128_si64(_mm_srli_si128(counts,8)));
  // Every quoted field already proved that its two endpoint quotes exist.
  // Equality of the total count rules out ALL other quotes, including inside
  // unquoted fields; an extra quote cannot be canceled by a missing endpoint.
  return field==N-1&&append(size)&&quotes==expected_quotes;
#else
  return csv_scalar(line,fields);
#endif
}

inline bool number(SV s,uint64_t limit,uint64_t& result) {
  if(s.empty())return false;
  while(s.size()>19&&s.front()=='0')s.remove_prefix(1);
  if(s.size()>19)return number_scalar(s,limit,result);
  // Any 19 decimal digits fit uint64_t. Defer the requested limit check until
  // the end; 20-digit uint64_t bounds still use the overflow-checked fallback.
  uint64_t value=0;
#if defined(__SSE2__)
  while(s.size()>=8) {
    const __m128i input=_mm_loadl_epi64(reinterpret_cast<const __m128i*>(s.data()));
    const __m128i digits=_mm_sub_epi8(input,_mm_set1_epi8('0'));
    const unsigned valid=static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_subs_epu8(digits,_mm_set1_epi8(9)),_mm_setzero_si128())));
    if((valid&255)!=255)return false;
    const __m128i pairs=_mm_madd_epi16(_mm_unpacklo_epi8(digits,_mm_setzero_si128()),
        _mm_setr_epi16(10,1,10,1,10,1,10,1));
    const __m128i groups=_mm_madd_epi16(_mm_packs_epi32(pairs,_mm_setzero_si128()),
        _mm_setr_epi16(100,1,100,1,100,1,100,1));
    const uint64_t number=static_cast<unsigned>(_mm_cvtsi128_si32(groups))*10000u+
        static_cast<unsigned>(_mm_cvtsi128_si32(_mm_srli_si128(groups,4)));
    value=value*100000000+number;s.remove_prefix(8);
  }
#endif
  for(unsigned char c:s) {
    const unsigned digit=c-'0';if(digit>9)return false;
    value=value*10+digit;
  }
  if(value>limit)return false;
  result=value;return true;
}

inline bool timestamp(SV s,int64_t& out) {
  if(s.empty())return false;
  if((s.size()!=20&&s.size()!=25)||s[4]!='-') {
    uint64_t value;if(!number(s,static_cast<uint64_t>(INT64_MAX),value))return false;
    out=static_cast<int64_t>(value);return true;
  }
#if defined(__SSE2__)
  const __m128i first=_mm_loadu_si128(reinterpret_cast<const __m128i*>(s.data()));
  const __m128i digits=_mm_sub_epi8(first,_mm_set1_epi8('0'));
  const unsigned digit_mask=static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(
      _mm_subs_epu8(digits,_mm_set1_epi8(9)),_mm_setzero_si128())));
  const __m128i separators=_mm_setr_epi8(0,0,0,0,'-',0,0,'-',0,0,'T',0,0,':',0,0);
  const unsigned separator_mask=static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(first,separators)));
  if((digit_mask&0xdb6fu)!=0xdb6fu||(separator_mask&0x2490u)!=0x2490u||s[16]!=':')return false;
#else
  if(s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':')return false;
  constexpr unsigned positions[]={0,1,2,3,5,6,8,9,11,12,14,15};
  for(unsigned pos:positions)if(s[pos]<'0'||s[pos]>'9')return false;
#endif
  if(static_cast<unsigned>(s[17])-'0'>9u||static_cast<unsigned>(s[18])-'0'>9u)return false;
  const auto two=[&](unsigned p){return (s[p]-'0')*10+s[p+1]-'0';};
  const int year=(s[0]-'0')*1000+(s[1]-'0')*100+two(2);
  const int month=two(5),day=two(8),hour=two(11),minute=two(14),second=two(17);
  if(month<1||month>12||day<1||day>31||hour>23||minute>59||second>59)return false;
  int offset=0;
  if(s.size()==20) {if(s[19]!='Z')return false;}
  else {
    if((s[19]!='+'&&s[19]!='-')||s[22]!=':')return false;
    for(unsigned p:{20u,21u,23u,24u})if(static_cast<unsigned>(s[p])-'0'>9u)return false;
    const int oh=two(20),om=two(23);if(oh>23||om>59)return false;
    offset=(oh*60+om)*(s[19]=='+'?1:-1);
  }
  struct DateEntry {uint32_t key=0;int64_t days=0;};
  static thread_local DateEntry dates[64];
  // A lossless key over every four-digit year and validated month/day range.
  // Cache entries are calendar facts, independent of datasets or manifests.
  const uint32_t key=static_cast<uint32_t>(year*512+month*32+day);
  auto& cached=dates[key&63];
  int64_t days;
  if(cached.key==key)days=cached.days;
  else {
    constexpr int lengths[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
    const int maxday=lengths[month]+(month==2&&year%4==0&&(year%100!=0||year%400==0));
    if(day>maxday)return false;
    days=civil_days(year,month,day);cached={key,days};
  }
  out=days*DAY+(hour*3600+minute*60+second)*1000LL-offset*60000LL;
  return true;
}
}
