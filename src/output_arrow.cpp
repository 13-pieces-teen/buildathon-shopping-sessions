#include "types.hpp"
#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <limits>
#include <mutex>
#include <thread>

// Alternative to output.cpp: path is the final Parquet output directory.
// Compile exactly one of these two implementations of write_results.
namespace shopping {
namespace {
constexpr size_t ROW_GROUP_SIZE = 262144;
using Bytes = parquet::ByteArray;

Bytes bytes(SV value) {
  // Parquet BYTE_ARRAY lengths are signed 32-bit on disk. Do not truncate a
  // legal C++ string_view silently, even though inputs normally have short IDs.
  if(value.size()>size_t(INT32_MAX))throw std::runtime_error("Parquet string exceeds 2 GiB");
  return Bytes(static_cast<uint32_t>(value.size()),
               reinterpret_cast<const uint8_t*>(value.empty() ? "" : value.data()));
}

std::shared_ptr<parquet::schema::GroupNode> schema(bool campaign) {
  using parquet::Repetition;
  using parquet::Type;
  using parquet::LogicalType;
  using parquet::schema::PrimitiveNode;
  parquet::schema::NodeVector fields;
  const auto text=[&](const char* name) {
    fields.push_back(PrimitiveNode::Make(name,Repetition::OPTIONAL,LogicalType::String(),Type::BYTE_ARRAY));
  };
  const auto date=[&] {
    fields.push_back(PrimitiveNode::Make("day",Repetition::OPTIONAL,LogicalType::Date(),Type::INT32));
  };
  const auto integer=[&](const char* name) {
    fields.push_back(PrimitiveNode::Make(name,Repetition::OPTIONAL,Type::INT64));
  };
  // All fields are nullable in pa.Table.from_arrays, as in write_parquet.py.
  if(campaign) {
    text("campaign_id"); text("channel"); date(); integer("sessions");
    integer("purchasing_sessions"); integer("revenue_cents"); integer("assisted_sessions");
  } else {
    text("user_id"); date(); integer("sessions"); integer("events");
    integer("revenue_cents"); text("top_category");
  }
  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema",Repetition::REQUIRED,fields));
}

class File {
  std::shared_ptr<arrow::io::FileOutputStream> sink;
  std::unique_ptr<parquet::ParquetFileWriter> writer;
public:
  File(const std::string& path,bool campaign) {
    auto opened=arrow::io::FileOutputStream::Open(path);
    if(!opened.ok())throw std::runtime_error(opened.status().ToString());
    sink=std::move(opened).ValueOrDie();
    parquet::WriterProperties::Builder properties;
    properties.compression(parquet::Compression::SNAPPY)->disable_statistics()
        ->version(parquet::ParquetVersion::PARQUET_2_6);
    writer=parquet::ParquetFileWriter::Open(sink,schema(campaign),properties.build());
  }
  parquet::RowGroupWriter* group() {return writer->AppendRowGroup();}
  void close() {
    writer->Close();
    const auto status=sink->Close();
    if(!status.ok())throw std::runtime_error(status.ToString());
  }
};

template<class Type, class Values>
void column(parquet::RowGroupWriter* group,const std::vector<int16_t>& definitions,
            const Values& values) {
  auto* writer=static_cast<parquet::TypedColumnWriter<Type>*>(group->NextColumn());
  const int64_t written=writer->WriteBatch(static_cast<int64_t>(definitions.size()),
      definitions.data(),nullptr,values.data());
  if(written!=static_cast<int64_t>(values.size()))
    throw std::runtime_error("Parquet column value count mismatch");
  writer->Close();
}

struct UserColumns {
  std::vector<Bytes> users,categories;
  std::vector<int32_t> days;
  std::vector<int64_t> sessions,events,revenue;
  std::vector<int16_t> defined,category_defined;
  explicit UserColumns(size_t count) {
    const size_t capacity=std::min(count,ROW_GROUP_SIZE);
    users.reserve(capacity); categories.reserve(capacity); days.reserve(capacity);
    sessions.reserve(capacity); events.reserve(capacity); revenue.reserve(capacity);
    defined.reserve(capacity); category_defined.reserve(capacity);
  }
  void append(const Dataset& data,const UserRow& row) {
    // ByteArray only stores a pointer and length. Dataset and Results outlive
    // every worker, so string bytes need no intermediate concatenation/copy.
    users.push_back(bytes(row.user)); days.push_back(row.day);
    sessions.push_back(row.sessions); events.push_back(row.events); revenue.push_back(row.revenue);
    defined.push_back(1);
    const bool present=row.category!=INVALID;
    category_defined.push_back(present ? 1 : 0);
    if(present)categories.push_back(bytes(data.categories.at(row.category)));
  }
  void flush(File& file) {
    if(users.empty())return;
    auto* group=file.group();
    column<parquet::ByteArrayType>(group,defined,users);
    column<parquet::Int32Type>(group,defined,days);
    column<parquet::Int64Type>(group,defined,sessions);
    column<parquet::Int64Type>(group,defined,events);
    column<parquet::Int64Type>(group,defined,revenue);
    // WriteBatch consumes only the defined values: categories is packed, and
    // a valid empty string is distinct from an absent category (definition 0).
    column<parquet::ByteArrayType>(group,category_defined,categories);
    group->Close();
    users.clear(); categories.clear(); days.clear(); sessions.clear(); events.clear();
    revenue.clear(); defined.clear(); category_defined.clear();
  }
};

void write_user_part(const Dataset& data,const Results& results,const std::string& path,
                     size_t part,size_t stride,size_t count) {
  File file(path+"/user_daily/part-"+std::to_string(part)+".parquet",false);
  UserColumns columns(count);
  size_t written=0;
  for(size_t p=part;p<results.users.size();p+=stride) {
    for(const auto& row:results.users[p]) {
      columns.append(data,row); ++written;
      if(columns.users.size()==ROW_GROUP_SIZE)columns.flush(file);
    }
  }
  if(written!=count)throw std::runtime_error("Parquet user row count mismatch");
  columns.flush(file);
  file.close(); // also writes a complete schema when there were no rows
}

void write_campaigns(const Dataset& data,const Results& results,const std::string& path) {
  File file(path+"/campaign_daily/part-0.parquet",true);
  const auto& rows=results.campaigns;
  const size_t capacity=std::min(rows.size(),ROW_GROUP_SIZE);
  std::vector<Bytes> ids,channels; ids.reserve(capacity); channels.reserve(capacity);
  std::vector<int32_t> days; days.reserve(capacity);
  std::vector<int64_t> sessions,purchasing,revenue,assists;
  sessions.reserve(capacity); purchasing.reserve(capacity); revenue.reserve(capacity); assists.reserve(capacity);
  std::vector<int16_t> defined; defined.reserve(capacity);
  for(size_t start=0;start<rows.size();) {
    const size_t end=start+std::min(ROW_GROUP_SIZE,rows.size()-start);
    for(size_t i=start;i<end;++i) {
      const auto& row=rows[i];
      if(row.campaign==data.campaigns.size()) {
        ids.push_back(bytes("organic")); channels.push_back(bytes("organic"));
      } else {
        const auto& campaign=data.campaigns.at(row.campaign);
        ids.push_back(bytes(campaign.id)); channels.push_back(bytes(campaign.channel));
      }
      days.push_back(row.day); sessions.push_back(row.sessions); purchasing.push_back(row.purchasing);
      revenue.push_back(row.revenue); assists.push_back(row.assists); defined.push_back(1);
    }
    auto* group=file.group();
    column<parquet::ByteArrayType>(group,defined,ids);
    column<parquet::ByteArrayType>(group,defined,channels);
    column<parquet::Int32Type>(group,defined,days);
    column<parquet::Int64Type>(group,defined,sessions);
    column<parquet::Int64Type>(group,defined,purchasing);
    column<parquet::Int64Type>(group,defined,revenue);
    column<parquet::Int64Type>(group,defined,assists);
    group->Close();
    ids.clear(); channels.clear(); days.clear(); sessions.clear(); purchasing.clear();
    revenue.clear(); assists.clear(); defined.clear();
    start=end;
  }
  file.close();
}
}

void write_results(const Dataset& data,const Results& results,const std::string& path) {
  std::filesystem::create_directories(path+"/user_daily");
  std::filesystem::create_directories(path+"/campaign_daily");
  const auto count=std::getenv("NCPU");
  const size_t concurrency=size_t(std::max(1,count ? std::atoi(count) : 8));
  const size_t output_parts=std::min(results.users.size(),concurrency);
  struct Job {size_t part,rows; bool campaign;};
  std::vector<Job> jobs;
  for(size_t part=0;part<output_parts;++part) {
    size_t rows=0;
    for(size_t p=part;p<results.users.size();p+=output_parts)rows+=results.users[p].size();
    if(rows)jobs.push_back({part,rows,false});
  }
  if(jobs.empty())jobs.push_back({0,0,false});
  jobs.push_back({0,results.campaigns.size(),true});

  // Parallelize independent files, without Arrow's nested thread pool. Each
  // worker retains at most one row group's temporary columns and encoders.
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
        const auto& job=jobs[index];
        if(job.campaign)write_campaigns(data,results,path);
        else write_user_part(data,results,path,job.part,std::max(size_t(1),output_parts),job.rows);
      }
    } catch(...) {
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
