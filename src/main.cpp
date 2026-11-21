#include "types.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sys/resource.h>

int main(int argc, char** argv) {
  try {
    const auto env = [](const char* key, const char* fallback) { auto p = std::getenv(key); return std::string(p ? p : fallback); };
    auto input = argc > 1 ? std::string(argv[1]) : env("DATA_DIR", "/data");
    auto output = argc > 2 ? std::string(argv[2]) : env("SCRATCH_DIR", "/scratch") + "/columns";
    unsigned threads = std::max(1, std::stoi(env("NCPU", "8")));
    unsigned partitions = std::max(1, std::stoi(env("PARTITIONS", "128")));
    auto start = std::chrono::steady_clock::now(), last = start;
    auto phase = [&](const char* label) {
      auto now = std::chrono::steady_clock::now();
      rusage usage{}; getrusage(RUSAGE_SELF, &usage);
      std::cerr << label << " seconds=" << std::chrono::duration<double>(now-last).count()
        << " elapsed=" << std::chrono::duration<double>(now-start).count()
        << " max_rss_kib=" << usage.ru_maxrss << '\n';
      last = now;
    };
    auto data = shopping::load_dataset(input, threads, partitions);
    phase("ingest");
    std::cerr << "parsed_events=" << data.parsed_events << " surviving_events=" << data.surviving_events << '\n';
    auto result = shopping::aggregate(data, threads);
    phase("aggregate");
    std::filesystem::create_directories(output);
    shopping::write_results(data, result, output);
    phase("columns");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "shopping-sessions: " << error.what() << '\n';
    return 1;
  }
}
