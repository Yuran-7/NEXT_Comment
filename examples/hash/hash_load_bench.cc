#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "util/GlobalSecRobinHood.h"

// 定义一个与 RocksDB BlockHandle 二进制布局兼容的轻量结构
// RocksDB 的 BlockHandle 通常由两个 64 位字段组成：offset 和 size。
// 这里仅用于加载/搜索性能测试，不依赖其成员语义。
struct BlockHandle {
  std::uint64_t offset{0};
  std::uint64_t size{0};
};

using Clock = std::chrono::high_resolution_clock;
using micros = std::chrono::microseconds;

int main(int argc, char** argv) {
  // Args: [path] [repeat]
  const char* path = argc > 1 ? argv[1] : "/NV1/ysh/NEXT/examples/testdb/global_hash";
  int repeat = argc > 2 ? std::atoi(argv[2]) : 100;

  std::cout << "File: " << path << "\n"
            << "Repeat: " << repeat << "\n";

  ROCKSDB_NAMESPACE::GlobalSecRobinHood<double, BlockHandle> index;

  auto t0 = Clock::now();
  bool ok = index.Load(path);
  auto t1 = Clock::now();
  if (!ok) {
    std::cerr << "Load failed for: " << path << "\n";
    return 1;
  }
  auto load_us = std::chrono::duration_cast<micros>(t1 - t0).count();

  // Print basic stats
  size_t uniq = index.UniqueKeyCount();
  size_t total = index.Size();
  std::cout << "Load time: " << load_us << " us\n";
  std::cout << "Unique keys: " << uniq << ", total handles: " << total << "\n";

  if (uniq == 0) {
    std::cerr << "No keys in index, cannot test search.\n";
    return 1;
  }

  // 从哈希表中随机抽取一个 key
  std::cout << "\n=== Randomly selecting a key from index ===\n";
  
  // 使用 RangeSearch 遍历所有 key（由于是哈希表，范围是全部）
  std::vector<double> all_keys;
  all_keys.reserve(uniq);
  
  auto collect_start = Clock::now();
  index.RangeSearch(
    std::numeric_limits<double>::lowest(),
    std::numeric_limits<double>::max(),
    [&all_keys](const double& k, int, const BlockHandle&) {
      // 只收集每个 key 一次
      if (all_keys.empty() || all_keys.back() != k) {
        all_keys.push_back(k);
      }
      return true; // 继续遍历
    }
  );
  auto collect_end = Clock::now();
  auto collect_us = std::chrono::duration_cast<micros>(collect_end - collect_start).count();
  
  std::cout << "Collected " << all_keys.size() << " keys in " << collect_us << " us\n";

  // 随机选择一个 key
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dis(0, all_keys.size() - 1);
  size_t random_idx = dis(gen);
  double key = all_keys[random_idx];
  
  std::cout << "Randomly selected key[" << random_idx << "]: " 
            << std::setprecision(17) << key << "\n\n";

  // Single search
  std::unordered_multimap<int, BlockHandle> out;
  auto s0 = Clock::now();
  bool found = index.Search(key, out);
  auto s1 = Clock::now();
  auto one_us = std::chrono::duration_cast<micros>(s1 - s0).count();
  std::cout << "Search x1: " << one_us << " us, found=" << (found ? "true" : "false")
            << ", results=" << out.size() << "\n";

  // 100x (or repeat) searches
  out.clear();
  auto m0 = Clock::now();
  for (int i = 0; i < repeat; ++i) {
    index.Search(key, out);
  }
  auto m1 = Clock::now();
  auto many_us = std::chrono::duration_cast<micros>(m1 - m0).count();
  std::cout << "Search x" << repeat << ": " << many_us << " us"
            << ", avg=" << (repeat > 0 ? (many_us / repeat) : 0) << " us/call\n";

  return 0;
}
// g++ -O3 -std=c++17 -I.. -I../include hash/hash_load_bench.cc -o hash/hash_load_bench
// 例如：./hash/hash_load_bench /NV1/ysh/NEXT/examples/testdb/global_hash 100