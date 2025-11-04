#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
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

  // ============================================================
  // 从哈希表中采样得到若干个互不相同的 key
  // ============================================================
  std::cout << "\n=== Sampling distinct keys from index ===\n";

  std::vector<double> all_keys;
  all_keys.reserve(uniq);

  auto collect_start = Clock::now();
  index.RangeSearch(
      std::numeric_limits<double>::lowest(),
      std::numeric_limits<double>::max(),
      [&all_keys](const double& k, int, const BlockHandle&) {
        // 假设 RangeSearch 对每个 key 可能会回调多次（不同 BlockHandle），
        // 这里简单用“与上一个 key 不同”来去重（前提是 RangeSearch 输出按 key 有序）。
        if (all_keys.empty() || all_keys.back() != k) {
          all_keys.push_back(k);
        }
        return true;  // 继续遍历
      });
  auto collect_end = Clock::now();
  auto collect_us =
      std::chrono::duration_cast<micros>(collect_end - collect_start).count();

  std::cout << "Collected " << all_keys.size() << " unique keys in "
            << collect_us << " us\n";

  if (all_keys.empty()) {
    std::cerr << "No keys collected, cannot test search.\n";
    return 1;
  }

  // 使用随机打乱 + 取前 N 个，保证采样出的 key 互不相同
  std::random_device rd;
  std::mt19937 gen(rd());
  std::shuffle(all_keys.begin(), all_keys.end(), gen);

  size_t sample_n =
      std::min<size_t>(static_cast<size_t>(repeat), all_keys.size());
  if (sample_n < static_cast<size_t>(repeat)) {
    std::cout << "Warning: only " << all_keys.size()
              << " distinct keys available, "
              << "sampling " << sample_n << " keys instead of " << repeat
              << ".\n";
  }

  std::vector<double> sample_keys(all_keys.begin(),
                                  all_keys.begin() + sample_n);

  // 打印第一个采样 key
  double first_key = sample_keys[0];
  std::cout << "Sample[0] key: " << std::setprecision(17) << first_key << "\n\n";

  // 对第一个 key 做一次单次查询
  std::unordered_map<int, std::vector<BlockHandle>> out;
  auto s0 = Clock::now();
  bool found = index.Search(first_key, out);
  auto s1 = Clock::now();
  auto one_us = std::chrono::duration_cast<micros>(s1 - s0).count();
  std::cout << "Search x1 (for Sample[0]): " << one_us
            << " us, found=" << (found ? "true" : "false")
            << ", results=" << out.size() << "\n";

  // 对 sample_n 个不同的 key 逐个搜索，统计总时间和平均时间
  auto m0 = Clock::now();
  for (size_t i = 0; i < sample_n; ++i) {
    out.clear();  // 如果不关心结果，可以保留，但不清也问题不大
    index.Search(sample_keys[i], out);
  }
  auto m1 = Clock::now();
  auto many_us = std::chrono::duration_cast<micros>(m1 - m0).count();
  std::cout << "Search " << sample_n
            << " distinct keys: " << many_us << " us"
            << ", avg=" << (sample_n > 0 ? (many_us / sample_n) : 0)
            << " us/call\n";

  return 0;
}

// g++ -O3 -std=c++17 -I.. -I../include hash/hash_load_bench.cc -o hash/hash_load_bench
// 例如：./hash/hash_load_bench /NV1/ysh/NEXT/examples/testdb/global_hash 100
