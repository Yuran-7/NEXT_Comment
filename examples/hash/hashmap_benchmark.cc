// 哈希表性能对比测试
// std::unordered_map vs robin_hood::unordered_map
// 测试场景：插入、查找、删除、迭代

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <iomanip>
#include <unordered_map>
#include <algorithm>
#include "util/robin_hood.h"

// 计时器
class Timer {
 public:
  Timer() : start_(std::chrono::high_resolution_clock::now()) {}
  
  void Reset() {
    start_ = std::chrono::high_resolution_clock::now();
  }
  
  double ElapsedMs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start_).count();
  }
  
  double ElapsedSeconds() const {
    return ElapsedMs() / 1000.0;
  }
  
 private:
  std::chrono::high_resolution_clock::time_point start_;
};

// 测试数据生成器
class DataGenerator {
 public:
  DataGenerator(uint64_t seed = 42) 
    : gen_(seed), 
      int_dist_(0, 1000000000),
      double_dist_(0.0, 1000000.0) {}
  
  int NextInt() {
    return int_dist_(gen_);
  }
  
  double NextDouble() {
    return double_dist_(gen_);
  }
  
  uint64_t NextUint64() {
    return static_cast<uint64_t>(gen_());
  }
  
 private:
  std::mt19937_64 gen_;
  std::uniform_int_distribution<int> int_dist_;
  std::uniform_real_distribution<double> double_dist_;
};

// 测试结果
struct BenchmarkResult {
  std::string name;
  double insert_ms = 0.0;
  double lookup_ms = 0.0;
  double delete_ms = 0.0;
  double iterate_ms = 0.0;
  size_t memory_bytes = 0;
  size_t final_size = 0;
  
  void Print() const {
    std::cout << "\n========== " << name << " ==========\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "插入耗时:   " << std::setw(10) << insert_ms << " ms  ("
              << (insert_ms / final_size * 1000000.0) << " ns/op)\n";
    std::cout << "查找耗时:   " << std::setw(10) << lookup_ms << " ms  ("
              << (lookup_ms / final_size * 1000000.0) << " ns/op)\n";
    std::cout << "删除耗时:   " << std::setw(10) << delete_ms << " ms  ("
              << (delete_ms / (final_size/2) * 1000000.0) << " ns/op)\n";
    std::cout << "迭代耗时:   " << std::setw(10) << iterate_ms << " ms\n";
    std::cout << "最终大小:   " << std::setw(10) << final_size << " 条\n";
    if (memory_bytes > 0) {
      std::cout << "内存占用:   " << std::setw(10) 
                << (memory_bytes / 1024.0 / 1024.0) << " MB\n";
    }
  }
};

// std::unordered_map 测试
template<typename Key, typename Value>
BenchmarkResult BenchmarkStdMap(size_t num_elements) {
  BenchmarkResult result;
  result.name = "std::unordered_map";
  
  std::unordered_map<Key, Value> map;
  DataGenerator gen;
  Timer timer;
  
  // 准备测试数据
  std::vector<Key> keys;
  keys.reserve(num_elements);
  for (size_t i = 0; i < num_elements; ++i) {
    if constexpr (std::is_same_v<Key, int>) {
      keys.push_back(gen.NextInt());
    } else if constexpr (std::is_same_v<Key, double>) {
      keys.push_back(gen.NextDouble());
    } else {
      keys.push_back(gen.NextUint64());
    }
  }
  
  // 1. 插入测试
  std::cout << "[std::unordered_map] 插入 " << num_elements << " 条数据...\n";
  timer.Reset();
  for (size_t i = 0; i < num_elements; ++i) {
    map[keys[i]] = static_cast<Value>(i);
    
    // 每100万打印进度
    if ((i + 1) % 1000000 == 0) {
      std::cout << "  已插入 " << (i + 1) / 1000000 << "M... ("
                << timer.ElapsedMs() << " ms)\n";
    }
  }
  result.insert_ms = timer.ElapsedMs();
  result.final_size = map.size();
  std::cout << "  插入完成！实际大小: " << map.size() << "\n";
  
  // 2. 查找测试（查找所有key）
  std::cout << "[std::unordered_map] 查找测试...\n";
  timer.Reset();
  size_t found_count = 0;
  for (const auto& key : keys) {
    auto it = map.find(key);
    if (it != map.end()) {
      ++found_count;
    }
  }
  result.lookup_ms = timer.ElapsedMs();
  std::cout << "  查找完成！找到 " << found_count << " 条\n";
  
  // 3. 迭代测试
  std::cout << "[std::unordered_map] 迭代测试...\n";
  timer.Reset();
  size_t sum = 0;
  for (const auto& kv : map) {
    sum += static_cast<size_t>(kv.second);
  }
  result.iterate_ms = timer.ElapsedMs();
  std::cout << "  迭代完成！总和: " << sum << "\n";
  
  // 4. 删除测试（删除一半）
  std::cout << "[std::unordered_map] 删除测试（删除一半数据）...\n";
  timer.Reset();
  size_t delete_count = 0;
  for (size_t i = 0; i < keys.size(); i += 2) {
    if (map.erase(keys[i]) > 0) {
      ++delete_count;
    }
  }
  result.delete_ms = timer.ElapsedMs();
  std::cout << "  删除完成！删除 " << delete_count << " 条，剩余 " << map.size() << " 条\n";
  
  // 估算内存（粗略）
  result.memory_bytes = map.size() * (sizeof(Key) + sizeof(Value) + 24); // 24字节开销
  
  return result;
}

// robin_hood::unordered_map 测试
template<typename Key, typename Value>
BenchmarkResult BenchmarkRobinHoodMap(size_t num_elements) {
  BenchmarkResult result;
  result.name = "robin_hood::unordered_map";
  
  robin_hood::unordered_map<Key, Value> map;
  DataGenerator gen;
  Timer timer;
  
  // 准备测试数据（使用相同种子保证公平）
  std::vector<Key> keys;
  keys.reserve(num_elements);
  for (size_t i = 0; i < num_elements; ++i) {
    if constexpr (std::is_same_v<Key, int>) {
      keys.push_back(gen.NextInt());
    } else if constexpr (std::is_same_v<Key, double>) {
      keys.push_back(gen.NextDouble());
    } else {
      keys.push_back(gen.NextUint64());
    }
  }
  
  // 1. 插入测试
  std::cout << "[robin_hood::unordered_map] 插入 " << num_elements << " 条数据...\n";
  timer.Reset();
  for (size_t i = 0; i < num_elements; ++i) {
    map[keys[i]] = static_cast<Value>(i);
    
    if ((i + 1) % 1000000 == 0) {
      std::cout << "  已插入 " << (i + 1) / 1000000 << "M... ("
                << timer.ElapsedMs() << " ms)\n";
    }
  }
  result.insert_ms = timer.ElapsedMs();
  result.final_size = map.size();
  std::cout << "  插入完成！实际大小: " << map.size() << "\n";
  
  // 2. 查找测试
  std::cout << "[robin_hood::unordered_map] 查找测试...\n";
  timer.Reset();
  size_t found_count = 0;
  for (const auto& key : keys) {
    auto it = map.find(key);
    if (it != map.end()) {
      ++found_count;
    }
  }
  result.lookup_ms = timer.ElapsedMs();
  std::cout << "  查找完成！找到 " << found_count << " 条\n";
  
  // 3. 迭代测试
  std::cout << "[robin_hood::unordered_map] 迭代测试...\n";
  timer.Reset();
  size_t sum = 0;
  for (const auto& kv : map) {
    sum += static_cast<size_t>(kv.second);
  }
  result.iterate_ms = timer.ElapsedMs();
  std::cout << "  迭代完成！总和: " << sum << "\n";
  
  // 4. 删除测试
  std::cout << "[robin_hood::unordered_map] 删除测试（删除一半数据）...\n";
  timer.Reset();
  size_t delete_count = 0;
  for (size_t i = 0; i < keys.size(); i += 2) {
    if (map.erase(keys[i]) > 0) {
      ++delete_count;
    }
  }
  result.delete_ms = timer.ElapsedMs();
  std::cout << "  删除完成！删除 " << delete_count << " 条，剩余 " << map.size() << " 条\n";
  
  // 估算内存
  result.memory_bytes = map.size() * (sizeof(Key) + sizeof(Value) + 16); // 更紧凑
  
  return result;
}

// 性能对比分析
void CompareResults(const BenchmarkResult& std_result, 
                   const BenchmarkResult& rh_result) {
  std::cout << "\n" << std::string(70, '=') << "\n";
  std::cout << "性能对比分析\n";
  std::cout << std::string(70, '=') << "\n";
  
  auto print_speedup = [](const std::string& op, double std_time, double rh_time) {
    double speedup = std_time / rh_time;
    std::cout << std::setw(12) << op << ": ";
    if (speedup > 1.0) {
      std::cout << "robin_hood 快 " << std::fixed << std::setprecision(2) 
                << speedup << "x 倍 🚀";
    } else {
      std::cout << "std 快 " << std::fixed << std::setprecision(2)
                << (1.0 / speedup) << "x 倍";
    }
    std::cout << "  (" << std::setw(8) << std_time << " ms vs " 
              << std::setw(8) << rh_time << " ms)\n";
  };
  
  print_speedup("插入", std_result.insert_ms, rh_result.insert_ms);
  print_speedup("查找", std_result.lookup_ms, rh_result.lookup_ms);
  print_speedup("删除", std_result.delete_ms, rh_result.delete_ms);
  print_speedup("迭代", std_result.iterate_ms, rh_result.iterate_ms);
  
  // 内存对比
  if (std_result.memory_bytes > 0 && rh_result.memory_bytes > 0) {
    double memory_ratio = static_cast<double>(std_result.memory_bytes) / rh_result.memory_bytes;
    std::cout << "\n内存占用: robin_hood 节省 " 
              << std::fixed << std::setprecision(1)
              << ((1.0 - 1.0/memory_ratio) * 100.0) << "%\n";
  }
  
  // 综合评分
  double total_std = std_result.insert_ms + std_result.lookup_ms + 
                     std_result.delete_ms + std_result.iterate_ms;
  double total_rh = rh_result.insert_ms + rh_result.lookup_ms + 
                    rh_result.delete_ms + rh_result.iterate_ms;
  double overall_speedup = total_std / total_rh;
  
  std::cout << "\n综合性能: robin_hood 快 " << std::fixed << std::setprecision(2)
            << overall_speedup << "x 倍 ";
  if (overall_speedup > 2.0) {
    std::cout << "🚀🚀🚀\n";
  } else if (overall_speedup > 1.5) {
    std::cout << "🚀🚀\n";
  } else {
    std::cout << "🚀\n";
  }
  
  // 推荐
  std::cout << "\n" << std::string(70, '=') << "\n";
  std::cout << "推荐建议:\n";
  std::cout << std::string(70, '=') << "\n";
  
  if (overall_speedup > 2.0) {
    std::cout << "✅ 强烈推荐使用 robin_hood::unordered_map！\n";
    std::cout << "   性能提升超过 2 倍，且内存占用更少。\n";
  } else if (overall_speedup > 1.5) {
    std::cout << "✅ 推荐使用 robin_hood::unordered_map\n";
    std::cout << "   性能有明显提升。\n";
  } else if (overall_speedup > 1.2) {
    std::cout << "⚡ 可以考虑使用 robin_hood::unordered_map\n";
    std::cout << "   性能略有提升。\n";
  } else {
    std::cout << "ℹ️  两者性能接近，可根据其他因素选择。\n";
  }
}

// 主测试函数
template<typename Key, typename Value>
void RunBenchmark(const std::string& type_name, size_t num_elements) {
  std::cout << "\n" << std::string(70, '=') << "\n";
  std::cout << "测试类型: " << type_name << "\n";
  std::cout << "数据量: " << (num_elements / 1000000.0) << " 百万条\n";
  std::cout << std::string(70, '=') << "\n";
  
  // 测试 std::unordered_map
  auto std_result = BenchmarkStdMap<Key, Value>(num_elements);
  std_result.Print();
  
  std::cout << "\n" << std::string(70, '-') << "\n";
  
  // 测试 robin_hood::unordered_map
  auto rh_result = BenchmarkRobinHoodMap<Key, Value>(num_elements);
  rh_result.Print();
  
  // 对比分析
  CompareResults(std_result, rh_result);
}

int main() {
  std::cout << "=== 哈希表性能对比测试 ===\n";
  std::cout << "std::unordered_map vs robin_hood::unordered_map\n";
  std::cout << "\n⚠️  测试需要几分钟，请耐心等待...\n";
  
  // 测试1: int -> int (1000万条)
  std::cout << "\n\n" << std::string(70, '#') << "\n";
  std::cout << "### 测试 1: <int, int> - 1000万条数据 ###\n";
  std::cout << std::string(70, '#') << "\n";
  RunBenchmark<int, int>("<int, int>", 10000000);
  
  // 测试2: double -> int (1000万条)
  std::cout << "\n\n" << std::string(70, '#') << "\n";
  std::cout << "### 测试 2: <double, int> - 1000万条数据 ###\n";
  std::cout << std::string(70, '#') << "\n";
  RunBenchmark<double, int>("<double, int>", 10000000);
  
  // 测试3: uint64_t -> uint64_t (500万条，避免太慢)
  std::cout << "\n\n" << std::string(70, '#') << "\n";
  std::cout << "### 测试 3: <uint64_t, uint64_t> - 500万条数据 ###\n";
  std::cout << std::string(70, '#') << "\n";
  RunBenchmark<uint64_t, uint64_t>("<uint64_t, uint64_t>", 5000000);
  
  std::cout << "\n\n=== 所有测试完成！===\n";
  
  return 0;
}

// g++ -O3 -std=c++17 -I.. -I../include hash/hashmap_benchmark.cc -o hash/hashmap_benchmark