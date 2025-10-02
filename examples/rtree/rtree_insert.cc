#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <type_traits>

#include "util/RTree_mem.h"

using namespace ROCKSDB_NAMESPACE;

// 为了在一个文件里支持两种 Value 类型（int 与 GlobalSecIndexValue），
// 并避免引入 RocksDB 内部重头文件导致的编译依赖，这里定义最小可用的 GSI 结构。
struct BlockHandle {
  BlockHandle() : offset_(0), size_(0) {}
  BlockHandle(uint64_t off, uint64_t sz) : offset_(off), size_(sz) {}
  uint64_t offset() const { return offset_; }
  uint64_t size() const { return size_; }
  bool operator==(const BlockHandle& rhs) const {
    return offset_ == rhs.offset_ && size_ == rhs.size_;
  }
  uint64_t offset_;
  uint64_t size_;
};

struct GlobalSecIndexValue {
  int id;
  uint64_t filenum;
  BlockHandle blkhandle;
  GlobalSecIndexValue() : id(0), filenum(0), blkhandle() {}
  GlobalSecIndexValue(int _id, uint64_t _filenum, BlockHandle _bh)
      : id(_id), filenum(_filenum), blkhandle(_bh) {}
  bool operator==(const GlobalSecIndexValue& rhs) const {
    return id == rhs.id && filenum == rhs.filenum && blkhandle == rhs.blkhandle;
  }
};

// 生成不同 Value 类型的辅助模板
template <typename V>
inline V MakeValue(int64_t i, double x0, double x1) {
  static_assert(sizeof(V) == 0, "MakeValue not specialized for this type");
  return V{};
}

template <>
inline int MakeValue<int>(int64_t i, double /*x0*/, double /*x1*/) {
  return static_cast<int>(i);
}

template <>
inline GlobalSecIndexValue MakeValue<GlobalSecIndexValue>(int64_t i, double x0, double x1) {
  double center = 0.5 * (x0 + x1);
  uint64_t bucket = static_cast<uint64_t>(center * 1e6); // 简单映射到桶
  GlobalSecIndexValue gsi;
  gsi.id = static_cast<int>(i & 0x7fffffff);
  gsi.filenum = static_cast<uint64_t>(i % 1'000'000ULL);
  gsi.blkhandle = BlockHandle(bucket * 4096ULL, 4096ULL);
  return gsi;
}

template <typename V>
static void RunRTreeBench(int64_t total_inserts, double coord_min, double coord_max,
                          bool use_random, const std::string& out_path) {
  RTree<V, double, 1, double> tree;

  std::mt19937_64 rng(987654321ULL);
  std::uniform_real_distribution<double> uni(coord_min, coord_max);

  const auto t0 = std::chrono::high_resolution_clock::now();

  for (int64_t i = 0; i < total_inserts; ++i) {
    double x0, x1;
    if (use_random) {
      x0 = uni(rng);
      x1 = uni(rng);
      if (x1 < x0) std::swap(x0, x1);
    } else {
      double t = static_cast<double>(i) / static_cast<double>(total_inserts);
      x0 = coord_min + (coord_max - coord_min) * t;
      x1 = x0;
    }
    double min[1] = {x0};
    double max[1] = {x1};

    V val = MakeValue<V>(i, x0, x1);
    tree.Insert(min, max, val);

    if ((i % 100'000) == 0 && i != 0) {
      auto now = std::chrono::high_resolution_clock::now();
      double sec = std::chrono::duration_cast<std::chrono::duration<double>>(now - t0).count();
      double mops = (i / 1e6) / sec;
      std::cout << "Progress: " << i << "/" << total_inserts
                << " inserts, elapsed: " << sec << " s, throughput: " << mops
                << " M inserts/s" << std::endl;
    }
  }

  const auto t1 = std::chrono::high_resolution_clock::now();
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  const double seconds = ns / 1e9;
  const double mops = (total_inserts / 1e6) / seconds;

  std::cout << "Insertion time: " << seconds << " s\n";
  std::cout << "Throughput: " << mops << " M inserts/s\n";
  std::cout << "Count() = " << tree.Count() << std::endl;

  if (!out_path.empty()) {
    const auto s0 = std::chrono::high_resolution_clock::now();
    bool ok = tree.Save(out_path.c_str());
    const auto s1 = std::chrono::high_resolution_clock::now();
    double save_sec = std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count() / 1e9;
    std::cout << "Save result: " << (ok ? "OK" : "FAIL")
              << ", time: " << save_sec << " s, path=\"" << out_path << "\"\n";
  }
}

int main(int argc, char** argv) {
  // 配置：总插入条数与坐标范围
  int64_t total_inserts = 300'000;  // 300,000 条
  double coord_min = 0.0;
  double coord_max = 1.0;  // 1D 区间 [x_min, x_max]
  bool use_random = true;  // 默认随机
  int value_type = 1;      // 1=int, 2=GlobalSecIndexValue
  std::string out_path;    // 可选：持久化输出文件

  if (argc >= 2) {
    int64_t v = std::stoll(argv[1]);
    if (v > 0) total_inserts = v;
  }
  if (argc >= 3) {
    coord_min = std::stod(argv[2]);
  }
  if (argc >= 4) {
    coord_max = std::stod(argv[3]);
  }
  if (argc >= 5) {
    std::string mode = argv[4];
    if (mode == "seq" || mode == "sequential") use_random = false;
  }
  if (argc >= 6) {
    out_path = argv[5];
  }
  if (argc >= 7) {
    // 第 6 个参数选择 Value 类型：1=int，2=GlobalSecIndexValue
    value_type = std::stoi(argv[6]);
  }


  std::cout << "RTree 1D insert benchmark\n";
  std::cout << "total_inserts = " << total_inserts
            << ", range = [" << coord_min << ", " << coord_max << "]"
            << ", pattern = " << (use_random ? "random" : "sequential")
            << std::endl;

  std::cout << "ValueType = " << (value_type == 2 ? "GlobalSecIndexValue" : "int") << std::endl;
  if (value_type == 2) {
    RunRTreeBench<GlobalSecIndexValue>(total_inserts, coord_min, coord_max, use_random, out_path);
  } else {
    RunRTreeBench<int>(total_inserts, coord_min, coord_max, use_random, out_path);
  }

  return 0;
}

// g++ -O3 -std=c++17 -I.. -I../include rtree/rtree_insert.cc -o rtree/rtree_insert
// 选择 Value 类型：第6个参数 1=int（默认），2=GlobalSecIndexValue
// ./rtree/rtree_insert 3000000 0.0 1.0 random /NV1/ysh/NEXT/examples/rtree/global_rtree 1
// ./rtree/rtree_insert 300000 0.0 1.0 random /NV1/ysh/NEXT/examples/rtree/global_rtree 2