#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <type_traits>

#include "util/BPlusTree.h"

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
inline V MakeValue(int64_t i, int key) {
  // 通用模板未实现，后面提供特化
  static_assert(sizeof(V) == 0, "MakeValue not specialized for this type");
  return V{};
}

template <>
inline int MakeValue<int>(int64_t i, int /*key*/) {
  return static_cast<int>(i);
}

template <>
inline GlobalSecIndexValue MakeValue<GlobalSecIndexValue>(int64_t i, int key) {
  GlobalSecIndexValue gsi;
  gsi.id = static_cast<int>(i & 0x7fffffff);
  gsi.filenum = static_cast<uint64_t>(i % 1'000'000ULL);
  gsi.blkhandle = BlockHandle(static_cast<uint64_t>(static_cast<uint64_t>(key) * 4096ULL),
                              4096ULL);
  return gsi;
}

template <typename V>
static void RunBench(int64_t total_inserts, int64_t distinct_keys, bool use_random,
                     const std::string& out_path) {
  BPlusTree<int, V> tree;

  std::mt19937_64 rng(123456789ULL);
  std::uniform_int_distribution<int> uni(0, static_cast<int>(distinct_keys - 1));

  auto t0 = std::chrono::high_resolution_clock::now();

  for (int64_t i = 0; i < total_inserts; ++i) {
    int key = use_random ? uni(rng) : static_cast<int>(i % distinct_keys);
    V value = MakeValue<V>(i, key);
    tree.Insert(key, value);

    if ((i % 5'000'000) == 0 && i != 0) {
      auto now = std::chrono::high_resolution_clock::now();
      double sec = std::chrono::duration_cast<std::chrono::duration<double>>(now - t0).count();
      double mops = (i / 1e6) / sec;
      std::cout << "Progress: " << i << "/" << total_inserts
                << " inserts, elapsed: " << sec << " s, throughput: " << mops
                << " M inserts/s" << std::endl;
    }
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  double seconds = ns / 1e9;
  double mops = (total_inserts / 1e6) / seconds;

  std::cout << "Insertion time: " << seconds << " s\n";
  std::cout << "Throughput: " << mops << " M inserts/s\n";

  if (!out_path.empty()) {
    auto s0 = std::chrono::high_resolution_clock::now();
    bool ok = tree.Save(out_path.c_str());
    auto s1 = std::chrono::high_resolution_clock::now();
    double save_sec = std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count() / 1e9;
    std::cout << "Save result: " << (ok ? "OK" : "FAIL") << ", time: " << save_sec << " s\n";
  }

  // 抽样读取
  t0 = std::chrono::high_resolution_clock::now();
  for (int probe : {0, 1, 2, 42, 9999, static_cast<int>(distinct_keys - 1)}) {
    if (probe < 0 || probe >= distinct_keys) continue;
    std::vector<V> vals = tree.Get(probe);
    std::cout << "Key " << probe << " -> count = " << vals.size();
    if constexpr (std::is_same<V, GlobalSecIndexValue>::value) {
      if (!vals.empty()) {
        const auto& v = vals.back();
        std::cout << ", sample: {id=" << v.id
                  << ", filenum=" << v.filenum
                  << ", blk=(off=" << v.blkhandle.offset()
                  << ", size=" << v.blkhandle.size() << ")}";
      }
    }
    std::cout << "\n";
  }
  t1 = std::chrono::high_resolution_clock::now();
  double search_sec = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / 1e9;
  std::cout << "Sample search time: " << search_sec << " s\n";
}

int main(int argc, char** argv) {
  // 配置：总插入条数与不同 key 的数量
  int64_t total_inserts = 3'000'000;   // 3,000,000 条记录
  int64_t distinct_keys = 200'000;     // 200,000 个不同的 key（int）
  std::string out_path = "/NV1/ysh/NEXT/examples/bplustree/global_bptree";  // 可选：持久化输出文件
  int value_type = 1; // 1=int, 2=GlobalSecIndexValue

  // 可选：从命令行覆盖（例如 ./bplustree_bench 3000000 200000 random）
  bool use_random = false;
  if (argc >= 2) {
    int64_t v = std::stoll(argv[1]);
    std::cout << "v=" << v << std::endl;
    if (v > 0) total_inserts = v;
    std::cout << "total_inserts=" << total_inserts << std::endl;
  }
  if (argc >= 3) {
    int64_t v = std::stoll(argv[2]);
    if (v > 0) distinct_keys = v;
  }
  if (argc >= 4) {
    std::string mode = argv[3];
    if (mode == "random" || mode == "rand") use_random = true;
  }
  if (argc >= 5) {
    out_path = argv[4];
  }
  if (argc >= 6) {
    // 第 6 个参数选择 Value 类型：1=int，2=GlobalSecIndexValue
    value_type = std::stoi(argv[5]);
  }

  std::cout << "B+Tree insert benchmark\n";
  std::cout << "total_inserts = " << total_inserts
            << ", distinct_keys = " << distinct_keys
            << ", pattern = " << (use_random ? "random" : "round-robin(mod)")
            << (out_path.empty() ? "" : (std::string(", save=\"") + out_path + "\""))
            << std::endl;

  std::cout << "ValueType = " << (value_type == 2 ? "GlobalSecIndexValue" : "int") << std::endl;

  if (value_type == 2) {
    RunBench<GlobalSecIndexValue>(total_inserts, distinct_keys, use_random, out_path);
  } else {
    RunBench<int>(total_inserts, distinct_keys, use_random, out_path);
  }
  return 0;
}

// g++ -O3 -std=c++17 -I.. -I../include bplustree/bplustree_insert.cc -o bplustree/bplustree_insert
// 选择 Value 类型：第6个参数 1=int（默认），2=GlobalSecIndexValue
// ./bplustree/bplustree_insert 30000000 1000000 random /NV1/ysh/NEXT/examples/bplustree/global_bptree 1
// ./bplustree/bplustree_insert 30000000 7000000 random /NV1/ysh/NEXT/examples/bplustree/global_bptree 2
// ./bplustree/bplustree_insert 3000000 200000 random /NV1/ysh/NEXT/examples/bplustree/global_bptree 1
// ./bplustree/bplustree_insert 1000000 60000 random /NV1/ysh/NEXT/examples/bplustree/global_bptree 2