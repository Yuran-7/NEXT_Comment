#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "util/BPlusTree.h"

using namespace ROCKSDB_NAMESPACE;

// Minimal GSI structures to avoid pulling heavy RocksDB headers
struct BlockHandle {
  BlockHandle() : offset_(0), size_(0) {}
  BlockHandle(uint64_t off, uint64_t sz) : offset_(off), size_(sz) {}
  uint64_t offset() const { return offset_; }
  uint64_t size() const { return size_; }
  bool operator==(const BlockHandle& rhs) const { return offset_ == rhs.offset_ && size_ == rhs.size_; }
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

template <typename V>
static int RunBPTQuery(const std::string& index_path,
                       int64_t num_queries,
                       size_t sample_keys,
                       const std::string& query_mode,
                       int64_t range_width) {
  BPlusTree<int, V> tree;

  // Load timing
  auto t0 = std::chrono::high_resolution_clock::now();
  bool ok = tree.Load(index_path.c_str());
  auto t1 = std::chrono::high_resolution_clock::now();
  if (!ok) {
    std::cerr << "Load failed for " << index_path << "\n";
    return 2;
  }
  double load_sec = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
  std::cout << "Load OK, time: " << load_sec << " s" << std::endl;

  // Sample existing keys via a full-range RangeSearch, collecting unique keys up to sample_keys
  std::vector<int> keys;
  keys.reserve(sample_keys);
  const int min_key = std::numeric_limits<int>::min();
  const int max_key = std::numeric_limits<int>::max();
  tree.RangeSearch(min_key, max_key, [&](const int& k, const V& /*v*/){
    if (keys.empty() || k != keys.back()) keys.push_back(k);
    return keys.size() < sample_keys; // stop when enough
  });

  if (keys.empty()) {
    std::cerr << "No keys found in index; aborting queries.\n";
    return 3;
  }

  // Query timing
  std::mt19937_64 rng(123456789ULL);
  std::uniform_int_distribution<size_t> pick(0, keys.size() - 1);
  size_t total_results = 0;

  auto q0 = std::chrono::high_resolution_clock::now();
  if (query_mode == "range" || query_mode == "r") {
    // Range queries: for each query, pick a center key and query [center - w/2, center + (w - w/2)]
    auto clamp_int = [](int64_t x) {
      if (x < static_cast<int64_t>(std::numeric_limits<int>::min())) return std::numeric_limits<int>::min();
      if (x > static_cast<int64_t>(std::numeric_limits<int>::max())) return std::numeric_limits<int>::max();
      return static_cast<int>(x);
    };
    int64_t w = range_width < 0 ? 0 : range_width;
    for (int64_t i = 0; i < num_queries; ++i) {
      int center = keys[pick(rng)];
      int64_t half = w / 2;
      int l = clamp_int(static_cast<int64_t>(center) - half);
      int r = clamp_int(static_cast<int64_t>(center) + (w - half));
      if (l > r) std::swap(l, r);
      tree.RangeSearch(l, r, [&](const int& /*k*/, const V& /*v*/){
        ++total_results; // count every value visited
        return true;     // do not early-stop
      });
    }
  } else {
    // Point queries
    for (int64_t i = 0; i < num_queries; ++i) {
      int k = keys[pick(rng)];
      auto vals = tree.Get(k);
      total_results += vals.size();
    }
  }
  auto q1 = std::chrono::high_resolution_clock::now();
  double query_sec = std::chrono::duration_cast<std::chrono::duration<double>>(q1 - q0).count();
  double qps = num_queries / query_sec;
  std::cout << "Queries: " << num_queries << ", mode: " << (query_mode == "range" || query_mode == "r" ? "range" : "point")
            << ", time: " << query_sec << " s, QPS: " << qps
            << ", total_results: " << total_results;
  if (query_mode == "range" || query_mode == "r") {
    std::cout << ", range_width: " << range_width;
  }
  std::cout << std::endl;

  // Show a sample lookup
  if (query_mode == "range" || query_mode == "r") {
    int center = keys.front();
    int64_t w = range_width < 0 ? 0 : range_width;
    int64_t half = w / 2;
    int l = static_cast<int>(std::max<int64_t>(std::numeric_limits<int>::min(), static_cast<int64_t>(center) - half));
    int r = static_cast<int>(std::min<int64_t>(std::numeric_limits<int>::max(), static_cast<int64_t>(center) + (w - half)));
    size_t cnt = 0;
    tree.RangeSearch(l, r, [&](const int& /*k*/, const V& /*v*/){ ++cnt; return true; });
    std::cout << "Sample range [" << l << ", " << r << "] -> count = " << cnt << std::endl;
  } else {
    int probe = keys.front();
    auto vals = tree.Get(probe);
    std::cout << "Sample key " << probe << " -> count = " << vals.size();
    if constexpr (std::is_same<V, GlobalSecIndexValue>::value) {
      if (!vals.empty()) {
        const auto& v = vals.back();
        std::cout << ", sample: {id=" << v.id
                  << ", filenum=" << v.filenum
                  << ", blk=(off=" << v.blkhandle.offset()
                  << ", size=" << v.blkhandle.size() << ")}";
      }
    }
    std::cout << std::endl;
  }
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <index_path> [num_queries=1000000] [value_type=1|2] [sample_keys=100000] [query_mode=point|range] [range_width=1000]\n";
    return 1;
  }

  std::string index_path = argv[1];
  int64_t num_queries = (argc >= 3) ? std::stoll(argv[2]) : 1'000'000LL;
  int value_type = (argc >= 4) ? std::stoi(argv[3]) : 1; // 1=int, 2=GlobalSecIndexValue
  size_t sample_keys = (argc >= 5) ? static_cast<size_t>(std::stoull(argv[4])) : static_cast<size_t>(100'000);
  std::string query_mode = (argc >= 6) ? std::string(argv[5]) : std::string("point");
  int64_t range_width = (argc >= 7) ? std::stoll(argv[6]) : 1000LL;

  std::cout << "B+Tree query benchmark\n"
            << "index_path = " << index_path
            << ", num_queries = " << num_queries
            << ", ValueType = " << (value_type == 2 ? "GlobalSecIndexValue" : "int")
            << ", sample_keys = " << sample_keys
            << ", query_mode = " << query_mode
            << (query_mode == "range" || query_mode == "r" ? (std::string(", range_width = ") + std::to_string(range_width)) : std::string(""))
            << std::endl;

  if (value_type == 2) {
    return RunBPTQuery<GlobalSecIndexValue>(index_path, num_queries, sample_keys, query_mode, range_width);
  } else {
    return RunBPTQuery<int>(index_path, num_queries, sample_keys, query_mode, range_width);
  }
}

// Build:
// g++ -O3 -std=c++17 -I.. -I../include bplustree/bplustree_query.cc -o bplustree/bplustree_query
// Run examples:
// Point:
// ./bplustree/bplustree_query /NV1/ysh/NEXT/examples/bplustree/global_bptree 1000000 1 200000 point
// ./bplustree/bplustree_query /NV1/ysh/NEXT/examples/bplustree/global_bptree 1000000 2 200000 point
// Range (width=1000):
// ./bplustree/bplustree_query /NV1/ysh/NEXT/examples/bplustree/global_bptree 200000 1 200000 range 1000
// ./bplustree/bplustree_query /NV1/ysh/NEXT/examples/bplustree/global_bptree 200000 2 200000 range 1000
