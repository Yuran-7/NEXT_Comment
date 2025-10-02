#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "util/RTree_mem.h"

using namespace ROCKSDB_NAMESPACE;

// Minimal GSI structures
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
static int RunRTreeQuery(const std::string& index_path, int64_t num_queries, size_t sample_items) {
  using RT = RTree<V, double, 1, double>;
  RT tree;

  // Load
  auto t0 = std::chrono::high_resolution_clock::now();
  bool ok = tree.Load(index_path.c_str());
  auto t1 = std::chrono::high_resolution_clock::now();
  if (!ok) {
    std::cerr << "Load failed for " << index_path << "\n";
    return 2;
  }
  double load_sec = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
  std::cout << "Load OK, time: " << load_sec << " s" << std::endl;

  // Sample items via iterator to get bounding boxes we'll query back
  std::vector<std::pair<double,double>> ranges;
  ranges.reserve(sample_items);
  typename RT::Iterator it;
  tree.GetFirst(it);
  while (!tree.IsNull(it) && ranges.size() < sample_items) {
    double mn[1], mx[1];
    it.GetBounds(mn, mx);
    ranges.emplace_back(mn[0], mx[0]);
    tree.GetNext(it);
  }
  if (ranges.empty()) {
    std::cerr << "No items found in index; aborting queries.\n";
    return 3;
  }

  // Query timing: search these ranges at random
  std::mt19937_64 rng(987654321ULL);
  std::uniform_int_distribution<size_t> pick(0, ranges.size() - 1);
  size_t hits = 0;

  auto q0 = std::chrono::high_resolution_clock::now();
  for (int64_t i = 0; i < num_queries; ++i) {
    const auto& rr = ranges[pick(rng)];
    double mn[1] = { rr.first };
    double mx[1] = { rr.second };
    auto res = tree.Search(mn, mx, [&](const V&) { return true; });
    hits += res.size();
  }
  auto q1 = std::chrono::high_resolution_clock::now();
  double query_sec = std::chrono::duration_cast<std::chrono::duration<double>>(q1 - q0).count();
  double qps = num_queries / query_sec;
  std::cout << "Queries: " << num_queries << ", time: " << query_sec << " s, QPS: " << qps
            << ", total_hits: " << hits << std::endl;

  // Show a sample search
  {
    const auto& rr = ranges.front();
    double mn[1] = { rr.first };
    double mx[1] = { rr.second };
    auto res = tree.Search(mn, mx, [&](const V&) { return true; });
    std::cout << "Sample range [" << rr.first << ", " << rr.second << "] -> count = " << res.size();
    if constexpr (std::is_same<V, GlobalSecIndexValue>::value) {
      if (!res.empty()) {
        const auto& v = res.back();
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
    std::cerr << "Usage: " << argv[0] << " <index_path> [num_queries=100000] [value_type=1|2] [sample_items=100000]\n";
    return 1;
  }

  std::string index_path = argv[1];
  int64_t num_queries = (argc >= 3) ? std::stoll(argv[2]) : 100'000LL;
  int value_type = (argc >= 4) ? std::stoi(argv[3]) : 1; // 1=int, 2=GlobalSecIndexValue
  size_t sample_items = (argc >= 5) ? static_cast<size_t>(std::stoull(argv[4])) : static_cast<size_t>(100'000);

  std::cout << "R-Tree query benchmark\n"
            << "index_path = " << index_path
            << ", num_queries = " << num_queries
            << ", ValueType = " << (value_type == 2 ? "GlobalSecIndexValue" : "int")
            << ", sample_items = " << sample_items
            << std::endl;

  if (value_type == 2) {
    return RunRTreeQuery<GlobalSecIndexValue>(index_path, num_queries, sample_items);
  } else {
    return RunRTreeQuery<int>(index_path, num_queries, sample_items);
  }
}

// Build:
// g++ -O3 -std=c++17 -I.. -I../include rtree/rtree_query.cc -o rtree/rtree_query
// Run examples:
// ./rtree/rtree_query /NV1/ysh/NEXT/examples/rtree/global_rtree 200000 1 200000
// ./rtree/rtree_query /NV1/ysh/NEXT/examples/rtree/global_rtree 200000 2 200000
