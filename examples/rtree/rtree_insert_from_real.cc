#include <chrono>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "util/RTree_mem.h"

using namespace ROCKSDB_NAMESPACE;

// GlobalSecIndexValue 结构定义
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

int main(int argc, char** argv) {
  // 默认参数
  std::string input_file = "entries_keys_collection.txt";
  std::string output_file = "";  // R树保存路径（可选）
  int64_t max_inserts = 55000;   // 最大插入数量

  // 解析命令行参数
  if (argc >= 2) {
    input_file = argv[1];
  }
  if (argc >= 3) {
    max_inserts = std::stoll(argv[2]);
  }
  if (argc >= 4) {
    output_file = argv[3];
  }

  std::cout << "=== RTree Insert from Real Data ===" << std::endl;
  std::cout << "Input file: " << input_file << std::endl;
  std::cout << "Max inserts: " << max_inserts << std::endl;
  if (!output_file.empty()) {
    std::cout << "Output file: " << output_file << std::endl;
  }
  std::cout << std::endl;

  // 创建 1D R树
  RTree<GlobalSecIndexValue, double, 1, double> tree;

  // 打开输入文件
  std::ifstream infile(input_file);
  if (!infile.is_open()) {
    std::cerr << "Error: Cannot open input file: " << input_file << std::endl;
    return 1;
  }

  int64_t total_inserted = 0;
  int64_t line_count = 0;
  int sst_count = 0;
  
  const auto t0 = std::chrono::high_resolution_clock::now();

  std::string line;
  while (std::getline(infile, line) && total_inserted < max_inserts) {
    line_count++;
    
    // 跳过空行
    if (line.empty()) {
      continue;
    }
    
    // 检测 SST 标记行
    if (line.find("=== New SST") != std::string::npos) {
      sst_count++;
      std::cout << "Processing SST #" << sst_count << "..." << std::endl;
      continue;
    }
    
    // 跳过 Entry[] 格式的行（如果有的话）
    if (line.find("Entry[") != std::string::npos) {
      continue;
    }
    
    // 解析数据行：期望格式为 "min max" 或 "min\tmax"
    std::istringstream iss(line);
    double min_val, max_val;
    
    if (!(iss >> min_val >> max_val)) {
      // 解析失败，跳过此行
      std::cerr << "Warning: Cannot parse line " << line_count << ": " << line << std::endl;
      continue;
    }
    
    // 确保 min <= max
    if (min_val > max_val) {
      std::swap(min_val, max_val);
    }
    
    // 创建 GlobalSecIndexValue（值可以是任意的）
    GlobalSecIndexValue gsi;
    gsi.id = static_cast<int>(total_inserted);
    gsi.filenum = static_cast<uint64_t>(sst_count);
    // 使用范围中心点映射到偏移量
    double center = 0.5 * (min_val + max_val);
    uint64_t offset = static_cast<uint64_t>(center * 1e6) * 4096ULL;
    gsi.blkhandle = BlockHandle(offset, 4096ULL);
    
    // 插入到 R树
    double min[1] = {min_val};
    double max[1] = {max_val};
    tree.Insert(min, max, gsi);
    
    total_inserted++;
    
    // 每 10000 条打印进度
    if (total_inserted % 10000 == 0) {
      auto now = std::chrono::high_resolution_clock::now();
      double sec = std::chrono::duration_cast<std::chrono::duration<double>>(now - t0).count();
      double kops = (total_inserted / 1000.0) / sec;
      std::cout << "Progress: " << total_inserted << "/" << max_inserts
                << " inserts, elapsed: " << sec << " s, throughput: " << kops
                << " K inserts/s" << std::endl;
    }
    
    // 达到最大插入数量则退出
    if (total_inserted >= max_inserts) {
      std::cout << "Reached max inserts limit: " << max_inserts << std::endl;
      break;
    }
  }

  infile.close();

  const auto t1 = std::chrono::high_resolution_clock::now();
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  const double seconds = ns / 1e9;
  const double kops = (total_inserted / 1000.0) / seconds;

  std::cout << "\n=== Summary ===" << std::endl;
  std::cout << "Total SSTs processed: " << sst_count << std::endl;
  std::cout << "Total lines read: " << line_count << std::endl;
  std::cout << "Total inserts: " << total_inserted << std::endl;
  std::cout << "Insertion time: " << seconds << " s" << std::endl;
  std::cout << "Throughput: " << kops << " K inserts/s" << std::endl;
  std::cout << "RTree Count(): " << tree.Count() << std::endl;

  // 保存 R树到文件（如果指定了输出路径）
  if (!output_file.empty()) {
    std::cout << "\nSaving RTree to file..." << std::endl;
    const auto s0 = std::chrono::high_resolution_clock::now();
    bool ok = tree.Save(output_file.c_str());
    const auto s1 = std::chrono::high_resolution_clock::now();
    double save_sec = std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count() / 1e9;
    std::cout << "Save result: " << (ok ? "OK" : "FAIL")
              << ", time: " << save_sec << " s, path=\"" << output_file << "\"" << std::endl;
  }

  return 0;
}

// 编译命令：
// g++ -O3 -std=c++17 -I.. -I../include rtree/rtree_insert_from_real.cc -o rtree/rtree_insert_from_real
//
// 使用示例：
// ./rtree/rtree_insert_from_real /NV1/ysh/NEXT/examples/testdb/entries_keys_collection.txt 55000 /NV1/ysh/NEXT/examples/rtree/global_rtree
// ./rtree/rtree_insert_from_real entries_keys_collection.txt 55000
// ./rtree/rtree_insert_from_real entries_keys_collection.txt
