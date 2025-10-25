#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "util/BPlusTree.h"

using namespace ROCKSDB_NAMESPACE;

// 简化的 BlockHandle 和 GlobalSecIndexValue 结构
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
  BPlusTree<int, BlockHandle> tree;
  
  // 配置参数
  const int64_t records_per_batch = 200'000;  // 每批插入20万条
  const int64_t num_batches = 4;              // 4批
  const int64_t total_records = records_per_batch * num_batches;  // 总共80万条
  const int64_t distinct_keys = total_records / 10;  // 80,000个不同的key（每个key出现10次）
  
  std::cout << "=== B+Tree Insert-Delete-Reinsert Benchmark ===" << std::endl;
  std::cout << "Total records: " << total_records << std::endl;
  std::cout << "Distinct keys: " << distinct_keys << std::endl;
  std::cout << "Records per key: 10" << std::endl;
  std::cout << "Batches: " << num_batches << " x " << records_per_batch << " records" << std::endl;
  std::cout << std::endl;
  
  // ===== 阶段1: 4次批量插入 =====
  std::cout << "Phase 1: Inserting " << total_records << " records in " << num_batches << " batches..." << std::endl;
  
  auto t0 = std::chrono::high_resolution_clock::now();
  
  for (int batch = 0; batch < num_batches; ++batch) {
    auto batch_start = std::chrono::high_resolution_clock::now();
    
    for (int64_t i = 0; i < records_per_batch; ++i) {
      int64_t global_idx = batch * records_per_batch + i;
      int key = static_cast<int>(global_idx % distinct_keys);  // 确保每个key出现10次
      
      int id = static_cast<int>(global_idx);
      int filenum = static_cast<uint64_t>(batch + 10);  // filenum: 10, 11, 12, 13
      BlockHandle blkhandle = BlockHandle(static_cast<uint64_t>(key) * 4096ULL, 4096ULL);

      tree.Insert(key, filenum, blkhandle);
    }
    
    auto batch_end = std::chrono::high_resolution_clock::now();
    double batch_sec = std::chrono::duration_cast<std::chrono::duration<double>>(batch_end - batch_start).count();
    double batch_mops = (records_per_batch / 1e6) / batch_sec;
    
    std::cout << "  Batch " << (batch + 1) << "/" << num_batches 
              << " completed: " << batch_sec << " s, "
              << batch_mops << " M inserts/s" << std::endl;
  }
  
  auto t1 = std::chrono::high_resolution_clock::now();
  double insert_sec = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
  double insert_mops = (total_records / 1e6) / insert_sec;
  
  std::cout << "Phase 1 total time: " << insert_sec << " s" << std::endl;
  std::cout << "Phase 1 throughput: " << insert_mops << " M inserts/s" << std::endl;
  std::cout << std::endl;
  
  
  // ===== 阶段2: 删除所有80万条记录 =====
  std::cout << "Phase 2: Deleting all " << total_records << " records..." << std::endl;
  
  t0 = std::chrono::high_resolution_clock::now();
  
  int64_t delete_count = 0;
  for (int batch = 0; batch < num_batches; ++batch) {
    auto batch_start = std::chrono::high_resolution_clock::now();
    
    for (int64_t i = 0; i < records_per_batch; ++i) {
      int64_t global_idx = batch * records_per_batch + i;
      int key = static_cast<int>(global_idx % distinct_keys);
      

      int filenum = static_cast<uint64_t>(batch + 10);  // 与插入时相同的filenum
      bool deleted = tree.Delete(key, filenum);
      if (deleted) delete_count++;
    }
    
    auto batch_end = std::chrono::high_resolution_clock::now();
    double batch_sec = std::chrono::duration_cast<std::chrono::duration<double>>(batch_end - batch_start).count();
    double batch_mops = (records_per_batch / 1e6) / batch_sec;
    
    std::cout << "  Delete batch " << (batch + 1) << "/" << num_batches 
              << " completed: " << batch_sec << " s, "
              << batch_mops << " M deletes/s" << std::endl;
  }
  
  t1 = std::chrono::high_resolution_clock::now();
  double delete_sec = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
  double delete_mops = (total_records / 1e6) / delete_sec;
  
  std::cout << "Phase 2 total time: " << delete_sec << " s" << std::endl;
  std::cout << "Phase 2 throughput: " << delete_mops << " M deletes/s" << std::endl;
  std::cout << "Successfully deleted: " << delete_count << "/" << total_records << " records" << std::endl;
  std::cout << std::endl;
  
  
  // ===== 阶段3: 重新插入80万条记录（相同key，不同value） =====
  std::cout << "Phase 3: Re-inserting " << total_records << " records with new values..." << std::endl;
  
  t0 = std::chrono::high_resolution_clock::now();
  
  for (int batch = 0; batch < num_batches; ++batch) {
    auto batch_start = std::chrono::high_resolution_clock::now();
    
    for (int64_t i = 0; i < records_per_batch; ++i) {
      int64_t global_idx = batch * records_per_batch + i;
      int key = static_cast<int>(global_idx % distinct_keys);  // 相同的key
      
      int id = static_cast<int>(global_idx);
      int filenum = static_cast<uint64_t>(batch + 10);  // filenum: 10, 11, 12, 13
      BlockHandle blkhandle = BlockHandle(static_cast<uint64_t>(key) * 4096ULL, 4096ULL);
      tree.Insert(key, filenum, blkhandle);
    }
    
    auto batch_end = std::chrono::high_resolution_clock::now();
    double batch_sec = std::chrono::duration_cast<std::chrono::duration<double>>(batch_end - batch_start).count();
    double batch_mops = (records_per_batch / 1e6) / batch_sec;
    
    std::cout << "  Re-insert batch " << (batch + 1) << "/" << num_batches 
              << " completed: " << batch_sec << " s, "
              << batch_mops << " M inserts/s" << std::endl;
  }
  
  t1 = std::chrono::high_resolution_clock::now();
  double reinsert_sec = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
  double reinsert_mops = (total_records / 1e6) / reinsert_sec;
  
  std::cout << "Phase 3 total time: " << reinsert_sec << " s" << std::endl;
  std::cout << "Phase 3 throughput: " << reinsert_mops << " M inserts/s" << std::endl;
  std::cout << std::endl;
  
  
  // ===== 总结 =====
  std::cout << "=== Summary ===" << std::endl;
  std::cout << "Phase 1 (Insert):    " << insert_sec << " s, " << insert_mops << " M ops/s" << std::endl;
  std::cout << "Phase 2 (Delete):    " << delete_sec << " s, " << delete_mops << " M ops/s" << std::endl;
  std::cout << "Phase 3 (Re-insert): " << reinsert_sec << " s, " << reinsert_mops << " M ops/s" << std::endl;
  std::cout << "Total time: " << (insert_sec + delete_sec + reinsert_sec) << " s" << std::endl;
  
  return 0;
}

// 编译命令（在 NEXT_Comment 根目录执行）：
// g++ -O3 -std=c++17 -I.. -I../include bplustree/bplustree_insert_delete.cc -o bplustree/bplustree_insert_delete
//
// 运行命令：
// ./bplustree/bplustree_insert_delete
