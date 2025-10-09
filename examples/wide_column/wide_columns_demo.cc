#include <iostream>
#include <string>
#include <vector>
#include <array>
#include <cassert>
#include <chrono>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/wide_columns.h"

using namespace ROCKSDB_NAMESPACE;

// Helper to print a WideColumns container
void PrintWideColumns(const WideColumns& columns) {
  std::cout << "{";
  bool first = true;
  for (const auto& kv : columns) {
    if (!first) std::cout << ", ";
    first = false;
    // WideColumn exposes name() and value() rather than pair.first/second
    std::cout << kv.name().ToString() << ":" << kv.value().ToString();
  }
  std::cout << "}";
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <db_path>" << std::endl;
    return 1;
  }
  std::string db_path = argv[1];

  Options options;
  options.create_if_missing = true;

  DB* db = nullptr;
  Status s = DB::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }

  ColumnFamilyHandle* default_cf = db->DefaultColumnFamily();

  // 1. Put a wide-column entity with default column present
  const std::string key1 = "user:1";
  WideColumns columns1{{kDefaultWideColumnName, "Alice"}, {"age", "30"}, {"city", "Paris"}};
  s = db->PutEntity(WriteOptions(), default_cf, key1, columns1);
  if (!s.ok()) {
    std::cerr << "PutEntity for key1 failed: " << s.ToString() << std::endl;
    return 1;
  }

  // 2. Put a wide-column entity without an explicit default column; reading value returns empty
  const std::string key2 = "user:2";
  WideColumns columns2{{"name", "Bob"}, {"age", "24"}};
  s = db->PutEntity(WriteOptions(), default_cf, key2, columns2);
  if (!s.ok()) {
    std::cerr << "PutEntity for key2 failed: " << s.ToString() << std::endl;
    return 1;
  }

  // 3. Plain key-value for comparison
  const std::string key3 = "user:3";
  const std::string value3 = "Charlie";
  s = db->Put(WriteOptions(), default_cf, key3, value3);
  if (!s.ok()) {
    std::cerr << "Put for key3 failed: " << s.ToString() << std::endl;
    return 1;
  }

  // 4. Batch insert another wide-column entity
  const std::string key4 = "user:4";
  WideColumns columns4{{kDefaultWideColumnName, "Diana"}, {"hobby", "Ski"}};
  WriteBatch batch;
  s = batch.PutEntity(default_cf, key4, columns4);
  if (!s.ok()) {
    std::cerr << "WriteBatch PutEntity key4 failed: " << s.ToString() << std::endl;
    return 1;
  }
  s = db->Write(WriteOptions(), &batch);
  if (!s.ok()) {
    std::cerr << "Write batch failed: " << s.ToString() << std::endl;
    return 1;
  }

  // Single Get on key1 value and entity
  {
    PinnableSlice value;
    s = db->Get(ReadOptions(), default_cf, key1, &value);
    std::cout << "Get value(key1): status=" << s.ToString() << ", value='" << value.ToString() << "'\n";
    PinnableWideColumns wide; // 主要包含两个成员变量：WideColumns columns_和 PinnableSlice value_
    s = db->GetEntity(ReadOptions(), default_cf, key1, &wide);
    std::cout << "GetEntity(key1): status=" << s.ToString() << ", columns=";
    PrintWideColumns(wide.columns());
    std::cout << "\n";
  }

  // Get key2 value (expected empty) and entity
  {
    PinnableSlice value;
    s = db->Get(ReadOptions(), default_cf, key2, &value);
    std::cout << "Get value(key2): status=" << s.ToString() << ", value_size=" << value.size() << " (empty means no default column)\n";
    PinnableWideColumns wide;
    s = db->GetEntity(ReadOptions(), default_cf, key2, &wide);
    std::cout << "GetEntity(key2): status=" << s.ToString() << ", columns=";
    PrintWideColumns(wide.columns());
    std::cout << "\n";
  }

  // Iterate over all entries
  {
    std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::cout << "Iter key=" << it->key().ToString() << ", value='" << it->value().ToString() << "'";
      PrintWideColumns(it->columns());
      std::cout << "\n";
    }
    if (!it->status().ok()) {
      std::cerr << "Iterator status error: " << it->status().ToString() << std::endl;
    }
  }

  // MultiGet over a set of keys
  {
    const size_t num_keys = 4;
    std::array<Slice, num_keys> keys{{key1, key2, key3, key4}};
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;
    db->MultiGet(ReadOptions(), default_cf, num_keys, &keys[0], &values[0], &statuses[0]);
    for (size_t i = 0; i < num_keys; ++i) {
      std::cout << "MultiGet[" << i << "]: key=" << keys[i].ToString() << ", status=" << statuses[i].ToString() << ", value='" << values[i].ToString() << "'\n";
    }
  }

  // Error case: duplicate columns should return Corruption
  {
    WideColumns bad{{"dup", "v1"}, {"dup", "v2"}};
    s = db->PutEntity(WriteOptions(), default_cf, "badkey", bad);
    std::cout << "PutEntity duplicate columns status=" << s.ToString() << " (expect Corruption)\n";
  }

  // Demonstrate that Merge on a wide-column key is not supported (should still succeed putting entity, but merge reads NotSupported)
  {
    const std::string merge_key = "user:merge";
    WideColumns mc{{"attr", "base"}};
    s = db->PutEntity(WriteOptions(), default_cf, merge_key, mc);
    if (s.ok()) {
      s = db->Merge(WriteOptions(), default_cf, merge_key, "operand");
      std::cout << "Merge on wide-column key status=" << s.ToString() << " (often NotSupported in wide-column context)\n";
      PinnableSlice value;
      s = db->Get(ReadOptions(), default_cf, merge_key, &value);
      std::cout << "After Merge Get value status=" << s.ToString() << ", value='" << value.ToString() << "'\n";
    }
  }

  // Flush and reopen to ensure persistence
  s = db->Flush(FlushOptions());
  std::cout << "Flush status=" << s.ToString() << "\n";
  delete db;
  db = nullptr;
  s = DB::Open(options, db_path, &db);
  std::cout << "Reopen status=" << s.ToString() << "\n";
  default_cf = db->DefaultColumnFamily();
  if (s.ok()) {
    PinnableWideColumns wide;
    s = db->GetEntity(ReadOptions(), default_cf, key1, &wide);
    std::cout << "Post-reopen GetEntity(key1): status=" << s.ToString() << ", columns="; PrintWideColumns(wide.columns()); std::cout << "\n";
  }

  delete db;
  return 0;
}

/*
Example build (adjust paths / library names as needed):

g++ -std=c++17 -O0 -g \
  -I../include -I.. wide_column/wide_columns_demo.cc \
  -o wide_column/wide_columns_demo \
  ../librocksdb_debug.a \
  -lpthread -ldl -lz -lsnappy -llz4 -lbz2 -lzstd -lnuma -ltbb

Run:
  ./wide_column/wide_columns_demo /NV1/ysh/NEXT/examples/widedb
*/
