#include <algorithm>
#include <cstdio>
#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <chrono>
#include <math.h>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/rtree.h"
#include "util/hilbert_curve.h"
// #include "util/z_curve.h"


using namespace rocksdb;


std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValue) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    return val;
}

std::string serialize_query(double x_value_min, double x_value_max) {
    std::string query;
    query.append(reinterpret_cast<const char*>(&x_value_min), sizeof(double));  // 直接把浮点数的IEEE 754二进制表示存进去
    query.append(reinterpret_cast<const char*>(&x_value_max), sizeof(double));
    return query;
}

uint64_t decode_value(std::string& value) {
    return *reinterpret_cast<const uint64_t*>(value.data());
}

double deserialize_val(Slice val_slice) {
    double val;
    val = *reinterpret_cast<const double*>(val_slice.data());
    return val;
}

class NoiseComparator : public rocksdb::Comparator {
public:
    const char* Name() const {
        return "rocksdb.NoiseComparator";
    }

    int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
        Slice slice_a = Slice(const_a);
        Slice slice_b = Slice(const_b);

        // keypaths are the same, compare the value. The previous
        // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
        // to `.data()` can directly be used.
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());  // 意义不明
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());


        return slice_a.compare(slice_b);  // Slice类的一个方法
    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

class NoiseComparator1 : public rocksdb::Comparator {
public:
    const char* Name() const {
        return "rocksdb.NoiseComparator";
    }

    int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
        Slice slice_a = Slice(const_a);
        Slice slice_b = Slice(const_b);

        // keypaths are the same, compare the value. The previous
        // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
        // to `.data()` can directly be used.
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        // specific comparator to allow random output order
        return 1;
    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

int main(int argc, char* argv[]) {

    std::string kDBPath = argv[1];
    int querySize = int(atoi(argv[2]));
    std::ifstream queryFile(argv[3]);
    // std::string resultpath = argv[5];
    // std::ofstream resFile(resultpath);
    std::cout << "Query size: " << querySize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;
    NoiseComparator1 cmp1;
    options.sec_comparator = &cmp1;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics(); // 启用了statistics

    BlockBasedTableOptions block_based_options;

    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;
    block_based_options.create_sec_index_reader = true;
    block_based_options.sec_index_type = BlockBasedTableOptions::kOneDRtreeSec;
    
    // For global secondary index in memory
    options.create_global_sec_index = true; // to activate the global sec index
    options.global_sec_index_is_spatial = false;

    // Set the block cache to 64 MB
    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);

    options.force_consistency_checks = false;

    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;

    uint32_t id;
    uint32_t op;
    double low[2];

    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context; // 定义在util/rtree.h，继承IteratorContext

    std::chrono::nanoseconds totalDuration{0};
    int totalResults = 0;
    for (int i=0; i<querySize;i++) {
        queryFile >> low[0] >> low[1];

        auto start = std::chrono::high_resolution_clock::now();
        iterator_context.query_mbr = 
                serialize_query(low[0], low[1]);
        read_options.iterator_context = &iterator_context;
        read_options.is_secondary_index_scan = true;
        read_options.is_secondary_index_spatial = false;
        read_options.async_io = true;
        // std::cout << "create newiterator" << std::endl;
        std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));  // it动态类型是ArenaWrappedDBIter
        // std::cout << "created New iterator" << std::endl;
        int counter = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            // 打印第一轮查询(i == 0)的所有key，检查是否大致升序；已知key为4字节int
            if (i == 0) {
                const Slice key_slice = it->key();
                if (key_slice.size() >= sizeof(int)) {
                    int key_int = *reinterpret_cast<const int*>(key_slice.data());
                    std::cout << key_int << "\n";
                } else {
                    std::cout << "<invalid key size:" << key_slice.size() << ">\n";
                }
            }

            double value = deserialize_val(it->value());    // it->value()是返回所有值，deserialize_val将第一个double类型的面积取出来
            // std::cout << value << std::endl;
            counter ++;
        }
        auto end = std::chrono::high_resolution_clock::now(); 
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        totalDuration = totalDuration + duration;
        // resFile << "Results: \t" << counter << "\t" << totalDuration.count() << "\n";
        std::cout << "number of results: " << counter << std::endl;     
        // std::cout << "Query Duration: " << duration.count() << " nanoseconds" << std::endl;   
        totalResults += counter;
    }
    std::cout << "Execution time: " << totalDuration.count() / 1'000'000'000.0 << " seconds" << std::endl;
    std::cout << "Total number of results: " << totalResults << std::endl;

    sleep(5);
    db->Close();    

    delete db;

    return 0;
}

/*
g++ -g3 -O0 -std=c++17 \
  -faligned-new -DHAVE_ALIGNED_NEW \
  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX \
  -DOS_LINUX -fno-builtin-memcmp \
  -DROCKSDB_FALLOCATE_PRESENT -DSNAPPY -DGFLAGS=1 \
  -DZLIB -DBZIP2 -DLZ4 -DZSTD -DNUMA -DTBB \
  -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX \
  -DROCKSDB_BACKTRACE -DROCKSDB_RANGESYNC_PRESENT \
  -DROCKSDB_SCHED_GETCPU_PRESENT -DROCKSDB_AUXV_GETAUXVAL_PRESENT \
  -march=native -DHAVE_SSE42 -DHAVE_PCLMUL -DHAVE_AVX2 \
  -DHAVE_BMI -DHAVE_LZCNT -DHAVE_UINT128_EXTENSION \
  -fno-rtti secondary_index_read_num.cc \
  -o secondary_index_read_num ../librocksdb.a \
  -I../include -I.. \
  -lpthread -lrt -ldl -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd -lnuma -ltbb
 */

 // ./secondary_index_read_num /NV1/ysh/NEXT/examples/testdb 1000 /NV1/ysh/dataset/buildings_1m/buildings_1D_query_0.01