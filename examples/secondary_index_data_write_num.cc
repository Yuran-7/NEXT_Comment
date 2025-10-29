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
#include <ctime>
#include <iomanip>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/wide_columns.h"
#include "util/coding.h"
#include "util/rtree.h"
#include "util/hilbert_curve.h"
// #include "util/z_curve.h"


using namespace rocksdb;

std::string serialize_key(uint64_t iid, double xValue, double yValue) {
    std::string key;
    // The R-tree stores boxes, hence duplicate the input values
    key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    return key;
}

std::string serialize_id(int iid) { // int类型的key，如果想让它按整数值大小排序，请确保 key 的序列化使用大端序（big-endian）
    std::string key;  // 但实际上目前是小端序，256  (00 01 00 00)，1  (01 00 00 00)，256 会排在 1 前面
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValue) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    // val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    // val.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    // val.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    return val;
}

std::string serialize_query(uint64_t iid_min,
                            uint64_t iid_max, double value_min,
                            double value_max) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
    return key;
}

uint64_t decode_value(std::string& value) {
    return *reinterpret_cast<const uint64_t*>(value.data());
}

struct Key {
    Mbr mbr;
};


Key deserialize_key(Slice key_slice) {
    Key key;
    key.mbr = ReadKeyMbr(key_slice);
    return key;
}

// A comparator that interprets keys from Noise. It's a length prefixed
// string first (the keypath) followed by the value and the Internal Id.
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
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        return slice_a.compare(slice_b);

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

    int dataSize = int(atoi(argv[2]));
    std::ifstream dataFile(argv[3]);
    std::cout << "data size: " << dataSize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;
    NoiseComparator1 sec_cmp;
    options.sec_comparator = &sec_cmp;

    options.info_log_level = DEBUG_LEVEL;
    // options.statistics = rocksdb::CreateDBStatistics();

    options.max_write_buffer_number = 5;  // 可变MemTable：1个（固定） + 不可变MemTable：4个（可变）
    options.max_background_jobs = 8;   // max_flushes = 2, max_compactions = 6，db/db_impl/db_impl_compaction_flush.cc，2548行

    BlockBasedTableOptions block_based_options;

    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;    // 声明在include/rocksdb/table.h，默认false，主要使用在table/block_based/block_based_table_builder.cc
    block_based_options.create_sec_index_reader = true;  // 用在table/block_based/block_based_table_builder.cc，PrefetchIndexAndFilterBlocks函数
    block_based_options.sec_index_type = BlockBasedTableOptions::kBtreeSec; // kOneDRtreeSec，kBtreeSec，kRtreeSec
    
    // For global secondary index in memory
    options.create_global_sec_index = true;  // 初始化options/cf_options.cc下的ImmutableCFOptions的global_sec_index
    options.global_sec_index_is_btree =true;
    // To indicate the index attribute type
    options.global_sec_index_is_spatial = false;  // 默认是true

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));      // NewBlockBasedTableFactory声明在include/rocksdb/table.h，定义在table/block_based/block_based_table_factory.cc
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);    // SkipListSecFactory继承MemTableRepFactory
    
    options.allow_concurrent_memtable_write = false;  // 同一时间只有一个线程可以写入 MemTable

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;
    
    // Disable compression for all levels
    options.compression = rocksdb::kNoCompression;  // 没有执行压缩
    options.bottommost_compression = rocksdb::kNoCompression;

    Status s;
    
    // CSV schema:
    // id,area,min_lon,min_lat,max_lon,max_lat,geom,tags
    int id;
    double area;
    double min_lon, min_lat, max_lon, max_lat;
    std::string geom;
    std::string tags;

    // Failed to open, probably it doesn't exist yet. Try to create it and
    // insert data
    if (true) {
        // Remove all files in the database directory before creating/opening
        std::string rm_command = "rm -rf " + kDBPath + "/*";
        std::cout << "Removing existing database files: " << rm_command << std::endl;
        int rm_result = system(rm_command.c_str());
        if (rm_result != 0) {
            std::cout << "Warning: Failed to remove existing files, continuing anyway..." << std::endl;
        }
        
        options.create_if_missing = true;
        s = DB::Open(options, kDBPath, &db);
        std::cout << "Create if missing: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "start writing data (CSV with header: id,area,min_lon,min_lat,max_lon,max_lat,geom,tags)" << std::endl;
        // auto totalDuration = std::chrono::duration<long long, std::milli>(0);
        std::chrono::nanoseconds totalDuration{0};

        std::string line;
        int lineCount = 0;
        size_t totalKeySize = 0;
        size_t totalEntityValuesSize = 0; // sum of column values sizes

        // Helper: simple CSV parser supporting commas inside double quotes
        auto parse_csv = [](const std::string& input) {
            std::vector<std::string> fields;
            std::string cur;
            bool in_quotes = false;
            for (size_t i = 0; i < input.size(); ++i) {
                char c = input[i];
                if (c == '"') {
                    if (in_quotes && i + 1 < input.size() && input[i + 1] == '"') {
                        // Escaped quote
                        cur.push_back('"');
                        ++i;
                    } else {
                        in_quotes = !in_quotes;
                    }
                } else if (c == ',' && !in_quotes) {
                    fields.emplace_back(std::move(cur));
                    cur.clear();
                } else {
                    cur.push_back(c);
                }
            }
            fields.emplace_back(std::move(cur));
            // Trim trailing CR from last field if present (Windows line endings)
            if (!fields.empty() && !fields.back().empty() && fields.back().back() == '\r') {
                fields.back().pop_back();
            }
            return fields;
        };

        // Read and discard the header line if present
        if (std::getline(dataFile, line)) {
            auto header = parse_csv(line);
            // If the first column isn't numeric header, assume it's a header row; otherwise treat as data
            if (!(header.size() >= 1 && (header[0] == "id" || header[0] == "ID"))) {
                // This was actually a data row; process it below by pushing back into a buffer
                dataFile.seekg(0); // rewind to beginning if no header
            }
        }

        ColumnFamilyHandle* default_cf = nullptr;
        default_cf = db->DefaultColumnFamily();

        {
            using namespace std::chrono;
            auto now = system_clock::now();
            auto sec = time_point_cast<seconds>(now);
            auto micros = duration_cast<microseconds>(now - sec).count();
            std::time_t tt = system_clock::to_time_t(now);
            std::tm tm{};
            localtime_r(&tt, &tm);
            std::cout << std::put_time(&tm, "%H:%M:%S") << '.'
                      << std::setw(6) << std::setfill('0') << micros << std::endl;
        }
        while (std::getline(dataFile, line)) {
            if (lineCount == dataSize) {
                break;
            }
            if (line.empty()) {
                continue;
            }
            auto fields = parse_csv(line);
            if (fields.size() < 8) {
                // Skip malformed line
                continue;
            }
            lineCount++;
            auto start = std::chrono::high_resolution_clock::now();
            // Parse fields
            try {
                id = std::stoi(fields[0]);
            } catch (...) {
                continue;
            }
            try {
                area = std::stod(fields[1]);
                min_lon = std::stod(fields[2]);
                min_lat = std::stod(fields[3]);
                max_lon = std::stod(fields[4]);
                max_lat = std::stod(fields[5]);
            } catch (...) {
                // Skip lines with invalid numeric data
                continue;
            }
            geom = fields[6];
            tags = fields[7];

            std::string key = serialize_id(id);

            // Build wide columns for this entity
            std::string area_str = serialize_value(area);
            std::string min_lon_str = serialize_value(min_lon);
            std::string min_lat_str = serialize_value(min_lat);
            std::string max_lon_str = serialize_value(max_lon);
            std::string max_lat_str = serialize_value(max_lat);

            WideColumns columns{{"area", area_str},
                                {"min_lon", min_lon_str},
                                {"min_lat", min_lat_str},
                                {"max_lon", max_lon_str},
                                {"max_lat", max_lat_str},
                                {"geom", geom},
                                {"tags", tags}};

            totalKeySize += key.size();
            totalEntityValuesSize += area_str.size() + min_lon_str.size() + min_lat_str.size() +
                                     max_lon_str.size() + max_lat_str.size() + geom.size() + tags.size();

            if (lineCount % 100000 == 1) {
                std::cout << "processed " << lineCount << " records" << std::endl;
            }
            s = db->PutEntity(WriteOptions(), default_cf, key, columns);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            totalDuration = totalDuration + duration;
        }


        // Print current local time with microseconds precision, e.g., 19:56:26.782045
        {
            using namespace std::chrono;
            auto now = system_clock::now();
            auto sec = time_point_cast<seconds>(now);
            auto micros = duration_cast<microseconds>(now - sec).count();
            std::time_t tt = system_clock::to_time_t(now);
            std::tm tm{};
            localtime_r(&tt, &tm);
            std::cout << std::put_time(&tm, "%H:%M:%S") << '.'
                      << std::setw(6) << std::setfill('0') << micros << std::endl;
        }


        std::cout << "Status: " << s.ToString() << std::endl;
        assert(s.ok());

        // std::cout << "Status: " << s.ToString() << std::endl;

        std::cout << "end writing data" << std::endl;
        std::cout << "Total records written: " << lineCount << std::endl;
        std::cout << "Total key size: " << totalKeySize / 1024.0 / 1024.0 << " MB" << std::endl;
        std::cout << "Total entity values size (approx): " << totalEntityValuesSize / 1024.0 / 1024.0 << " MB" << std::endl;
        std::cout << "Total data size (approx): " << (totalKeySize + totalEntityValuesSize) / 1024.0 / 1024.0 << " MB" << std::endl;
        std::cout << "Execution time: " << totalDuration.count() / 1'000'000'000.0 << " seconds" << std::endl;

        sleep(5);    // 秒

        db->Close();

    }

    delete db;
    return 0;
}

/*
g++ -O3 -std=c++17 \
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
  -fno-rtti secondary_index_data_write_num.cc \
  -o secondary_index_data_write_num ../librocksdb.a \
  -I../include -I.. \
  -lpthread -lrt -ldl -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd -lnuma -ltbb -luring
 */


// ./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 33000000 /NV1/ysh/dataset/osm_building.csv
// ./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 1000000 /NV1/ysh/dataset/buildings_1m/buildings_1m.csv
// ./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 1000000 /NV1/ysh/dataset/buildings_1m/buildings_1m_10.csv
// ./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 10000000 /NV1/ysh/dataset/buildings_10m.csv
// ./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 10000000 /NV1/ysh/dataset/buildings_10m_10.csv