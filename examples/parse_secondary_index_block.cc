#include <iostream>
#include <fstream>
#include <vector>
#include <cstdint>
#include <cstring>
#include <iomanip>

// Varint解码函数
bool GetVarint64(const char** input, size_t* remaining, uint64_t* value) {
    const char* p = *input;
    const char* limit = p + *remaining;
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = static_cast<unsigned char>(*p);
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *input = p;
            *remaining = limit - p;
            *value = result;
            return true;
        }
    }
    return false;
}

// Fixed32解码
uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

struct ValueRange {
    double min;
    double max;
    
    void print() const {
        std::cout << "[" << min << ", " << max << "]";
    }
};

struct BlockHandle {
    uint64_t offset;
    uint64_t size;
    
    void print() const {
        std::cout << "offset=" << offset << ", size=" << size;
    }
};

struct IndexEntry {
    ValueRange key;
    BlockHandle value;
};

class SecondaryIndexBlockParser {
private:
    std::vector<char> block_data_;
    std::vector<uint32_t> restart_points_;
    uint32_t num_restarts_;
    
public:
    bool ReadBlockFromFile(const std::string& filename, uint64_t offset, uint64_t size) {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "无法打开文件: " << filename << std::endl;
            return false;
        }
        
        // 定位到指定offset
        file.seekg(offset, std::ios::beg);
        if (!file.good()) {
            std::cerr << "无法定位到offset: " << offset << std::endl;
            return false;
        }
        
        // 读取指定大小的数据
        block_data_.resize(size);
        file.read(block_data_.data(), size);
        if (file.gcount() != static_cast<std::streamsize>(size)) {
            std::cerr << "读取数据失败，期望 " << size << " 字节，实际读取 " << file.gcount() << " 字节" << std::endl;
            return false;
        }
        
        file.close();
        return true;
    }
    
    bool ParseBlockFooter() {
        if (block_data_.size() < 8) {
            std::cerr << "Block数据太小" << std::endl;
            return false;
        }
        
        // 读取最后4字节的footer
        size_t footer_offset = block_data_.size() - 4;
        uint32_t block_footer = DecodeFixed32(&block_data_[footer_offset]);
        
        // 解包 footer 获取 num_restarts
        // footer的低28位是num_restarts，高4位是index_type
        num_restarts_ = block_footer & 0x0FFFFFFF;
        uint32_t index_type = block_footer >> 28;
        
        std::cout << "Block Footer信息:" << std::endl;
        std::cout << "  Index Type: " << index_type << std::endl;
        std::cout << "  Num Restarts: " << num_restarts_ << std::endl;
        
        // 读取restart points数组
        size_t restarts_offset = footer_offset - num_restarts_ * 4;
        if (restarts_offset > block_data_.size()) {
            std::cerr << "Restart points偏移量无效" << std::endl;
            return false;
        }
        
        restart_points_.clear();
        for (uint32_t i = 0; i < num_restarts_; i++) {
            uint32_t restart = DecodeFixed32(&block_data_[restarts_offset + i * 4]);
            restart_points_.push_back(restart);
        }
        
        return true;
    }
    
    std::vector<IndexEntry> ParseEntries() {
        std::vector<IndexEntry> entries;
        
        if (restart_points_.empty()) {
            std::cerr << "没有restart points" << std::endl;
            return entries;
        }
        
        // 计算数据区域的结束位置（restart数组开始位置）
        size_t data_end = block_data_.size() - 4 - num_restarts_ * 4;
        
        std::cout << "\n开始解析条目..." << std::endl;
        std::cout << "数据区域大小: " << data_end << " 字节\n" << std::endl;
        
        size_t offset = 0;
        int entry_count = 0;
        std::string last_key_str;
        
        while (offset < data_end) {
            entry_count++;
            std::cout << "=== Entry #" << entry_count << " (offset=" << offset << ") ===" << std::endl;
            
            const char* p = &block_data_[offset];
            size_t remaining = data_end - offset;
            
            // 读取shared_bytes
            uint64_t shared_bytes;
            if (!GetVarint64(&p, &remaining, &shared_bytes)) {
                std::cerr << "无法读取shared_bytes" << std::endl;
                break;
            }
            std::cout << "  Shared bytes: " << shared_bytes << std::endl;
            
            // 读取unshared_bytes
            uint64_t unshared_bytes;
            if (!GetVarint64(&p, &remaining, &unshared_bytes)) {
                std::cerr << "无法读取unshared_bytes" << std::endl;
                break;
            }
            std::cout << "  Unshared bytes: " << unshared_bytes << std::endl;
            
            // 读取value_length
            uint64_t value_length;
            if (!GetVarint64(&p, &remaining, &value_length)) {
                std::cerr << "无法读取value_length" << std::endl;
                break;
            }
            std::cout << "  Value length: " << value_length << std::endl;
            
            // 读取key_delta
            if (remaining < unshared_bytes) {
                std::cerr << "数据不足以读取key_delta" << std::endl;
                break;
            }
            std::string key_delta(p, unshared_bytes);
            p += unshared_bytes;
            remaining -= unshared_bytes;
            
            // 重建完整的key
            std::string full_key;
            if (shared_bytes > 0) {
                if (shared_bytes > last_key_str.size()) {
                    std::cerr << "shared_bytes超过last_key大小" << std::endl;
                    break;
                }
                full_key = last_key_str.substr(0, shared_bytes) + key_delta;
            } else {
                full_key = key_delta;
            }
            last_key_str = full_key;
            
            // 解析key为ValueRange
            IndexEntry entry;
            if (full_key.size() == 16) {
                memcpy(&entry.key.min, full_key.data(), 8);
                memcpy(&entry.key.max, full_key.data() + 8, 8);
                std::cout << "  Key (ValueRange): ";
                entry.key.print();
                std::cout << std::endl;
            } else {
                std::cout << "  Key长度异常: " << full_key.size() << " (期望16字节)" << std::endl;
            }
            
            // 读取value
            if (remaining < value_length) {
                std::cerr << "数据不足以读取value" << std::endl;
                break;
            }
            std::string value_str(p, value_length);
            p += value_length;
            remaining -= value_length;
            
            // 解析value为BlockHandle
            const char* val_p = value_str.data();
            size_t val_remaining = value_str.size();
            if (GetVarint64(&val_p, &val_remaining, &entry.value.offset) &&
                GetVarint64(&val_p, &val_remaining, &entry.value.size)) {
                std::cout << "  Value (BlockHandle): ";
                entry.value.print();
                std::cout << std::endl;
            } else {
                std::cout << "  无法解析BlockHandle" << std::endl;
            }
            
            entries.push_back(entry);
            
            // 更新offset
            offset = data_end - remaining;
            std::cout << std::endl;
        }
        
        return entries;
    }
    
    void PrintRestartPoints() {
        std::cout << "\nRestart Points:" << std::endl;
        for (size_t i = 0; i < restart_points_.size(); i++) {
            std::cout << "  [" << i << "] offset=" << restart_points_[i] << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "用法: " << argv[0] << " <SST文件路径> <offset> <size>" << std::endl;
        std::cerr << "示例: " << argv[0] << " /path/to/sst_file 12345 1024" << std::endl;
        return 1;
    }
    
    std::string filename = argv[1];
    uint64_t offset = std::stoull(argv[2]);
    uint64_t size = std::stoull(argv[3]);
    
    std::cout << "========================================" << std::endl;
    std::cout << "二级索引Block解析器" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "文件: " << filename << std::endl;
    std::cout << "Offset: " << offset << std::endl;
    std::cout << "Size: " << size << std::endl;
    std::cout << "========================================\n" << std::endl;
    
    SecondaryIndexBlockParser parser;
    
    // 读取block数据
    if (!parser.ReadBlockFromFile(filename, offset, size)) {
        return 1;
    }
    std::cout << "✓ 成功读取Block数据\n" << std::endl;
    
    // 解析footer和restart points
    if (!parser.ParseBlockFooter()) {
        return 1;
    }
    std::cout << "✓ 成功解析Block Footer\n" << std::endl;
    
    // 打印restart points
    parser.PrintRestartPoints();
    
    // 解析所有条目
    std::vector<IndexEntry> entries = parser.ParseEntries();
    
    // 打印汇总信息
    std::cout << "========================================" << std::endl;
    std::cout << "解析完成！" << std::endl;
    std::cout << "总共解析出 " << entries.size() << " 个条目" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}

// g++ -g3 -O0 -std=c++17 -o parse_secondary_index_block parse_secondary_index_block.cc
// ./parse_secondary_index_block testdb/000016.sst 67593921 508