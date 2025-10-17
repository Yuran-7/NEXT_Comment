# TableCache 详解：为什么需要 TableCache？

## 一、TableCache 的核心作用

### 1. 定义与概念
```cpp
// table_cache.h 注释
// Manages caching for TableReader objects for a column family. 
// The actual cache is allocated separately and passed to the constructor. 
// TableCache wraps around the underlying SST file readers by providing 
// Get(), MultiGet() and NewIterator() methods that hide the instantiation,
// caching and access to the TableReader.
```

**TableCache 是 RocksDB 中用来缓存 TableReader 对象的管理器**，它不是缓存数据本身，而是缓存**读取 SST 文件的对象（TableReader）**。

### 2. TableReader 是什么？
```cpp
class BlockBasedTable : public TableReader {
  // TableReader 是一个抽象类，BlockBasedTable 是其实现
  // 每个 TableReader 对应一个 SST 文件
  // 包含了：
  // - 文件句柄 (RandomAccessFileReader)
  // - 索引块 (Index Block)
  // - 过滤器 (Filter/Bloom Filter)
  // - 元数据 (Footer, Properties)
  // - 各种缓存的 Block
};
```

## 二、为什么需要 TableCache？

### 问题场景：没有 TableCache 会怎样？

#### 场景 1：频繁打开同一个 SST 文件
```cpp
// 假设没有 TableCache，每次读取都要：
Status Get(const Slice& key) {
  // 1. 打开文件（系统调用，耗时）
  std::unique_ptr<RandomAccessFile> file;
  env->NewRandomAccessFile("000123.sst", &file);  // 慢！
  
  // 2. 读取 Footer（4KB，磁盘 I/O）
  Footer footer;
  ReadFooterFromFile(file, &footer);  // 慢！
  
  // 3. 读取 Index Block（可能几百 KB，磁盘 I/O）
  Block index_block;
  ReadIndexBlock(file, footer.index_handle, &index_block);  // 慢！
  
  // 4. 读取 Filter Block（可能几十 KB，磁盘 I/O）
  FilterBlockReader filter;
  ReadFilterBlock(file, footer.filter_handle, &filter);  // 慢！
  
  // 5. 创建 TableReader 对象（内存分配）
  auto table_reader = new BlockBasedTable(...);  // 慢！
  
  // 6. 最后才能查询数据
  table_reader->Get(key, value);
  
  // 7. 用完就销毁（浪费！）
  delete table_reader;
  file->Close();
}

// 如果 1000 次查询同一个文件，就要重复上述步骤 1000 次！
// 每次都要：打开文件 + 读 Footer + 读 Index + 读 Filter + 创建对象
```

#### 性能损失分析（无 TableCache）：
```
单次 TableReader 创建成本：
├─ 文件打开：         1-5 ms     (系统调用)
├─ 读取 Footer：      0.1-1 ms   (4KB I/O)
├─ 读取 Index Block： 1-10 ms    (几百 KB I/O)
├─ 读取 Filter Block：0.5-5 ms   (几十 KB I/O)
├─ 对象构造：         0.1-0.5 ms (内存分配)
└─ 总计：            ~3-21 ms

1000 次查询同一文件 = 3000-21000 ms = 3-21 秒！
```

### 解决方案：使用 TableCache

```cpp
// 有了 TableCache：
Status Get(const Slice& key) {
  // 1. 先查缓存（极快，内存查找）
  Cache::Handle* handle = table_cache->Lookup(file_number);
  
  if (handle != nullptr) {  // 缓存命中！
    // 2. 直接使用已经创建好的 TableReader（0.01 ms）
    TableReader* reader = cache->Value(handle);
    reader->Get(key, value);  // 直接查询
    cache->Release(handle);
    return;
  }
  
  // 缓存未命中时才创建（第一次访问）
  // 创建后放入缓存，下次直接用
  CreateTableReaderAndCache(file_number);
}

// 1000 次查询同一文件：
// - 第 1 次：3-21 ms（创建 TableReader）
// - 第 2-1000 次：每次 0.01 ms（缓存命中）
// 总计：~20 ms vs 3000-21000 ms（提升 150-1050 倍！）
```

## 三、TableCache 的内部结构

### 1. 代码结构
```cpp
class TableCache {
 private:
  const ImmutableOptions& ioptions_;
  Cache* cache_;  // 实际的 LRU 缓存（通常是 ShardedLRUCache）
  
  // cache_ 中存储的是：
  // Key:   文件编号 (uint64_t file_number)
  // Value: TableReader* (BlockBasedTable 对象)
  
 public:
  // 查找或创建 TableReader
  Status FindTable(..., Cache::Handle** handle);
  
  // 从缓存中获取 TableReader
  TableReader* GetTableReaderFromHandle(Cache::Handle* handle);
  
  // 获取数据（内部使用 FindTable）
  Status Get(...);
  
  // 创建迭代器（内部使用 FindTable）
  InternalIterator* NewIterator(...);
};
```

### 2. FindTable 工作流程（核心方法）
```cpp
Status TableCache::FindTable(..., Cache::Handle** handle) {
  uint64_t file_number = file_meta.fd.GetNumber();
  
  // 步骤 1：查找缓存
  *handle = cache_->Lookup(file_number);
  
  if (*handle != nullptr) {
    // 缓存命中！直接返回
    return Status::OK();  // 耗时：~0.01 ms
  }
  
  // 步骤 2：缓存未命中，需要创建 TableReader
  
  // 2.1 加锁（避免多个线程重复创建同一个文件的 TableReader）
  MutexLock load_lock(loader_mutex_.get(file_number));
  
  // 2.2 再次检查缓存（double-check，可能其他线程已创建）
  *handle = cache_->Lookup(file_number);
  if (*handle != nullptr) {
    return Status::OK();  // 其他线程已创建
  }
  
  // 2.3 真正创建 TableReader
  std::unique_ptr<TableReader> table_reader;
  Status s = GetTableReader(
      file_options, internal_comparator, file_meta,
      &table_reader, ...);  // 耗时：3-21 ms
  
  if (!s.ok()) {
    return s;
  }
  
  // 2.4 插入缓存
  cache_->Insert(file_number, table_reader.release(), 
                 charge, &DeleteEntry<TableReader>, handle);
  
  return Status::OK();
}
```

### 3. 缓存插入与删除
```cpp
// 插入缓存（在 FindTable 中）
cache_->Insert(
    file_number,              // Key: 文件编号
    table_reader.release(),   // Value: TableReader 对象
    charge,                   // 占用内存大小
    &DeleteEntry<TableReader>,// 删除回调函数
    handle                    // 返回的句柄
);

// 删除回调（当缓存满时，LRU 淘汰旧条目）
template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;  // 删除 TableReader，自动关闭文件
}
```

## 四、使用示例

### 示例 1：Get 操作
```cpp
// db/table_cache.cc
Status TableCache::Get(...) {
  // 1. 尝试从 FileDescriptor 中获取已有的 TableReader
  TableReader* table_reader = file_meta.fd.table_reader;
  
  Cache::Handle* handle = nullptr;
  if (table_reader == nullptr) {
    // 2. 通过 FindTable 查找或创建 TableReader
    Status s = FindTable(
        options, file_options, internal_comparator, file_meta, 
        &handle, ...);  // 第一次：3-21 ms，后续：0.01 ms
    
    if (s.ok()) {
      table_reader = GetTableReaderFromHandle(handle);
    }
  }
  
  // 3. 使用 TableReader 读取数据
  if (s.ok()) {
    s = table_reader->Get(read_options, k, get_context, ...);
  }
  
  // 4. 释放句柄（不删除 TableReader，保留在缓存中）
  if (handle != nullptr) {
    ReleaseHandle(handle);  // 只是减少引用计数
  }
  
  return s;
}
```

### 示例 2：NewIterator 操作
```cpp
// db/table_cache.cc
InternalIterator* TableCache::NewIterator(...) {
  // 1. 先检查 FileDescriptor 中的 table_reader
  TableReader* table_reader = file_meta.fd.table_reader;
  
  Cache::Handle* handle = nullptr;
  if (table_reader == nullptr) {
    // 2. 通过 FindTable 获取（缓存命中率高）
    Status s = FindTable(..., &handle, ...);
    if (s.ok()) {
      table_reader = GetTableReaderFromHandle(handle);
    }
  }
  
  // 3. 创建迭代器
  InternalIterator* result = nullptr;
  if (s.ok()) {
    result = table_reader->NewIterator(
        read_options, prefix_extractor, arena, 
        skip_filters, caller, ...);  // 在 Arena 中创建
    
    // 4. 如果使用了缓存句柄，注册清理回调
    if (handle != nullptr) {
      result->RegisterCleanup(&UnrefEntry, cache_, handle);
      // 迭代器销毁时会自动 Release handle
    }
  }
  
  return result;
}
```

## 五、性能对比

### 对比实验：读取 1000 次同一个 SST 文件

#### 场景 A：没有 TableCache
```
每次读取都要：
├─ 打开文件：1000 次 × 2 ms = 2000 ms
├─ 读 Footer：1000 次 × 0.5 ms = 500 ms
├─ 读 Index：1000 次 × 5 ms = 5000 ms
├─ 读 Filter：1000 次 × 2 ms = 2000 ms
├─ 创建对象：1000 次 × 0.2 ms = 200 ms
└─ 总耗时：9700 ms = 9.7 秒
```

#### 场景 B：使用 TableCache
```
第 1 次读取：
├─ 缓存查找：0.01 ms (miss)
├─ 打开文件：2 ms
├─ 读 Footer：0.5 ms
├─ 读 Index：5 ms
├─ 读 Filter：2 ms
├─ 创建对象：0.2 ms
├─ 插入缓存：0.05 ms
└─ 小计：9.76 ms

第 2-1000 次读取：
├─ 缓存查找：999 次 × 0.01 ms = 9.99 ms (hit!)
└─ 小计：9.99 ms

总耗时：9.76 + 9.99 = 19.75 ms

性能提升：9700 / 19.75 = 491 倍！
```

### 真实场景：多文件读取

假设数据库有 100 个 SST 文件，查询 10000 次，均匀分布：

#### 没有 TableCache：
```
10000 次查询 × 10 ms = 100,000 ms = 100 秒
```

#### 使用 TableCache（缓存大小可容纳 100 个 TableReader）：
```
第一轮：100 个文件各创建一次 = 100 × 10 ms = 1000 ms
后续 9900 次：全部缓存命中 = 9900 × 0.01 ms = 99 ms
总耗时：1000 + 99 = 1099 ms ≈ 1.1 秒

性能提升：100 / 1.1 = 90 倍！
```

## 六、TableCache 的关键优化

### 1. 避免重复打开文件
```cpp
// 文件打开是系统调用，非常慢
// 没有缓存：每次查询都要 open() 系统调用
// 有缓存：文件保持打开状态，复用文件句柄
```

### 2. 避免重复读取元数据
```cpp
// SST 文件结构：
// [Data Blocks] [Meta Blocks] [Index Block] [Filter Block] [Footer]
//                                               ↑            ↑
//                           每次都要读这两个（几百 KB）

// 没有缓存：每次查询都要读 Index + Filter
// 有缓存：Index + Filter 常驻内存
```

### 3. 减少内存分配开销
```cpp
// TableReader 对象很大（包含很多成员）
struct BlockBasedTable {
  std::unique_ptr<RandomAccessFileReader> file;
  Footer footer;
  std::unique_ptr<IndexReader> index_reader;      // 几百 KB
  std::unique_ptr<FilterBlockReader> filter;      // 几十 KB
  std::shared_ptr<const TableProperties> props;
  Cache::Handle* rep_;
  // ... 还有很多成员
};

// 没有缓存：每次查询都要 new/delete
// 有缓存：对象复用，减少内存分配/释放
```

### 4. 线程安全的并发访问
```cpp
// TableCache 内部使用分片的 LRU Cache（ShardedLRUCache）
// 多个线程可以并发访问不同的文件
// 访问同一文件时，通过 loader_mutex_ 避免重复创建
```

## 七、内存管理

### 缓存容量控制
```cpp
// 通过 Options 配置
Options options;
options.max_open_files = 1000;  // 最多缓存 1000 个 TableReader

// 或者设置 table_cache 大小
options.table_cache_numshardbits = 6;  // 64 个分片
```

### LRU 淘汰策略
```cpp
// 当缓存满时，使用 LRU 算法淘汰最久未使用的 TableReader
// 1. 新插入的 TableReader 会淘汰旧的
// 2. 被淘汰的 TableReader 会被删除（关闭文件）
// 3. 下次访问时重新创建
```

### 引用计数
```cpp
// Cache::Handle 使用引用计数
Cache::Handle* handle = cache_->Lookup(key);  // refs = 1
// ... 使用 TableReader
cache_->Release(handle);  // refs = 0，可以被淘汰

// 如果 refs > 0，即使 LRU 算法选中也不会删除
// 确保正在使用的 TableReader 不会被意外删除
```

## 八、总结

### TableCache 的核心价值

| 方面 | 没有 TableCache | 有 TableCache | 提升倍数 |
|------|----------------|--------------|---------|
| 文件打开 | 每次查询都打开 | 保持打开状态 | 100-1000x |
| 元数据读取 | 每次读 Index/Filter | 常驻内存 | 100-500x |
| 对象创建 | 每次 new/delete | 对象复用 | 50-100x |
| 总体性能 | 10 ms/查询 | 0.01 ms/查询 | 100-1000x |

### 适用场景
1. **热点数据**：频繁访问的 SST 文件，缓存命中率极高
2. **范围扫描**：同一个文件的多次迭代器创建
3. **Point Lookup**：单点查询多个 key 时复用 TableReader
4. **并发查询**：多线程并发读取不同文件

### 关键设计
1. **缓存 TableReader 而非数据**：TableReader 包含索引和过滤器，查询效率高
2. **分片锁**：减少锁竞争，提高并发性能
3. **引用计数**：防止正在使用的对象被淘汰
4. **懒加载**：第一次访问时才创建，避免启动时开销

### 结论
**TableCache 是 RocksDB 性能优化的关键组件之一**，通过缓存 TableReader 对象，避免了大量重复的文件 I/O 和对象创建开销，在实际应用中可以带来 **100-1000 倍的性能提升**。没有 TableCache，RocksDB 的读性能将极其低下，基本无法用于生产环境。
