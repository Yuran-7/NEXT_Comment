# Version::storage_info_ 详解

## 一、storage_info_ 是什么？

### 1. 定义
```cpp
// db/version_set.h - Version 类的成员变量
class Version {
 private:
  VersionStorageInfo storage_info_;  // 存储该版本的所有元数据
  
 public:
  VersionStorageInfo* storage_info() { return &storage_info_; }
  const VersionStorageInfo* storage_info() const { return &storage_info_; }
};
```

### 2. 类型：VersionStorageInfo
```cpp
// VersionStorageInfo 类存储了与每个 Version 关联的存储信息
// 包括 LSM 树的层级数、每层的文件信息、标记为 compaction 的文件、blob 文件等
class VersionStorageInfo {
 public:
  VersionStorageInfo(
      const InternalKeyComparator* internal_comparator,
      const Comparator* user_comparator, 
      int num_levels,
      CompactionStyle compaction_style,
      VersionStorageInfo* src_vstorage,
      bool _force_consistency_checks);
  
  // ... 大量的成员函数和成员变量
};
```

## 二、storage_info_ 里面存了什么？

### 核心数据结构（私有成员变量）

```cpp
class VersionStorageInfo {
 private:
  // ========== 1. 基础配置 ==========
  const InternalKeyComparator* internal_comparator_;  // 内部 key 比较器
  const Comparator* user_comparator_;                 // 用户 key 比较器
  int num_levels_;                                    // LSM 树的层数（通常是 7）
  int num_non_empty_levels_;                          // 非空层的数量
  CompactionStyle compaction_style_;                  // Compaction 风格
  
  // ========== 2. 文件列表（最核心！）==========
  std::vector<FileMetaData*>* files_;  // 每层的 SST 文件列表
  // files_[0] = L0 层的所有 SST 文件
  // files_[1] = L1 层的所有 SST 文件
  // ...
  // files_[6] = L6 层的所有 SST 文件
  
  // ========== 3. 文件快速查找结构 ==========
  FileLocations file_locations_;  // 文件编号 -> (层级, 位置) 的映射
  // 例如：file_locations_[12345] = FileLocation(level=2, position=10)
  //       表示文件 12345 在 L2 层的第 10 个位置
  
  autovector<LevelFilesBrief> level_files_brief_;  // 每层文件的简要信息
  // 用于快速访问文件的 key range、文件大小等元数据
  
  FileIndexer file_indexer_;  // 文件索引器，用于快速定位重叠文件
  
  // ========== 4. Blob 文件（Wide Column 相关）==========
  BlobFiles blob_files_;  // Blob 文件列表（存储大 value）
  
  // ========== 5. Compaction 相关 ==========
  std::vector<std::vector<int>> files_by_compaction_pri_;  
  // 按 compaction 优先级排序的文件索引
  // files_by_compaction_pri_[level][i] 是 files_[level] 中第 i 大的文件索引
  
  std::vector<int> next_file_to_compact_by_size_;  
  // 每层下一个要 compact 的文件索引
  
  autovector<std::pair<int, FileMetaData*>> files_marked_for_compaction_;
  // 被标记为需要 compaction 的文件列表（手动标记或文件损坏）
  
  autovector<std::pair<int, FileMetaData*>> expired_ttl_files_;
  // TTL 过期的文件列表
  
  autovector<std::pair<int, FileMetaData*>> files_marked_for_periodic_compaction_;
  // 定期 compaction 的文件列表
  
  autovector<std::pair<int, FileMetaData*>> bottommost_files_;
  // 最底层的文件（没有更低层级的 key 可以覆盖它们）
  
  autovector<std::pair<int, FileMetaData*>> bottommost_files_marked_for_compaction_;
  // 最底层中需要 compaction 的文件
  
  // ========== 6. Compaction Score ==========
  std::vector<double> compaction_score_;   // 每层的 compaction 分数
  std::vector<int> compaction_level_;      // 按分数排序的层级列表
  // 例如：compaction_score_ = [0.5, 1.2, 0.8, 0.3, ...]
  //       compaction_level_  = [1, 2, 0, 3, ...]  # 按分数从高到低排序
  
  // ========== 7. Level 配置 ==========
  int base_level_;                          // L0 数据应该 compact 到的层级
  double level_multiplier_;                 // 层级大小倍数
  std::vector<uint64_t> level_max_bytes_;   // 每层的最大字节数
  
  // ========== 8. 统计信息 ==========
  uint64_t accumulated_file_size_;          // 累积的文件大小
  uint64_t accumulated_raw_key_size_;       // 累积的原始 key 大小
  uint64_t accumulated_raw_value_size_;     // 累积的原始 value 大小
  uint64_t accumulated_num_non_deletions_;  // 累积的非删除条目数
  uint64_t accumulated_num_deletions_;      // 累积的删除条目数
  uint64_t current_num_non_deletions_;      // 当前非删除条目数
  uint64_t current_num_deletions_;          // 当前删除条目数
  uint64_t current_num_samples_;            // 当前采样数
  uint64_t estimated_compaction_needed_bytes_;  // 估计需要 compact 的字节数
  
  // ========== 9. 其他 ==========
  bool level0_non_overlapping_;             // L0 文件是否不重叠
  bool finalized_;                          // 是否已完成初始化
  std::vector<InternalKey> compact_cursor_; // Round-robin compaction 的游标
  Arena arena_;                             // 用于分配 level_files_brief_ 的内存
  SequenceNumber bottommost_files_mark_threshold_;  // 最底层文件标记阈值
  SequenceNumber oldest_snapshot_seqnum_;   // 最老的 snapshot 序列号
};
```

## 三、storage_info_ 什么时候被填充？

### 整体流程图
```
数据库启动/Compaction 完成
    ↓
创建新 Version
    ↓
Version 构造函数初始化 storage_info_（空的）
    ↓
VersionBuilder::SaveTo(storage_info_)  ←← 关键步骤！
    ↓
storage_info_ 被填充文件列表
    ↓
storage_info_.PrepareForVersionAppend()
    ↓
storage_info_.SetFinalized()
    ↓
storage_info_ 完全就绪，可以使用
```

### 详细填充过程

#### 阶段 1：Version 构造函数（初始化）
```cpp
// db/version_set.cc - 行 2203-2247
Version::Version(ColumnFamilyData* column_family_data, VersionSet* vset,
                 const FileOptions& file_opt,
                 const MutableCFOptions mutable_cf_options,
                 const std::shared_ptr<IOTracer>& io_tracer,
                 uint64_t version_number)
    : env_(vset->env_),
      clock_(vset->clock_),
      cfd_(column_family_data),
      // ... 其他成员初始化
      storage_info_(  // ← 这里初始化 storage_info_
          (cfd_ == nullptr) ? nullptr : &cfd_->internal_comparator(),
          (cfd_ == nullptr) ? nullptr : cfd_->user_comparator(),
          cfd_ == nullptr ? 0 : cfd_->NumberLevels(),  // 层数，通常是 7
          cfd_ == nullptr ? kCompactionStyleLevel
                          : cfd_->ioptions()->compaction_style,
          (cfd_ == nullptr || cfd_->current() == nullptr)
              ? nullptr
              : cfd_->current()->storage_info(),  // 参考前一个版本
          cfd_ == nullptr ? false : cfd_->ioptions()->force_consistency_checks),
      // ... 其他成员初始化
{
  // 此时 storage_info_ 已经被构造，但 files_ 等成员都是空的
  // 只有基础配置（比较器、层数等）被设置
}
```

**此时 storage_info_ 的状态：**
```
storage_info_ {
  internal_comparator_ = &cfd->internal_comparator()  ✓ 已设置
  user_comparator_ = cfd->user_comparator()           ✓ 已设置
  num_levels_ = 7                                     ✓ 已设置
  files_ = new std::vector<FileMetaData*>[7]         ✓ 已分配，但为空
  files_[0] = []  // 空
  files_[1] = []  // 空
  ...
  files_[6] = []  // 空
  finalized_ = false                                  ✓ 未完成
}
```

#### 阶段 2：VersionBuilder::SaveTo（填充文件）
```cpp
// db/version_builder.cc
void VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  // 遍历所有层级
  for (int level = 0; level < num_levels_; level++) {
    // 获取该层的文件元数据
    const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;
    
    // 排序文件
    // L0: 按 largest_seqno 降序（新文件在前）
    // L1+: 按 smallest_key 升序（保证有序）
    std::sort(levels_[level].added_files->begin(),
              levels_[level].added_files->end(), cmp);
    
    // 逐个添加文件到 vstorage（即 storage_info_）
    for (auto* f : *levels_[level].added_files) {
      vstorage->AddFile(level, f);  // ← 关键：填充 files_[level]
    }
  }
  
  // 添加 blob 文件
  for (const auto& blob_file : blob_files_) {
    vstorage->AddBlobFile(blob_file);
  }
}
```

**VersionStorageInfo::AddFile 的实现：**
```cpp
// db/version_set.cc
void VersionStorageInfo::AddFile(int level, FileMetaData* f) {
  assert(f->refs > 0);
  
  // 添加到文件列表
  files_[level].push_back(f);  // ← 核心：填充 files_ 数组
  
  // 如果是 L0 或者是第一个文件，更新边界
  if (level == 0 || files_[level].size() == 1) {
    // 更新统计信息等
  }
}
```

**此时 storage_info_ 的状态：**
```
storage_info_ {
  files_[0] = [file_001.sst, file_002.sst, file_003.sst]  ✓ L0 文件
  files_[1] = [file_010.sst, file_011.sst, ...]           ✓ L1 文件
  files_[2] = [file_020.sst, file_021.sst, ...]           ✓ L2 文件
  ...
  files_[6] = [file_060.sst, file_061.sst, ...]           ✓ L6 文件
  
  blob_files_ = [blob_001, blob_002, ...]                 ✓ Blob 文件
  
  finalized_ = false  // 还未完成
}
```

#### 阶段 3：PrepareForVersionAppend（准备辅助数据结构）
```cpp
// db/version_set.cc
void VersionStorageInfo::PrepareForVersionAppend(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options) {
  
  // 1. 计算补偿大小（考虑 compaction 开销）
  ComputeCompensatedSizes();
  
  // 2. 更新非空层数
  UpdateNumNonEmptyLevels();
  
  // 3. 生成文件索引器（用于快速查找重叠文件）
  GenerateFileIndexer();  // 填充 file_indexer_
  
  // 4. 生成每层文件简要信息（用于快速访问）
  GenerateLevelFilesBrief();  // 填充 level_files_brief_
  
  // 5. 生成文件位置索引（文件编号 -> 层级+位置）
  GenerateFileLocationIndex();  // 填充 file_locations_
  
  // 6. 生成最底层文件列表
  GenerateBottommostFiles();  // 填充 bottommost_files_
  
  // 7. 如果是 L0，检查文件是否不重叠
  GenerateLevel0NonOverlapping();  // 设置 level0_non_overlapping_
  
  // 8. 更新统计信息
  UpdateAccumulatedStats();
  
  // 9. 计算每层的最大字节数
  CalculateBaseBytes(immutable_options, mutable_cf_options);
  
  // 10. 按 compaction 优先级更新文件列表
  UpdateFilesByCompactionPri(immutable_options, mutable_cf_options);
}
```

**此时 storage_info_ 的状态：**
```
storage_info_ {
  // 原有的文件列表
  files_[0-6] = [...]  ✓
  
  // 新增的辅助结构
  file_locations_ = {
    12345 -> FileLocation(level=2, position=10),
    12346 -> FileLocation(level=2, position=11),
    ...
  }  ✓ 文件快速查找
  
  level_files_brief_[0-6] = [
    {num_files=3, files=[{smallest, largest, fd}, ...]},
    ...
  ]  ✓ 每层文件简要信息
  
  file_indexer_ = {...}  ✓ 文件索引器
  
  bottommost_files_ = [(6, file_060.sst), ...]  ✓ 最底层文件
  
  num_non_empty_levels_ = 5  ✓ 非空层数
  level0_non_overlapping_ = false  ✓ L0 文件重叠状态
  
  accumulated_file_size_ = 1234567890  ✓ 统计信息
  accumulated_raw_key_size_ = 123456789
  ...
  
  finalized_ = false  // 还未完成
}
```

#### 阶段 4：SetFinalized（完成初始化）
```cpp
// db/version_set.cc
void VersionStorageInfo::SetFinalized() {
  // 1. 计算 compaction 分数
  ComputeCompactionScore(immutable_options, mutable_cf_options);
  // 填充 compaction_score_ 和 compaction_level_
  
  // 2. 计算需要 compaction 的文件
  ComputeFilesMarkedForCompaction();
  // 填充 files_marked_for_compaction_
  
  // 3. 计算 TTL 过期文件
  ComputeExpiredTtlFiles(ioptions, ttl);
  // 填充 expired_ttl_files_
  
  // 4. 计算定期 compaction 文件
  ComputeFilesMarkedForPeriodicCompaction(ioptions, periodic_compaction_seconds);
  // 填充 files_marked_for_periodic_compaction_
  
  // 5. 计算底层需要 compaction 的文件
  ComputeBottommostFilesMarkedForCompaction();
  // 填充 bottommost_files_marked_for_compaction_
  
  // 6. 估计需要 compaction 的字节数
  EstimateCompactionBytesNeeded(mutable_cf_options);
  // 设置 estimated_compaction_needed_bytes_
  
  finalized_ = true;  // ← 标记为已完成
}
```

**最终 storage_info_ 的完整状态：**
```
storage_info_ {
  // ========== 文件列表 ==========
  files_[0] = [file_001, file_002, file_003]  ✓
  files_[1] = [file_010, file_011, ...]       ✓
  ...
  files_[6] = [file_060, file_061, ...]       ✓
  
  // ========== 快速查找结构 ==========
  file_locations_ = {12345 -> (2,10), ...}    ✓
  level_files_brief_ = [...]                  ✓
  file_indexer_ = {...}                       ✓
  
  // ========== Compaction 相关 ==========
  compaction_score_ = [0.5, 1.2, 0.8, ...]    ✓ L1 分数最高
  compaction_level_ = [1, 2, 0, ...]          ✓ 按分数排序
  
  files_marked_for_compaction_ = [...]        ✓
  expired_ttl_files_ = [...]                  ✓
  files_marked_for_periodic_compaction_ = [...] ✓
  bottommost_files_ = [...]                   ✓
  bottommost_files_marked_for_compaction_ = [...] ✓
  
  // ========== 配置 ==========
  base_level_ = 1                             ✓
  level_max_bytes_ = [0, 1GB, 10GB, ...]      ✓
  
  // ========== 统计信息 ==========
  accumulated_file_size_ = 1234567890         ✓
  estimated_compaction_needed_bytes_ = 100MB  ✓
  ...
  
  // ========== 状态标记 ==========
  finalized_ = true  ✓✓✓ 完全就绪！
}
```

## 四、填充时机总结

### 1. 数据库启动时（Recover）
```cpp
// db/version_set.cc - VersionSet::Recover()
Status VersionSet::Recover(...) {
  // 1. 读取 MANIFEST 文件
  // 2. 重放所有 VersionEdit
  // 3. 构建 VersionBuilder
  
  VersionBuilder builder(...);
  for (auto& edit : edits) {
    builder.Apply(edit);  // 应用每个编辑操作
  }
  
  // 4. 创建新 Version
  Version* v = new Version(...);
  
  // 5. 保存到 storage_info_  ← 填充时机 1
  builder.SaveTo(v->storage_info());
  
  // 6. 准备并完成
  v->storage_info()->PrepareForVersionAppend(...);
  v->storage_info()->SetFinalized();
  
  // 7. 安装到 VersionSet
  vset->AppendVersion(cfd, v);
}
```

### 2. Compaction 完成时（LogAndApply）
```cpp
// db/version_set.cc - VersionSet::LogAndApply()
Status VersionSet::LogAndApply(...) {
  // 1. 创建新 Version
  Version* v = new Version(...);
  
  // 2. 构建 VersionBuilder（基于当前版本 + 新的编辑）
  VersionBuilder builder(current_version->storage_info());
  builder.Apply(edit);  // 应用 compaction 的结果
  
  // 3. 保存到新 Version  ← 填充时机 2
  builder.SaveTo(v->storage_info());
  
  // 4. 准备并完成
  v->storage_info()->PrepareForVersionAppend(...);
  v->storage_info()->SetFinalized();
  
  // 5. 写入 MANIFEST
  WriteToManifest(edit);
  
  // 6. 安装新 Version
  vset->AppendVersion(cfd, v);
}
```

### 3. Flush 完成时
```cpp
// 与 Compaction 类似，也是通过 LogAndApply
// Flush 生成新的 L0 文件，通过 VersionEdit 记录
// 然后创建新 Version，填充 storage_info_
```

## 五、使用示例

### 示例 1：获取某层的文件列表
```cpp
Version* current = cfd->current();
const std::vector<FileMetaData*>& level2_files = 
    current->storage_info()->LevelFiles(2);

for (FileMetaData* f : level2_files) {
  std::cout << "File: " << f->fd.GetNumber() 
            << ", Size: " << f->fd.GetFileSize()
            << ", Range: [" << f->smallest << ", " << f->largest << "]"
            << std::endl;
}
```

### 示例 2：查找文件位置
```cpp
uint64_t file_number = 12345;
auto location = current->storage_info()->GetFileLocation(file_number);

if (location.IsValid()) {
  int level = location.GetLevel();
  size_t pos = location.GetPosition();
  std::cout << "File " << file_number 
            << " is at L" << level 
            << " position " << pos << std::endl;
}
```

### 示例 3：获取 Compaction 候选
```cpp
// 获取最高分数的层级
int level = current->storage_info()->CompactionScoreLevel(0);
double score = current->storage_info()->CompactionScore(0);

std::cout << "L" << level << " has the highest score: " << score << std::endl;

if (score >= 1.0) {
  // 需要 compaction
  const std::vector<FileMetaData*>& files = 
      current->storage_info()->LevelFiles(level);
  // 选择文件进行 compaction
}
```

## 六、关键点总结

1. **storage_info_ 是 Version 的核心成员**，存储了该版本的所有 LSM 树元数据

2. **填充时机**：
   - 数据库启动时（Recover）
   - Compaction 完成时（LogAndApply）
   - Flush 完成时（LogAndApply）

3. **填充过程**：
   ```
   Version 构造
     → VersionBuilder::SaveTo  （填充文件列表）
     → PrepareForVersionAppend （构建辅助结构）
     → SetFinalized           （计算 compaction 分数）
     → 完全就绪
   ```

4. **存储内容**：
   - 每层的 SST 文件列表（files_）
   - 文件快速查找结构（file_locations_, file_indexer_）
   - Compaction 相关信息（分数、候选文件）
   - 统计信息（文件大小、key 数量等）

5. **不可变性**：一旦 `finalized_ = true`，storage_info_ 就不再改变，成为该 Version 的不可变快照
