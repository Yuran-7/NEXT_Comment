//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <algorithm>
#include <cassert>
#include <limits>

#include "rocksdb/slice.h"
#include "util/autovector.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                          std::string& output) {
  if (columns.size() >
      static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::InvalidArgument("Too many wide columns");
  }

  PutVarint32(&output, kCurrentVersion);

  PutVarint32(&output, static_cast<uint32_t>(columns.size()));

  for (size_t i = 0; i < columns.size(); ++i) {
    const WideColumn& column = columns[i];

    const Slice& name = column.name();
    if (name.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column name too long");
    }
    if (i > 0 && columns[i - 1].name().compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order"); // 宽列序列化格式要求“列名”必须是按字典序排序的，严格递增
    }

    const Slice& value = column.value();
    if (value.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column value too long");
    }

    PutLengthPrefixedSlice(&output, name);
    PutVarint32(&output, static_cast<uint32_t>(value.size()));
  }

  for (const auto& column : columns) {
    const Slice& value = column.value();

    output.append(value.data(), value.size());
  }

  return Status::OK();
}

Status WideColumnSerialization::Deserialize(Slice& input,
                                            WideColumns& columns) {
  assert(columns.empty());

  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) { // util/coding.h
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kCurrentVersion) {
    return Status::NotSupported("Unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  columns.reserve(num_columns);

  autovector<uint32_t, 16> column_value_sizes;  // 最多 16 个元素的空间作为“内联容量”(inline capacity)，只有当元素数量超过 16 时才会退化成使用堆分配（类似 std::vector 的动态扩容）
  column_value_sizes.reserve(num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    if (!columns.empty() && columns.back().name().compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }

    columns.emplace_back(name, Slice());  // 此时只得到了列名，列值还没有

    uint32_t value_size = 0;
    if (!GetVarint32(&input, &value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    column_value_sizes.emplace_back(value_size);  // 把value_size先存起来，后面再用
  }

  const Slice data(input);
  size_t pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    const uint32_t value_size = column_value_sizes[i];

    if (pos + value_size > data.size()) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    columns[i].value() = Slice(data.data() + pos, value_size);

    pos += value_size;
  }

  return Status::OK();
}

WideColumns::const_iterator WideColumnSerialization::Find(
    const WideColumns& columns, const Slice& column_name) {
  const auto it =
      std::lower_bound(columns.cbegin(), columns.cend(), column_name,
                       [](const WideColumn& lhs, const Slice& rhs) {
                         return lhs.name().compare(rhs) < 0;
                       });  // 在有序的 columns 数组中，用二分查找找到第一个列名 >= column_name 的位置

  if (it == columns.cend() || it->name() != column_name) {
    return columns.cend();
  }

  return it;
}

Status WideColumnSerialization::GetValueOfDefaultColumn(Slice& input,
                                                        Slice& value) {
  WideColumns columns;

  const Status s = Deserialize(input, columns); // 经历了完整的反序列化过程
  if (!s.ok()) {
    return s;
  }

  if (columns.empty() || columns[0].name() != kDefaultWideColumnName) {
    value.clear();
    return Status::OK();
  }

  value = columns[0].value();

  return Status::OK();
}

Status WideColumnSerialization::GetValuesByColumnNames(
    Slice& input, const std::vector<Slice>& column_names,
    std::vector<Slice>& values) {
  
  // 初始化输出：所有值先设为空
  values.clear();
  values.resize(column_names.size());
  
  if (column_names.empty()) {
    return Status::OK();
  }

  // 1. 解析版本号
  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }
  if (version > kCurrentVersion) {
    return Status::NotSupported("Unsupported wide column version");
  }

  // 2. 解析列数
  uint32_t num_columns = 0;
  if (!GetVarint32(&input, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }
  if (!num_columns) {
    return Status::OK();  // 没有列，所有值保持为空
  }

  // 3. 解析索引区（列名+值大小），同时记录需要的列
  struct ColumnInfo {
    size_t request_index;  // 在用户请求的 column_names 中的索引
    uint32_t value_size;   // 值的大小
    size_t value_offset;   // 值在值区的偏移量
  };
  
  std::vector<ColumnInfo> matched_columns;  // 存储匹配上的列信息
  size_t value_offset = 0;  // 当前值在值区的偏移量
  size_t request_idx = 0;   // 当前处理到 column_names 的哪个位置
  
  for (uint32_t i = 0; i < num_columns; ++i) {
    // 解析列名
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    // 解析值大小
    uint32_t value_size = 0;
    if (!GetVarint32(&input, &value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    // 双指针法：检查当前列名是否匹配用户请求
    // 由于两边都是排序的，可以高效匹配
    while (request_idx < column_names.size() && 
           column_names[request_idx].compare(name) < 0) {
      // 用户请求的列名小于当前列名，说明请求的列不存在，跳过
      ++request_idx;
    }
    
    if (request_idx < column_names.size() && 
        column_names[request_idx].compare(name) == 0) {
      // 匹配上了！记录这个列的信息
      matched_columns.push_back({request_idx, value_size, value_offset});
      ++request_idx;
    }

    // 更新值区偏移量
    value_offset += value_size;
  }

  // 4. 现在 input 指向值区的开始位置
  const char* value_data = input.data();
  const size_t total_value_size = input.size();

  // 5. 根据记录的信息提取值
  for (const auto& info : matched_columns) {
    if (info.value_offset + info.value_size > total_value_size) {
      return Status::Corruption("Error decoding wide column value payload");
    }
    
    // 直接构造指向原始数据的 Slice，零拷贝
    values[info.request_index] = 
        Slice(value_data + info.value_offset, info.value_size);
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
