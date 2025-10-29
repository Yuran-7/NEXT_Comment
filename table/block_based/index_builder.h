//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <assert.h>
#include <cinttypes>

#include <list>
#include <string>
#include <unordered_map>
#include <iostream>

#include "rocksdb/comparator.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "util/rtree.h"

namespace ROCKSDB_NAMESPACE {
// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  static IndexBuilder* CreateIndexBuilder(
      BlockBasedTableOptions::IndexType index_type,
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const InternalKeySliceTransform* int_key_slice_transform,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents;
    std::unordered_map<std::string, Slice> meta_blocks;
  };
  explicit IndexBuilder(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& /*key*/) {}

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  inline Status Finish(IndexBlocks* index_blocks) {
    // Throw away the changes to last_partition_block_handle. It has no effect
    // on the first call to Finish anyway.
    BlockHandle last_partition_block_handle;
    return Finish(index_blocks, last_partition_block_handle); // 在283行
  }

  // This override of Finish can be utilized to build the 2nd level index in
  // PartitionIndexBuilder.
  //
  // index_blocks will be filled with the resulting index data. If the return
  // value is Status::InComplete() then it means that the index is partitioned
  // and the callee should keep calling Finish until Status::OK() is returned.
  // In that case, last_partition_block_handle is pointer to the block written
  // with the result of the last call to Finish. This can be utilized to build
  // the second level index pointing to each block of partitioned indexes. The
  // last call to Finish() that returns Status::OK() populates index_blocks with
  // the 2nd level index content.
  virtual Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle) = 0;

  // Get the size for index block. Must be called after ::Finish.
  virtual size_t IndexSize() const = 0;

  virtual bool seperator_is_key_plus_seq() { return true; }

 protected:
  const InternalKeyComparator* comparator_;
  // Set after ::Finish is called
  size_t index_size_ = 0;
};

class SecondaryIndexBuilder {
 public:
  static SecondaryIndexBuilder* CreateSecIndexBuilder(
      BlockBasedTableOptions::SecondaryIndexType sec_index_type,
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const InternalKeySliceTransform* int_key_slice_transform,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt,
      const std::vector<Slice>& sec_index_columns);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents; // 单个二级索引块的全部内容，508B
    std::unordered_map<std::string, Slice> meta_blocks;
  };
  explicit SecondaryIndexBuilder(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}
  // explicit SecondaryIndexBuilder(const Comparator* comparator)
  //     : comparator_(comparator) {}

  virtual ~SecondaryIndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& /*key*/) {}

  // Check if this is an embedded secondary index (default: true)
  // Override in subclasses that support non-embedded mode
  virtual bool IsEmbedded() const { return true; }

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  inline Status Finish(IndexBlocks* index_blocks) {
    // Throw away the changes to last_partition_block_handle. It has no effect
    // on the first call to Finish anyway.
    BlockHandle last_partition_block_handle;
    return Finish(index_blocks, last_partition_block_handle);
  }

  // This override of Finish can be utilized to build the 2nd level index in
  // PartitionIndexBuilder.
  //
  // index_blocks will be filled with the resulting index data. If the return
  // value is Status::InComplete() then it means that the index is partitioned
  // and the callee should keep calling Finish until Status::OK() is returned.
  // In that case, last_partition_block_handle is pointer to the block written
  // with the result of the last call to Finish. This can be utilized to build
  // the second level index pointing to each block of partitioned indexes. The
  // last call to Finish() that returns Status::OK() populates index_blocks with
  // the 2nd level index content.
  virtual Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle) = 0;

  // Get the size for index block. Must be called after ::Finish.
  virtual size_t IndexSize() const = 0;

  virtual bool seperator_is_key_plus_seq() { return true; }

  virtual void get_Secondary_Entries(std::vector<std::pair<std::string, BlockHandle>>* sec_entries) {
    (void) sec_entries;
  }

 protected:
  const InternalKeyComparator* comparator_;
  // const Comparator* comparator_;
  // Set after ::Finish is called
  size_t index_size_ = 0;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(
      const InternalKeyComparator* comparator,
      const int index_block_restart_interval, const uint32_t format_version,
      const bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode,
      bool include_first_key)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval,
                             true /*use_delta_encoding*/,
                             use_value_delta_encoding),
        index_block_builder_without_seq_(index_block_restart_interval,
                                         true /*use_delta_encoding*/,
                                         use_value_delta_encoding),
        use_value_delta_encoding_(use_value_delta_encoding),
        include_first_key_(include_first_key),
        shortening_mode_(shortening_mode) {
    // Making the default true will disable the feature for old versions
    seperator_is_key_plus_seq_ = (format_version <= 2);
  }

  virtual void OnKeyAdded(const Slice& key) override {
    if (include_first_key_ && current_block_first_internal_key_.empty()) {
      current_block_first_internal_key_.assign(key.data(), key.size());
    }
  }

  virtual void AddIndexEntry(std::string* last_key_in_current_block,  // 向索引块添加新的索引条目
                             const Slice* first_key_in_next_block,  // 参数：当前块最后一个key、下一个块第一个key、数据块位置信息
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {  // 如果存在下一个数据块
      if (shortening_mode_ !=  // 如果启用了键缩短模式
          BlockBasedTableOptions::IndexShorteningMode::kNoShortening) {
        FindShortestInternalKeySeparator(*comparator_->user_comparator(),  // 查找最短的分隔键（例如："the quick brown fox" 和 "the who" -> "the r"）
                                         last_key_in_current_block,
                                         *first_key_in_next_block); // 生成的分隔键会直接修改传入的参数 last_key_in_current_block
      }
      if (!seperator_is_key_plus_seq_ &&  // 检查分隔符是否需要包含序列号
          comparator_->user_comparator()->Compare(  // 如果用户键相同
              ExtractUserKey(*last_key_in_current_block),
              ExtractUserKey(*first_key_in_next_block)) == 0) {
        seperator_is_key_plus_seq_ = true;  // 则分隔符必须包含序列号来区分
      }
    } else {  // 如果这是最后一个数据块
      if (shortening_mode_ == BlockBasedTableOptions::IndexShorteningMode::  // 如果允许缩短分隔符和后继键
                                  kShortenSeparatorsAndSuccessor) {
        FindShortInternalKeySuccessor(*comparator_->user_comparator(),  // 查找最短的后继键
                                      last_key_in_current_block);
      }
    }
    auto sep = Slice(*last_key_in_current_block);  // 获取处理后的分隔键

    assert(!include_first_key_ || !current_block_first_internal_key_.empty());  // 断言：如果需要包含首键，则首键不能为空
    IndexValue entry(block_handle, current_block_first_internal_key_);  // 创建索引值（包含块句柄和首键）
    std::string encoded_entry;  // 完整编码的条目
    std::string delta_encoded_entry;  // 增量编码的条目
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);  // 编码索引值（完整编码）
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {  // 如果启用增量编码且存在上一个句柄
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,  // 使用增量编码（只存储与上一个句柄的差异）
                     &last_encoded_handle_);
    } else {  // 否则（第一个块或禁用增量编码）
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;  // 更新上一个块句柄，供下次增量编码使用
    const Slice delta_encoded_entry_slice(delta_encoded_entry);  // 创建增量编码切片
    index_block_builder_.Add(sep, encoded_entry, &delta_encoded_entry_slice);  // 将索引条目添加到索引块构建器（包含序列号版本）
    if (!seperator_is_key_plus_seq_) {  // 如果分隔符不需要序列号
      index_block_builder_without_seq_.Add(ExtractUserKey(sep), encoded_entry,  // 同时添加到无序列号的索引块构建器（只用用户键）
                                           &delta_encoded_entry_slice);
    }

    current_block_first_internal_key_.clear();  // 清空当前块首键，准备处理下一个块
  }

  using IndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& /*last_partition_block_handle*/) override {
    if (seperator_is_key_plus_seq_) {
      index_blocks->index_block_contents = index_block_builder_.Finish();
    } else {
      index_blocks->index_block_contents =
          index_block_builder_without_seq_.Finish();  // table/block_based/block_builder.cc
    }
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
  }

  virtual size_t IndexSize() const override { return index_size_; }

  virtual bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  // Changes *key to a short string >= *key.
  //
  static void FindShortestInternalKeySeparator(const Comparator& comparator,
                                               std::string* start,
                                               const Slice& limit);

  static void FindShortInternalKeySuccessor(const Comparator& comparator,
                                            std::string* key);

  friend class PartitionedIndexBuilder;
  friend class RtreeIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
  BlockBuilder index_block_builder_without_seq_;
  const bool use_value_delta_encoding_;
  bool seperator_is_key_plus_seq_;
  const bool include_first_key_;
  BlockBasedTableOptions::IndexShorteningMode shortening_mode_;
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  std::string current_block_first_internal_key_;
};

// HashIndexBuilder contains a binary-searchable primary index and the
// metadata for secondary hash index construction.
// The metadata for hash index consists two parts:
//  - a metablock that compactly contains a sequence of prefixes. All prefixes
//    are stored consectively without any metadata (like, prefix sizes) being
//    stored, which is kept in the other metablock.
//  - a metablock contains the metadata of the prefixes, including prefix size,
//    restart index and number of block it spans. The format looks like:
//
// +-----------------+---------------------------+---------------------+
// <=prefix 1
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// <=prefix 2
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// |                                                                   |
// | ....                                                              |
// |                                                                   |
// +-----------------+---------------------------+---------------------+
// <=prefix n
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
//
// The reason of separating these two metablocks is to enable the efficiently
// reuse the first metablock during hash index construction without unnecessary
// data copy or small heap allocations for prefixes.
class HashIndexBuilder : public IndexBuilder {
 public:
  explicit HashIndexBuilder(
      const InternalKeyComparator* comparator,
      const SliceTransform* hash_key_extractor,
      int index_block_restart_interval, int format_version,
      bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode)
      : IndexBuilder(comparator),
        primary_index_builder_(comparator, index_block_restart_interval,
                               format_version, use_value_delta_encoding,
                               shortening_mode, /* include_first_key */ false),
        hash_key_extractor_(hash_key_extractor) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    ++current_restart_index_;
    primary_index_builder_.AddIndexEntry(last_key_in_current_block,
                                         first_key_in_next_block, block_handle);
  }

  virtual void OnKeyAdded(const Slice& key) override {
    auto key_prefix = hash_key_extractor_->Transform(key);
    bool is_first_entry = pending_block_num_ == 0;

    // Keys may share the prefix
    if (is_first_entry || pending_entry_prefix_ != key_prefix) {
      if (!is_first_entry) {
        FlushPendingPrefix();
      }

      // need a hard copy otherwise the underlying data changes all the time.
      // TODO(kailiu) std::to_string() is expensive. We may speed up can avoid
      // data copy.
      pending_entry_prefix_ = key_prefix.ToString();
      pending_block_num_ = 1;
      pending_entry_index_ = static_cast<uint32_t>(current_restart_index_);
    } else {
      // entry number increments when keys share the prefix reside in
      // different data blocks.
      auto last_restart_index = pending_entry_index_ + pending_block_num_ - 1;
      assert(last_restart_index <= current_restart_index_);
      if (last_restart_index != current_restart_index_) {
        ++pending_block_num_;
      }
    }
  }

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    if (pending_block_num_ != 0) {
      FlushPendingPrefix();
    }
    Status s = primary_index_builder_.Finish(index_blocks,
                                             last_partition_block_handle);
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesBlock.c_str(), prefix_block_});
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesMetadataBlock.c_str(), prefix_meta_block_});
    return s;
  }

  virtual size_t IndexSize() const override {
    return primary_index_builder_.IndexSize() + prefix_block_.size() +
           prefix_meta_block_.size();
  }

  virtual bool seperator_is_key_plus_seq() override {
    return primary_index_builder_.seperator_is_key_plus_seq();
  }

 private:
  void FlushPendingPrefix() {
    prefix_block_.append(pending_entry_prefix_.data(),
                         pending_entry_prefix_.size());
    PutVarint32Varint32Varint32(
        &prefix_meta_block_,
        static_cast<uint32_t>(pending_entry_prefix_.size()),
        pending_entry_index_, pending_block_num_);
  }

  ShortenedIndexBuilder primary_index_builder_;
  const SliceTransform* hash_key_extractor_;

  // stores a sequence of prefixes
  std::string prefix_block_;
  // stores the metadata of prefixes
  std::string prefix_meta_block_;

  // The following 3 variables keeps unflushed prefix and its metadata.
  // The details of block_num and entry_index can be found in
  // "block_hash_index.{h,cc}"
  uint32_t pending_block_num_ = 0;
  uint32_t pending_entry_index_ = 0;
  std::string pending_entry_prefix_;

  uint64_t current_restart_index_ = 0;
};

/**
 * IndexBuilder for two-level indexing. Internally it creates a new index for
 * each partition and Finish then in order when Finish is called on it
 * continiously until Status::OK() is returned.
 *
 * The format on the disk would be I I I I I I IP where I is block containing a
 * partition of indexes built using ShortenedIndexBuilder and IP is a block
 * containing a secondary index on the partitions, built using
 * ShortenedIndexBuilder.
 */
class PartitionedIndexBuilder : public IndexBuilder {
 public:
  static PartitionedIndexBuilder* CreateIndexBuilder(
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt); // 在index_builder.cc中定义

  explicit PartitionedIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt,
                                   const bool use_value_delta_encoding);

  virtual ~PartitionedIndexBuilder();

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }
  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }
  size_t NumPartitions() const;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

  // Called when an external entity (such as filter partition builder) request
  // cutting the next partition
  void RequestPartitionCut();

  virtual bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  bool get_use_value_delta_encoding() { return use_value_delta_encoding_; }

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;
  // Set after ::Finish is called
  size_t partition_cnt_ = 0;

  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<ShortenedIndexBuilder> value;
  };
  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  BlockBuilder index_block_builder_;              // top-level index builder
  BlockBuilder index_block_builder_without_seq_;  // same for user keys
  // the active partition index builder
  ShortenedIndexBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  const BlockBasedTableOptions& table_opt_;
  bool seperator_is_key_plus_seq_;
  bool use_value_delta_encoding_;
  // true if an external entity (such as filter partition builder) request
  // cutting the next partition
  bool partition_cut_requested_ = true;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
  BlockHandle last_encoded_handle_;
};

class RtreeIndexLevelBuilder : public IndexBuilder {
 public:
  explicit RtreeIndexLevelBuilder(
      const InternalKeyComparator* comparator,
      const int index_block_restart_interval, const uint32_t format_version,
      const bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode,
      bool include_first_key)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval,
                             true /*use_delta_encoding*/,
                             use_value_delta_encoding),
        use_value_delta_encoding_(use_value_delta_encoding),
        include_first_key_(include_first_key),
        shortening_mode_(shortening_mode) {
    (void)format_version;
    // std::cout << "created RtreeIndexLevelBuilder" << std::endl;
  }

  virtual void OnKeyAdded(const Slice& key) override {
    Slice key_temp = Slice(key);
    Mbr mbr = ReadKeyMbr(key_temp);
    expandMbr(enclosing_mbr_, mbr);
    // std::cout << "enclosing_mbr_ after expansion: " << enclosing_mbr_ << std::endl;
  }

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);
    (void)last_key_in_current_block;
    (void)first_key_in_next_block;

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(serializeMbr(enclosing_mbr_)), encoded_entry, &delta_encoded_entry_slice);

    enclosing_mbr_.clear();
  }

  virtual void AddIndexEntry(const BlockHandle& block_handle,
                             std::string enclosing_mbr_string) {
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(enclosing_mbr_string), encoded_entry, &delta_encoded_entry_slice);

    enclosing_mbr_.clear();
  }

  using IndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& /*last_partition_block_handle*/) override {
    
    index_blocks->index_block_contents = index_block_builder_.Finish();
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
  }

  virtual size_t IndexSize() const override { return index_size_; }

  virtual bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  friend class RtreeIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
  const bool use_value_delta_encoding_;
  bool seperator_is_key_plus_seq_;
  const bool include_first_key_;
  BlockBasedTableOptions::IndexShorteningMode shortening_mode_;
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  std::string current_block_first_internal_key_;
  Mbr enclosing_mbr_;
  void expandMbr(Mbr& to_expand, Mbr expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      if (expander.iid.min < to_expand.iid.min) {
        to_expand.iid.min = expander.iid.min;
      }
      if (expander.iid.max > to_expand.iid.max) {
        to_expand.iid.max = expander.iid.max;
      }
      if (expander.first.min < to_expand.first.min) {
        to_expand.first.min = expander.first.min;
      }
      if (expander.first.max > to_expand.first.max) {
        to_expand.first.max = expander.first.max;
      }
      if (expander.second.min < to_expand.second.min) {
        to_expand.second.min = expander.second.min;
      }
      if (expander.second.max > to_expand.second.max) {
        to_expand.second.max = expander.second.max;
      }
    }
  }
};


class RtreeIndexBuilder : public IndexBuilder { // 这个属于对key做的索引，和kBinarySearch、kHashSearch同一级别
 public:
  static RtreeIndexBuilder* CreateIndexBuilder(
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt);

  explicit RtreeIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt,
                                   const bool use_value_delta_encoding);

  virtual ~RtreeIndexBuilder();

  virtual void OnKeyAdded(const Slice& key) override;

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }
  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }
  size_t NumPartitions() const;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

  // Called when an external entity (such as filter partition builder) request
  // cutting the next partition
  void RequestPartitionCut();

  bool get_use_value_delta_encoding() { return use_value_delta_encoding_; }

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;
  // Set after ::Finish is called
  size_t partition_cnt_ = 0;

  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<RtreeIndexLevelBuilder> value;
    // Entry& operator=(Entry& other) {
    //     if (this != &other) {
    //         key = other.key;
    //         value.reset(other.value.release());
    //     }
    //     return *this;
    // }
  };
  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  std::list<Entry> next_level_entries_;  // list of partitioned indexes and their keys
  BlockBuilder index_block_builder_;              // top-level index builder
  // the active partition index builder
  RtreeIndexLevelBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  const BlockBasedTableOptions& table_opt_;
  bool use_value_delta_encoding_;
  // true if an external entity (such as filter partition builder) request
  // cutting the next partition
  bool partition_cut_requested_ = true;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
  BlockHandle last_encoded_handle_;
  Mbr sub_index_enclosing_mbr_;
  Mbr enclosing_mbr_;
  uint32_t rtree_level_;
  std::string rtree_height_str_;
  void expandMbr(Mbr& to_expand, Mbr expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      if (expander.iid.min < to_expand.iid.min) {
        to_expand.iid.min = expander.iid.min;
      }
      if (expander.iid.max > to_expand.iid.max) {
        to_expand.iid.max = expander.iid.max;
      }
      if (expander.first.min < to_expand.first.min) {
        to_expand.first.min = expander.first.min;
      }
      if (expander.first.max > to_expand.first.max) {
        to_expand.first.max = expander.first.max;
      }
      if (expander.second.min < to_expand.second.min) {
        to_expand.second.min = expander.second.min;
      }
      if (expander.second.max > to_expand.second.max) {
        to_expand.second.max = expander.second.max;
      }
    }
  }
};

class RtreeSecondaryIndexLevelBuilder : public SecondaryIndexBuilder {
 public:
  explicit RtreeSecondaryIndexLevelBuilder(
      const InternalKeyComparator* comparator,
      // const Comparator* comparator,
      const int index_block_restart_interval, const uint32_t format_version,
      const bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode,
      bool include_first_key)
      : SecondaryIndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval,
                             true /*use_delta_encoding*/,
                             use_value_delta_encoding),
        use_value_delta_encoding_(use_value_delta_encoding),
        include_first_key_(include_first_key),
        shortening_mode_(shortening_mode) {
    (void)format_version;
    // std::cout << "created RtreeSecondaryIndexLevelBuilder" << std::endl;
  }

  virtual void OnKeyAdded(const Slice& value) override {
    Slice val_temp = Slice(value);
    Mbr mbr = ReadValueMbr(val_temp);
    expandMbrExcludeIID(enclosing_mbr_, mbr);
  }

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);
    (void)last_key_in_current_block;
    (void)first_key_in_next_block;

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(serializeMbrExcludeIID(enclosing_mbr_)), encoded_entry, &delta_encoded_entry_slice);

    enclosing_mbr_.clear();
  }

  virtual void AddIndexEntry(const BlockHandle& block_handle,
                             std::string enclosing_mbr_string) {
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(enclosing_mbr_string), encoded_entry, &delta_encoded_entry_slice);

    enclosing_mbr_.clear();
  }

  using SecondaryIndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& /*last_partition_block_handle*/) override {
    
    index_blocks->index_block_contents = index_block_builder_.Finish();
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
  }

  virtual size_t IndexSize() const override { return index_size_; }

  virtual bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  friend class RtreeSecondaryIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
  const bool use_value_delta_encoding_;
  bool seperator_is_key_plus_seq_;
  const bool include_first_key_;
  BlockBasedTableOptions::IndexShorteningMode shortening_mode_;
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  std::string current_block_first_internal_key_;
  Mbr enclosing_mbr_;
  void expandMbrExcludeIID(Mbr& to_expand, Mbr expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      if (expander.first.min < to_expand.first.min) {
        to_expand.first.min = expander.first.min;
      }
      if (expander.first.max > to_expand.first.max) {
        to_expand.first.max = expander.first.max;
      }
      if (expander.second.min < to_expand.second.min) {
        to_expand.second.min = expander.second.min;
      }
      if (expander.second.max > to_expand.second.max) {
        to_expand.second.max = expander.second.max;
      }
    }
  }
};

class RtreeSecondaryIndexBuilder : public SecondaryIndexBuilder {
 public:
  static RtreeSecondaryIndexBuilder* CreateIndexBuilder(
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt);

  explicit RtreeSecondaryIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt,
                                   const bool use_value_delta_encoding);

  virtual ~RtreeSecondaryIndexBuilder();

  virtual void OnKeyAdded(const Slice& value) override;

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }

  void get_Secondary_Entries(std::vector<std::pair<std::string, BlockHandle>>* sec_entries) override;

  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }
  size_t NumPartitions() const;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

  // Called when an external entity (such as filter partition builder) request
  // cutting the next partition
  void RequestPartitionCut();

  bool get_use_value_delta_encoding() { return use_value_delta_encoding_; }

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;
  // Set after ::Finish is called
  size_t partition_cnt_ = 0;

  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<RtreeSecondaryIndexLevelBuilder> value;
    // Entry& operator=(Entry& other) {
    //     if (this != &other) {
    //         key = other.key;
    //         value.reset(other.value.release());
    //     }
    //     return *this;
    // }
    std::vector<std::string> sec_value;
  };
  struct DataBlockEntry {
    BlockHandle datablockhandle;
    std::string datablocklastkey;
    std::string subindexenclosingmbr;
  };
  void AddIdxEntry(DataBlockEntry datablkentry, bool last=false);
  DataBlockEntry last_index_entry_;

  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  std::list<Entry> next_level_entries_;  // list of partitioned indexes and their keys
  std::list<DataBlockEntry> data_block_entries_;
  std::vector<Mbr> tuple_mbrs_;  // vector of each tuple for secondary index builder

  std::vector<std::pair<std::string, BlockHandle>> sec_entries_;

  BlockBuilder index_block_builder_;              // top-level index builder
  // the active partition index builder
  RtreeSecondaryIndexLevelBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  bool firstlayer = true;
  const BlockBasedTableOptions& table_opt_;
  bool use_value_delta_encoding_;
  // true if an external entity (such as filter partition builder) request
  // cutting the next partition
  bool partition_cut_requested_ = true;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
  BlockHandle last_encoded_handle_;
  Mbr sub_index_enclosing_mbr_;
  Mbr enclosing_mbr_;
  Mbr sec_enclosing_mbr_;
  std::vector<std::string> sec_mbrs_;
  Mbr temp_sec_mbr_;
  uint32_t rtree_level_;
  std::string rtree_height_str_;
  void expandMbrExcludeIID(Mbr& to_expand, Mbr expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      if (expander.first.min < to_expand.first.min) {
        to_expand.first.min = expander.first.min;
      }
      if (expander.first.max > to_expand.first.max) {
        to_expand.first.max = expander.first.max;
      }
      if (expander.second.min < to_expand.second.min) {
        to_expand.second.min = expander.second.min;
      }
      if (expander.second.max > to_expand.second.max) {
        to_expand.second.max = expander.second.max;
      }
    }
  }
};

class OneDRtreeSecondaryIndexLevelBuilder : public SecondaryIndexBuilder {
 public:
  explicit OneDRtreeSecondaryIndexLevelBuilder(
      const InternalKeyComparator* comparator,
      // const Comparator* comparator,
      const int index_block_restart_interval, const uint32_t format_version,
      const bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode,
      bool include_first_key)
      : SecondaryIndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval,
                             true /*use_delta_encoding*/,
                             use_value_delta_encoding),
        use_value_delta_encoding_(use_value_delta_encoding),
        include_first_key_(include_first_key),
        shortening_mode_(shortening_mode) {
    (void)format_version;
  }

  virtual void OnKeyAdded(const Slice& value) override {
    Slice val_temp = Slice(value);
    double numerical_val = *reinterpret_cast<const double*>(val_temp.data());
    expandValrange(enclosing_valrange_, numerical_val);
  }

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {  // 这个是重写了父类的方法，但是没有被使用，和下面那个函数一模一样
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);
    (void)last_key_in_current_block;
    (void)first_key_in_next_block;

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(serializeValueRange(enclosing_valrange_)), encoded_entry, &delta_encoded_entry_slice);

    enclosing_valrange_.clear();
  }

  virtual void AddIndexEntry(const BlockHandle& block_handle,
                             std::string enclosing_valrange_string) { // AddIdxEntry函数内调用sub_index_builder_->AddIndexEntry
    // Encode the block handle and construct leaf node
    // std::string handle_encoding;
    // block_handle.EncodeTo(&handle_encoding);
    // LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    // leaf_nodes_.push_back(leaf_node);

    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {  // 不进入
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    index_block_builder_.Add(Slice(enclosing_valrange_string), encoded_entry, &delta_encoded_entry_slice);  // 往buffer_中写入条目，key是[min, max]，value是编码后的IndexValue

    enclosing_valrange_.clear();
  }

  using SecondaryIndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& /*last_partition_block_handle*/) override {
    
    index_blocks->index_block_contents = index_block_builder_.Finish();
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
  }

  virtual size_t IndexSize() const override { return index_size_; }

  virtual bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  friend class OneDRtreeSecondaryIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
  const bool use_value_delta_encoding_;
  bool seperator_is_key_plus_seq_;
  const bool include_first_key_;
  BlockBasedTableOptions::IndexShorteningMode shortening_mode_;
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  std::string current_block_first_internal_key_;
  ValueRange enclosing_valrange_;
  void expandValrange(ValueRange& to_expand, double expander) {
    if (to_expand.empty()) {
      to_expand.set_range(expander, expander);
    } else {
      if (expander < to_expand.range.min) {
        to_expand.range.min = expander;
      }
      if (expander > to_expand.range.max) {
        to_expand.range.max = expander;
      }
    }
  }
};

class OneDRtreeSecondaryIndexBuilder : public SecondaryIndexBuilder {
 public:
  static OneDRtreeSecondaryIndexBuilder* CreateIndexBuilder(
      const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt,
      const std::vector<Slice>& sec_index_columns);

  explicit OneDRtreeSecondaryIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt,
                                   const bool use_value_delta_encoding,
                                   const std::vector<Slice>& sec_index_columns);

  virtual ~OneDRtreeSecondaryIndexBuilder();

  virtual void OnKeyAdded(const Slice& value) override;

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }
  void get_Secondary_Entries(std::vector<std::pair<std::string, BlockHandle>>* sec_entries) override;
  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }
  size_t NumPartitions() const;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

  // Called when an external entity (such as filter partition builder) request
  // cutting the next partition
  void RequestPartitionCut();

  bool get_use_value_delta_encoding() { return use_value_delta_encoding_; }

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;
  // Set after ::Finish is called
  size_t partition_cnt_ = 0;

  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder> value;
    // Entry& operator=(Entry& other) {
    //     if (this != &other) {
    //         key = other.key;
    //         value.reset(other.value.release());
    //     }
    //     return *this;
    // }
    std::vector<std::string> sec_value;
  };
  struct DataBlockEntry {
    BlockHandle datablockhandle;
    std::string datablocklastkey;
    std::string subindexenclosingvalrange;
  };
  void AddIdxEntry(DataBlockEntry datablkentry, bool last=false);
  DataBlockEntry last_index_entry_;

  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  std::list<Entry> next_level_entries_;  // list of partitioned indexes and their keys
  std::list<DataBlockEntry> data_block_entries_;

  std::vector<ValueRange> tuple_valranges_; // for secondary index building
  std::vector<std::pair<std::string, BlockHandle>> sec_entries_; // for secondary global index

  BlockBuilder index_block_builder_;  // 没有被使用
  // the active partition index builder
  OneDRtreeSecondaryIndexLevelBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  bool firstlayer = true;
  const BlockBasedTableOptions& table_opt_;
  bool use_value_delta_encoding_;
  // true if an external entity (such as filter partition builder) request
  // cutting the next partition
  bool partition_cut_requested_ = true;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
  BlockHandle last_encoded_handle_;
  ValueRange sub_index_enclosing_valrange_;
  ValueRange enclosing_valrange_; // 粗粒度（整个子索引块的总范围）512B
  ValueRange sec_enclosing_valrange_;
  std::vector<std::string> sec_valranges_;  // 细粒度（满足条件下，子索引块内部的多个小范围）
  ValueRange temp_sec_valrange_;
  uint32_t rtree_level_;
  std::string rtree_height_str_;

  std::vector<Slice> sec_index_columns_; // 用户指定的二级索引列
  void expandValrange(ValueRange& to_expand, ValueRange expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      if (expander.range.min < to_expand.range.min) {
        to_expand.range.min = expander.range.min;
      }
      if (expander.range.max > to_expand.range.max) {
        to_expand.range.max = expander.range.max;
      }
    }
  }
};

class BtreeSecondaryIndexBuilder : public SecondaryIndexBuilder {
 public:
  static BtreeSecondaryIndexBuilder* CreateIndexBuilder(
    const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt,
    const std::vector<Slice>& sec_index_columns,
    const bool is_embedded = false); // return new BtreeSecondaryIndexBuilder

  explicit BtreeSecondaryIndexBuilder(const InternalKeyComparator* comparator,
                                 const BlockBasedTableOptions& table_opt,
                                 const bool use_value_delta_encoding,
                                 const std::vector<Slice>& sec_index_columns,
                                 const bool is_embedded);  // 构造函数
  virtual ~BtreeSecondaryIndexBuilder();

  virtual void OnKeyAdded(const Slice& value) override;

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }
  void get_Secondary_Entries(std::vector<std::pair<std::string, BlockHandle>>* sec_entries) override;

  bool IsEmbedded() const override { return is_embedded_; }

 private:
  struct DataBlockEntry {
    BlockHandle datablockhandle;
    std::string datablocklastkey;
    std::string sec_value;
  };
  std::list<DataBlockEntry> data_block_entries_;

  void AddIdxEntry(DataBlockEntry datablkentry, bool last=false);
  void MakeNewSubIndexBuilder();  // 创建新的子索引构建器
  
  std::string serializeSecValues(const double& sec_values) {
    std::string serialized;
    serialized.append(reinterpret_cast<const char*>(&sec_values), sizeof(sec_values));
    return serialized;
  }

  // Entry 结构：包含索引块的信息
  struct Entry {
    std::string key;  // 最小的 sec_value（第一个 double 值）
    std::unique_ptr<BlockBuilder> value;  // 索引块构建器
  };
  std::list<Entry> entries_;  // 已完成的索引块列表

  const BlockBasedTableOptions& table_opt_;
  bool use_value_delta_encoding_;
  std::vector<std::pair<std::string, BlockHandle>> sec_entries_; // for secondary global index
  std::vector<Slice> sec_index_columns_; // 用户指定的二级索引列
  bool is_embedded_; // 是否是嵌入式二级索引

  BlockBuilder* sub_index_builder_;  // 当前正在构建的索引块
  std::string sub_index_first_key_;  // 当前索引块的第一个 key（最小值）
  std::unique_ptr<FlushBlockPolicy> flush_policy_;  // 刷新策略（控制块大小 512 字节）
  bool partition_cut_requested_;  // 是否请求分区切割
  bool finishing_indexes;  // 是否正在完成索引（多次调用 Finish）
  
  BlockBuilder index_block_builder_;  // 备用（未使用）
  std::vector<double> data_values_;  // 临时存储当前 data block 的 double 值
  std::string last_block_data_;  // 缓存最近一次 Finish 生成的索引块内容，确保生命周期覆盖写入过程
};

class HashSecondaryIndexBuilder : public SecondaryIndexBuilder {
 public:
  static HashSecondaryIndexBuilder* CreateIndexBuilder(
    const ROCKSDB_NAMESPACE::InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt,
    const std::vector<Slice>& sec_index_columns,
    const bool is_embedded = false); // return new HashSecondaryIndexBuilder

  explicit HashSecondaryIndexBuilder(const InternalKeyComparator* comparator,
                                 const BlockBasedTableOptions& table_opt,
                                 const bool use_value_delta_encoding,
                                 const std::vector<Slice>& sec_index_columns,
                                 const bool is_embedded);  // 构造函数
  virtual ~HashSecondaryIndexBuilder();

  virtual void OnKeyAdded(const Slice& value) override;

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t IndexSize() const override { return index_size_; }
  void get_Secondary_Entries(std::vector<std::pair<std::string, BlockHandle>>* sec_entries) override;

  bool IsEmbedded() const override { return is_embedded_; }

 private:
  struct DataBlockEntry {
    BlockHandle datablockhandle;
    std::string datablocklastkey;
    std::string sec_value;
  };
  std::list<DataBlockEntry> data_block_entries_;

  void AddIdxEntry(DataBlockEntry datablkentry, bool last=false);
  void MakeNewSubIndexBuilder();  // 创建新的子索引构建器
  
  std::string serializeSecValues(const double& sec_values) {
    std::string serialized;
    serialized.append(reinterpret_cast<const char*>(&sec_values), sizeof(sec_values));
    return serialized;
  }

  // Entry 结构：包含索引块的信息
  struct Entry {
    std::string key;  // 最小的 sec_value（第一个 double 值）
    std::unique_ptr<BlockBuilder> value;  // 索引块构建器
  };
  std::list<Entry> entries_;  // 已完成的索引块列表

  const BlockBasedTableOptions& table_opt_;
  bool use_value_delta_encoding_;
  std::vector<std::pair<std::string, BlockHandle>> sec_entries_; // for secondary global index
  std::vector<Slice> sec_index_columns_; // 用户指定的二级索引列
  bool is_embedded_; // 是否是嵌入式二级索引

  BlockBuilder* sub_index_builder_;  // 当前正在构建的索引块
  std::string sub_index_first_key_;  // 当前索引块的第一个 key（最小值）
  std::unique_ptr<FlushBlockPolicy> flush_policy_;  // 刷新策略（控制块大小 512 字节）
  bool partition_cut_requested_;  // 是否请求分区切割
  bool finishing_indexes;  // 是否正在完成索引（多次调用 Finish）
  
  BlockBuilder index_block_builder_;  // 备用（未使用）
  std::vector<double> data_values_;  // 临时存储当前 data block 的 double 值
  std::string last_block_data_;  // 缓存最近一次 Finish 生成的索引块内容，确保生命周期覆盖写入过程
};
}  // namespace ROCKSDB_NAMESPACE
