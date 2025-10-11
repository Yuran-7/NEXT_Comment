//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/index_builder.h"

#include <assert.h>

#include <cinttypes>
#include <list>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/flush_block_policy.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/format.h"
#include "util/rtree.h"
#include "util/z_curve.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  IndexBuilder* result = nullptr;
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ false);
      break;
    }
    case BlockBasedTableOptions::kHashSearch: {
      // Currently kHashSearch is incompatible with index_block_restart_interval
      // > 1
      assert(table_opt.index_block_restart_interval == 1);
      result = new HashIndexBuilder(
          comparator, int_key_slice_transform,
          table_opt.index_block_restart_interval, table_opt.format_version,
          use_value_delta_encoding, table_opt.index_shortening);
      break;
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      result = PartitionedIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      break;
    }
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: { // 这个不是新增的
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ true);
      break;
    }
    case BlockBasedTableOptions::kRtreeSearch: {
      // result = new RtreeIndexLevelBuilder(
      //     comparator, table_opt.index_block_restart_interval,
      //     table_opt.format_version, use_value_delta_encoding,
      //     table_opt.index_shortening, /* include_first_key */ false);
      result = RtreeIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      // result = PartitionedIndexBuilder::CreateIndexBuilder(
      //     comparator, use_value_delta_encoding, table_opt);
      break;
    }
    default: {
      assert(!"Do not recognize the index type ");
      break;
    }
  }
  return result;
}

// Create a Secondary index builder based on its type.
SecondaryIndexBuilder* SecondaryIndexBuilder::CreateSecIndexBuilder(
    BlockBasedTableOptions::SecondaryIndexType sec_index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt,
    const std::vector<Slice>& sec_index_columns) {
  (void) int_key_slice_transform;
  SecondaryIndexBuilder* result = nullptr;
  switch (sec_index_type) {
    case BlockBasedTableOptions::kRtreeSec: {
      result = RtreeSecondaryIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      break;
    }
    case BlockBasedTableOptions::kOneDRtreeSec: {
      result = OneDRtreeSecondaryIndexBuilder::CreateIndexBuilder(
         comparator, use_value_delta_encoding, table_opt, sec_index_columns);
      break;
    }
    default: {
      assert(!"Do not recognize the index type ");
      break;
    }
  }
  return result;
}

void ShortenedIndexBuilder::FindShortestInternalKeySeparator(
    const Comparator& comparator, std::string* start, const Slice& limit) {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  comparator.FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() <= user_start.size() &&
      comparator.Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(*start, tmp) < 0);
    assert(InternalKeyComparator(&comparator).Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void ShortenedIndexBuilder::FindShortInternalKeySuccessor(
    const Comparator& comparator, std::string* key) {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  comparator.FindShortSuccessor(&tmp);
  if (tmp.size() <= user_key.size() && comparator.Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

PartitionedIndexBuilder* PartitionedIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new PartitionedIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

PartitionedIndexBuilder::PartitionedIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : IndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      index_block_builder_without_seq_(table_opt.index_block_restart_interval,
                                       true /*use_delta_encoding*/,
                                       use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      // We start by false. After each partition we revise the value based on
      // what the sub_index_builder has decided. If the feature is disabled
      // entirely, this will be set to true after switching the first
      // sub_index_builder. Otherwise, it could be set to true even one of the
      // sub_index_builders could not safely exclude seq from the keys, then it
      // wil be enforced on all sub_index_builders on ::Finish.
      seperator_is_key_plus_seq_(false),
      use_value_delta_encoding_(use_value_delta_encoding) {}

PartitionedIndexBuilder::~PartitionedIndexBuilder() {
  delete sub_index_builder_;
}

void PartitionedIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new ShortenedIndexBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  // Set sub_index_builder_->seperator_is_key_plus_seq_ to true if
  // seperator_is_key_plus_seq_ is true (internal-key mode) (set to false by
  // default on Creation) so that flush policy can point to
  // sub_index_builder_->index_block_builder_
  if (seperator_is_key_plus_seq_) {
    sub_index_builder_->seperator_is_key_plus_seq_ = true;
  }

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      // Note: this is sub-optimal since sub_index_builder_ could later reset
      // seperator_is_key_plus_seq_ but the probability of that is low.
      sub_index_builder_->seperator_is_key_plus_seq_
          ? sub_index_builder_->index_block_builder_
          : sub_index_builder_->index_block_builder_without_seq_));
  partition_cut_requested_ = false;
}

void PartitionedIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void PartitionedIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                      first_key_in_next_block, block_handle);
    if (!seperator_is_key_plus_seq_ &&
        sub_index_builder_->seperator_is_key_plus_seq_) {
      // then we need to apply it to all sub-index builders and reset
      // flush_policy to point to Block Builder of sub_index_builder_ that store
      // internal keys.
      seperator_is_key_plus_seq_ = true;
      flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
          table_opt_.metadata_block_size, table_opt_.block_size_deviation,
          sub_index_builder_->index_block_builder_));
    }
    sub_index_last_key_ = std::string(*last_key_in_current_block);
    entries_.push_back(
        {sub_index_last_key_,
         std::unique_ptr<ShortenedIndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(*last_key_in_current_block, handle_encoding);
      if (do_flush) {
        entries_.push_back(
            {sub_index_last_key_,
             std::unique_ptr<ShortenedIndexBuilder>(sub_index_builder_)});
        cut_filter_block = true;
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                      first_key_in_next_block, block_handle);
    sub_index_last_key_ = std::string(*last_key_in_current_block);
    if (!seperator_is_key_plus_seq_ &&
        sub_index_builder_->seperator_is_key_plus_seq_) {
      // then we need to apply it to all sub-index builders and reset
      // flush_policy to point to Block Builder of sub_index_builder_ that store
      // internal keys.
      seperator_is_key_plus_seq_ = true;
      flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
          table_opt_.metadata_block_size, table_opt_.block_size_deviation,
          sub_index_builder_->index_block_builder_));
    }
  }
}

Status PartitionedIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  assert(sub_index_builder_ == nullptr);
  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    std::string handle_delta_encoding;
    PutVarsignedint64(
        &handle_delta_encoding,
        last_partition_block_handle.size() - last_encoded_handle_.size());
    last_encoded_handle_ = last_partition_block_handle;
    const Slice handle_delta_encoding_slice(handle_delta_encoding);
    index_block_builder_.Add(last_entry.key, handle_encoding,
                             &handle_delta_encoding_slice);
    if (!seperator_is_key_plus_seq_) {
      index_block_builder_without_seq_.Add(ExtractUserKey(last_entry.key),
                                           handle_encoding,
                                           &handle_delta_encoding_slice);
    }
    entries_.pop_front();
  }
  // If there is no sub_index left, then return the 2nd level index.
  if (UNLIKELY(entries_.empty())) {
    if (seperator_is_key_plus_seq_) {
      index_blocks->index_block_contents = index_block_builder_.Finish();
    } else {
      index_blocks->index_block_contents =
          index_block_builder_without_seq_.Finish();
    }
    top_level_index_size_ = index_blocks->index_block_contents.size();
    index_size_ += top_level_index_size_;
    return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();
    // Apply the policy to all sub-indexes
    entry.value->seperator_is_key_plus_seq_ = seperator_is_key_plus_seq_;
    auto s = entry.value->Finish(index_blocks);
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t PartitionedIndexBuilder::NumPartitions() const { return partition_cnt_; }


RtreeIndexBuilder* RtreeIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new RtreeIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

RtreeIndexBuilder::RtreeIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : IndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      use_value_delta_encoding_(use_value_delta_encoding),
      rtree_level_(1) {}

RtreeIndexBuilder::~RtreeIndexBuilder() {
  delete sub_index_builder_;
}

void RtreeIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new RtreeIndexLevelBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      sub_index_builder_->index_block_builder_));
  partition_cut_requested_ = false;
}

void RtreeIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void RtreeIndexBuilder::OnKeyAdded(const Slice& key){
    Slice key_temp = Slice(key);
    Mbr mbr = ReadKeyMbr(key_temp);
    expandMbr(sub_index_enclosing_mbr_, mbr);
    // std::cout << "sub_index_enclosing_mbr_ after expansion: " << sub_index_enclosing_mbr_ << std::endl;
  }

void RtreeIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  expandMbr(enclosing_mbr_, sub_index_enclosing_mbr_);
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(block_handle, serializeMbr(sub_index_enclosing_mbr_));

    sub_index_last_key_ = std::string(*last_key_in_current_block);
    // std::cout << "push_back the last sub_index builder" << std::endl;
    entries_.push_back(
        {serializeMbr(enclosing_mbr_),
         std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
    enclosing_mbr_.clear();
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(*last_key_in_current_block, handle_encoding);
      if (do_flush) {
        // std::cout << "push_back a full sub_index builder" << std::endl;
        entries_.push_back(
            {serializeMbr(enclosing_mbr_),
             std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(block_handle, serializeMbr(sub_index_enclosing_mbr_));
    sub_index_last_key_ = std::string(*last_key_in_current_block);

  }
  sub_index_enclosing_mbr_.clear();
}

Status RtreeIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  // std::cout << "RtreeIndexBuilder Finish" << std::endl;
  // std::cout << "entries_ size: " << entries_.size() << std::endl;
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  // assert(sub_index_builder_ == nullptr);
  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    // std::string handle_encoding;
    // last_partition_block_handle.EncodeTo(&handle_encoding);
    // std::string handle_delta_encoding;
    // PutVarsignedint64(
    //     &handle_delta_encoding,
    //     last_partition_block_handle.size() - last_encoded_handle_.size());
    // last_encoded_handle_ = last_partition_block_handle;
    // const Slice handle_delta_encoding_slice(handle_delta_encoding);
    // // std::cout << "add index entry into top level index: " << ReadQueryMbr(last_entry.key) << std::endl;
    // index_block_builder_.Add(last_entry.key, handle_encoding,
    //                          &handle_delta_encoding_slice);

    // add entry to the next_sub_index_builder
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(last_entry.key, handle_encoding);
      if (do_flush) {
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(
            {serializeMbr(enclosing_mbr_),
             std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);
    expandMbr(enclosing_mbr_, ReadQueryMbr(last_entry.key));

    entries_.pop_front();
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {

    // update R-tree height
    rtree_level_++;

    if (sub_index_builder_ != nullptr){
      next_level_entries_.push_back(
          {serializeMbr(enclosing_mbr_),
          std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
      enclosing_mbr_.clear();
      sub_index_builder_ = nullptr;
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {
      Entry& entry = next_level_entries_.front();
      auto s = entry.value->Finish(index_blocks);
      // std::cout << "writing the top-level index block with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();
      PutVarint32(&rtree_height_str_, rtree_level_);
      index_blocks->meta_blocks.insert(
        {kRtreeIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) {
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;

    next_level_entries_.clear();
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t RtreeIndexBuilder::NumPartitions() const { return partition_cnt_; }

// void RtreeIndexLevelBuilder::FindShortestInternalKeySeparator(
//     const Comparator& comparator, std::string* start, const Slice& limit) {
//   // Attempt to shorten the user portion of the key
//   Slice user_start = ExtractUserKey(*start);
//   Slice user_limit = ExtractUserKey(limit);
//   std::string tmp(user_start.data(), user_start.size());
//   comparator.FindShortestSeparator(&tmp, user_limit);
//   if (tmp.size() <= user_start.size() &&
//       comparator.Compare(user_start, tmp) < 0) {
//     // User key has become shorter physically, but larger logically.
//     // Tack on the earliest possible number to the shortened user key.
//     PutFixed64(&tmp,
//                PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
//     assert(InternalKeyComparator(&comparator).Compare(*start, tmp) < 0);
//     assert(InternalKeyComparator(&comparator).Compare(tmp, limit) < 0);
//     start->swap(tmp);
//   }
// }

// void RtreeIndexLevelBuilder::FindShortInternalKeySuccessor(
//     const Comparator& comparator, std::string* key) {
//   Slice user_key = ExtractUserKey(*key);
//   std::string tmp(user_key.data(), user_key.size());
//   comparator.FindShortSuccessor(&tmp);
//   if (tmp.size() <= user_key.size() && comparator.Compare(user_key, tmp) < 0) {
//     // User key has become shorter physically, but larger logically.
//     // Tack on the earliest possible number to the shortened user key.
//     PutFixed64(&tmp,
//                PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
//     assert(InternalKeyComparator(&comparator).Compare(*key, tmp) < 0);
//     key->swap(tmp);
//   }
// }

RtreeSecondaryIndexBuilder* RtreeSecondaryIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new RtreeSecondaryIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

RtreeSecondaryIndexBuilder::RtreeSecondaryIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : SecondaryIndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      use_value_delta_encoding_(use_value_delta_encoding),
      rtree_level_(1) {}

RtreeSecondaryIndexBuilder::~RtreeSecondaryIndexBuilder() {
  delete sub_index_builder_;
}

void RtreeSecondaryIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new RtreeSecondaryIndexLevelBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      sub_index_builder_->index_block_builder_));
  partition_cut_requested_ = false;
}

void RtreeSecondaryIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void RtreeSecondaryIndexBuilder::OnKeyAdded(const Slice& value){
    Slice val_temp = Slice(value);
    Mbr mbr = ReadValueMbr(val_temp);
    tuple_mbrs_.push_back(mbr);
    // expandMbrExcludeIID(sub_index_enclosing_mbr_, mbr);
  }

void RtreeSecondaryIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  // expandMbrExcludeIID(enclosing_mbr_, sub_index_enclosing_mbr_);
  (void) first_key_in_next_block;
  std::string datablocklastkeystr = std::string(*last_key_in_current_block);
  for (const Mbr& m :tuple_mbrs_) {
    DataBlockEntry dbe;
    dbe.datablockhandle = block_handle;
    dbe.datablocklastkey = datablocklastkeystr;
    dbe.subindexenclosingmbr = serializeMbrExcludeIID(m);
    data_block_entries_.push_back(dbe);
  }
  tuple_mbrs_.clear();
  
  // dbe.subindexenclosingmbr = serializeMbrExcludeIID(sub_index_enclosing_mbr_);
  // data_block_entries_.push_back(dbe);
  // sub_index_enclosing_mbr_.clear();    

}

void RtreeSecondaryIndexBuilder::AddIdxEntry(DataBlockEntry datablkentry, bool last) {
  expandMbrExcludeIID(enclosing_mbr_, ReadSecQueryMbr(datablkentry.subindexenclosingmbr));

  expandMbrExcludeIID(temp_sec_mbr_, ReadSecQueryMbr(datablkentry.subindexenclosingmbr));
  if (GetMbrArea(temp_sec_mbr_) > 0.005 && !sec_enclosing_mbr_.empty()) {
    sec_mbrs_.emplace_back(serializeMbrExcludeIID(sec_enclosing_mbr_));
    sec_enclosing_mbr_.clear();
    temp_sec_mbr_.clear();
    expandMbrExcludeIID(temp_sec_mbr_, ReadSecQueryMbr(datablkentry.subindexenclosingmbr));
  }

  expandMbrExcludeIID(sec_enclosing_mbr_, ReadSecQueryMbr(datablkentry.subindexenclosingmbr));
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(last == true)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingmbr);

    // std::cout << "pushed mbr: " << enclosing_mbr_ << std::endl;
    sub_index_last_key_ = serializeMbrExcludeIID(enclosing_mbr_);
    // std::cout << "push_back the last sub_index builder" << std::endl;

    // if (GetMbrArea(sec_enclosing_mbr_) > 0.0005) {
    //   std::cout << "big mbr found: " << sec_enclosing_mbr_ << std::endl;
    // } else {
    //   std::cout << "small mbr" << std::endl;
    // }

    sec_mbrs_.emplace_back(serializeMbrExcludeIID(sec_enclosing_mbr_));
    sec_enclosing_mbr_.clear();
    temp_sec_mbr_.clear();

    entries_.push_back(
        {serializeMbrExcludeIID(enclosing_mbr_),
         std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_),
         sec_mbrs_});
    sec_mbrs_.clear();
    enclosing_mbr_.clear();
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      std::string enclosing_mbr_encoding;
      enclosing_mbr_encoding = serializeMbrExcludeIID(enclosing_mbr_);
      datablkentry.datablockhandle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(enclosing_mbr_encoding, handle_encoding);
      if (do_flush) {
        // std::cout << "flush with enclosing mbr: " << sec_enclosing_mbr_ << std::endl;
        // std::cout << "push_back a full sub_index builder" << std::endl;
        // std::cout << "pushed mbr: " << enclosing_mbr_ << std::endl;

        // if (GetMbrArea(sec_enclosing_mbr_) > 0.0005) {
        //   std::cout << "big mbr found: " << sec_enclosing_mbr_ << std::endl;
        // } else {
        //   std::cout << "small mbr" << std::endl;
        // }

        sec_mbrs_.emplace_back(serializeMbrExcludeIID(sec_enclosing_mbr_));
        sec_enclosing_mbr_.clear();
        temp_sec_mbr_.clear();
        entries_.push_back(
            {serializeMbrExcludeIID(enclosing_mbr_),
             std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_),
             sec_mbrs_});
        sec_mbrs_.clear();
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingmbr);
    sub_index_last_key_ = serializeMbrExcludeIID(enclosing_mbr_);

  }
}

Status RtreeSecondaryIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {

  if (finishing_indexes == false) {
    
    data_block_entries_.sort([](const DataBlockEntry& a, const DataBlockEntry& b){
      Mbr a_mbr = ReadSecQueryMbr(a.subindexenclosingmbr);
      Mbr b_mbr = ReadSecQueryMbr(b.subindexenclosingmbr);

      // defining z-curve based on grid cells on data space
      // for dataset Tweet
      // double x_min = -179.9942;
      // double x_max = 179.9868;
      // double y_min = -90;
      // double y_max = 90;
      // for dataset buildings
      double x_min = -179.7582827;
      double x_max = 179.8440789;
      double y_min = -89.9678358;
      double y_max = 82.5114551;
      int n = 262144;

      // using the centre point of each mbr for z-value computation
      double x_a = (a_mbr.first.min + a_mbr.first.max) / 2;
      double y_a = (a_mbr.second.min + a_mbr.second.max) / 2;
      double x_b = (b_mbr.first.min + b_mbr.first.max) / 2;
      double y_b = (b_mbr.second.min + b_mbr.second.max) / 2;
      // double x_a = a_mbr.first.min;
      // double y_a = a_mbr.second.min;
      // double x_b = b_mbr.first.min;
      // double y_b = b_mbr.second.min;

      // compute the z-values
      uint32_t x_a_int = std::min(int(floor((x_a - x_min)  / ((x_max - x_min) / n))), n-1);
      uint32_t y_a_int = std::min(int(floor((y_a - y_min)  / ((y_max - y_min) / n))), n-1);
      uint32_t x_b_int = std::min(int(floor((x_b - x_min)  / ((x_max - x_min) / n))), n-1);
      uint32_t y_b_int = std::min(int(floor((y_b - y_min)  / ((y_max - y_min) / n))), n-1);

      // compare the z-values
      int comp = comp_z_order(x_a_int, y_a_int, x_b_int, y_b_int);
      return comp <= 0;     
    });
    
    std::list<DataBlockEntry>::iterator it;
    // int count = 0;
    // std::cout << "data block entries size: " << data_block_entries_.size() << std::endl;
    for (it = data_block_entries_.begin(); it != data_block_entries_.end(); ++it) {
      if (it == --data_block_entries_.end()) {
        // std::cout << "last entry added" << std::endl;
        // count++;
        AddIdxEntry(*it, true);
      } else {
        AddIdxEntry(*it);
        // count++;
      }
      // AddIdxEntry(*it);
    }
    // AddIdxEntry(last_index_entry_, true);
    // std::cout << "count number: " << count << std::endl;
    // std::cout << "entries_ size: " << entries_.size() << std::endl;
  }
  
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  // assert(sub_index_builder_ == nullptr);

  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    if (firstlayer == true) {
      for (const std::string& sec_mbr_s: last_entry.sec_value) {
        sec_entries_.emplace_back(std::make_pair(sec_mbr_s, last_partition_block_handle));
      }
      // sec_entries_.emplace_back(std::make_pair(last_entry.key, last_partition_block_handle));      
    }

    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(last_entry.key, handle_encoding);
      if (do_flush) {
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(
            {serializeMbrExcludeIID(enclosing_mbr_),
             std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);
    expandMbrExcludeIID(enclosing_mbr_, ReadValueMbr(last_entry.key));

    entries_.pop_front();
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {

    // update R-tree height
    rtree_level_++;

    // PutVarint32(&rtree_height_str_, rtree_level_);
    // index_blocks->meta_blocks.insert(
    //   {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});    

    // return Status::OK();

    firstlayer = false;
    if (sub_index_builder_ != nullptr){
      next_level_entries_.push_back(
          {serializeMbrExcludeIID(enclosing_mbr_),
          std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
      enclosing_mbr_.clear();
      sub_index_builder_ = nullptr;
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {
      Entry& entry = next_level_entries_.front();
      auto s = entry.value->Finish(index_blocks);
      // std::cout << "writing the top-level index block with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();
      PutVarint32(&rtree_height_str_, rtree_level_);
      index_blocks->meta_blocks.insert(
        {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) {
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;

    next_level_entries_.clear();
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t RtreeSecondaryIndexBuilder::NumPartitions() const { return partition_cnt_; }

void RtreeSecondaryIndexBuilder::get_Secondary_Entries(
  std::vector<std::pair<std::string, BlockHandle>>* sec_entries) {
  *sec_entries = sec_entries_;
  sec_entries_.clear();
}

OneDRtreeSecondaryIndexBuilder* OneDRtreeSecondaryIndexBuilder::CreateIndexBuilder(  // 创建一维R树辅助索引构建器的工厂方法
    const InternalKeyComparator* comparator,  // 内部键比较器
    const bool use_value_delta_encoding,  // 是否使用值增量编码
    const BlockBasedTableOptions& table_opt,
    const std::vector<Slice>& sec_index_columns) {
  return new OneDRtreeSecondaryIndexBuilder(comparator, table_opt,  // 返回新创建的一维R树辅助索引构建器实例
                                     use_value_delta_encoding,
                                     sec_index_columns);
}

OneDRtreeSecondaryIndexBuilder::OneDRtreeSecondaryIndexBuilder(  // 构造函数
    const InternalKeyComparator* comparator,  // 内部键比较器
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding,
    const std::vector<Slice>& sec_index_columns)
    : SecondaryIndexBuilder(comparator),  // 调用基类构造函数，用comparator初始化comparator_
      index_block_builder_(table_opt.index_block_restart_interval,  // 初始化索引块构建器
                           true /*use_delta_encoding*/,  // 启用增量编码
                           use_value_delta_encoding),  // 值增量编码选项
      sub_index_builder_(nullptr),  // 子索引构建器初始化为空
      table_opt_(table_opt),  // 保存表选项
      use_value_delta_encoding_(use_value_delta_encoding),  // 保存值增量编码选项
      rtree_level_(1),    // R树层级初始化为1（叶子层）
      sec_index_columns_(sec_index_columns) {}

OneDRtreeSecondaryIndexBuilder::~OneDRtreeSecondaryIndexBuilder() {  // 析构函数
  delete sub_index_builder_;  // 释放子索引构建器内存
}

void OneDRtreeSecondaryIndexBuilder::MakeNewSubIndexBuilder() {  // 创建新的子索引构建器
  assert(sub_index_builder_ == nullptr);  // 断言当前没有子索引构建器
  sub_index_builder_ = new OneDRtreeSecondaryIndexLevelBuilder(  // 创建新的一维R树辅助索引层级构建器
      comparator_, table_opt_.index_block_restart_interval,  // 使用比较器和重启间隔
      table_opt_.format_version, use_value_delta_encoding_,  // 格式版本和增量编码选项
      table_opt_.index_shortening, /* include_first_key */ false);  // 索引缩短模式，不包含首键

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(  // 重置刷新策略
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,  // 元数据块大小(512)和偏差(10)
      sub_index_builder_->index_block_builder_));  // 使用子索引构建器的块构建器
  partition_cut_requested_ = false;  // 重置分区切割请求标志
}

void OneDRtreeSecondaryIndexBuilder::RequestPartitionCut() {  // 请求切割分区
  partition_cut_requested_ = true;  // 设置分区切割请求标志为真
}

void OneDRtreeSecondaryIndexBuilder::OnKeyAdded(const Slice& value) {  // table\block_based\block_based_table_builder.cc调用的
    Slice val_temp = Slice(value);  // 创建值的临时切片
    std::vector<Slice> extracted_values;
    WideColumnSerialization::GetValuesByColumnNames(val_temp, sec_index_columns_, extracted_values);  // 提取指定列的值
    double numerical_val = *reinterpret_cast<const double*>(extracted_values[0].data());  // 将值的前8字节解释为double类型的数值（一维辅助索引基于数值构建）
    ValueRange temp_val_range;  // 创建临时值范围对象
    temp_val_range.set_range(numerical_val, numerical_val);  // 传入一个min，一个max
    // expandValrange(sub_index_enclosing_valrange_, temp_val_range);
    tuple_valranges_.emplace_back(temp_val_range);
}

void OneDRtreeSecondaryIndexBuilder::AddIndexEntry( // table\block_based\block_based_table_builder.cc调用的
    std::string* last_key_in_current_block,  // 当前数据块的最后一个key，注意是key，不是value
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {  // 当前数据块的句柄
  (void) first_key_in_next_block;  // 不需要
  std::string datablcoklastkeystr = std::string(*last_key_in_current_block);  // 保存数据块最后一个key
  for (const ValueRange& v: tuple_valranges_) {  // 遍历当前数据块收集的所有值范围（对应算法第4行：for KVpair ∈ datablock）
    DataBlockEntry dbe;  // 创建数据块条目
    dbe.datablockhandle = block_handle;  // 设置数据块句柄（位置信息）
    dbe.datablocklastkey = datablcoklastkeystr;  // 设置数据块最后一个key
    dbe.subindexenclosingvalrange = serializeValueRange(v);  // 二级属性值加入dbe
    data_block_entries_.push_back(dbe);  // std::list<DataBlockEntry>
  }
  tuple_valranges_.clear();  // 清空值范围向量，准备处理下一个数据块
}


void OneDRtreeSecondaryIndexBuilder::AddIdxEntry(DataBlockEntry datablkentry, bool last) {  // 将数据块条目添加到辅助索引（对应算法第8-10行）
  expandValrange(enclosing_valrange_, ReadValueRange(datablkentry.subindexenclosingvalrange));  // 扩展包围值范围以包含当前条目的值范围
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key

  expandValrange(temp_sec_valrange_, ReadValueRange(datablkentry.subindexenclosingvalrange));  // 扩展临时辅助值范围
  if (temp_sec_valrange_.range.max-temp_sec_valrange_.range.min > 0.005 && !sec_enclosing_valrange_.empty()) {  // 如果临时范围跨度超过阈值0.005且辅助包围范围非空
    sec_valranges_.emplace_back(serializeValueRange(sec_enclosing_valrange_));  // 保存当前辅助包围范围（用于全局索引）
    sec_enclosing_valrange_.clear();  // 清空辅助包围范围
    temp_sec_valrange_.clear();  // 清空临时辅助范围
    expandValrange(temp_sec_valrange_, ReadValueRange(datablkentry.subindexenclosingvalrange));  // 重新开始扩展临时范围
  }

  expandValrange(sec_enclosing_valrange_, ReadValueRange(datablkentry.subindexenclosingvalrange));  // 扩展辅助包围值范围

  if (UNLIKELY(last == true)) {  // 如果这是最后一个条目（对应算法处理最后一个数据块）
    if (sub_index_builder_ == nullptr) {  // 如果子索引构建器为空
      MakeNewSubIndexBuilder();  // 创建新的子索引构建器
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingvalrange);  // 向子索引添加条目

    sub_index_last_key_ = serializeValueRange(enclosing_valrange_);  // 保存当前包围值范围作为子索引的最后一个键

    sec_valranges_.emplace_back(serializeValueRange(sec_enclosing_valrange_));  // 添加最后的辅助包围范围
    sec_enclosing_valrange_.clear();  // 清空辅助包围范围
    temp_sec_valrange_.clear();  // 清空临时辅助范围

    entries_.push_back(  // 将完成的子索引添加到条目列表
        {serializeValueRange(enclosing_valrange_),  // 包围值范围作为键
         std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_),  // 子索引构建器
         sec_valranges_});  // 辅助值范围列表（用于全局索引GL）
    sec_valranges_.clear();  // 清空辅助值范围列表
    enclosing_valrange_.clear();  // 清空包围值范围
    sub_index_builder_ = nullptr;  // 重置子索引构建器指针
    cut_filter_block = true;  // 设置过滤块切割标志
  } else {  // 如果不是最后一个条目
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;  // 句柄编码字符串
      std::string enclosing_valrange_encoding;  // 包围值范围编码字符串
      enclosing_valrange_encoding = serializeValueRange(enclosing_valrange_);  // 序列化包围值范围
      datablkentry.datablockhandle.EncodeTo(&handle_encoding);  // 编码数据块句柄
      bool do_flush =  // 判断是否需要刷新（对应算法第11行：if SB.size() > δ）
          partition_cut_requested_ ||  // 请求了分区切割
          flush_policy_->Update(enclosing_valrange_encoding, handle_encoding);  // 或刷新策略要求刷新
      if (do_flush) {  // 如果需要刷盘（对应算法第12-14行）

        sec_valranges_.emplace_back(serializeValueRange(sec_enclosing_valrange_));  // 保存辅助包围范围
        sec_enclosing_valrange_.clear();  // 清空辅助包围范围
        temp_sec_valrange_.clear();  // 清空临时辅助范围

        // std::cout << "push_back a full sub_index builder" << std::endl;
        // std::cout << "pushed valuerange: " << enclosing_valrange_ << std::endl;
        entries_.push_back(  // 将当前子索引添加到条目列表（对应算法第12行：S ← SB; flush到SST）
            {serializeValueRange(enclosing_valrange_),  // 包围值范围作为索引键
             std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_),  // 子索引构建器
             sec_valranges_});  // 在一维的情况下，这个参数和第一个参数实际上是一样的
        sec_valranges_.clear();  // 清空辅助值范围列表
        enclosing_valrange_.clear();  // 清空包围值范围
        sub_index_builder_ = nullptr;  // 重置子索引构建器（对应算法第14行：SB ← ∅; 创建新辅助索引块）
      }
    }
    if (sub_index_builder_ == nullptr) {  // 如果子索引构建器为空
      MakeNewSubIndexBuilder();  // 创建新的子索引构建器
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingvalrange);  // table/block_based/index_builder.h，1118行
    sub_index_last_key_ = serializeValueRange(enclosing_valrange_);  // 更新子索引最后一个键

  }
}


Status OneDRtreeSecondaryIndexBuilder::Finish(  // 完成辅助索引构建
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {  // 索引块和最后一个分区块句柄

  if (finishing_indexes == false) {  // 只有第一次调用才会进入
    
    data_block_entries_.sort([](const DataBlockEntry& a, const DataBlockEntry& b) {  // 对数据块条目排序（对应算法第6行：Sort V based on sec_val）
      ValueRange a_valrange = ReadValueRange(a.subindexenclosingvalrange);  // 读取条目a的值范围
      ValueRange b_valrange = ReadValueRange(b.subindexenclosingvalrange);  // 读取条目b的值范围

      // using the centre point of each interval for comparison
      double c_a = (a_valrange.range.min + a_valrange.range.max) / 2;  // 计算a的中心点（用于一维排序）
      double c_b = (b_valrange.range.min + b_valrange.range.max) / 2;  // 计算b的中心点

      return c_a <= c_b;  // 按中心点升序排序
    });
    
    std::list<DataBlockEntry>::iterator it;  // 数据块条目迭代器
    // int count = 0;
    // std::cout << "data block entries size: " << data_block_entries_.size() << std::endl;
    for (it = data_block_entries_.begin(); it != data_block_entries_.end(); ++it) {  // 遍历排序后的数据块条目（对应算法第7行：while V.size() > 0）
      if (it == --data_block_entries_.end()) {  // 如果是最后一个条目
        // std::cout << "last entry added" << std::endl;
        // count++;
        AddIdxEntry(*it, true);  // 添加最后一个条目
      } else {  // 如果不是最后一个条目
        AddIdxEntry(*it);
        // count++;
      }
      // AddIdxEntry(*it);
    }
    // AddIdxEntry(last_index_entry_, true);
    // std::cout << "count number: " << count << std::endl;
    // std::cout << "entries_ size: " << entries_.size() << std::endl;
  }
  
  if (partition_cnt_ == 0) {  // 如果分区计数为0
    partition_cnt_ = entries_.size();  // 设置分区计数为条目列表大小
  }

  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();  // 【关键】获取第一个条目（这个条目就是之前push_back到entries_的）

    if (firstlayer == true) {  // 如果是第一层（叶子层）
      for (const std::string& sec_val_s: last_entry.sec_value){  // last_entry是SST中的一个二级索引block，sec_value是一个vector，一维情况下，vector中只要一个数据，就是整个block的value范围
        sec_entries_.emplace_back(std::make_pair(sec_val_s, last_partition_block_handle));  // 【GL生成】添加到全局索引GL（对应算法第13行：GL ← <SB.val_range, SB.location>）
                                                                                              // last_partition_block_handle是上一次Finish()返回的BlockHandle，表示该子索引块在SST中的位置
      }
      // sec_entries_.emplace_back(std::make_pair(last_entry.key, last_partition_block_handle));
    }

    if (sub_index_builder_ != nullptr) {    // 如果子索引构建器存在
      std::string handle_encoding;  // 句柄编码
      last_partition_block_handle.EncodeTo(&handle_encoding);  // 编码最后一个分区块句柄
      bool do_flush =  // 判断是否需要刷新
          partition_cut_requested_ ||  // 请求了分区切割
          flush_policy_->Update(last_entry.key, handle_encoding);  // 或刷新策略要求刷新
      if (do_flush) {  // 如果需要刷新
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(  // 添加到下一层条目列表（构建上层R树节点）
            {serializeValueRange(enclosing_valrange_),  // 序列化包围值范围
             std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});  // 子索引构建器
        enclosing_valrange_.clear();  // 清空包围值范围
        sub_index_builder_ = nullptr;  // 重置子索引构建器
      }
    }
    if (sub_index_builder_ == nullptr) {  // 如果子索引构建器为空
      MakeNewSubIndexBuilder();  // 创建新的子索引构建器
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);  // 添加条目到子索引
    expandValrange(enclosing_valrange_, ReadValueRange(last_entry.key));  // 扩展包围值范围

    entries_.pop_front();  // 【关键】移除已处理的第一个条目（下次循环处理下一个）
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {  // UNLIKELY表示这种情况不常发生，是一个分支预测提示宏，即使entries_为空，由于下面的else还是返回Status::Incomplete()，所以这个if一定会进入，且是最后一次进入

    // update R-tree height
    rtree_level_++;  // 增加R树层级

    firstlayer = false;  // 标记不再是第一层
    if (sub_index_builder_ != nullptr){  // 如果还有未完成的子索引构建器
      next_level_entries_.push_back(  // std::List<Entry>
          {serializeValueRange(enclosing_valrange_),  // 序列化包围值范围
          std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});  // 子索引构建器
      enclosing_valrange_.clear();  // 清空包围值范围
      sub_index_builder_ = nullptr;  // 重置子索引构建器
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {  // 如果下一层只有一个条目（R树已到根节点）
      Entry& entry = next_level_entries_.front();  // 获取唯一的条目
      auto s = entry.value->Finish(index_blocks);  // 完成该条目的索引块写入
      // std::cout << "writing the top-level index block with enclosing valuerange: " << ReadValueRange(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();  // 累加索引大小
      PutVarint32(&rtree_height_str_, rtree_level_);  // 记录R树高度
      index_blocks->meta_blocks.insert(  // 将R树高度插入元数据块
        {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;  // 直接返回
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) { // 从next_level_entries_移动条目到entries_
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();  // 获取第一个条目
    auto s = entry.value->Finish(index_blocks);  // 完成该条目的索引块写入
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();  // 累加索引大小
    finishing_indexes = true;  // 标记正在完成索引

    next_level_entries_.clear();  // 清空下一层条目列表
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();  // 返回未完成状态（还有更多层需要处理）

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {  // entries_中还有条目需要处理
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();  // 【关键】获取entries_中的第一个条目

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();  // 累加索引大小
    finishing_indexes = true;  // 标记正在完成索引
    return s.ok() ? Status::Incomplete() : s;  // 返回未完成状态（继续处理下一个条目）
                                                // Status::Incomplete()告诉调用者"还有更多块要写"，需要再次调用Finish()
  }
}

size_t OneDRtreeSecondaryIndexBuilder::NumPartitions() const { return partition_cnt_; }  // 返回分区数量

void OneDRtreeSecondaryIndexBuilder::get_Secondary_Entries( // 一维情况下，就是获取所有二级索引块的值范围和位置
  std::vector<std::pair<std::string, BlockHandle>>* sec_entries) {
    *sec_entries = sec_entries_;
    sec_entries_.clear();
  }

}  // namespace ROCKSDB_NAMESPACE
