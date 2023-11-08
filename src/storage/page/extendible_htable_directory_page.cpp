//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  int max_array_size = 1 << max_depth_;
  for (int i = 0; i < max_array_size; ++i) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto half_size = Size() / 2;
  if (bucket_idx < half_size) {
    return ForwardSplitImageIdx(bucket_idx);
  }
  // bucket_idx > half_size
  return BackwardSplitImageIdx(bucket_idx);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // ASSUMPTION: the caller first called IncrLocalDepth(idx, new_depth), thus now local_depth = global_depth + 1
  // NOTE: this function does NOT handle bucket split, it is called after they are split
  //        hence they only update the pointers of the grown table

  int old_size = 1 << global_depth_;
  ++global_depth_;

  for (int idx = 0; idx < old_size; ++idx) {
    auto split_img_idx = ForwardSplitImageIdx(idx);
    bucket_page_ids_[split_img_idx] = bucket_page_ids_[idx];
    local_depths_[split_img_idx] = local_depths_[idx];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  auto size = Size();
  uint32_t decr_to_depth = 0;
  for (uint32_t i = 0; i < size; ++i) {
    if (local_depths_[i] > decr_to_depth) {
      decr_to_depth = local_depths_[i];
    }
  }
  assert(decr_to_depth < global_depth_);
  global_depth_ = decr_to_depth;
  // we do not care about the contents in the bucket that are
  // logically removed and their pointers, since they are
  // now outside [0, size) valid region
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // if it can shrink, the local depth is always less than global depth
  uint32_t size = Size();
  for (uint32_t i = 0; i < size; ++i) {
    assert(local_depths_[i] <= global_depth_);
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  ++local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  --local_depths_[bucket_idx];
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return GetDepthMask(global_depth_); }
auto ExtendibleHTableDirectoryPage::GetDepthMask(uint32_t depth) -> uint32_t { return ((1 << depth) - 1); }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  return GetDepthMask(local_depths_[bucket_idx]);
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return HTABLE_DIRECTORY_ARRAY_SIZE; }
auto ExtendibleHTableDirectoryPage::ForwardSplitImageIdx(uint32_t bucket_idx) const -> uint32_t {
  // NOTE: invariant:
  // for all idx (s.t. LD < GD): (#ptrs to idx = 2^(GD-LG))
  // so each iteration we only need to map one more!

  // should be called after the growth of global depth
  return bucket_idx | (1 << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::BackwardSplitImageIdx(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx & ~(1 << (global_depth_ - 1));
}

}  // namespace bustub
