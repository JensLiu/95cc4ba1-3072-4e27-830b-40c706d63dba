//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  max_size_ = max_size;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  int idx = Key2Idx(key, cmp);
  if (idx == -1) {
    return false;
  }
  value = array_[idx].second;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (size_ == max_size_) {
    return false;
  }

  if (Key2Idx(key, cmp) != -1) {
    return false;
  }

  int idx = GetOneSlot();
  assert(idx != -1);
  array_[idx] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  // we should not keep the bucket compact
  int idx = Key2Idx(key, cmp);
  if (idx == -1) {
    return false;
  }
  PutOneSlot(idx);
  return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  PutOneSlot(bucket_idx);
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  assert(bucket_idx < size_);
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  assert(bucket_idx < size_);
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  assert(bucket_idx < size_);
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Key2Idx(const K &key, const KC &cmp) const -> int {
  for (int i = 0; i < static_cast<int>(size_); ++i) {
    auto &pair = array_[i];
    if (cmp(key, pair.first) == 0) {
      return i;
    }
  }
  return -1;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::GetOneSlot() -> int {
  return size_ < max_size_ ? static_cast<int>(size_++) : -1;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::PutOneSlot(int idx) {
  // NOTE: this is expensive, we can use bitmap
  //  since we have no means of tracking whether a slot is free nor not we have to do a
  //  move now, otherwise adding additional metadata corrupts the data format that are stored on the disk
  assert(idx >= 0 && idx < static_cast<int>(size_));
  --size_;
  for (int i = idx; i < static_cast<int>(size_); ++i) {
    array_[i] = array_[i + 1];
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::RemoveAll() {
  size_ = 0;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::InsertWithoutCheck(const K &key, const V &value) {
  assert(size_ <= max_size_);
  array_[GetOneSlot()] = {key, value};
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;

template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
