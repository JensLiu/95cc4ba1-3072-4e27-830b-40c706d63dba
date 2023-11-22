//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define MERGE_ONLY_ON_EMPTY
#define MERGE_ONLY_ON_SAME_DEPTH

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // initialisation
  auto header_page_guard = bpm_->NewPageGuarded(&header_page_id_);  // page guard will be dropped when out of scope
  auto *header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  //  std::cout << "hash table: header max depth: " << header_max_depth << " directory max depth: " <<
  //  directory_max_depth
  //            << " bucket max size: " << bucket_max_size << "\n";
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);
  //  std::cout << "get value: " << key << "\n";

  ReadPageGuard header_guard = HeaderPage().UpgradeRead();
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash)));
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard dir_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_guard.As<ExtendibleHTableDirectoryPage>();
  auto buk_page_id = dir_page->GetBucketPageId(dir_page->HashToBucketIndex(hash));
  if (buk_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard buk_guard = bpm_->FetchPageRead(buk_page_id);
  const auto *buk_page = buk_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  bool has_value;
  has_value = buk_page->Lookup(key, value, cmp_);
  assert(result->empty());
  result->push_back(value);
  if (!has_value) {
    // erase the vector in case of invalid values being used
    result->erase(result->begin(), result->end());
    return false;
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto ret = InsertOptimistically(key, value, transaction);
  if (ret == CrabbingRetType::REDO_PESSIMISTICALLY) {
    ret = InsertPessimistically(key, value, transaction);
  }
  if (ret == CrabbingRetType::SUCCESS) {
    return true;
  }
  return false;
}

// crabbing algorithm
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertOptimistically(const K &key, const V &value, Transaction *transaction)
    -> CrabbingRetType {
  auto hash = Hash(key);

  //  std::cout << "insert <" << key << ", " << value << ">\n";

  // holding read lock all the way
  ReadPageGuard header_guard = HeaderPage().UpgradeRead();
  const auto *header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto opt = static_cast<std::optional<BasicPageGuard>>(Hash2DirPageOrNull(hash, header_page));
  if (!opt.has_value()) {
    return CrabbingRetType::REDO_PESSIMISTICALLY;
  }
  ReadPageGuard dir_guard = opt->UpgradeRead();

  header_guard.Drop();

  const auto *dir_page = dir_guard.As<ExtendibleHTableDirectoryPage>();
  opt = static_cast<std::optional<BasicPageGuard>>(Hash2BukPageOrNull(hash, dir_page));
  if (!opt.has_value()) {
    return CrabbingRetType::REDO_PESSIMISTICALLY;
  }

  // NOTE: should not drop dir guard here since another thread on the pessimistic path may update
  //  the directory if they can get the write guard.

  // hold write lock
  WritePageGuard buk_guard = opt->UpgradeWrite();

  auto *buk_page =
      static_cast<ExtendibleHTableBucketPage<K, V, KC> *>(buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>());

  V val;
  bool has_value;
  has_value = buk_page->Lookup(key, val, cmp_);
  if (has_value) {
    return CrabbingRetType::FAILURE;
  }

  // fast: has slot in the bucket
  if (buk_page->Insert(key, value, cmp_)) {
    return CrabbingRetType::SUCCESS;
  }

  return CrabbingRetType::REDO_PESSIMISTICALLY;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertPessimistically(const K &key, const V &value, Transaction *transaction)
    -> DiskExtendibleHashTable::CrabbingRetType {
  // NOTE: because we do not know if we need to modify the mapping or not, so we take a conservative approach

  auto hash = Hash(key);
  // holding read lock all the way
  WritePageGuard header_guard = HeaderPage().UpgradeWrite();
  auto *header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  WritePageGuard dir_guard = Hash2DirPageOrAllocate(hash, header_page).UpgradeWrite();

  // since the header page would never drop
  header_guard.Drop();

  // slow: do split
  if (SplitUntilFit(key, value, dir_guard)) {
    return CrabbingRetType::SUCCESS;
  }

  return CrabbingRetType::FAILURE;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitUntilFit(const K &key, const V &value, WritePageGuard &dir_guard) -> bool {
  auto hash = Hash(key);
  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  while (true) {
    auto should_hash_to_buk_idx = dir_page->HashToBucketIndex(hash);
    WritePageGuard should_hash_to_buk_guard = Hash2BukPageOrAllocate(hash, dir_page).UpgradeWrite();
    auto *should_hash_to_buk_page = static_cast<ExtendibleHTableBucketPage<K, V, KC> *>(
        should_hash_to_buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>());
    if (should_hash_to_buk_page->Insert(key, value, cmp_)) {
      return true;
    }

    // if still has no space, then split
    if (!LocalCanSplit(should_hash_to_buk_idx, dir_page)) {
      return false;
    }
    WritePageGuard split_buk_guard = SplitBucket(dir_guard, should_hash_to_buk_guard, should_hash_to_buk_idx);
  }

  assert(0);  // return
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::LocalCanSplit(uint32_t buk_idx, ExtendibleHTableDirectoryPage *dir_page)
    -> bool {
  return dir_page->GetLocalDepth(buk_idx) < dir_page->GetMaxDepth();
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(WritePageGuard &dir_guard, WritePageGuard &buk_guard,
                                                    uint32_t buk_idx) -> WritePageGuard {
  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto *buk_page = buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  assert(dir_page->GetLocalDepth(buk_idx) < dir_page->GetMaxDepth());
  assert(dir_page->GetLocalDepth(buk_idx) <= dir_page->GetGlobalDepth());

  // increase bucket depth in the directory page
  dir_page->IncrLocalDepth(buk_idx);

  // increase global depth, this takes care of mapping pointers of split images
  auto buk_local_depth = dir_page->GetLocalDepth(buk_idx);
  if (buk_local_depth > dir_page->GetGlobalDepth()) {
    // only increase global depth when local depth is greater than it
    dir_page->IncrGlobalDepth();
  }

  auto buk_split_idx = dir_page->GetSplitImageIndex(buk_idx);

  // create a split bucket and map the split image to the newly split bucket
  page_id_t buk_split_page_id;
  WritePageGuard buk_split_guard = AllocateOneBucketPage(&buk_split_page_id).UpgradeWrite();
  auto *buk_split_page = buk_split_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  dir_page->SetBucketPageId(buk_split_idx, buk_split_page_id);
  dir_page->SetLocalDepth(buk_split_idx, buk_local_depth);  // sync depth

  // redistribute contents among the two buckets
  std::vector<std::pair<K, V>> all_pairs;
  for (size_t i = 0; i < buk_page->Size(); ++i) {
    all_pairs.push_back(buk_page->EntryAt(i));
  }

  // worst time complexity is O(#elems) if we just empty the original bucket and insert them as new
  // rather than calling RemoveAt for every unfitted element, the worse time complexity of which
  // is O(#elems^2) since RemoveAt shifts elements
  buk_page->RemoveAll();  // RemoveAll sets size_ to 0, and does nothing else
  for (auto &pair : all_pairs) {
    auto pair_buk_idx = dir_page->HashToBucketIndex(Hash(pair.first));
    if (pair_buk_idx == buk_idx) {
      buk_page->InsertWithoutCheck(pair.first, pair.second);
    } else {
      assert(pair_buk_idx == buk_split_idx);
      buk_split_page->InsertWithoutCheck(pair.first, pair.second);
    }
  }

  //  dir_page->PrintDirectory();

  return buk_split_guard;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::CombineTwoBuckets(uint32_t lower_buk_idx, WritePageGuard &lower_buk_guard,
                                                          uint32_t upper_buk_idx, WritePageGuard &&upper_buk_guard,
                                                          WritePageGuard &dir_guard) -> bool {
  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  assert(lower_buk_idx < upper_buk_idx);

#ifdef MERGE_ONLY_ON_SAME_DEPTH
  if (dir_page->GetLocalDepth(lower_buk_idx) != dir_page->GetLocalDepth(upper_buk_idx)) {
    return false;
  }
#endif

  auto *lower_buk_page = static_cast<ExtendibleHTableBucketPage<K, V, KC> *>(
      lower_buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>());

  auto *upper_buk_page = static_cast<ExtendibleHTableBucketPage<K, V, KC> *>(
      upper_buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>());

  assert(lower_buk_page != nullptr && upper_buk_page != nullptr);

  auto lower_buk_page_id = lower_buk_guard.PageId();
  auto upper_buk_page_id = upper_buk_guard.PageId();
  assert(lower_buk_page_id == dir_page->GetBucketPageId(lower_buk_idx));
  assert(upper_buk_page_id == dir_page->GetBucketPageId(upper_buk_idx));

#ifdef MERGE_ONLY_ON_EMPTY
  if (lower_buk_page->Size() != 0 && upper_buk_page->Size() != 0) {
#else
  if (lower_buk_page->Size() + upper_buk_page->Size() > bucket_max_size_) {
#endif
    upper_buk_guard.Drop();  // drop it (release latch) to allow the caller to use std::move
    return false;
  }

  // combinable
  // move all contents from upper indexed buk to lower indexed buk
  for (uint32_t i = 0; i < upper_buk_page->Size(); ++i) {
    auto &pair = upper_buk_page->EntryAt(i);
    lower_buk_page->InsertWithoutCheck(pair.first, pair.second);
  }
  dir_page->DecrLocalDepth(lower_buk_idx);
  // we should first drop the guard to decrease reference count, otherwise would not be able to delete
  upper_buk_guard.Drop();

  // since we hold write lock, no other worker has the page open, thus we can delete it
  bool delete_successful = bpm_->DeletePage(upper_buk_page_id);  // should not use upper buk guard
  assert(delete_successful);

  dir_page->SetBucketPageId(upper_buk_idx, lower_buk_page_id);
  dir_page->SetLocalDepth(upper_buk_idx, dir_page->GetLocalDepth(lower_buk_idx));

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  //  dir_page->PrintDirectory();

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBuckets(uint32_t buk_idx, WritePageGuard &&buk_guard,
                                                     WritePageGuard &dir_guard) {
  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  while (true) {
    auto buk_page_id = dir_page->GetBucketPageId(buk_idx);
    auto buk_img_idx = dir_page->GetSplitImageIndex(buk_idx);
    auto buk_img_page_id = dir_page->GetBucketPageId(buk_img_idx);
    if (buk_page_id == buk_img_page_id) {
      break;
    }
    WritePageGuard buk_img_guard = bpm_->FetchPageWrite(buk_img_page_id);
    bool keep_combining = false;
    if (buk_idx < buk_img_idx) {
      keep_combining = CombineTwoBuckets(buk_idx, buk_guard, buk_img_idx, std::move(buk_img_guard), dir_guard);
    } else {  // buk_idx > buk_img_idx
      keep_combining = CombineTwoBuckets(buk_img_idx, buk_img_guard, buk_idx, std::move(buk_guard), dir_guard);
      buk_idx = buk_img_idx;
      buk_guard = std::move(buk_img_guard);
    }
    if (!keep_combining) {
      break;
    }
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // NOTE: we need to keep a reference of the page guard, otherwise they will be dropped
  //  and locks will be released while we are still using the page's data!
  auto hash = Hash(key);
  //  std::cout << "remove: " << key << "\n";

  WritePageGuard header_guard = HeaderPage().UpgradeWrite();
  auto *header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(dir_idx));
  if (dir_page_id == INVALID_PAGE_ID) {
    //    std::cout << "--warning: there's no dir page for index " << dir_idx << " for key " << key << "\n";
    return false;
  }

  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_page_id);
  header_guard.Drop();

  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto buk_idx = dir_page->HashToBucketIndex(hash);
  auto buk_page_id = dir_page->GetBucketPageId(buk_idx);
  if (buk_page_id == INVALID_PAGE_ID) {
    //    std::cout << "--warning: there's no buk page for index " << dir_idx << " for key " << key << "\n";
    return false;
  }

  WritePageGuard buk_guard = bpm_->FetchPageWrite(buk_page_id);
  auto *buk_page =
      static_cast<ExtendibleHTableBucketPage<K, V, KC> *>(buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>());

  bool remove_successful = buk_page->Remove(key, cmp_);
  if (!remove_successful) {
    return false;
  }

  MergeBuckets(buk_idx, std::move(buk_guard), dir_guard);

  return true;
}

// helper functions

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::HeaderPage() const -> BasicPageGuard {
  return bpm_->FetchPageBasic(header_page_id_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash2DirPageOrAllocate(const uint32_t &hash,
                                                               ExtendibleHTableHeaderPage *header_page, bool *allocated)
    -> BasicPageGuard {
  assert(header_page != nullptr);

  uint32_t dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(dir_idx));
  if (dir_page_id == INVALID_PAGE_ID) {
    AllocateOneDirectoryPage(&dir_page_id);
    header_page->SetDirectoryPageId(dir_idx, dir_page_id);
    if (allocated != nullptr) {
      *allocated = true;
    }
  }
  return bpm_->FetchPageBasic(dir_page_id);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash2BukPageOrAllocate(const uint32_t &hash,
                                                               ExtendibleHTableDirectoryPage *dir_page, bool *allocated)
    -> BasicPageGuard {
  assert(dir_page != nullptr);
  auto buk_idx = dir_page->HashToBucketIndex(hash);
  page_id_t buk_page_id = dir_page->GetBucketPageId(buk_idx);
  if (buk_page_id == INVALID_PAGE_ID) {
    AllocateOneBucketPage(&buk_page_id);
    dir_page->SetBucketPageId(buk_idx, buk_page_id);
    if (allocated != nullptr) {
      *allocated = true;
    }
  }

  return bpm_->FetchPageBasic(buk_page_id);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash2BukPageOrNull(const uint32_t &hash,
                                                           const ExtendibleHTableDirectoryPage *dir_page) const
    -> std::optional<BasicPageGuard> {
  assert(dir_page != nullptr);
  auto buk_idx = dir_page->HashToBucketIndex(hash);
  page_id_t buk_page_id = dir_page->GetBucketPageId(buk_idx);
  if (buk_page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  BasicPageGuard guard = bpm_->FetchPageBasic(buk_page_id);
  if (guard.GetData() == nullptr) {
    assert(0);
    return std::nullopt;
  }
  return std::make_optional<>(std::move(guard));
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash2DirPageOrNull(const uint32_t &hash,
                                                           const ExtendibleHTableHeaderPage *header_page) const
    -> std::optional<BasicPageGuard> {
  assert(header_page != nullptr);

  uint32_t dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(dir_idx));
  if (dir_page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  return std::make_optional<>(bpm_->FetchPageBasic(dir_page_id));
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash2BukOrFail(const uint32_t &hash,
                                                       const ExtendibleHTableDirectoryPage *dir_page) const
    -> BasicPageGuard {
  auto opt = Hash2BukPageOrNull(hash, dir_page);
  assert(opt.has_value());
  return std::move(opt.value());
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::AllocateOneDirectoryPage(page_id_t *page_id) -> BasicPageGuard {
  page_id_t page_id_local;
  BasicPageGuard dir_guard = bpm_->NewPageGuarded(&page_id_local);
  auto *dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);  // NOTE: init bucket page
  *page_id = page_id_local;
  return dir_guard;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::AllocateOneBucketPage(page_id_t *page_id) -> BasicPageGuard {
  page_id_t page_id_local;
  BasicPageGuard buk_guard = bpm_->NewPageGuarded(&page_id_local);
  auto *buk_page = buk_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  buk_page->Init(bucket_max_size_);  // NOTE: init bucket page
  *page_id = page_id_local;
  return buk_guard;
}

// unused apis

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::RemovePessimistically(const K &key, Transaction *transaction)
    -> DiskExtendibleHashTable::CrabbingRetType {
  return DiskExtendibleHashTable::CrabbingRetType::FAILURE;
}
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::RemoveOptimistically(const K &key, Transaction *transaction)
    -> DiskExtendibleHashTable::CrabbingRetType {
  return DiskExtendibleHashTable::CrabbingRetType::FAILURE;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
