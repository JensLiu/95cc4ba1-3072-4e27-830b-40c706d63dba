//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.h
//
// Identification: src/include/container/disk/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * Implementation of extendible hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table grows/shrinks dynamically as buckets become full/empty.
 */
template <typename K, typename V, typename KC>
class DiskExtendibleHashTable {
 public:
  /**
   * @brief Creates a new DiskExtendibleHashTable.
   *
   * @param name
   * @param bpm buffer pool manager to be used
   * @param cmp comparator for keys
   * @param hash_fn the hash function
   * @param header_max_depth the max depth allowed for the header page
   * @param directory_max_depth the max depth allowed for the directory page
   * @param bucket_max_size the max size allowed for the bucket page array
   */
  explicit DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm, const KC &cmp,
                                   const HashFunction<K> &hash_fn, uint32_t header_max_depth = HTABLE_HEADER_MAX_DEPTH,
                                   uint32_t directory_max_depth = HTABLE_DIRECTORY_MAX_DEPTH,
                                   uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)));

  /** TODO(P2): Add implementation
   * Inserts a key-value pair into the hash table.
   *
   * @param key the key to create
   * @param value the value to be associated with the key
   * @param transaction the current transaction
   * @return true if insert succeeded, false otherwise
   */
  auto Insert(const K &key, const V &value, Transaction *transaction = nullptr) -> bool;

  /** TODO(P2): Add implementation
   * Removes a key-value pair from the hash table.
   *
   * @param key the key to delete
   * @param value the value to delete
   * @param transaction the current transaction
   * @return true if remove succeeded, false otherwise
   */
  auto Remove(const K &key, Transaction *transaction = nullptr) -> bool;

  /** TODO(P2): Add implementation
   * Get the value associated with a given key in the hash table.
   *
   * Note(fall2023): This semester you will only need to support unique key-value pairs.
   *
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @param transaction the current transaction
   * @return the value(s) associated with the given key
   */
  auto GetValue(const K &key, std::vector<V> *result, Transaction *transaction = nullptr) const -> bool;

  /**
   * Helper function to verify the integrity of the extendible hash table's directory.
   */
  void VerifyIntegrity() const;

  /**
   * Helper function to expose the header page id.
   */
  auto GetHeaderPageId() const -> page_id_t;

  /**
   * Helper function to print out the HashTable.
   */
  void PrintHT() const;

 private:
  /**
   * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
   * for extendible hashing.
   *
   * @param key the key to hash
   * @return the down-casted 32-bit hash
   */
  auto Hash(K key) const -> uint32_t;

  auto InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx, uint32_t hash, const K &key,
                            const V &value) -> bool;

  auto InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx, const K &key, const V &value)
      -> bool;

  void UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx,
                              page_id_t new_bucket_page_id, uint32_t new_local_depth, uint32_t local_depth_mask);

  void MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                      ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                      uint32_t local_depth_mask);

  auto SplitBucket(WritePageGuard &dir_guard, WritePageGuard &buk_guard, uint32_t buk_idx) -> WritePageGuard;

  auto SplitUntilFit(const K &key, const V &value, WritePageGuard &dir_guard) -> bool;

  auto LocalCanSplit(uint32_t buk_idx, ExtendibleHTableDirectoryPage *dir_page) -> bool;

  auto CombineTwoBuckets(uint32_t lower_buk_idx, WritePageGuard &lower_buk_guard, uint32_t upper_buk_idx,
                         WritePageGuard &&upper_buk_guard, WritePageGuard &dir_guard) -> bool;

  void MergeBuckets(uint32_t buk_idx, WritePageGuard &&buk_guard, WritePageGuard &dir_guard);

  void MergeOnlyWhenBothEmpty(uint32_t buk_idx, WritePageGuard &&buk_guard, WritePageGuard &dir_guard);

  // member variables
  std::string index_name_;
  BufferPoolManager *bpm_;
  KC cmp_;
  HashFunction<K> hash_fn_;
  uint32_t header_max_depth_;
  uint32_t directory_max_depth_;
  uint32_t bucket_max_size_;
  page_id_t header_page_id_;

  // crabbing algorithm
  enum class CrabbingRetType {
    FAILURE = 0,
    SUCCESS,
    REDO_PESSIMISTICALLY,
  };
  auto InsertOptimistically(const K &key, const V &value, Transaction *transaction) -> CrabbingRetType;
  auto InsertPessimistically(const K &key, const V &value, Transaction *transaction) -> CrabbingRetType;

  auto RemoveOptimistically(const K &key, Transaction *transaction = nullptr) -> CrabbingRetType;
  auto RemovePessimistically(const K &key, Transaction *transaction = nullptr) -> CrabbingRetType;

  // helper functions
  auto HeaderPage() const -> BasicPageGuard;
  auto Hash2DirPageOrAllocate(const uint32_t &hash, ExtendibleHTableHeaderPage *header_page, bool *allocated = nullptr)
      -> BasicPageGuard;
  auto Hash2BukPageOrAllocate(const uint32_t &hash, ExtendibleHTableDirectoryPage *dir_page, bool *allocated = nullptr)
      -> BasicPageGuard;
  auto Hash2DirPageOrNull(const uint32_t &hash, const ExtendibleHTableHeaderPage *header_page) const
      -> std::optional<BasicPageGuard>;
  auto Hash2BukPageOrNull(const uint32_t &hash, const ExtendibleHTableDirectoryPage *dir_page) const
      -> std::optional<BasicPageGuard>;
  auto Hash2BukOrFail(const uint32_t &hash, const ExtendibleHTableDirectoryPage *dir_page) const -> BasicPageGuard;
  auto AllocateOneDirectoryPage(page_id_t *page_id) -> BasicPageGuard;
  auto AllocateOneBucketPage(page_id_t *page_id) -> BasicPageGuard;
};

}  // namespace bustub
