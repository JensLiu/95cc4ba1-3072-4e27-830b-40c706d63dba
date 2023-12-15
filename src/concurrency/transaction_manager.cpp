//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "libfort/lib/fort.hpp"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // NOTE(jens): can have multiple txns of the same resd_ts value, since the version they pulled are the same
  txn_ref->read_ts_.store(last_commit_ts_);
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // traverse the write set to update written entries
  for (const auto &[oid, rids] : txn->GetWriteSets()) {
    auto &table = catalog_->GetTable(oid)->table_;
    for (const auto &rid : rids) {
      const auto &[meta, tuple] = table->GetTuple(rid);
      table->UpdateTupleMeta(TupleMeta{commit_ts, meta.is_deleted_}, rid);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.store(commit_ts);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // mark unused undo link

  // we need to collect all modified table heap
  struct GCValue {
    std::unordered_set<RID> rids_;
  };

  std::map<table_oid_t, GCValue> gc_list;
  // collect modified table and rids
  // expensive when tuples most transactions writes to are mostly the same
  for (const auto &txn : txn_map_) {
    // for each transaction
    auto ws = txn.second->GetWriteSets();
    for (const auto &[oid, w_rids] : ws) {
      // for each write record, collect their table and rids
      if (auto itr = gc_list.find(oid); itr == gc_list.end()) {
        std::unordered_set<RID> rids;
        for (const auto &rid : w_rids) {
          rids.insert(rid);
        }
        gc_list[oid] = {rids};
      } else {
        auto &rids = itr->second.rids_;
        for (const auto &rid : w_rids) {
          rids.insert(rid);
        }
      }
    }
  }

  // traverse the rids
  for (const auto &[oid, gc_val] : gc_list) {
    const auto table_info = catalog_->GetTable(oid);
    for (const auto rid : gc_val.rids_) {
      const auto &[base_meta, base_tuple] = table_info->table_->GetTuple(rid);
      VersionLinkGarbageCollection(rid, base_meta);
      // TxnMgrDbg("after gc", this, table_info, table_info->table_.get());
    }
  }

  // clean committed transactions whose undo logs are all invalid
  auto itr = txn_map_.begin();
  while (itr != txn_map_.end()) {
    const auto txn_state = itr->second->state_.load();
    const auto txn_read_ts = itr->second->read_ts_.load();

    bool removable = true;
    if (txn_state != TransactionState::COMMITTED && txn_state != TransactionState::ABORTED) {
      assert(txn_read_ts >= running_txns_.GetWatermark());
      removable = false;
    } else {
      // committed txn, check if it holds undo logs used by other transactions
      removable = true;
      for (const auto &log : itr->second->undo_logs_) {
        if (log.ts_ != INVALID_TS) {
          removable = false;
        }
      }
    }
    if (removable) {
      const auto remove_itr = itr++;
      txn_map_.erase(remove_itr);
    } else {
      ++itr;
    }
  }
}

void TransactionManager::VersionLinkGarbageCollection(const RID &rid, const TupleMeta &base_meta) {
  const auto watermark = running_txns_.GetWatermark();
  const auto version_link = GetVersionLinkPtrUnsafe(rid);
  if (version_link == nullptr) {
    return;
  }
  assert(version_link->prev_ == GetVersionLink(rid)->prev_);

  // traverse the version link
  UndoLink *link = &version_link->prev_;

  std::list<UndoLink *> gc_links;
  while (link->IsValid()) {
    const auto log = GetUndoLogPtrUnsafe(*link);
    assert(log->prev_version_ == GetUndoLog(*link).prev_version_);
    if (log->ts_ <= watermark) {
      gc_links.emplace_back(link);
    }
    link = &log->prev_version_;
  }

  if (gc_links.empty()) {
    return;
  }

  // if the base tuple's ts is less or equals to the watermark, it should be chosen as the record
  // so we do not need any of the version links. otherwise we should resort to version link for tuples
  if (!(base_meta.ts_ < TXN_START_ID && base_meta.ts_ <= watermark)) {
    gc_links.pop_front();
    // we want the front to point to an invalid link, not invalidate the front...
    // so it should be the second element
  }

  for (const auto &link_ptr : gc_links) {
    auto itr = txn_map_.find(link_ptr->prev_txn_);
    assert(itr != txn_map_.end());
    itr->second->undo_logs_[link_ptr->prev_log_idx_].ts_ =
        INVALID_TS;                        // mark with invalid ts to indicate that it is invisible
    link_ptr->prev_txn_ = INVALID_TXN_ID;  // invalidate along the way
  }
}

auto TransactionManager::GetVersionLinkPtrUnsafe(RID rid) -> VersionUndoLink * {
  auto iter = version_info_.find(rid.GetPageId());
  if (iter == version_info_.end()) {
    return nullptr;
  }
  std::shared_ptr<PageVersionInfo> pg_ver_info = iter->second;
  auto iter2 = pg_ver_info->prev_version_.find(rid.GetSlotNum());
  if (iter2 == pg_ver_info->prev_version_.end()) {
    return nullptr;
  }
  auto &version_link = iter2->second;
  return &version_link;
}
auto TransactionManager::GetUndoLogPtrUnsafe(const UndoLink &link) -> UndoLog * {
  auto iter = txn_map_.find(link.prev_txn_);
  if (iter == txn_map_.end()) {
    return nullptr;
  }
  auto txn = iter->second;
  std::cout << "undo log found in " << txn->GetTransactionId() - TXN_START_ID << "\n";
  return &txn->undo_logs_[link.prev_log_idx_];
}

}  // namespace bustub
