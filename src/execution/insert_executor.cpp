//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_oid = plan_->GetTableOid();
  auto *cat = exec_ctx_->GetCatalog();
  auto *table_info = cat->GetTable(table_oid);
  auto indexes = cat->GetTableIndexes(table_info->name_);

  Tuple tuple_to_insert;
  RID tmp_rid;  // we did not use this variable
  int32_t row_cnt = 0;

  while (child_executor_->Next(&tuple_to_insert, &tmp_rid)) {

    // NOTE(jens) check for index conflict before insertion to prevent works in vain
    // the task only has one primary key index
    assert(indexes.empty() || indexes.size() == 1);
    for (auto &index : indexes) {
      assert(index->is_primary_key_);
      if (index->is_primary_key_) {
        auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
        assert(idx_spanning_col_attrs.size() == 1);
        auto key_to_insert =
            tuple_to_insert.KeyFromTuple(table_info->schema_, index->key_schema_, idx_spanning_col_attrs);
        auto ht_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index->index_.get());
        assert(ht_index != nullptr);
        std::vector<RID> result;
        ht_index->ScanKey(key_to_insert, &result, exec_ctx_->GetTransaction());
        assert(result.empty() || result.size() == 1);
        if (!result.empty() && result[0].GetPageId() != INVALID_PAGE_ID) {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("insert executor: write-write conflict");
        }
      }
    }

    // timestamp as transaction id, meaning this transaction is modifying the tuple in table heap
    // and the result is only local until it commits.
    // the table heap's insert function synchronises the insertion so that no two inserts can have the same rid
    auto inserted_rid =
        table_info->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionId(), false}, tuple_to_insert,
                                        exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->GetTableOid());
    if (!inserted_rid.has_value()) {
      continue;
    }

    // set empty version link, since this is an insert and no president tuple
    // other txns cannot see it
    exec_ctx_->GetTransactionManager()->UpdateVersionLink(*inserted_rid, std::nullopt);
    // append to write list so that commit can finalise these entries by changing its ts to commit ts
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info->oid_, *inserted_rid);

    // insert index
    assert(indexes.empty() || indexes.size() == 1);
    for (auto &index : indexes) {
      assert(index->is_primary_key_);
      if (index->is_primary_key_) {
        auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
        assert(idx_spanning_col_attrs.size() == 1);
        auto key_to_insert =
            tuple_to_insert.KeyFromTuple(table_info->schema_, index->key_schema_, idx_spanning_col_attrs);
        if (!index->index_->InsertEntry(key_to_insert, *inserted_rid, exec_ctx_->GetTransaction())) {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("insert executor: write-write conflict. some txn snicked in");
        }
      } else {
        // TODO(jens): maybe implement this
      }
    }

    ++row_cnt;
  }

  // send the result
  *tuple = Tuple({{TypeId::INTEGER, row_cnt}}, &GetOutputSchema());

  if (row_cnt != 0) {
    // has more than one row inserted which means that
    // the next call it not the first of the batch
    batch_begin_ = false;
    return true;
  }
  if (batch_begin_) {
    // inserting nothing to the table, should return true to report the result (tuple 0)
    // otherwise the caller would think it as an end signal and discard the result
    batch_begin_ = false;
    return true;
  }
  // not the beginning of the batch and zero rows inserted -> end of this batch.
  batch_begin_ = true;  // reset the flag for the next batch
  return false;
}

}  // namespace bustub
