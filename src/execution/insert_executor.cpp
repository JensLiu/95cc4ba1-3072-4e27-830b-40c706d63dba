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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

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
    auto inserted_rid =
        table_info->table_->InsertTuple(/* TupleMeta */ {std::time(nullptr), false}, tuple_to_insert,
                                        exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->GetTableOid());
    if (!inserted_rid.has_value()) {
      continue;
    }
    for (auto &index : indexes) {
      auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
      auto key_to_insert =
          tuple_to_insert.KeyFromTuple(table_info->schema_, index->key_schema_, idx_spanning_col_attrs);
      index->index_->InsertEntry(key_to_insert, inserted_rid.value(), exec_ctx_->GetTransaction());
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
