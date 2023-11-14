//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = cat->GetTableIndexes(table_info->name_);

  Tuple tuple_to_delete;
  RID rid_to_remove;
  int32_t row_cnt = 0;

  // its child is a sequential scan or a filter
  while (child_executor_->Next(&tuple_to_delete, &rid_to_remove)) {
    table_info->table_->UpdateTupleMeta({std::time(nullptr), true}, rid_to_remove);  // remove the tuple
    // update index
    for (auto &index : indexes) {
      auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
      auto key_to_remove =
          tuple_to_delete.KeyFromTuple(table_info->schema_, index->key_schema_, idx_spanning_col_attrs);
      index->index_->DeleteEntry(key_to_remove, rid_to_remove, exec_ctx_->GetTransaction());  // rid is unused
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
