//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  auto indexes = cat->GetTableIndexes(table_info_->name_);

  Tuple tuple_to_delete;
  RID rid_to_remove;
  int32_t row_cnt = 0;

  // its child is a sequential scan or a filter
  while (child_executor_->Next(&tuple_to_delete, &rid_to_remove)) {
    // update tuple
    std::vector<Value> updated_values;
    for (auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&tuple_to_delete, table_info_->schema_));
    }

    Tuple tuple_to_insert{updated_values, &table_info_->schema_};

    auto &table = table_info_->table_;
    table->UpdateTupleMeta({std::time(nullptr), true}, rid_to_remove);  // remove the tuple
    auto rid_inserted = table->InsertTuple({std::time(nullptr), false}, {updated_values, &table_info_->schema_},
                                           exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info_->oid_);

    if (!rid_inserted.has_value()) {
      continue;
    }

    // update index
    for (auto &index : indexes) {
      auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
      auto key_to_remove =
          tuple_to_delete.KeyFromTuple(table_info_->schema_, index->key_schema_, idx_spanning_col_attrs);
      auto key_to_insert =
          tuple_to_insert.KeyFromTuple(table_info_->schema_, index->key_schema_, idx_spanning_col_attrs);
      index->index_->DeleteEntry(key_to_remove, rid_to_remove, exec_ctx_->GetTransaction());  // rid is unused
      index->index_->InsertEntry(key_to_insert, rid_inserted.value(), exec_ctx_->GetTransaction());
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
