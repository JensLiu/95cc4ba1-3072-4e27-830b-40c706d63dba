//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto cat = exec_ctx_->GetCatalog();
  auto index_info = cat->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  table_info_ = cat->GetTable(plan_->table_oid_);

  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(plan_->filter_predicate_.get());
  BUSTUB_ENSURE(cmp_expr != nullptr, "should have a comparison that's valid")
  BUSTUB_ENSURE(cmp_expr->comp_type_ == ComparisonType::Equal, "should have an equal comparison")
  BUSTUB_ENSURE(cmp_expr->children_.size() == 2, "should have exactly two children")
  const auto value = dynamic_cast<const ConstantValueExpression *>(cmp_expr->children_[1].get())->val_;
  // scan in the index
  htable->ScanKey({{value}, &index_info->key_schema_}, &result_, exec_ctx_->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // fetch from table

  while (cursor < result_.size()) {
    RID table_heap_rid = result_[cursor++];
    auto version_link = exec_ctx_->GetTransactionManager()->GetVersionLink(table_heap_rid);
    if (version_link.has_value() && version_link->in_progress_) {
      // in progress tuple cannot be accessed?
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("index scan executor: scanning tuple being updated by another txn");
    }
    auto [base_meta, base_tuple] = table_info_->table_->GetTuple(table_heap_rid);
    const auto tuple_snapshot = GetTupleSnapshot(exec_ctx_, plan_, base_meta, base_tuple);
    if (tuple_snapshot.has_value()) {
      *rid = table_heap_rid;
      *tuple = *tuple_snapshot;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
