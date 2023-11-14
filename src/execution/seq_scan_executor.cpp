//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // init iterator to track the cursor of this executor node
  auto cat = exec_ctx_->GetCatalog();
  auto table_info = cat->GetTable(plan_->GetTableOid());
  // construct the iterator as a heap object
  // operator++ of the iterator fetches the table everytime when called
  itr_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // since we are using iterator model, only one gets returned
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  for (; !itr_->IsEnd(); ++*itr_) {
    auto [meta, tpl] = itr_->GetTuple();
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ == nullptr ||
          plan_->filter_predicate_->Evaluate(&tpl, table_info->schema_).CastAs(TypeId::BOOLEAN).GetAs<bool>()) {
        *tuple = tpl;
        *rid = itr_->GetRID();
        ++*itr_;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
