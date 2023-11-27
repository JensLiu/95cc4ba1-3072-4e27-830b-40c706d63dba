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

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();

  const auto txn_mngr = exec_ctx_->GetTransactionManager();
  const auto txn = exec_ctx_->GetTransaction();
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  tuple_delete_handler_ = std::make_unique<TupleDeleteHandler>(txn_mngr, txn, table_info);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = cat->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();

  Tuple tmp_tuple;
  RID rid_to_remove;
  int32_t row_cnt = 0;

  // its child is a sequential scan or a filter
  // a tuple can only be removed when its version is in the table heap
  while (child_executor_->Next(&tmp_tuple, &rid_to_remove)) {
    if (const auto &[ok, err_msg] = tuple_delete_handler_->DeleteTuple(rid_to_remove); !ok) {
      txn->SetTainted();
      throw ExecutionException("delete executor: " + err_msg);
    }

    ++row_cnt;
  }

  // send the result
  *tuple = Tuple({{TypeId::INTEGER, row_cnt}}, &GetOutputSchema());
  HANDLE_NON_BLOCKING_EXECUTOR_RETURN(row_cnt);
}

}  // namespace bustub
