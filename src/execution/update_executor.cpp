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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  const auto txn = exec_ctx_->GetTransaction();
  auto txn_mngr = exec_ctx_->GetTransactionManager();
  auto indexes = cat->GetTableIndexes(table_info_->name_);
  auto &table = table_info_->table_;
  Tuple tuple_to_update;
  RID rid_to_update;
  int32_t row_cnt = 0;

  // NOTE: the child executor could be scan node, it may return the snapshot version of the tuple
  //  We will NOT update if the tuple is in the version link (read-only), because it is old
  //  and is shared among transactions.
  //  Updating it may result in overwriting the committed updates which have newer ts (future)!
  while (child_executor_->Next(&tuple_to_update, &rid_to_update)) {  // its child is a sequential scan or a filter
    // get the base tuple
    auto [base_meta, base_tuple] = table->GetTuple(rid_to_update);

    // check for write-write conflict
    if (IsWriteWriteConflict(base_meta, txn->GetReadTs(), txn->GetTransactionId())) {
      txn->SetTainted();
      throw ExecutionException("update executor: write-write conflict. trying to rewrite the future");
    }

    // we are now modifying the base tuple
    assert(IsTupleContentEqual(tuple_to_update, table->GetTuple(rid_to_update).second));

    // update tuple
    std::vector<Value> updated_values;
    for (auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&tuple_to_update, table_info_->schema_));
    }

    const auto first_modification = base_meta.ts_ != txn->GetTransactionId();
    const auto updated_tuple = Tuple{updated_values, &table_info_->schema_};

    // race condition: transactions with the same read ts wanting to modify the same tuple
    // we use atomic CAS function to update the tuple
    if (!table->UpdateTupleInPlace({txn->GetTransactionId(), false}, updated_tuple, rid_to_update,
                                   [txn](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
                                     return meta.ts_ < TXN_START_ID || meta.ts_ == txn->GetTransactionId();
                                   })) {
      txn->SetTainted();
      throw ExecutionException("update executor: write-write conflict, other transaction snicked in");
    }

    if (first_modification) {
      auto undo_log =
          GenerateDiffLog(tuple_to_update, updated_values, &table_info_->schema_,
                          base_meta.ts_);  // use ts from base meta because it might be older than read ts of the txn
      LinkIntoVersionChainCAS(undo_log, rid_to_update, txn, txn_mngr);
      txn->AppendWriteSet(table_info_->oid_, rid_to_update);
    } else {
      // if multiple updates take place, we may need to add some newly changed fields to the undo log
      const auto version_link = txn_mngr->GetVersionLink(rid_to_update);
      if (version_link.has_value()) {
        // get log index. since we has
        const auto log_idx = version_link.value().prev_.prev_log_idx_;
        // get the old diff log
        const auto old_diff_log = txn->GetUndoLog(log_idx);
        // update undo log in place
        txn->ModifyUndoLog(log_idx, UpdateDiffLog(base_tuple, updated_values, old_diff_log, &table_info_->schema_));
      }
      // it can have no undo logs and has multiple modifications,
      // e.g. after insert a txn (ts = txn id) do multiple updates (ts = txn id)
      // in this case we do not need to put any diff logs since there's no need
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
