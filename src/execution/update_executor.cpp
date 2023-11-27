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
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  auto cat = exec_ctx_->GetCatalog();
  auto table_info = cat->GetTable(plan_->GetTableOid());
  table_info_ = table_info;
  is_pk_attribute_modifier_ = IsModifyingPKAttributes();

  auto txn_mngr = exec_ctx_->GetTransactionManager();
  auto lk_mngr = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto indexes = cat->GetTableIndexes(table_info->name_);

  tuple_insert_handler_ = std::make_unique<TupleInsertHandler>(txn_mngr, lk_mngr, txn, table_info, indexes);
  tuple_delete_handler_ = std::make_unique<TupleDeleteHandler>(txn_mngr, txn, table_info);

  // implement as a pipeline blocker because it may update the primary key
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    update_buffer_.emplace_back(rid, tuple);
  }

  cursor_ = 0;
}

auto UpdateExecutor::IsModifyingPKAttributes() -> bool {
  std::unordered_set<int> pk_col_idxes;
  const auto &indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  const auto index_opt = GetPrimaryKeyIndex(indexes);
  if (!index_opt.has_value()) {
    return false;
  }
  const auto index = index_opt.value();
  for (const auto &col : index->key_schema_.GetColumns()) {
    auto col_idx = table_info_->schema_.GetColIdx(col.GetName());
    pk_col_idxes.insert(col_idx);
  }

  const auto &expressions = plan_->target_expressions_;

  assert(index != nullptr);
  for (int col_idx = 0; col_idx < expressions.size(); ++col_idx) {
    auto expr = expressions[col_idx];
    auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
    auto val_expr = dynamic_cast<ConstantValueExpression *>(expr.get());
    if (col_expr != nullptr) {
      // identity
      assert(col_expr->GetColIdx() == col_idx);
    } else {
      if (pk_col_idxes.count(col_idx) > 0) {
        return true;
      }
    }
  }
  return false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple_ret, RID * /*rid*/) -> bool {
  BLOCKING_EXECUTOR_BEGIN_EXEC

  int row_cnt = 0;

  if (is_pk_attribute_modifier_) {
    row_cnt = UpdateModifyingPKAttributes();
  } else {
    row_cnt = UpdateWithoutModifyingPKAttributes();
  }

  // send the result
  *tuple_ret = Tuple({{TypeId::INTEGER, row_cnt}}, &GetOutputSchema());

  BLOCKING_EXECUTOR_END_EXEC
}

auto UpdateExecutor::UpdateModifyingPKAttributes() -> int {
  const auto txn = exec_ctx_->GetTransaction();
  TxnMgrDbg("before deletion", exec_ctx_->GetTransactionManager(), table_info_, table_info_->table_.get());
  // NOTE(jens): the delete handler checks for write-write conflict and marks the
  //  tuple deleted in the table heap. it changes its ts to txn_id, so that
  //  operations after would not have any conflicts
  for (auto &[rid, tuple] : update_buffer_) {
    if (const auto &[ok, err_msg] = tuple_delete_handler_->DeleteTuple(rid); !ok) {
      txn->SetTainted();
      throw ExecutionException("update executor: " + err_msg);
    }
  }

  TxnMgrDbg("after deletion", exec_ctx_->GetTransactionManager(), table_info_, table_info_->table_.get());

  for (auto &[rid, tuple] : update_buffer_) {

    std::vector<Value> updated_values;
    for (auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&tuple, table_info_->schema_));
    }
    auto updated_tuple = Tuple{updated_values, &table_info_->schema_};

    if (const auto &[ok, err_msg] = tuple_insert_handler_->InsertTuple(updated_tuple); !ok) {
      txn->SetTainted();
      throw ExecutionException("update executor: " + err_msg);
    }
  }

  TxnMgrDbg("after insertion", exec_ctx_->GetTransactionManager(), table_info_, table_info_->table_.get());

  return update_buffer_.size();
}

auto UpdateExecutor::UpdateWithoutModifyingPKAttributes() -> int {
  auto cat = exec_ctx_->GetCatalog();
  const auto txn = exec_ctx_->GetTransaction();
  auto indexes = cat->GetTableIndexes(table_info_->name_);
  auto &table = table_info_->table_;

  // NOTE: the child executor could be scan node, it may return the snapshot version of the tuple
  //  We will NOT update if the tuple is in the version link (read-only), because it is old
  //  and is shared among transactions.
  //  Updating it may result in overwriting the committed updates which have newer ts (future)!
  while (cursor_ < update_buffer_.size()) {  // its child is a sequential scan or a filter

    auto &[rid, tuple] = update_buffer_[cursor_++];

    // get the base tuple
    auto [base_meta, base_tuple] = table->GetTuple(rid);

    // check for write-write conflict
    if (IsWriteWriteConflict(base_meta, txn->GetReadTs(), txn->GetTransactionId())) {
      txn->SetTainted();
      throw ExecutionException("update executor: write-write conflict. trying to rewrite the future");
    }

    // we are now modifying the base tuple
    TxnMgrDbg("before insert", exec_ctx_->GetTransactionManager(), table_info_, table_info_->table_.get());
    assert(IsTupleContentEqual(tuple, base_tuple));

    // update tuple
    if (const auto &[ok, err_msg] = HandleNonPKAttributesUpdate(base_meta, base_tuple, rid); !ok) {
      txn->SetTainted();
      throw ExecutionException("update executor: " + err_msg);
    }
  }

  return update_buffer_.size();
}

auto UpdateExecutor::HandleNonPKAttributesUpdate(TupleMeta &base_meta, Tuple &base_tuple, RID &rid)
    -> std::pair<bool, std::string> {
  const auto txn = exec_ctx_->GetTransaction();
  const auto txn_mngr = exec_ctx_->GetTransactionManager();
  auto &table = table_info_->table_;

  std::vector<Value> updated_values;
  for (auto &expr : plan_->target_expressions_) {
    updated_values.push_back(expr->Evaluate(&base_tuple, table_info_->schema_));
  }
  const auto updated_tuple = Tuple{updated_values, &table_info_->schema_};

  if (base_meta.ts_ != txn->GetTransactionId()) {  // first modification
    auto undo_log = GenerateDiffLog(
        base_tuple, updated_values, &table_info_->schema_, base_meta.ts_,
        base_meta.is_deleted_);  // use ts from base meta because it might be older than read ts of the txn
    VersionChainPushFrontCAS(undo_log, rid, txn, txn_mngr);
    txn->AppendWriteSet(table_info_->oid_, rid);
  } else {  // not the first modification
    // if multiple updates take place, we may need to add some newly changed fields to the undo log
    const auto version_link = txn_mngr->GetVersionLink(rid);
    if (version_link.has_value()) {
      // get log index. since we has
      const auto log_idx = version_link.value().prev_.prev_log_idx_;

      // NOTE(jens): since we added index support, it may be the case that a tuple is deleted and the current
      //  txn inserted it, so that the previous version is a delete marker. we do not want to update the delete marker
      //  because we consider this a different tuple, and logs before the delete marker belongs to the deleted tuple
      // get the old diff log
      const auto old_diff_log = txn->GetUndoLog(log_idx);
      if (!old_diff_log.is_deleted_) {
        // update undo log in place
        txn->ModifyUndoLog(log_idx, UpdateDiffLog(base_tuple, updated_values, old_diff_log, &table_info_->schema_));
      } else {
        assert(version_link->prev_.prev_txn_ == txn->GetTransactionId());
      }
    }
    // it can have no undo logs and has multiple modifications,
    // e.g. after insert a txn (ts = txn id) do multiple updates (ts = txn id)
    // in this case we do not need to put any diff logs since there's no need
  }

  // NOTE: race condition may happen and there might be duplicated log (with same ts and delta values)
  //  but it does not affect the integrity of the tuple. If we mark the tuple in the table heap first
  //  there will be a lost of old version between the tuple is marked ts = txn_id and the old log gets
  //  pushed into the version chain.
  if (!table->UpdateTupleInPlace({txn->GetTransactionId(), false}, updated_tuple, rid,
                                 [txn](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
                                   return meta.ts_ < TXN_START_ID || meta.ts_ == txn->GetTransactionId();
                                 })) {
    return {false, "update executor: write-write conflict, other transaction snicked in"};
  }
  return {true, ""};
}

}  // namespace bustub
