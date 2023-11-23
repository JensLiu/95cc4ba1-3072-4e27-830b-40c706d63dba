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
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto &table = table_info->table_;
  auto indexes = cat->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mngr = exec_ctx_->GetTransactionManager();

  Tuple tuple_to_remove;
  RID rid_to_remove;
  int32_t row_cnt = 0;

  // its child is a sequential scan or a filter
  while (child_executor_->Next(&tuple_to_remove, &rid_to_remove)) {
    auto [base_meta, base_tuple] = table->GetTuple(rid_to_remove);
    if (IsWriteWriteConflict(base_meta, txn->GetReadTs(), txn->GetTransactionId())) {
      txn->SetTainted();
      throw ExecutionException("delete executor: write-write conflict. trying to delete the future");
    }

    // already passed the initial write-write conflict test

    // race condition: transactions with the same read ts wanting to modify the same tuple both saw that
    // the tuple is free to modify (passed the W-W conflict test because they have the same read ts) and
    // go appended the log

    // create undo log for the first delete
    if (base_meta.ts_ != txn->GetTransactionId()) {  // first modification
      auto undo_log = GenerateDeleteLog(tuple_to_remove, &table_info->schema_, base_meta.ts_);
      LinkIntoVersionChainCAS(undo_log, rid_to_remove, txn, txn_mngr);  // ATOMIC
      txn->AppendWriteSet(table_info->oid_, rid_to_remove);
    } else {
      // it may be updated and then deleted, thus we need to update the partial schema to record all changes
      const auto link = txn_mngr->GetUndoLink(rid_to_remove);
      if (link.has_value()) {
        const auto old_diff_log = txn_mngr->GetUndoLog(link.value());
        const auto tuple_snapshot = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, {old_diff_log});
        assert(tuple_snapshot.has_value());
        // use snapshot value because the current table heap tuple may be modified by the txn and should not be visible
        // to other transactions
        auto undo_log = GenerateDeleteLog(tuple_snapshot.value(), &table_info->schema_, old_diff_log.ts_);
        undo_log.prev_version_ = old_diff_log.prev_version_;  // link into the chain
        txn->ModifyUndoLog(link->prev_log_idx_, undo_log);    // ATOMIC
      }
    }

    // NOTE(jens): update the table heap in the last step to AVOID LOST VERSION
    //  it may cause duplicated log in the table heap and the version chain.
    //  when collecting logs and reconstructing tuples, the txn with the same ts would see
    //  (1) the TABLE HEAP VERSION and use that without scanning the version chain, or
    //  (2) when a newer txn updates the chain and the current log is pushed into the version chain,
    //      the reconstructor just applying the SAME delta multiple times to the tuple, which does
    //      not affect the integrity of the tuple
    //  and the garbage collector will take care of the false logs when they are no longer accessed.
    if (!table->UpdateTupleInPlace({txn->GetTransactionId(), true}, GenerateNullTupleForSchema(&table_info->schema_),
                                   rid_to_remove, [txn](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
                                     return meta.ts_ < TXN_START_ID || meta.ts_ == txn->GetTransactionId();
                                   })) {
      txn->SetTainted();
      throw ExecutionException("delete executor: write-write conflict. another txn snicked in");
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
