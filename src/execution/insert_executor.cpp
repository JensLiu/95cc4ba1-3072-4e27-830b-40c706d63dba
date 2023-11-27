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

void InsertExecutor::Init() {
  child_executor_->Init();
  const auto cat = exec_ctx_->GetCatalog();
  auto table_info = cat->GetTable(plan_->GetTableOid());
  auto indexes = cat->GetTableIndexes(table_info->name_);

  tuple_insert_handler_ =
      std::make_unique<TupleInsertHandler>(exec_ctx_->GetTransactionManager(), exec_ctx_->GetLockManager(),
                                           exec_ctx_->GetTransaction(), table_info, indexes);
  indexes_ = indexes;
  table_info_ = table_info;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple_ret, RID *rid_ret) -> bool {
  auto txn = exec_ctx_->GetTransaction();

  Tuple tuple_to_insert;
  RID dummy_rid;
  int32_t row_cnt = 0;

  // bool ok{false};
  // std::string err_msg;
  // std::optional<RID> rid;

  while (child_executor_->Next(&tuple_to_insert, &dummy_rid)) {
    if (const auto [ok, err_msg] = tuple_insert_handler_->InsertTuple(tuple_to_insert); !ok) {
      txn->SetTainted();
      throw ExecutionException("insert executor: " + err_msg);
    }
    // //    TxnMgrDbg("before insert " + tuple_to_insert.ToString(&table_info_->schema_),
    // //    exec_ctx_->GetTransactionManager(),
    // //              table_info_, table_info_->table_.get());
    // //    std::cout << "insert tuple: " + tuple_to_insert.ToString(&table_info_->schema_) << "\n";
    //
    // // NOTE(jens): check for index conflict before insertion to prevent work in vain
    // // the task only has one primary key index
    // std::tie(ok, err_msg, rid) = TupleMayInsert(tuple_to_insert);
    // if (!ok) {
    //   txn->SetTainted();
    //   throw ExecutionException("insert executor: " + err_msg);
    // }
    //
    // // check if the primary index points to a deleted rid
    // if (rid.has_value()) {
    //   std::tie(ok, err_msg) = HandleDirtyInsert(tuple_to_insert, *rid);
    // } else {
    //   std::tie(ok, err_msg) = HandleFreshInsert(tuple_to_insert);
    // }
    // if (!ok) {
    //   txn->SetTainted();
    //   throw ExecutionException("insert executor: " + err_msg);
    // }
    // //    TxnMgrDbg("after insert " + tuple_to_insert.ToString(&table_info_->schema_),
    // //    exec_ctx_->GetTransactionManager(),
    // //              table_info_, table_info_->table_.get());
    ++row_cnt;
  }

  // send the result
  *tuple_ret = Tuple({{TypeId::INTEGER, row_cnt}}, &GetOutputSchema());

  HANDLE_NON_BLOCKING_EXECUTOR_RETURN(row_cnt);
}

auto InsertExecutor::HandleFreshInsert(Tuple &tuple) -> std::pair<bool, std::string> {
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mngr = exec_ctx_->GetTransactionManager();
  auto inserted_rid =
      table_info_->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionId(), false}, tuple,
                                       exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->GetTableOid());
  // NOTE(jens): if an index scan is executing after a tuple is inserted into the table heap but
  //  before the index is updated, it may not see the tuple (unless we lock the index)

  if (!inserted_rid.has_value()) {  // failed to insert
    txn->SetTainted();
    return {false, "failed to insert"};
  }

  // append to write list so that commit can finalise these entries by changing its ts to commit ts
  exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *inserted_rid);

  // set empty version link, since this is an insert and no president tuple.
  // (other txns cannot see it since ts = txn_id)
  txn_mngr->UpdateVersionLink(*inserted_rid, std::nullopt);

  // insert to index
  const auto &[ok, err_msg] = OnInsertCreateIndex(tuple, *inserted_rid);
  if (!ok) {
    return {false, err_msg};
  }

  return {true, ""};
}

auto InsertExecutor::HandleDirtyInsert(Tuple &tuple, const RID &rid) -> std::pair<bool, std::string> {
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mngr = exec_ctx_->GetTransactionManager();
  // 'lock' other transactions from updating the index

  // insert a new undo log into the front
  const auto &[base_meta, base_tuple] = table_info_->table_->GetTuple(rid);

  bool is_first_modification = base_meta.ts_ != txn->GetTransactionId();
  if (is_first_modification) {
    assert(base_meta.ts_ < TXN_START_ID);
    auto new_undo_log = GenerateDeleteMarker(&plan_->OutputSchema(), base_meta.ts_);

    // update, lock the version link
    // no race condition, only one update will atomically set in_progress and the other will fail
    if (!VersionUndoLinkPushFrontCAS(
            new_undo_log, rid, /* in_progress */ true, txn, txn_mngr, [](std::optional<VersionUndoLink> link) -> bool {
              // CAS check function, atomic
              if (link.has_value()) {
                return !link->in_progress_;  // if the current link is not in progress, we can insert
              }
              return true;  // if the there's no undo link, we can surely insert
            })) {
      // this is a CAS change, failing means that other txn is already modifying the index
      return {false, "write-write conflict. another txn snicked in"};
    }
  }

  // 'lock' other transactions from updating the value while inserting
  // NOTE: other transactions cannot update the value because to them, this tuple is deleted
  //  it is not visible from sequential scan, and only visible from index scan.
  if (!table_info_->table_->UpdateTupleInPlace(
          {txn->GetTransactionId(), false}, tuple, rid,
          /*CAS check*/ [&txn](const TupleMeta &meta, const Tuple &table, RID rid) -> bool {
            return meta.ts_ <= txn->GetReadTs() || meta.ts_ == txn->GetTransactionId();
          })) {
    // cannot happen because this record can only be accessed by index at the moment
    // and any other txn wanting to insert will see that the version link in progress
    // and abort...
    assert(0);
  }

  if (is_first_modification) {
    // clear in_progress flag
    const auto in_progress_version_link = txn_mngr->GetVersionLink(rid);
    assert(in_progress_version_link.has_value());
    // no need to use CAS because only this txn can access it
    txn_mngr->UpdateVersionLink(rid, std::make_optional<VersionUndoLink>({in_progress_version_link->prev_, false}));
    // append to write list so that commit can finalise these entries by changing its ts to commit ts
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, rid);
  }

  return {true, ""};
}

auto InsertExecutor::OnInsertCreateIndex(Tuple &tuple, RID &rid) -> std::pair<bool, std::string> {
  auto index_opt = GetPrimaryKeyIndex(indexes_);
  if (!index_opt.has_value()) {
    return {true, "no primary key index"};
  }
  auto index = index_opt.value();
  auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
  assert(idx_spanning_col_attrs.size() == 1);
  auto key_to_insert = tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, idx_spanning_col_attrs);
  if (!index->index_->InsertEntry(key_to_insert, rid, exec_ctx_->GetTransaction())) {
    return {false, "write-write conflict. some txn snicked in"};
  }
  return {true, ""};
}

auto InsertExecutor::TupleMayInsert(Tuple &tuple_to_insert) -> std::tuple<bool, std::string, std::optional<RID>> {
  const auto txn = exec_ctx_->GetTransaction();
  const auto txn_mngr = exec_ctx_->GetTransactionManager();
  // only supports primary key
  const auto rid = QueryRIDFromPrimaryKeyIndex(indexes_, tuple_to_insert, table_info_->schema_, txn);
  if (rid.has_value()) {
    const auto &[base_meta, base_tuple] = table_info_->table_->GetTuple(*rid);
    if (base_meta.ts_ > txn->GetReadTs() && base_meta.ts_ != txn->GetTransactionId()) {
      return {false, "write-write conflict", std::nullopt};
    }
    // ts <= txn_read_ts or ts = txn_id
    if (!base_meta.is_deleted_) {
      return {false, "violation of unique primary key constraint", std::nullopt};
    }
    // check not in progress
    const auto &version_link = txn_mngr->GetVersionLink(*rid);
    if (version_link.has_value() && version_link->in_progress_ && base_meta.ts_ != txn->GetTransactionId()) {
      return {false, "write-write conflict", std::nullopt};
    }
  }

  return {true, "", rid};
}

}  // namespace bustub
