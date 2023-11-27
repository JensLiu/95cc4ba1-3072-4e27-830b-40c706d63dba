#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/plans/abstract_plan.h"
#include "executor_context.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

/**
 * this function does not check for tuples in the table heap to see if the latest modification with
 * special ts, i.e. txn_id, is the current accessing txn. the caller should check for that
 * @param txn_mgr
 * @param rid
 * @param start_ts
 * @param result
 * @return
 */
auto CollectUndoLogs(TransactionManager *txn_mgr, const RID &rid, timestamp_t start_ts, std::vector<UndoLog> *result)
    -> bool;

/**
 * get tuple with the corresponding version.
 * If the tuple if currently modified by the same txn, it returns the modified tuple
 * Otherwise return the tuple snapshot when it was read by the txn
 * @param exec_ctx
 * @param plan
 * @param itr
 * @return
 */
auto GetTupleSnapshot(ExecutorContext *exec_ctx, const AbstractPlanNode *plan, TableIterator &itr)
    -> std::optional<Tuple>;

auto GetTupleSnapshot(ExecutorContext *exec_ctx, const AbstractPlanNode *plan, const TupleMeta &base_meta,
                      const Tuple &base_tuple) -> std::optional<Tuple>;

auto ConstructPartialSchema(const std::vector<bool> &modified_fields, const Schema *base_schema)
    -> std::pair<Schema, std::vector<int>>;

auto IsWriteWriteConflict(const TupleMeta &update_meta, timestamp_t read_ts, txn_id_t txn_id) -> bool;

/**
 * calculates the diff log, does not set the prev_version, the caller should set it explicitly
 * the diff log should record the state of the tuple snapshot at read_ts
 * @param base_tuple
 * @param updated_values
 * @param tuple_schema
 * @param read_ts
 * @param is_deleted
 * @return
 */
auto GenerateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const Schema *tuple_schema,
                     timestamp_t read_ts, bool is_previously_deleted) -> UndoLog;

auto UpdateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const UndoLog &old_diff_log,
                   const Schema *tuple_schema) -> UndoLog;

/**
 * when a txn wants to delete a tuple, it generates a delete log which has the whole tuple and a delete marker
 * @param base_tuple
 * @param tuple_schema
 * @param log_ts
 * @param is_previously_deleted
 * @return
 */
auto GenerateFullLog(const Tuple &base_tuple, const Schema *tuple_schema, timestamp_t log_ts,
                     bool is_previously_deleted) -> UndoLog;

/**
 * task 4.2: when index points to a deleted tuple, and the txn wants to insert to it, it should generate
 * a delete marker to indicate that at log_ts the tuple has been removed
 * @param tuple_schema
 * @param log_ts
 * @return
 */
auto GenerateDeleteMarker(const Schema *tuple_schema, timestamp_t log_ts) -> UndoLog;

auto VersionChainPushFrontCAS(UndoLog &undo_log, const RID &rid, Transaction *txn, TransactionManager *txn_mngr,
                              std::function<bool(std::optional<UndoLink>)> &&check = nullptr) -> bool;

auto VersionUndoLinkPushFrontCAS(UndoLog &head_undo_log, const RID &rid, bool in_progress, Transaction *txn,
                                 TransactionManager *txn_mngr,
                                 std::function<bool(std::optional<VersionUndoLink>)> &&check) -> bool;

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple;

auto GetPrimaryKeyIndex(const std::vector<IndexInfo *> &indexes) -> std::optional<IndexInfo *>;

auto QueryRIDFromPrimaryKeyIndex(std::vector<IndexInfo *> &indexes, Tuple &tuple, Schema &tuple_schema,
                                 Transaction *txn) -> std::optional<RID>;

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

class TupleInsertHandler {
 public:
  TupleInsertHandler(TransactionManager *txn_mngr, LockManager *lk_mngr, Transaction *txn, TableInfo *table_info,
                     const std::vector<IndexInfo *> &indexes)
      : txn_mngr_(txn_mngr), lk_mngr_(lk_mngr), txn_(txn), table_info_(table_info), indexes_(indexes) {}

  auto InsertTuple(Tuple &tuple) -> std::pair<bool, std::string>;

 private:
  auto HandleFreshInsert(Tuple &tuple) const -> std::pair<bool, std::string>;

  auto HandleDirtyInsert(const Tuple &tuple, const RID &rid) const -> std::pair<bool, std::string>;

  auto OnInsertCreateIndex(Tuple &tuple, const RID &rid) const -> std::pair<bool, std::string>;

  auto TupleMayInsert(Tuple &tuple_to_insert) -> std::tuple<bool, std::string, std::optional<RID>>;

  TransactionManager *txn_mngr_;
  LockManager *lk_mngr_;
  Transaction *txn_;
  TableInfo *table_info_;
  std::vector<IndexInfo *> indexes_;
};

/**
 * A txn can remove the tuple only if the version its holding is in the table heap (newest)
 * Delete executor, update executor may use this
 */
class TupleDeleteHandler {
 public:
  TupleDeleteHandler(TransactionManager *txn_mngr, Transaction *txn, TableInfo *table_info)
      : txn_mngr_(txn_mngr), txn_(txn), table_info_(table_info) {}

  auto DeleteTuple(const RID &rid) -> std::pair<bool, std::string>;

 private:
  TransactionManager *txn_mngr_;
  Transaction *txn_;
  TableInfo *table_info_;
};

#define INSTALL_NON_BLOCKING_EXECUTOR_RETURN_HANDLER                    \
  /* to record if this is the first call of the batch to distinguish */ \
  /* between zero insert and end of the batch */                        \
  bool batch_begin_ { true }

#define INSTALL_BLOCKING_EXECUTOR_RETURN_HANDLER \
  bool batch_begin_ { true }

#define BLOCKING_EXECUTOR_BEGIN_EXEC                   \
  if (!batch_begin_) { /* it can only execute once! */ \
    return false;                                      \
  }                                                    \
  batch_begin_ = false;

#define BLOCKING_EXECUTOR_END_EXEC return true;

#define HANDLE_NON_BLOCKING_EXECUTOR_RETURN(row_cnt_var)                                    \
  if (row_cnt_var != 0) {                                                                   \
    /* has more than one row inserted which means */                                        \
    /* that the next call it not the first of the batch */                                  \
    batch_begin_ = false;                                                                   \
    return true;                                                                            \
  }                                                                                         \
  if (batch_begin_) {                                                                       \
    /* inserting nothing to the table, should return true to report the result (tuple 0) */ \
    /* otherwise the caller would think it as an end signal and discard the result */       \
    batch_begin_ = false;                                                                   \
    return true;                                                                            \
  }                                                                                         \
  /* not the beginning of the batch and zero rows inserted -> end of this batch. */         \
  batch_begin_ = true; /* reset the flag for the next batch */                              \
  return false

}  // namespace bustub
