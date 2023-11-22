#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
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
                     timestamp_t read_ts) -> UndoLog;

auto UpdateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const UndoLog &old_diff_log,
                   const Schema *tuple_schema) -> UndoLog;

auto GenerateDeleteLog(const Tuple &base_tuple, const Schema *tuple_schema, timestamp_t log_ts) -> UndoLog;

auto LinkIntoVersionChainCAS(UndoLog &undo_log, const RID &rid, Transaction *txn, TransactionManager *txn_mngr,
                             std::function<bool(std::optional<UndoLink>)> &&check = nullptr) -> UndoLink;

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple;

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

}  // namespace bustub
