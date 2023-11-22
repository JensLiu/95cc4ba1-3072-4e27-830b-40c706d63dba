#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // the newest tuple is deleted, and there's no undo logs
  // -> the tuple is deleted
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }

  // the original state is deleted -> tuple is deleted
  if (!undo_logs.empty() && undo_logs.back().is_deleted_) {
    return std::nullopt;
  }

  std::vector<Value> values;
  const auto col_cnt = schema->GetColumnCount();
  values.reserve(col_cnt);
  // retrieve data from the base tuple
  for (int i = 0; i < col_cnt; ++i) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // apply undo logs to values
  for (const auto &log : undo_logs) {
    // construct the partial schema
    const auto [partial_schema, partial_col_attrs] = ConstructPartialSchema(log.modified_fields_, schema);
    // apply to old values
    for (int i = 0; i < partial_col_attrs.size(); ++i) {
      const auto &partial_tuple = log.tuple_;
      const auto &col_idx = partial_col_attrs[i];
      values[col_idx] = partial_tuple.GetValue(&partial_schema, i);
    }
  }

  return std::make_optional<Tuple>(values, schema);
}

auto ConstructPartialSchema(const std::vector<bool> &modified_fields, const Schema *base_schema)
    -> std::pair<Schema, std::vector<int>> {
  std::vector<Column> partial_cols;    // used to construct the partial schema
  std::vector<int> partial_col_attrs;  // used to retrieve data from log tuple
  for (int i = 0; i < modified_fields.size(); ++i) {
    if (modified_fields[i]) {
      partial_cols.push_back(base_schema->GetColumn(i));
      partial_col_attrs.push_back(i);
    }
  }
  return {Schema(partial_cols), partial_col_attrs};
}

auto CollectUndoLogs(TransactionManager *txn_mgr, const RID &rid, const timestamp_t start_ts,
                     std::vector<UndoLog> *result) -> bool {
  const auto link_opt = txn_mgr->GetUndoLink(rid);

  if (!link_opt.has_value()) {
    // does not have the record at RID
    result->clear();
    return false;
  }

  auto link = link_opt.value();

  while (link.IsValid()) {
    const auto log = txn_mgr->GetUndoLog(link);
    assert(log.ts_ < TXN_START_ID);
    // traverse the log link, collect until log.ts equals ts or just older than start_ts
    result->push_back(log);
    if (log.ts_ < start_ts) {
      break;
    }
    link = log.prev_version_;
  }

  if (result->empty()) {
    return false;
  }

  // clear older the log
  // read one ts more than the start_ts, verify if the additional reading is up-to-date
  // e.g. start_ts = 2
  // link -> 3 -> *2 -> 1 -> stop  ( second_last = start_ts > last ) -> in this case we want to remove ts=1
  // link -> 3 -> *1 -> stop       ( second_last > start_ts > last )
  // link -> *1 -> stop            (               start_ts > last. special case: when there are only older versions )
  if (result->size() >= 2) {
    const auto &second_last = result->at(result->size() - 2);
    if (second_last.ts_ == start_ts) {
      result->pop_back();
    }
  }

  // clear unreachable log
  // if the link does not reach at most (monotonically decreasing) start_ts, i.e. last.ts_ > start_ts
  // it is not visible
  // e.g. start_ts = 2
  // link -> 5 -> 4 -> 3 -> end
  if (start_ts >= 0 && result->back().ts_ > start_ts) {
    // unreachable result, meaning the tuple does not exist at the point of `until_ts`
    // start_ts < 0 means that we collect all the logs possible, used in debug mode
    result->clear();
    return false;
  }
  // else last_ts <= start_ts -> at or before start_ts, exists a record
  // or we are in debug mode, start_ts < 0, collect all possible record
  return true;
}

void CollectUndoLinks(TransactionManager *txn_mgr, const RID &rid, const timestamp_t until_ts,
                      std::vector<UndoLink> *result) {
  const auto link_opt = txn_mgr->GetUndoLink(rid);
  if (!link_opt.has_value()) {
    return;
  }
  auto link = link_opt.value();
  while (link.IsValid()) {
    const auto log = txn_mgr->GetUndoLog(link);
    if (log.ts_ < until_ts) {  // traverse the log link, collect until log.ts equals ts or just above ts
      break;
    }
    result->push_back(link);
    link = log.prev_version_;
  }
}

auto GetTupleSnapshot(ExecutorContext *exec_ctx, const AbstractPlanNode *plan, TableIterator &itr)
    -> std::optional<Tuple> {
  const auto [base_meta, base_tuple] = itr.GetTuple();
  const auto txn = exec_ctx->GetTransaction();
  const auto txn_read_ts = txn->GetReadTs();
  const auto base_ts = base_meta.ts_;

  // check of the most recent version is in the table heap
  if ((base_ts >= TXN_START_ID && base_ts == txn->GetTransactionId()) ||
      (base_ts < TXN_START_ID && base_ts <= txn_read_ts)) {
    // since on the version link, all timestamps are older than base ts
    // if the base ts is less than read ts, then it is the most recent one to get
    if (base_meta.is_deleted_) {  // deleted by the current txn
      return std::nullopt;
    }
    return {base_tuple};
  }

  std::vector<UndoLog> undo_logs;

  if (CollectUndoLogs(exec_ctx->GetTransactionManager(), base_tuple.GetRid(), txn_read_ts, &undo_logs)) {
    return ReconstructTuple(plan->output_schema_.get(), base_tuple, base_meta, undo_logs);
  }

  // the record does not exist at the time of this transaction read
  return std::nullopt;
}

auto IsWriteWriteConflict(const TupleMeta &base_meta, timestamp_t read_ts, txn_id_t txn_id) -> bool {
  // write-write conflict is visible only when one of the transactions modifying the tuple
  // (1) updates its base meta ts to its txn_id
  // (2) commits and move the base version into the version chain

  const auto &base_ts = base_meta.ts_;
  if (base_ts == txn_id) {  // can infer: base_ts >= TXN_START_ID
    // the modifying txn is itself
    return false;
  }

  if (read_ts >= base_ts) {  // can inder: base_ts < TXN_START_ID
    // base_ts is newer than base_ts and no other transactions are modifying it
    return false;
  }

  // everything in the version link is read only, because they are shared by transactions
  // with the same read_ts, and one txn cannot affect others' view
  // only base_meta is updatable

  // 1. other txn has modified the tuple
  // 2. the tuple if of older version, i.e. in the version chain rather than in the base
  return true;
}

auto GenerateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const Schema *tuple_schema,
                     timestamp_t read_ts) -> UndoLog {
  const auto col_cnt = tuple_schema->GetColumnCount();
  assert(updated_values.size() == col_cnt || updated_values.empty());

  std::vector<bool> modified_fields;
  std::vector<Column> diff_cols;
  std::vector<Value> diff_vals;
  modified_fields.reserve(col_cnt);

  for (int i = 0; i < col_cnt; ++i) {
    const auto &old_value = base_tuple.GetValue(tuple_schema, i);
    const auto &updated_value = updated_values.at(i);
    if (old_value.CompareExactlyEquals(updated_value)) {
      modified_fields.push_back(false);
    } else {
      // is is_deleted flag set, it should always keep a record of all fields
      modified_fields.push_back(true);
      diff_cols.push_back(tuple_schema->GetColumn(i));
      diff_vals.push_back(old_value);  // diff log stores how to recover it back
    }
  }
  Schema diff_schema{diff_cols};
  // there is no other scenarios where deleted flag is set to true
  // because a record cannot be deleted and recovered
  return {false, modified_fields, Tuple{diff_vals, &diff_schema}, read_ts};
}

auto UpdateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const UndoLog &old_diff_log,
                   const Schema *tuple_schema) -> UndoLog {
  const auto col_cnt = tuple_schema->GetColumnCount();
  assert(updated_values.size() == col_cnt && old_diff_log.modified_fields_.size() == col_cnt || updated_values.empty());

  auto modified_fields = old_diff_log.modified_fields_;
  std::vector<Column> diff_cols;
  std::vector<Value> diff_vals;

  const auto &old_partial_tuple = old_diff_log.tuple_;
  const auto old_partial_schema = ConstructPartialSchema(modified_fields, tuple_schema).first;

  for (int i = 0, old_partial_col_idx = 0; i < col_cnt;  ++i) {
    const auto &old_value = base_tuple.GetValue(tuple_schema, i);
    const auto &updated_value = updated_values.at(i);
    if (modified_fields[i]) {
      // fields already in the log, do not update them
      diff_cols.push_back(tuple_schema->GetColumn(i));
      diff_vals.push_back(old_partial_tuple.GetValue(&old_partial_schema, old_partial_col_idx++));
      continue;
    }
    if (!old_value.CompareExactlyEquals(updated_value)) {
      // fields not in the log, add to the log along with its new values
      modified_fields[i] = true;
      diff_cols.push_back(tuple_schema->GetColumn(i));
      diff_vals.push_back(old_value);
    }
  }
  Schema diff_schema{diff_cols};
  return {false, modified_fields, Tuple{diff_vals, &diff_schema}, old_diff_log.ts_, old_diff_log.prev_version_};

}

auto GenerateDeleteLog(const Tuple &base_tuple, const Schema *tuple_schema, timestamp_t log_ts) -> UndoLog {
  const auto col_cnt = tuple_schema->GetColumnCount();
  std::vector<bool> modified_fields;
  modified_fields.reserve(col_cnt);
  for (int i = 0; i < col_cnt; ++i) {
    modified_fields.push_back(true);
  }
  return {false, modified_fields, base_tuple, log_ts};
}

auto LinkIntoVersionChainCAS(UndoLog &undo_log, const RID &rid, Transaction *txn, TransactionManager *txn_mngr,
                             std::function<bool(std::optional<UndoLink>)> &&check) -> UndoLink {
  // link to old versions
  const auto prev_version = txn_mngr->GetUndoLink(rid);
  if (prev_version.has_value()) {
    undo_log.prev_version_ = *prev_version;
  }

  // add undo log to the txn local storage
  const auto undo_link = txn->AppendUndoLog(undo_log);

  // update global version link
  txn_mngr->UpdateUndoLink(rid, undo_link, std::move(check));

  return undo_link;
}

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple {
  const auto &cols = schema->GetColumns();
  std::vector<Value> values;

  for (const auto &col : cols) {
    values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
  }

  return {values, schema};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  const auto make_ts_string = [=](const timestamp_t &ts) -> std::string {
    if (ts >= TXN_START_ID) {
      return fmt::format("txn{}", ts - TXN_START_ID);
    }
    return fmt::format("{}", ts);
  };

  auto make_tuple_delta_string = [&](const UndoLog &log) -> std::string {
    if (log.is_deleted_) {
      return "<del>";
    }

    std::stringstream ss;
    bool first = true;
    ss << "(";
    const auto partial_schema = ConstructPartialSchema(log.modified_fields_, &table_info->schema_).first;
    int col_idx = 0;
    for (int i = 0; i < log.modified_fields_.size(); ++i) {
      if (!first) {
        ss << ", ";
      } else {
        first = false;
      }
      if (log.modified_fields_[i]) {
        const auto value = log.tuple_.GetValue(&partial_schema, col_idx);
        ++col_idx;
        if (value.IsNull()) {
          ss << "<NULL>";
        } else {
          ss << value.ToString();
        }
      } else {
        ss << "_";
      }
    }
    ss << ")";
    return ss.str();
  };

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1

  auto itr = table_heap->MakeIterator();

  for (; !itr.IsEnd(); ++itr) {
    const auto [base_meta, base_tuple] = itr.GetTuple();
    const auto rid = itr.GetRID();
    // for each record
    fmt::println(stdout, "RID={}/{} ts={} {} tuple={}", rid.GetPageId(), rid.GetSlotNum(),
                 make_ts_string(base_meta.ts_), base_meta.is_deleted_ ? "<del marker>" : "",
                 base_tuple.ToString(&table_info->schema_));

    std::vector<UndoLink> links;
    CollectUndoLinks(txn_mgr, rid, -1, &links);  // get all undo logs of the tuple (until_ts = -1)
    for (auto &link : links) {
      const auto txn = txn_mgr->txn_map_.at(link.prev_txn_);
      const auto log = txn_mgr->GetUndoLog(link);
      const auto log_string = make_tuple_delta_string(log);
      fmt::println("\ttxn{}@{} {} ts={}", link.prev_txn_ - TXN_START_ID, txn->GetReadTs(), log_string, log.ts_);
    }
  }
}

}  // namespace bustub
