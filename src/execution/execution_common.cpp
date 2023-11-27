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
  const auto &[base_meta, base_tuple] = itr.GetTuple();
  return GetTupleSnapshot(exec_ctx, plan, base_meta, base_tuple);
}

auto GetTupleSnapshot(ExecutorContext *exec_ctx, const AbstractPlanNode *plan, const TupleMeta &base_meta,
                      const Tuple &base_tuple) -> std::optional<Tuple> {
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
                     timestamp_t read_ts, bool is_previously_deleted) -> UndoLog {
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

  // NOTE(jens): adding index support, see task 4.2, the log may have del marker
  //  because inserting tuple with the same primary key ends up in the RID slot
  return {is_previously_deleted, modified_fields, Tuple{diff_vals, &diff_schema}, read_ts};
}

auto UpdateDiffLog(const Tuple &base_tuple, const std::vector<Value> &updated_values, const UndoLog &old_diff_log,
                   const Schema *tuple_schema) -> UndoLog {
  const auto col_cnt = tuple_schema->GetColumnCount();
  assert((updated_values.size() == col_cnt && old_diff_log.modified_fields_.size() == col_cnt) ||
         updated_values.empty());

  auto modified_fields = old_diff_log.modified_fields_;
  std::vector<Column> diff_cols;
  std::vector<Value> diff_vals;

  const auto &old_partial_tuple = old_diff_log.tuple_;
  const auto old_partial_schema = ConstructPartialSchema(modified_fields, tuple_schema).first;

  for (int i = 0, old_partial_col_idx = 0; i < col_cnt; ++i) {
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
  return {old_diff_log.is_deleted_, modified_fields, Tuple{diff_vals, &diff_schema}, old_diff_log.ts_,
          old_diff_log.prev_version_};
}

auto GenerateFullLog(const Tuple &base_tuple, const Schema *tuple_schema, timestamp_t log_ts,
                     bool is_previously_deleted) -> UndoLog {
  const auto col_cnt = tuple_schema->GetColumnCount();
  std::vector<bool> modified_fields;
  modified_fields.reserve(col_cnt);
  for (int i = 0; i < col_cnt; ++i) {
    modified_fields.push_back(true);
  }
  return {is_previously_deleted, modified_fields, base_tuple, log_ts};
}

auto GenerateDeleteMarker(const Schema *tuple_schema, timestamp_t log_ts) -> UndoLog {
  std::vector<bool> modified_fields;
  modified_fields.reserve(tuple_schema->GetColumnCount());
  for (int i = 0; i < tuple_schema->GetColumnCount(); ++i) {
    modified_fields.push_back(false);
  }
  Schema empty_schema{{}};
  return {true, modified_fields, Tuple{{}, &empty_schema}, log_ts};
}

auto VersionChainPushFrontCAS(UndoLog &undo_log, const RID &rid, Transaction *txn, TransactionManager *txn_mngr,
                              std::function<bool(std::optional<UndoLink>)> &&check) -> bool {
  // link to old versions
  const auto prev_version = txn_mngr->GetUndoLink(rid);
  if (prev_version.has_value()) {
    undo_log.prev_version_ = *prev_version;
  }

  // add undo log to the txn local storage
  const auto undo_link = txn->AppendUndoLog(undo_log);

  // update global version link
  return txn_mngr->UpdateUndoLink(rid, undo_link, std::move(check));
}

auto VersionUndoLinkPushFrontCAS(UndoLog &head_undo_log, const RID &rid, bool in_progress, Transaction *txn,
                                 TransactionManager *txn_mngr,
                                 std::function<bool(std::optional<VersionUndoLink>)> &&check) -> bool {
  const auto prev_version_link = txn_mngr->GetVersionLink(rid);
  if (prev_version_link.has_value()) {
    std::cout << "previous version link exists\n";
    head_undo_log.prev_version_ = prev_version_link->prev_;
  }

  const auto undo_link = txn->AppendUndoLog(head_undo_log);
  return txn_mngr->UpdateVersionLink(rid, std::make_optional<VersionUndoLink>({undo_link, in_progress}),
                                     std::move(check));
}

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple {
  const auto &cols = schema->GetColumns();
  std::vector<Value> values;

  for (const auto &col : cols) {
    values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
  }

  return {values, schema};
}

auto GetPrimaryKeyIndex(const std::vector<IndexInfo *> &indexes) -> std::optional<IndexInfo *> {
  assert(indexes.empty() || indexes.size() == 1);
  std::vector<RID> result;
  for (auto &index : indexes) {
    assert(index->is_primary_key_);
    if (index->is_primary_key_) {
      return {index};
    }
  }
  return std::nullopt;
}

auto QueryRIDFromPrimaryKeyIndex(std::vector<IndexInfo *> &indexes, Tuple &tuple, Schema &tuple_schema,
                                 Transaction *txn) -> std::optional<RID> {
  std::vector<RID> result;
  auto index = GetPrimaryKeyIndex(indexes);
  if (!index.has_value()) {
    return std::nullopt;
  }
  auto &idx_spanning_col_attrs = index.value()->index_->GetKeyAttrs();
  assert(idx_spanning_col_attrs.size() == 1);
  auto primary_key = tuple.KeyFromTuple(tuple_schema, index.value()->key_schema_, idx_spanning_col_attrs);
  auto ht_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index.value()->index_.get());
  assert(ht_index != nullptr);
  ht_index->ScanKey(primary_key, &result, txn);
  assert(result.empty() || result.size() == 1);
  return result.empty() ? std::nullopt : std::make_optional<RID>(result[0]);
}

auto TupleInsertHandler::InsertTuple(Tuple &tuple) -> std::pair<bool, std::string> {
  bool ok{false};
  std::string err_msg;
  std::optional<RID> rid;

  // NOTE(jens): check for index conflict before insertion to prevent work in vain
  // the task only has one primary key index
  std::tie(ok, err_msg, rid) = TupleMayInsert(tuple);
  if (!ok) {
    return {false, err_msg};
  }

  // check if the primary index points to a deleted rid
  if (rid.has_value()) {
    std::tie(ok, err_msg) = HandleDirtyInsert(tuple, *rid);
  } else {
    std::tie(ok, err_msg) = HandleFreshInsert(tuple);
  }
  return {ok, err_msg};
}

auto TupleInsertHandler::HandleFreshInsert(Tuple &tuple) const -> std::pair<bool, std::string> {
  const auto inserted_rid =
      table_info_->table_->InsertTuple({txn_->GetTransactionId(), false}, tuple, lk_mngr_, txn_, table_info_->oid_);
  // NOTE(jens): if an index scan is executing after a tuple is inserted into the table heap but
  //  before the index is updated, it may not see the tuple (unless we lock the index)

  if (!inserted_rid.has_value()) {  // failed to insert
    txn_->SetTainted();
    return {false, "failed to insert"};
  }

  // append to write list so that commit can finalise these entries by changing its ts to commit ts
  txn_->AppendWriteSet(table_info_->oid_, *inserted_rid);

  // set empty version link, since this is an insert and no president tuple.
  // (other txns cannot see it since ts = txn_id)
  txn_mngr_->UpdateVersionLink(*inserted_rid, std::nullopt);

  // insert to index
  if (const auto &[ok, err_msg] = OnInsertCreateIndex(tuple, *inserted_rid); !ok) {
    return {false, err_msg};
  }

  return {true, ""};
}
auto TupleInsertHandler::HandleDirtyInsert(const Tuple &tuple, const RID &rid) const -> std::pair<bool, std::string> {
  // 'lock' other transactions from updating the index

  // insert a new undo log into the front
  const auto &[base_meta, base_tuple] = table_info_->table_->GetTuple(rid);

  const bool is_first_modification = base_meta.ts_ != txn_->GetTransactionId();
  if (is_first_modification) {
    assert(base_meta.ts_ < TXN_START_ID);

    // update, lock the version link
    // no race condition, only one update will atomically set in_progress and the other will fail
    if (auto new_undo_log = GenerateDeleteMarker(&table_info_->schema_, base_meta.ts_); !VersionUndoLinkPushFrontCAS(
            new_undo_log, rid, /* in_progress */ true, txn_, txn_mngr_,
            [](std::optional<VersionUndoLink> link) -> bool {
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
  if (const auto txn_forward = txn_; !table_info_->table_->UpdateTupleInPlace(
          {txn_->GetTransactionId(), false}, tuple, rid,
          /*CAS check*/ [&txn_forward](const TupleMeta &meta, const Tuple &table, RID) -> bool {
            return meta.ts_ <= txn_forward->GetReadTs() || meta.ts_ == txn_forward->GetTransactionId();
          })) {
    // cannot happen because this record can only be accessed by index at the moment
    // and any other txn wanting to insert will see that the version link in progress
    // and abort...
    assert(0);
  }

  if (is_first_modification) {
    // clear in_progress flag
    const auto in_progress_version_link = txn_mngr_->GetVersionLink(rid);
    assert(in_progress_version_link.has_value());
    // no need to use CAS because only this txn can access it
    txn_mngr_->UpdateVersionLink(rid, std::make_optional<VersionUndoLink>({in_progress_version_link->prev_, false}));
    // append to write list so that commit can finalise these entries by changing its ts to commit ts
    txn_->AppendWriteSet(table_info_->oid_, rid);
  }

  return {true, ""};
}

auto TupleInsertHandler::OnInsertCreateIndex(Tuple &tuple, const RID &rid) const -> std::pair<bool, std::string> {
  const auto index_opt = GetPrimaryKeyIndex(indexes_);
  if (!index_opt.has_value()) {
    return {true, "no primary key index"};
  }
  const auto index = index_opt.value();
  auto &idx_spanning_col_attrs = index->index_->GetKeyAttrs();
  assert(idx_spanning_col_attrs.size() == 1);
  const auto key_to_insert = tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, idx_spanning_col_attrs);
  if (!index->index_->InsertEntry(key_to_insert, rid, txn_)) {
    return {false, "write-write conflict. some txn snicked in"};
  }
  return {true, ""};
}

auto TupleInsertHandler::TupleMayInsert(Tuple &tuple_to_insert) -> std::tuple<bool, std::string, std::optional<RID>> {
  // only supports primary key
  const auto rid = QueryRIDFromPrimaryKeyIndex(indexes_, tuple_to_insert, table_info_->schema_, txn_);
  if (rid.has_value()) {
    const auto &[base_meta, base_tuple] = table_info_->table_->GetTuple(*rid);
    if (base_meta.ts_ > txn_->GetReadTs() && base_meta.ts_ != txn_->GetTransactionId()) {
      return {false, "write-write conflict", std::nullopt};
    }
    // ts <= txn_read_ts or ts = txn_id
    if (!base_meta.is_deleted_) {
      return {false, "violation of unique primary key constraint", std::nullopt};
    }
    // check not in progress
    const auto &version_link = txn_mngr_->GetVersionLink(*rid);
    if (version_link.has_value() && version_link->in_progress_ && base_meta.ts_ != txn_->GetTransactionId()) {
      return {false, "write-write conflict", std::nullopt};
    }
  }

  return {true, "", rid};
}

auto TupleDeleteHandler::DeleteTuple(const RID &rid) -> std::pair<bool, std::string> {
  const auto &table = table_info_->table_;
  // its child is a sequential scan or a filter

  auto [base_meta, base_tuple] = table->GetTuple(rid);
  if (IsWriteWriteConflict(base_meta, txn_->GetReadTs(), txn_->GetTransactionId())) {
    return {false, "write-write conflict. trying to delete the future"};
  }

  // already passed the initial write-write conflict test

  // create undo log for the first delete
  if (base_meta.ts_ != txn_->GetTransactionId()) {  // first modification
    auto undo_log = GenerateFullLog(base_tuple, &table_info_->schema_, base_meta.ts_, base_meta.is_deleted_);
    const auto txn_mngr_forward = txn_mngr_;
    // race condition is avoid by CAS checking
    VersionChainPushFrontCAS(undo_log, rid, txn_, txn_mngr_,
                             [txn_mngr_forward, undo_log](std::optional<UndoLink> head_undo_link) -> bool {
                               if (head_undo_link.has_value()) {
                                 const auto head_log = txn_mngr_forward->GetUndoLog(*head_undo_link);
                                 return head_log.ts_ < undo_log.ts_;
                               }
                               return true;
                             });  // ATOMIC
    txn_->AppendWriteSet(table_info_->oid_, rid);
  } else {
    // it may be updated and then deleted, thus we need to update the partial schema to record all changes
    // now since base_ts = txn_id, no other txns can update this tuple in table heap, they can only read it
    const auto link = txn_mngr_->GetUndoLink(rid);
    if (link.has_value()) {
      const auto old_diff_log = txn_mngr_->GetUndoLog(link.value());
      const auto tuple_snapshot = ReconstructTuple(&table_info_->schema_, base_tuple, base_meta, {old_diff_log});
      assert(tuple_snapshot.has_value());
      // use snapshot value because the current table heap tuple may be modified by the txn and should not be visible
      // to other transactions
      auto undo_log =
          GenerateFullLog(tuple_snapshot.value(), &table_info_->schema_, old_diff_log.ts_, old_diff_log.is_deleted_);
      undo_log.prev_version_ = old_diff_log.prev_version_;  // link into the chain
      txn_->ModifyUndoLog(link->prev_log_idx_, undo_log);   // ATOMIC
    }
  }

  // NOTE(jens): update the table heap in the last step to AVOID LOST VERSION
  if (const auto txn_forward = txn_;
      !table->UpdateTupleInPlace({txn_->GetTransactionId(), true}, GenerateNullTupleForSchema(&table_info_->schema_),
                                 rid, [&txn_forward](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
                                   return meta.ts_ < TXN_START_ID || meta.ts_ == txn_forward->GetTransactionId();
                                 })) {
    return {false, "write-write conflict. another txn snicked in"};
  }

  // NOTE(jens): we do not delete index since older txns can read the earlier version of the tuple
  //  once created, the index would always point to the same RID
  return {true, ""};
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

    auto version_link = txn_mgr->GetVersionLink(rid);

    fmt::println(stdout, "RID={}/{} ts={} {} tuple={} {}", rid.GetPageId(), rid.GetSlotNum(),
                 make_ts_string(base_meta.ts_), base_meta.is_deleted_ ? "<del marker>" : "",
                 base_tuple.ToString(&table_info->schema_),
                 version_link.has_value() && version_link->in_progress_ ? " -> in progress" : "");

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
