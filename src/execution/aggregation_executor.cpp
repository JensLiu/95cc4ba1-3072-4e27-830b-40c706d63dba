//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/string_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  // clear the hash table, otherwise will aggregate the result
  aht_.Clear();

  // build the hash table
  Tuple tpl;
  RID rid;
  bool empty = true;
  while (child_executor_->Next(&tpl, &rid)) {
    empty = false;
    aht_.InsertCombine(MakeAggregateKey(&tpl), MakeAggregateValue(&tpl));
  }
  // empty table and no group-bys in the plan because there are NO GROUPS
  if (empty && plan_->GetGroupBys().empty()) {
    // if empty, insert a null node to not return false in Next,
    // but rather return a value (be it null or 0 according to the rule)
    aht_.InitialiseForEmptyTable(MakeAggregateKeyNull());
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto &key = aht_iterator_.Key();
  auto &val = aht_iterator_.Val();
  ++aht_iterator_;
  std::vector<Value> tuple_value;
  tuple_value.insert(tuple_value.end(), key.group_bys_.begin(), key.group_bys_.end());
  tuple_value.insert(tuple_value.end(), val.aggregates_.begin(), val.aggregates_.end());
  *tuple = {tuple_value, &plan_->OutputSchema()};
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
