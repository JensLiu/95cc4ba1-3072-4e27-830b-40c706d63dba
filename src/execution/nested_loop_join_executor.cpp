//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&lhs_tpl_, &dummy_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &lhs_schema = left_executor_->GetOutputSchema();
  auto &rhs_schema = right_executor_->GetOutputSchema();
  auto predicate = plan_->Predicate();

  auto is_left_join = plan_->GetJoinType() == JoinType::LEFT;

  while (true) {
    while (right_executor_->Next(&rhs_tpl_, &dummy_rid_)) {
      if (plan_->Predicate()
              ->EvaluateJoin(&lhs_tpl_, lhs_schema, &rhs_tpl_, rhs_schema)
              .CastAs(TypeId::BOOLEAN)
              .GetAs<bool>()) {
        left_join_emit_null_ = false;  // has value, do not emit null
        *tuple = MergeTuples(&lhs_tpl_, &lhs_schema, &rhs_tpl_, &rhs_schema);
        *rid = dummy_rid_;
        return true;
      }
    }
    // no matching join
    if (is_left_join && left_join_emit_null_) {
      left_join_emit_null_ = false;  // each lhs only emit once
      *tuple = EmptyRhsTuple(&lhs_tpl_, &lhs_schema, &rhs_schema);
      *rid = dummy_rid_;
      return true;
    }
    // we make sure only when each rhs are matched, then we go to the next lhs
    if (!left_executor_->Next(&lhs_tpl_, &dummy_rid_)) {
      return false;
    }
    left_join_emit_null_ = true;  // reset for next lhs
    right_executor_->Init();      // reset right executor to the beginning
  }
  assert(0);
}
auto NestedLoopJoinExecutor::MergeTuples(const Tuple *lhs_tpl, const Schema *lhs_schema, const Tuple *rhs_tpl,
                                         const Schema *rhs_schema) const -> Tuple {
  std::vector<Value> vals;
  auto lhs_col_cnt = lhs_schema->GetColumnCount();
  auto rhs_col_cnt = rhs_schema->GetColumnCount();
  for (uint32_t i = 0; i < lhs_col_cnt; ++i) {
    vals.push_back(lhs_tpl->GetValue(lhs_schema, i));
  }
  for (uint32_t i = 0; i < rhs_col_cnt; ++i) {
    vals.push_back(rhs_tpl->GetValue(rhs_schema, i));
  }
  return {vals, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::EmptyRhsTuple(const Tuple *lhs_tpl, const Schema *lhs_schema,
                                           const Schema *rhs_schema) const -> Tuple {
  std::vector<Value> vals;
  auto lhs_col_cnt = lhs_schema->GetColumnCount();
  auto rhs_col_cnt = rhs_schema->GetColumnCount();
  for (uint32_t i = 0; i < lhs_col_cnt; ++i) {
    vals.push_back(lhs_tpl->GetValue(lhs_schema, i));
  }
  for (uint32_t i = 0; i < rhs_col_cnt; ++i) {
    // creates empty values
    vals.push_back(ValueFactory::GetNullValueByType(rhs_schema->GetColumn(i).GetType()));
  }
  return {vals, &GetOutputSchema()};
}

}  // namespace bustub
