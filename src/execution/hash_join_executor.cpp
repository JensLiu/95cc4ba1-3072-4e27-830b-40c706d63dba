//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  auto hash_join_plan = dynamic_cast<const HashJoinPlanNode *>(plan_);
  assert(hash_join_plan != nullptr);

  Tuple lhs_tpl;
  Tuple rhs_tpl;

  while (left_executor_->Next(&lhs_tpl, &dummy_rid_)) {
    JoinKey key;
    for (auto &col_expr : hash_join_plan->LeftJoinKeyExpressions()) {
      key.AddColValue(col_expr->Evaluate(&lhs_tpl, left_executor_->GetOutputSchema()));
    }
    if (join_ht_.count(key) == 0) {
      join_ht_[key] = {};
    }
    join_ht_[key].CombineLeft(lhs_tpl);
  }

  while (right_executor_->Next(&rhs_tpl, &dummy_rid_)) {
    JoinKey key;
    for (auto &col_expr : hash_join_plan->RightJoinKeyExpressions()) {
      key.AddColValue(col_expr->Evaluate(&rhs_tpl, right_executor_->GetOutputSchema()));
    }
    if (join_ht_.count(key) == 0) {
      join_ht_[key] = {};
    }
    join_ht_[key].CombineRight(rhs_tpl);
  }
  ht_itr_ = join_ht_.begin();
  lhs_cursor_ = 0;
  rhs_cursor_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const bool &is_left_join = plan_->GetJoinType() == JoinType::LEFT;

  // NOTE:
  // rhs_cursor_, lhs_cursor_ are the iterators for left, right tuples having the same JoinKey value
  // ht_itr_ is the hash table iterator for <JoinKey, JoinValue>
  // JoinKey contains the values of joining columns. (std::vector<Value>)
  // JoinValue contains tuples having the same key. (std::vector<Tuple>)

  // smaller search space for NLJ
  while (true) {
    // std::unmapped_map returns values with the same key, so no need to deal with conflict, if the keys do not match,
    // left_tuples or right_tuples would be empty
    if (ht_itr_ == join_ht_.end()) {
      return false;
    }
    auto &left_tuples = ht_itr_->second.left_tuples_;
    auto &right_tuples = ht_itr_->second.right_tuples_;
    if (!left_tuples.empty()) {  // if left tuples are empty, skip the sub nested loop
      while (rhs_cursor_ < ht_itr_->second.right_tuples_.size()) {
        hash_join_emit_null_ = false;
        *tuple = MergeTuples(&left_tuples[lhs_cursor_], &left_executor_->GetOutputSchema(), &right_tuples[rhs_cursor_],
                             &right_executor_->GetOutputSchema());
        *rid = dummy_rid_;
        rhs_cursor_++;
        return true;
      }
      if (is_left_join && hash_join_emit_null_) {
        hash_join_emit_null_ = false;
        *tuple = EmptyRhsTuple(&left_tuples[lhs_cursor_], &left_executor_->GetOutputSchema(),
                               &right_executor_->GetOutputSchema());
        *rid = dummy_rid_;
        return true;
      }
      // reset right itr to the beginning
      rhs_cursor_ = 0;
      // move left itr forward
      if (++lhs_cursor_ < ht_itr_->second.left_tuples_.size()) {
        hash_join_emit_null_ = true;
        continue;
      }
    }
    rhs_cursor_ = 0;
    lhs_cursor_ = 0;
    // after left tuples are traversed, go to the next bucket
    ++ht_itr_;
    hash_join_emit_null_ = true;
  }
}

auto HashJoinExecutor::MergeTuples(const Tuple *lhs_tpl, const Schema *lhs_schema, const Tuple *rhs_tpl,
                                   const Schema *rhs_schema) const -> Tuple {
  std::vector<Value> vals;
  auto lhs_col_cnt = lhs_schema->GetColumnCount();
  auto rhs_col_cnt = rhs_schema->GetColumnCount();
  for (uint32_t i = 0; i < lhs_col_cnt; ++i) {
    auto val = lhs_tpl->GetValue(lhs_schema, i);
    vals.push_back(lhs_tpl->GetValue(lhs_schema, i));
  }
  for (uint32_t i = 0; i < rhs_col_cnt; ++i) {
    vals.push_back(rhs_tpl->GetValue(rhs_schema, i));
  }
  return {vals, &GetOutputSchema()};
}

auto HashJoinExecutor::EmptyRhsTuple(const Tuple *lhs_tpl, const Schema *lhs_schema, const Schema *rhs_schema) const
    -> Tuple {
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
