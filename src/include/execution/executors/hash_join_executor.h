//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  const std::shared_ptr<AbstractExecutor> left_executor_;
  const std::shared_ptr<AbstractExecutor> right_executor_;
  RID dummy_rid_{};

  // we assume that both records can fit in memory
  std::unordered_map<JoinKey, JoinValue> join_ht_;
  std::unordered_map<JoinKey, JoinValue>::const_iterator ht_itr_;
  uint32_t lhs_cursor_;
  uint32_t rhs_cursor_;
  bool hash_join_emit_null_{true};

  Tuple MergeTuples(const Tuple *lhs_tpl, const Schema *lhs_schema, const Tuple *rhs_tpl,
                    const Schema *rhs_schema) const;
  Tuple EmptyRhsTuple(const Tuple *lhs_tpl, const Schema *lhs_schema, const Schema *rhs_schema) const;
};

}  // namespace bustub
