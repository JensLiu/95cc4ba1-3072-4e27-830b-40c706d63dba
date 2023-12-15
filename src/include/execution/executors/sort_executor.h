//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct SortKey {
  std::vector<Value> key_vals_;
  void AddCol(const Value &val) { key_vals_.push_back(val); }
  auto operator==(const SortKey &that) const -> bool {
    if (key_vals_.size() != that.key_vals_.size()) {
      return false;
    }
    for (uint32_t i = 0; i < key_vals_.size(); ++i) {
      if (key_vals_[i].CompareEquals(that.key_vals_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
  enum CmpResult { LESS_THAN = 0, EQUALS = 1, GREATER_THAN = 2 };
  auto static Compare(const SortKey &s1, const SortKey &s2, const std::vector<OrderByType> &order_by_types)
      -> CmpResult {
    // less than function
    for (uint32_t i = 0; i < s1.key_vals_.size(); ++i) {
      if (s1.key_vals_[i].CompareEquals(s2.key_vals_[i]) == CmpBool::CmpTrue) {
        continue;
      }
      const auto less_than = s1.key_vals_[i].CompareLessThan(s2.key_vals_[i]) == CmpBool::CmpTrue;
      if (order_by_types[i] == OrderByType::ASC || order_by_types[i] == OrderByType::DEFAULT) {
        return less_than ? CmpResult::LESS_THAN : CmpResult::GREATER_THAN;
      }
      if (order_by_types[i] == OrderByType::DESC) {
        return !less_than ? CmpResult::LESS_THAN : CmpResult::GREATER_THAN;
      }
      assert(0);  // cannot reach here
    }
    // equals exactly
    return CmpResult::EQUALS;
  };
};

struct SortValue {
  Tuple tuple_;
  RID rid_;
};
}  // namespace bustub

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::shared_ptr<AbstractExecutor> child_executor_;
  std::vector<std::pair<SortKey, SortValue>> container_;
  std::vector<OrderByType> order_by_types_;
  uint32_t cursor_;
};
}  // namespace bustub
