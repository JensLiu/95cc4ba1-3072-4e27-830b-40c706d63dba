//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct TopNKey {
  std::vector<Value> key_vals_;
  void AddCol(const Value &val) { key_vals_.push_back(val); }
  auto operator==(const TopNKey &that) const -> bool {
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
  auto static Compare(const TopNKey &k1, const TopNKey &k2, const std::vector<OrderByType> &order_by_types)
      -> CmpResult {
    // less than function
    for (uint32_t i = 0; i < k1.key_vals_.size(); ++i) {
      if (k1.key_vals_[i].CompareEquals(k2.key_vals_[i]) == CmpBool::CmpTrue) {
        continue;
      }
      const auto less_than = k1.key_vals_[i].CompareLessThan(k2.key_vals_[i]) == CmpBool::CmpTrue;
      if (order_by_types[i] == OrderByType::ASC || order_by_types[i] == OrderByType::DEFAULT) {
        return less_than ? CmpResult::LESS_THAN : CmpResult::GREATER_THAN;
      }
      if (order_by_types[i] == OrderByType::DESC) {
        return less_than ? CmpResult::GREATER_THAN : CmpResult::LESS_THAN;
      }
      assert(0);  // cannot reach here
    }
    // equals exactly
    return CmpResult::EQUALS;
  };
};

struct TopNValue {
  Tuple tuple_;
  RID rid_;
  void AddValue(const Tuple &tuple, const RID &rid) {
    tuple_ = tuple;
    rid_ = rid;
  }
};
}  // namespace bustub

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<OrderByType> order_by_types_;
  std::list<std::pair<TopNKey, TopNValue>> top_n_container_;
  std::list<std::pair<TopNKey, TopNValue>>::const_iterator bucket_itr_;
};
}  // namespace bustub
