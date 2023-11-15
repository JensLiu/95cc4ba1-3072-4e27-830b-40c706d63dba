//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_plan.h
//
// Identification: src/include/execution/plans/sort_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The SortPlanNode represents a sort operation. It will sort the input with
 * the given predicate.
 */
class SortPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new SortPlanNode instance.
   * @param output The output schema of this sort plan node
   * @param child The child plan node
   * @param order_bys The sort expressions and their order by types.
   */
  SortPlanNode(SchemaRef output, AbstractPlanNodeRef child,
               std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : AbstractPlanNode(std::move(output), {std::move(child)}), order_bys_(std::move(order_bys)) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Sort; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Sort should have exactly one child plan.");
    return GetChildAt(0);
  }

  /** @return Get sort by expressions */
  auto GetOrderBy() const -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & { return order_bys_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(SortPlanNode);

  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

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
  auto static Compare(const SortKey &s1, const SortKey &s2, const std::vector<OrderByType> &order_by_types)-> CmpResult {
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
