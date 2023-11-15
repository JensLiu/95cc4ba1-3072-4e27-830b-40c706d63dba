//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_plan.h
//
// Identification: src/include/execution/plans/topn_plan.h
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
 * The TopNPlanNode represents a top-n operation. It will gather the n extreme rows based on
 * limit and order expressions.
 */
class TopNPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new TopNPlanNode instance.
   * @param output The output schema of this TopN plan node
   * @param child The child plan node
   * @param order_bys The sort expressions and their order by types.
   * @param n Retain n elements.
   */
  TopNPlanNode(SchemaRef output, AbstractPlanNodeRef child,
               std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, std::size_t n)
      : AbstractPlanNode(std::move(output), {std::move(child)}), order_bys_(std::move(order_bys)), n_{n} {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::TopN; }

  /** @return The N (limit) */
  auto GetN() const -> size_t { return n_; }

  /** @return Get order by expressions */
  auto GetOrderBy() const -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & { return order_bys_; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "TopN should have exactly one child plan.");
    return GetChildAt(0);
  }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(TopNPlanNode);

  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  std::size_t n_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

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
  auto static Compare(const TopNKey &k1, const TopNKey &k2, const std::vector<OrderByType> &order_by_types) -> CmpResult{
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
