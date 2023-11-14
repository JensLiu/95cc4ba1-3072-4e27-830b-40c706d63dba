#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto NLJToHashJoinOptimisable(AbstractExpressionRef predicate, std::vector<AbstractExpressionRef> *left_cols,
                              std::vector<AbstractExpressionRef> *right_cols) -> bool {
  // a non-recursive DFS that checks for each expression to make sure that they are all equality tests
  std::vector<AbstractExpressionRef> queue;
  queue.push_back(predicate);
  while (!queue.empty()) {
    auto &p = queue.back();
    assert(p.get() != nullptr);
    queue.pop_back();
    // if valid, one of these should be non-null pointer
    const auto *logic_expr = dynamic_cast<const LogicExpression *>(p.get());
    const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(p.get());
    const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(p.get());
    if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
      assert(logic_expr->children_.size() == 2);
      queue.push_back(logic_expr->children_[1]);
      queue.push_back(logic_expr->children_[0]);
    } else if (cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
      assert(cmp_expr->children_.size() == 2);
      queue.push_back(cmp_expr->children_[1]);
      queue.push_back(cmp_expr->children_[0]);
    } else if (col_expr != nullptr) {
      // leaf node
      assert(col_expr->children_.empty());
      if (col_expr->GetTupleIdx() == 0) {
        left_cols->push_back(p);  // p is the AbstractExpressionRef of the column expression
      } else {
        right_cols->push_back(p);
      }
    } else {
      return false;
    }
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->children_) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimised_plan = plan->CloneWithChildren(std::move(children));

  if (optimised_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(optimised_plan.get());
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    if (NLJToHashJoinOptimisable(nlj_plan->predicate_, &left_key_expressions, &right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan->output_schema_, nlj_plan->children_[0],
                                                optimised_plan->children_[1], std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan->join_type_);
    }
  }
  return optimised_plan;
}

}  // namespace bustub
