#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->children_) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimised_plan = plan->CloneWithChildren(children);

  if (optimised_plan->GetType() == PlanType::Limit) {
    assert(optimised_plan->children_.size() == 1);
    const auto limit_plan = dynamic_cast<const LimitPlanNode *>(optimised_plan.get());
    const auto sort_plan = dynamic_cast<const SortPlanNode *>(optimised_plan->children_[0].get());
    if (sort_plan != nullptr) {
      assert(sort_plan->children_.size() == 1);
      return std::make_shared<TopNPlanNode>(sort_plan->output_schema_, sort_plan->children_[0], sort_plan->order_bys_,
                                            limit_plan->GetLimit());
    }
  }

  return optimised_plan;
}

}  // namespace bustub
