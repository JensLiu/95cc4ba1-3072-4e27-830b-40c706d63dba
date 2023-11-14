#include <memory>
#include <vector>
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");

    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);

      // check index
      const auto *predicate_single = dynamic_cast<const ComparisonExpression *>(filter_plan.predicate_.get());
      if (predicate_single != nullptr && predicate_single->comp_type_ == ComparisonType::Equal) {
        // for single comparison expression (without 'AND' or 'OR')
        BUSTUB_ASSERT(predicate_single->GetChildren().size() == 2,
                      "Comparison expression should have exactly two children");
        const auto indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
        const auto &col_idx =
            dynamic_cast<const ColumnValueExpression *>(predicate_single->GetChildAt(0).get())->GetColIdx();
        const auto &filter_col_name = catalog_.GetTable(seq_scan_plan.table_oid_)->schema_.GetColumn(col_idx).GetName();

        const IndexInfo *found_idx = nullptr;
        for (auto &idx_info : indexes) {
          const auto &idx_col_name = idx_info->key_schema_.GetColumn(0).GetName();
          if (filter_col_name == idx_col_name) {
            found_idx = idx_info;
            break;
          }
        }
        if (found_idx != nullptr) {
          return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                              found_idx->index_oid_, filter_plan.GetPredicate());
        }
      }

      // push down the predicate
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
