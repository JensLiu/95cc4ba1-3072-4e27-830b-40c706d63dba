#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  // check index
  if (plan->GetType() == PlanType::SeqScan) {
    const auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode *>(plan.get());
    assert(seq_scan_plan != nullptr);
    const auto *predicate_single = dynamic_cast<const ComparisonExpression *>(seq_scan_plan->filter_predicate_.get());
    if (predicate_single != nullptr && predicate_single->comp_type_ == ComparisonType::Equal) {
      // for single comparison expression (without 'AND' or 'OR')
      BUSTUB_ASSERT(predicate_single->GetChildren().size() == 2,
                    "Comparison expression should have exactly two children");
      const auto indexes = catalog_.GetTableIndexes(seq_scan_plan->table_name_);
      const auto &col_idx =
          dynamic_cast<const ColumnValueExpression *>(predicate_single->GetChildAt(0).get())->GetColIdx();
      const auto &filter_col_name = catalog_.GetTable(seq_scan_plan->table_oid_)->schema_.GetColumn(col_idx).GetName();

      const IndexInfo *found_idx = nullptr;
      for (auto &idx_info : indexes) {
        const auto &idx_col_name = idx_info->key_schema_.GetColumn(0).GetName();
        if (filter_col_name == idx_col_name) {
          found_idx = idx_info;
          break;
        }
      }
      if (found_idx != nullptr) {
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan->output_schema_, seq_scan_plan->table_oid_,
                                                   found_idx->index_oid_, seq_scan_plan->filter_predicate_);
      }
    }
  }
  return plan;
}

}  // namespace bustub
