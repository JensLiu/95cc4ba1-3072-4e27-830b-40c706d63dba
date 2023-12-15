#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  // reset containers and sort types
  order_by_types_.clear();
  container_.clear();
  cursor_ = 0;

  Tuple tpl;
  RID rid;
  auto sort_plan = dynamic_cast<const SortPlanNode *>(plan_);
  assert(sort_plan != nullptr);

  // initialise global sort order
  for (const auto &gp : sort_plan->order_bys_) {
    const auto &type = gp.first;
    order_by_types_.push_back(type);
  }

  while (child_executor_->Next(&tpl, &rid)) {
    SortKey key;
    for (const auto &gp : sort_plan->order_bys_) {
      const auto &expr = gp.second;
      key.AddCol(expr->Evaluate(&tpl, child_executor_->GetOutputSchema()));
    }
    SortValue val = {tpl, rid};
    container_.emplace_back(key, val);
  }
  std::sort(container_.begin(), container_.end(), [&](auto &k1, auto &k2) {
    return SortKey::Compare(k1.first, k2.first, order_by_types_) == SortKey::CmpResult::LESS_THAN;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < container_.size()) {
    const auto &value = container_[cursor_++].second;
    *tuple = value.tuple_;
    *rid = value.rid_;
    return true;
  }
  return false;
}

}  // namespace bustub
