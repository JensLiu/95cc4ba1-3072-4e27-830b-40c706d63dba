#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  // reset containers and sort types
  order_by_types_.clear();
  top_n_container_.clear();

  Tuple tpl;
  RID rid;
  auto top_n_plan = dynamic_cast<const TopNPlanNode *>(plan_);
  assert(top_n_plan != nullptr);

  // initialise global sort order
  for (const auto &gp : top_n_plan->order_bys_) {
    const auto &type = gp.first;
    order_by_types_.push_back(type);
  }

  while (child_executor_->Next(&tpl, &rid)) {
    TopNKey key;
    for (const auto &gp : top_n_plan->order_bys_) {
      const auto &expr = gp.second;
      key.AddCol(expr->Evaluate(&tpl, child_executor_->GetOutputSchema()));
    }

    TopNValue val = {{tpl}, {rid}};
    // sort logic
    if (top_n_container_.empty()) {
      // empty container, <key, val> is the least
      top_n_container_.emplace_front(key, val);
      continue;
    }

    if (TopNKey::Compare(key, top_n_container_.back().first, order_by_types_) == TopNKey::CmpResult::GREATER_THAN) {
      // greater than the last key
      if (top_n_container_.size() < top_n_plan->n_) {
        // if we have less than n <key, val>, insert since we want it as out result
        top_n_container_.emplace_back(key, val);
      }
      continue;
    }

    // top 0 < key < top n: find a place to insert
    for (auto itr = top_n_container_.begin(); itr != top_n_container_.end(); ++itr) {
      auto cmp_result = TopNKey::Compare(key, itr->first, order_by_types_);
      if (cmp_result == TopNKey::CmpResult::LESS_THAN || cmp_result == TopNKey::CmpResult::EQUALS) {
        // top 0 < ... < (key) < itr < ... < top n
        // top 0 < ... < (key = itr) < ... < top n
        // since the semantic is sort | limit, we do not want ranking, so evict when key = itr
        top_n_container_.insert(itr, {key, val});
        if (top_n_container_.size() > top_n_plan->n_) {
          top_n_container_.pop_back();  // delete the last element
        }
        break;
      }
      // greater than, keep finding top 0 < ... < key > itr < ... < top n
    }
  }

  // initialise iterator
  bucket_itr_ = top_n_container_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // we can have records less than n

  while (bucket_itr_ != top_n_container_.end()) {
    const auto &values = *bucket_itr_++;
    *tuple = values.second.tuple_;
    *rid = values.second.rid_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_n_container_.size(); }

}  // namespace bustub
