#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  const auto *fw_plan = dynamic_cast<const WindowFunctionPlanNode *>(plan_);
  assert(fw_plan != nullptr);

  child_executor_->Init();
  cursor_ = 0;
  tuple_build_buffer_.clear();
  tuple_build_buffer_.reserve(plan_->output_schema_->GetColumnCount());

  // clear function contexts
  function_contexts_.erase(function_contexts_.begin(), function_contexts_.end());
  for (const auto &function : fw_plan->window_functions_) {
    function_contexts_.emplace_front(
        function.first, WindowFunctionExecutionContext{&child_executor_->GetOutputSchema(), &function.second});
  }

  // prepare for function context, block
  while (true) {
    auto tpl = std::make_shared<Tuple>();  // use shared pointer to manage object (create a new one each time)
    if (!child_executor_->Next(tpl.get(), &dummy_rid_)) {
      break;
    }
    tuple_store_.push_back(tpl);
    for (auto &ctx : function_contexts_) {
      ctx.second.Insert(tpl);
    }
  }

  // process each context
  for (auto &ctx : function_contexts_) {
    ctx.second.Process();
  }

  // NOTE(jens): REMOVE FOR UNNECESSARY SORTING
  //  the project does not require different order-bys, so the answer is assumed to
  //  be produced in the order of the only order-by condition. We sort here assuming that all
  //  functions have the same order-by as the first one
  std::sort(tuple_store_.begin(), tuple_store_.end(), [&](auto &val1, auto &val2) {
    auto result = WFPartition::Compare(val1, val2, function_contexts_.front().second.GetOrderBys(),
                                       child_executor_->GetOutputSchema());
    return result == WFPartition::CmpResult::LESS_THAN;
  });
}
auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto *wf_plan = dynamic_cast<const WindowFunctionPlanNode *>(plan_);
  assert(wf_plan != nullptr);

  const auto *input_schema = &child_executor_->GetOutputSchema();
  const auto *output_schema = plan_->output_schema_.get();
  //  const auto output_size = output_schema->GetColumnCount();

  while (true) {
    if (cursor_ == tuple_store_.size()) {
      return false;
    }
    const auto &original_tuple = tuple_store_[cursor_++];

    // extract values in the tuple
    for (const auto &expr : wf_plan->columns_) {
      const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      assert(col_expr != nullptr);
      if (col_expr->GetColIdx() != static_cast<uint32_t>(-1)) {  // not a "placeholder"
        tuple_build_buffer_.push_back(expr->Evaluate(original_tuple.get(), *input_schema));
      }
    }

    for (const auto &ctx : function_contexts_) {
      auto value = ctx.second.GetValue(original_tuple);
      tuple_build_buffer_.push_back(value);
    }

    *tuple = {tuple_build_buffer_, output_schema};
    *rid = dummy_rid_;

    tuple_build_buffer_.clear();
    return true;
  }

  assert(0);
}
}  // namespace bustub
