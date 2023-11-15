//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

class WindowFunctionExecutionContext {
 public:
  WindowFunctionExecutionContext(const Schema *input_schema,
                                 const WindowFunctionPlanNode::WindowFunction *window_function)
      : input_schema_(input_schema), function_info_(window_function) {}
  WindowFunctionExecutionContext() = default;

  void Insert(std::shared_ptr<Tuple> &tuple) {
    auto value = Tuple2Value(tuple);
    auto partition = GetPartition(Tuple2Key(tuple));
    //    std::cout << "to insert: " << tuple->ToString(input_schema_) << ", value: " << value.ToString() << "\n";
    partition->InsertTuple(tuple, value);
  }

  void Process() {
    for (auto &partition : partition_ctx_) {
      // for each partition
      DoPartitionOrderBy(partition.second);
      DoFunction(partition.first, partition.second);
      partition.second.BuildIndex();  // after aggregation function, build index for constant lookup
    }
  }

  // expensive function O(#rows in the partition)
  auto GetValue(const std::shared_ptr<Tuple> &tuple) const -> Value {
    auto key = Tuple2Key(tuple);
    auto &partition = partition_ctx_.at(key);
    const auto value = partition.IndexLookup(tuple);
    assert(value.has_value());
    return value.value();
  }

  auto GetOrderBys() -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & {
    return function_info_->order_by_;
  }

 private:
  auto GetPartition(const WFPartitionKey &key) -> WFPartition * {
    if (partition_ctx_.count(key) == 0) {
      partition_ctx_[key] = {};
    }
    return &partition_ctx_[key];
  }

  auto Tuple2Key(const std::shared_ptr<Tuple> &tuple) const -> WFPartitionKey {
    WFPartitionKey key;
    // if there's no partition, the key is empty
    for (const auto &expr : function_info_->partition_by_) {
      auto val = expr->Evaluate(tuple.get(), *input_schema_);
      key.AddToKey(val);
    }
    return key;
  }

  auto Tuple2Value(const std::shared_ptr<Tuple> &tuple) const -> Value {
    auto col_val = function_info_->function_->Evaluate(tuple.get(), *input_schema_);
    return col_val;
  }

  void DoPartitionOrderBy(WFPartition &partition) {
    std::sort(partition.data_.begin(), partition.data_.end(), [&](auto &val1, auto &val2) {
      auto result = WFPartition::Compare(val1.first, val2.first, function_info_->order_by_, *input_schema_);
      return result == WFPartition::CmpResult::LESS_THAN;
    });
  }

  // aggregate functions
  void DoFunction(const WFPartitionKey &key, WFPartition &partition) {
    if (function_info_->order_by_.empty()) {
      BuildGlobalResult(key, partition);
    } else {
      BuildAccumulativeResult(key, partition);
    }
  }

  void BuildGlobalResult(const WFPartitionKey &key, WFPartition &partition) {
    const auto size = partition.data_.size();
    switch (function_info_->type_) {
      case WindowFunctionType::CountStarAggregate:
      case WindowFunctionType::CountAggregate: {
        for (uint32_t i = 0; i < size; ++i) {
          partition.data_[i].second = ValueFactory::GetIntegerValue(static_cast<int>(size));
        }
        return;
      }
      case WindowFunctionType::SumAggregate: {
        Value result = ValueFactory::GetZeroValueByType(function_info_->function_->GetReturnType());
        for (uint32_t i = 0; i < size; ++i) {
          result = result.Add(partition.data_[i].second);
        }
        for (uint32_t i = 0; i < size; ++i) {
          partition.data_[i].second = result;
        }
        return;
      }
      case WindowFunctionType::MinAggregate: {
        Value min_value = partition.data_[0].second;
        for (uint32_t i = 1; i < size; ++i) {
          const auto &current_value = partition.data_[i].second;
          if (current_value.CompareLessThan(min_value) == CmpBool::CmpTrue) {
            min_value = current_value;
          }
        }
        for (uint32_t i = 0; i < size; ++i) {
          partition.data_[i].second = min_value;
        }
        return;
      }
      case WindowFunctionType::MaxAggregate: {
        Value min_value = partition.data_[0].second;
        for (uint32_t i = 1; i < size; ++i) {
          const auto &current_value = partition.data_[i].second;
          if (current_value.CompareGreaterThan(min_value) == CmpBool::CmpTrue) {
            min_value = current_value;
          }
        }
        for (uint32_t i = 0; i < size; ++i) {
          partition.data_[i].second = min_value;
        }
        return;
      }
      case WindowFunctionType::Rank: {
        DoRank(partition);
        return;
      }
      default:
        assert(0);
    }
  }

  void BuildAccumulativeResult(const WFPartitionKey &key, WFPartition &partition) {
    const auto size = partition.data_.size();
    switch (function_info_->type_) {
      case WindowFunctionType::CountStarAggregate:
      case WindowFunctionType::CountAggregate: {
        for (uint32_t i = 0; i < size; ++i) {
          partition.data_[i].second = ValueFactory::GetIntegerValue(static_cast<int>(i) + 1);
        }
        return;
      }
      case WindowFunctionType::SumAggregate: {
        for (uint32_t i = 1; i < size; ++i) {
          auto &cur_val = partition.data_[i].second;
          auto &prev_val = partition.data_[i - 1].second;
          cur_val = cur_val.Add(prev_val);
        }
        return;
      }
      case WindowFunctionType::MinAggregate: {
        for (uint32_t i = 1; i < size; ++i) {
          auto &cur_val = partition.data_[i].second;
          auto &prev_val = partition.data_[i - 1].second;
          if (cur_val.CompareLessThan(prev_val) == CmpBool::CmpFalse) {
            cur_val = prev_val;
          }
        }
        return;
      }
      case WindowFunctionType::MaxAggregate: {
        for (uint32_t i = 1; i < size; ++i) {
          auto &cur_val = partition.data_[i].second;
          auto &prev_val = partition.data_[i - 1].second;
          if (cur_val.CompareGreaterThan(prev_val) == CmpBool::CmpFalse) {
            cur_val = prev_val;
          }
        }
        return;
      }
      case WindowFunctionType::Rank: {
        DoRank(partition);
        return;
      }
      default:
        assert(0);
    }
  }

  void DoRank(WFPartition &partition) {
    const auto size = partition.data_.size();
    int rank = 1;
    // since the column is sorted, we need to check for the rank
    auto cursor = partition.data_[0];  // we did not use a reference since this data will change
    for (int i = 1; i < size; ++i) {
      auto &current = partition.data_[i];
      const auto &result = WFPartition::Compare(current.first, cursor.first, function_info_->order_by_, *input_schema_);
      if (result == WFPartition::CmpResult::EQUALS) {
        current.second = ValueFactory::GetIntegerValue(rank);
      } else {
        assert(result == WFPartition::CmpResult::GREATER_THAN);
        current.second = ValueFactory::GetIntegerValue(i + 1);
        rank = i + 1;
        cursor = current;
      }
    }
  }

  const Schema *input_schema_{nullptr};  // schema of the table
  const WindowFunctionPlanNode::WindowFunction *function_info_{nullptr};
  std::unordered_map<WFPartitionKey, WFPartition> partition_ctx_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY
 * 0.2,0.3) FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::list<std::pair<uint32_t, WindowFunctionExecutionContext>> function_contexts_;
  std::vector<std::shared_ptr<Tuple>> tuple_store_;
  uint32_t cursor_;

  std::vector<Value> tuple_build_buffer_;

  RID dummy_rid_;
};

}  // namespace bustub
