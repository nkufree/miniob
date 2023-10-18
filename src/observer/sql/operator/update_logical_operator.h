#pragma once

#include "sql/operator/logical_operator.h"
#include <vector>
/**
 * @brief 逻辑算子，用于执行delete语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator 
{
public:
  UpdateLogicalOperator(Table *table, int attr,const Value* values);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::UPDATE;
  }
  Table *table() const
  {
    return table_;
  }
  const Value* values() const
  {
    return values_;
  }

  int attr() const
  {
    return attr_;
  }
private:
  Table *table_ = nullptr;
  int attr_;
  const Value* values_;
};