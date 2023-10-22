/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/01.
//

#include "common/log/log.h"
#include "sql/operator/project_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (children_.empty() || is_print_) {
    return RC::RECORD_EOF;
  }
  if(expressions_[0]->sys_func() < NO_SYS_FUNC)
  {
    PhysicalOperator *child = children_[0].get();
    std::vector<Value> values;
    while(RC::SUCCESS == child->next())
    {
        Tuple* tmp = child->current_tuple();
        for(std::unique_ptr<SysFuncExpr> &exp : expressions_)
        {
            exp->add_tuple(tmp);
        }
    }
    for(std::unique_ptr<SysFuncExpr> &exp : expressions_)
    {
        Value v;
        exp->get_value(v);
        values.push_back(v);
    }
    aggre_tuple_.set_cells(values);
    is_print_ = true;
    return RC::SUCCESS;
  }
  return children_[0]->next();
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple *ProjectPhysicalOperator::current_tuple()
{
    if(expressions_[0]->sys_func() < NO_SYS_FUNC)
    {
        return &aggre_tuple_;
    }
    else
    {
        tuple_.set_tuple(children_[0]->current_tuple());
        return &tuple_;
    }
  
}

void ProjectPhysicalOperator::add_expression(const std::unique_ptr<Expression>& expression)
{
    ASSERT(expression->type() == ExprType::SYS_FUNC, "project's expression should be SYS_FUNC type");
    SysFuncExpr* tmp = static_cast<SysFuncExpr*>(expression.get());
    std::unique_ptr<SysFuncExpr> p(new SysFuncExpr(*tmp));
    TupleCellSpec *spec = new TupleCellSpec(p->table_name(), p->field_name(), p->sys_func());
  tuple_.add_cell_spec(spec);
  expressions_.push_back(std::move(p));
}

void ProjectPhysicalOperator::add_projection(const Table *table, const FieldMeta *field_meta)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  TupleCellSpec *spec = new TupleCellSpec(table->name(), field_meta->name(), field_meta->name());
  tuple_.add_cell_spec(spec);
}
