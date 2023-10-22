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

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief 选择/投影物理算子
 * @ingroup PhysicalOperator
 */
class ProjectPhysicalOperator : public PhysicalOperator
{
public:
  ProjectPhysicalOperator()
  {}

  virtual ~ProjectPhysicalOperator() = default;

  void add_expression(const std::unique_ptr<Expression>& expression);
  void add_projection(const Table *table, const FieldMeta *field);

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::PROJECT;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const
  {
    if(expressions_[0]->sys_func() < NO_SYS_FUNC)
        return tuple_.cell_num();
    else
        return aggre_tuple_.cell_num();
  }

  Tuple *current_tuple() override;

private:
  ProjectTuple tuple_;
  ValueListTuple aggre_tuple_;
  std::vector<std::unique_ptr<SysFuncExpr>> expressions_;
  bool is_print_ = false;
};
