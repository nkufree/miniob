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
  if(specToAggre_.size() == 0)
  {
    return RC::SUCCESS;
  }
  const std::vector<TupleCellSpec *>& speces = tuple_.get_speces();
  int num = 0;
  if(RC::SUCCESS == child->next())
  {
    num++;
    Tuple *tuple = child->current_tuple();
    for(auto spec : speces)
    {
      Value cell;
      tuple->find_cell(*spec, cell);
      switch(specToAggre_[spec]) {
        case SYS_SUM: {
          specToValue_[spec] = Value(1);
        } break;
        default : {
          specToValue_[spec] = cell;
        }
      }
    }
  }
  while(RC::SUCCESS == child->next()) {
    num++;
    Tuple *tuple = child->current_tuple();
    for(auto spec : speces)
    {
      Value cell;
      tuple->find_cell(*spec, cell);
      switch(specToAggre_[spec])
      {
        case SYS_MAX:{
          cell.compare(specToValue_[spec]) > 0 ? specToValue_[spec] = cell : specToValue_[spec];
        } break;
        case SYS_MIN:{
          cell.compare(specToValue_[spec]) < 0 ? specToValue_[spec] = cell : specToValue_[spec];
        } break;
        case SYS_COUNT:{
          specToValue_[spec].set_int(specToValue_[spec].get_int() + 1);
        } break;
        case SYS_AVG:
        case SYS_SUM: {
          if(cell.attr_type() == INTS)
          {
            specToValue_[spec].set_int(specToValue_[spec].get_int() + cell.get_int());
          }
          else if(cell.attr_type() == FLOATS)
          {
            specToValue_[spec].set_float(specToValue_[spec].get_float() + cell.get_float());
          }
        } break;
      }
    }
  }
  for(auto spec : speces)
  {
    if(specToAggre_[spec] == SYS_AVG)
    {
      if(specToValue_[spec].attr_type() == INTS)
      {
        specToValue_[spec].set_float(specToValue_[spec].get_int() / num);
      }
      else if(specToValue_[spec].attr_type() == FLOATS)
      {
        specToValue_[spec].set_float(specToValue_[spec].get_float() / num);
      }
    }
  }
  tuple_.set_spec_aggre(&specToValue_);
  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (children_.empty() || specToAggre_.size() == 0) {
    return RC::RECORD_EOF;
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
  if(specToAggre_.size() == 0)
  {
    return &tuple_;
  }
  tuple_.set_tuple(children_[0]->current_tuple());
  return &tuple_;
}

void ProjectPhysicalOperator::add_projection(const Table *table, const FieldMeta *field_meta)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  TupleCellSpec *spec = new TupleCellSpec(table->name(), field_meta->name(), field_meta->name());
  tuple_.add_cell_spec(spec);
  specToAggre_[spec];
}
