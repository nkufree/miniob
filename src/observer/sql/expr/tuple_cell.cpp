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
// Created by WangYunlai on 2022/07/05.
//

#include <sstream>
#include "sql/expr/tuple_cell.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"

static std::string SysFuncToName[]
{
    "max",
    "min",
    "count",
    "count",
    "avg",
    "sum",
    std::string(""),
};

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias)
{
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  sys_func_ = NO_SYS_FUNC;
  sys_func_name_ = "";
  if (alias) {
    alias_ = alias;
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
    } else {
      alias_ = table_name_ + "." + field_name_;
    }
  }
}

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, SysFunc sys_func, const char *alias)
{
    if (table_name) {
    table_name_ = table_name;
    }
    if (field_name) {
        field_name_ = field_name;
    }
    sys_func_ = sys_func;
    sys_func_name_ = SysFuncToName[sys_func];
    if (alias) {
        alias_ = alias;
    } else {
        if (table_name_.empty()) {
        alias_ = field_name_;
        } else {
        alias_ = table_name_ + "." + field_name_;
        }
        if(sys_func != NO_SYS_FUNC) {
            if(sys_func == SYS_COUNT_NUM)
                alias = "*";
            alias_ = sys_func_name_ + "(" + alias_ + ")";
        }
    }
}

TupleCellSpec::TupleCellSpec(const char *field_name, SysFunc sf)
{
    if(field_name) {
        field_name_ = field_name;
    }
    sys_func_ = sf;
    sys_func_name_ = SysFuncToName[sf];
    alias_ = field_name;
    if(sf != NO_SYS_FUNC) {
        if(sf == SYS_COUNT_NUM)
            alias_ = "*";
        alias_ = sys_func_name_ + "(" + alias_ + ")";
    }
}


TupleCellSpec::TupleCellSpec(const char *alias)
{
  if (alias) {
    alias_ = alias;
  }
}
