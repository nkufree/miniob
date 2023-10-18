#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, int attr, const Value* values) : table_(table), attr_(attr), values_(values)
{}