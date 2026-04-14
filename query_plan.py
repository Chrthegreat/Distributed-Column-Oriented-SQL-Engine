from dataclasses import dataclass, field
from typing import List, Optional, Any

# Represents a single filter condition in the WHERE clause.
# Example:
#   WHERE age > 30
#   WHERE country = 'US'
#   WHERE amount BETWEEN 100 AND 200
@dataclass
class FilterPredicate:
    column: str        # Column being filtered (e.g., 'age', 'country')
    operator: str      # Comparison operator: '>', '<', '=', '>=', '<=', 'BETWEEN'
    value: Any         # Value to compare against (int, string, or list for BETWEEN)

# Represents one aggregate function in a SELECT clause.
# Example:
#   SELECT SUM(amount), COUNT(id)
#
# Each aggregate corresponds to one item in plan.aggregates.
@dataclass
class Aggregate:
    func: str      # Aggregate function name: 'SUM', 'COUNT', 'MIN', 'MAX'
    column: str    # Column the function applies to (e.g., 'amount')

# The execution plan produced by the SQL parser.
# This is the structure that gets sent to workers.
# It describes:
#   - Which table to scan
#   - Which columns to return
#   - Any filtering rules
#   - Any aggregate functions
#   - Any GROUP BY column
# Workers use this to know what operation to execute.
@dataclass
class QueryPlan:
    table: str                         # Table name to query (e.g., "users")
    select_columns: List[str]          # Columns to return (if no aggregates)
    aggregates: List[Aggregate] = field(default_factory=list)  # Optional aggregates
    filter: Optional[FilterPredicate] = None  # Optional WHERE clause
    group_by: Optional[str] = None     # Column to group by (if any)
