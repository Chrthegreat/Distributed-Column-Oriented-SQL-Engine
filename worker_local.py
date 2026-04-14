import os
from read_schema import TableSchema
from query_plan import QueryPlan

class LocalExecutor:
    """
    Executes a query locally for a single segment directory.

    Each worker runs one LocalExecutor, which:
        1. Loads columns from its segment into memory
        2. Applies filters (WHERE)
        3. Executes SELECT projections
        4. Executes GROUP BY + aggregates if needed

    The worker already received a parsed QueryPlan from the coordinator.
    """

    def __init__(self, segment_dir, schema_path):
        # Path to this segment's folder (contains file per column)
        self.segment_dir = segment_dir

        # Load full schema definition from table root (_schema.ssf)
        self.schema = TableSchema.from_file(schema_path)

    #  INTERNAL COLUMN LOADING
    def _read_column(self, col_name):
        """
        Loads a single column from disk (e.g., seg-000001/id.txt).

        Returns a Python list containing typed values:
            int, float, string, or None (for NULL)
        """
        # Look up this column in the schema
        col_def = next((c for c in self.schema.columns if c.name == col_name), None)
        if not col_def:
            raise ValueError(f"Column {col_name} not found")

        file_path = os.path.join(self.segment_dir, f"{col_name}.txt")
        data = []

        # Missing column file → treat as empty column
        if not os.path.exists(file_path):
            return []

        with open(file_path, 'r') as f:
            for line in f:
                val = line.strip()

                # Normalize NULL text → Python None
                if val == "NULL":
                    data.append(None)
                    continue

                # Cast text to typed value per schema
                if col_def.dtype in ['int32', 'int64']:
                    data.append(int(val))
                elif col_def.dtype == 'float64':
                    data.append(float(val))
                else:
                    data.append(val)

        return data

    #  VALUE CASTING FOR FILTERS
    def _cast_value(self, value, col_name):
        """
        Ensures that the filter constants match the schema type of the column.
        Examples:
            - single values:  '42' → 42
            - BETWEEN lists: ['10', '20'] → [10, 20]
        """
        # Recursive case: value list (used in BETWEEN)
        if isinstance(value, list):
            return [self._cast_value(v, col_name) for v in value]

        # Look up column type
        col_def = next((c for c in self.schema.columns if c.name == col_name), None)
        if not col_def:
            return value

        # Try casting to correct type
        try:
            if col_def.dtype in ['int32', 'int64']:
                # Using float → int handles strings like "10.0"
                return int(float(value))
            elif col_def.dtype == 'float64':
                return float(value)
            else:
                return str(value)
        except:
            return value  # Fallback: keep uncastable values as-is

    # PREDICATE EVALUATION (WHERE)
    def _evaluate_predicate(self, data_row, op, target_val):
        """
        Applies the filter operator to a single value.

        Example:
            data_row = 25
            op       = '>'
            target   = 20
            → returns True
        """

        if data_row is None:
            # NULL never satisfies comparisons
            return False

        if op == '>':  return data_row > target_val
        if op == '<':  return data_row < target_val
        if op == '>=': return data_row >= target_val
        if op == '<=': return data_row <= target_val
        if op == '=':  return data_row == target_val

        if op == 'BETWEEN':
            # target_val guaranteed to be [low, high]
            return target_val[0] <= data_row <= target_val[1]

        return False  # Unknown operator fallback

    def execute(self, plan: QueryPlan):
        """
        Executes the plan for this segment.

        PHASES:
            1. Determine which columns to load
            2. Load these columns into memory
            3. Apply WHERE filter
            4. Either:
                • perform a scan (SELECT)
                • perform grouping + aggregation (GROUP BY)
        """

        # PHASE 1: Determine columns needed for this query
        # Projection target columns
        if "*" in plan.select_columns:
            target_cols = [c.name for c in self.schema.columns]
        else:
            target_cols = plan.select_columns

        # Build set of all required columns:
        #   SELECT columns
        #   filter column
        #   GROUP BY column
        #   aggregate columns
        cols_to_load = set(target_cols)

        if plan.filter:
            cols_to_load.add(plan.filter.column)

        if plan.group_by:
            cols_to_load.add(plan.group_by)

        for agg in plan.aggregates:
            if agg.column != '*':
                cols_to_load.add(agg.column)

        # PHASE 2: Load required columns
        columns_data = {}
        row_count = 0

        for col in cols_to_load:
            columns_data[col] = self._read_column(col)
            row_count = len(columns_data[col])  # All columns have same length

        # PHASE 3: Apply WHERE filter (row index selection)
        if not plan.filter:
            # No filter → use all row indices
            valid_indices = range(row_count)

        else:
            valid_indices = []
            filter_data = columns_data[plan.filter.column]

            # Convert filter constants to correct Python types
            safe_target_val = self._cast_value(plan.filter.value, plan.filter.column)

            # Evaluate predicate row by row
            for i, val in enumerate(filter_data):
                if self._evaluate_predicate(val, plan.filter.operator, safe_target_val):
                    valid_indices.append(i)

        # PHASE 4A: Simple scan (SELECT without GROUP BY)
        if not plan.aggregates and not plan.group_by:
            results = []

            for i in valid_indices:
                row = []
                for col_name in target_cols:
                    row.append(columns_data[col_name][i])
                results.append(row)

            return {
                "type": "scan",
                "headers": target_cols,
                "data": results
            }

        # PHASE 4B: GROUP BY + AGGREGATE
        results = {}  # { group_key → [agg results] }

        uses_grouping = (plan.group_by is not None)
        group_col_data = columns_data[plan.group_by] if uses_grouping else []

        # Process each row belonging to this segment
        for i in valid_indices:

            # Determine group key
            key = group_col_data[i] if uses_grouping else "GLOBAL"

            # Initialize aggregate vector for this group
            if key not in results:
                results[key] = [None] * len(plan.aggregates)

            # Apply each aggregate function
            for agg_idx, agg in enumerate(plan.aggregates):

                raw_val = columns_data[agg.column][i]
                curr = results[key][agg_idx]

                # Skip NULL entries
                if raw_val is None:
                    continue
                # SUM
                if agg.func == 'SUM':
                    results[key][agg_idx] = (curr or 0) + raw_val
                # COUNT 
                elif agg.func == 'COUNT':
                    results[key][agg_idx] = (curr or 0) + 1
                # MIN 
                elif agg.func == 'MIN':
                    results[key][agg_idx] = raw_val if curr is None else min(curr, raw_val)
                # MAX 
                elif agg.func == 'MAX':
                    results[key][agg_idx] = raw_val if curr is None else max(curr, raw_val)
                # AVG 
                elif agg.func == 'AVG':
                    # Store running sums as [sum, count]
                    if curr is None:
                        results[key][agg_idx] = [raw_val, 1]
                    else:
                        results[key][agg_idx][0] += raw_val
                        results[key][agg_idx][1] += 1

        return {
            "type": "aggregate",
            "data": results
        }

