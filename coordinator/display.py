def print_results(final_result, plan):
    """
    Pretty-print the final merged result returned by the coordinator.
    Parameters
    ----------
    final_result : dict
        The merged dictionary returned by merge_results().
        Example format:
            {
                "type": "scan",
                "headers": [...],
                "data": [...]
            }

            or

            {
                "type": "aggregate",
                "data": { group_key: [list of aggregate values] }
            }

    plan : SQLPlan
        The parsed query plan (contains group_by, aggregates, etc.)
    """
    
    # CASE 1: SCAN OUTPUT  (SELECT * FROM table;, simple result set)
    if final_result['type'] == 'scan':

        # Extract column headers from result JSON
        headers = final_result['headers']

        # The width of the table = (# columns) * (estimated width per column)
        print("-" * (len(headers) * 13), flush=True)

        # Print header row aligned to 10 characters per column
        # Example:   id        | name      | price
        print(" | ".join([f"{h:<10}" for h in headers]), flush=True)

        # Print another separator line under the header
        print("-" * (len(headers) * 13), flush=True)

        # Print each row of data (list of row lists)
        for row in final_result['data']:

            # Convert each value:
            # None → "NULL"
            # Otherwise keep as string
            row_fmt = [f"{str(v) if v is not None else 'NULL':<10}" for v in row]

            # Join with column separators
            print(" | ".join(row_fmt), flush=True)

    # CASE 2: AGGREGATE OUTPUT  (GROUP BY ...)
    elif final_result['type'] == 'aggregate':

        # Build headers dynamically:
        # 1) GROUP BY column (optional)
        # 2) Each aggregate function, like SUM(price), AVG(height), etc.
        headers = []

        # If query includes GROUP BY, the first column is the grouping key
        if plan.group_by:
            headers.append(plan.group_by)

        # Add each aggregate name: e.g., SUM(amount), MAX(age)
        for agg in plan.aggregates:
            headers.append(f"{agg.func}({agg.column})")

        # Separator width = (# columns) * estimated width
        print("-" * (len(headers) * 15), flush=True)

        # Print formatted header row (12 chars per column)
        print(" | ".join([f"{h:<12}" for h in headers]), flush=True)

        # Separator line under header
        print("-" * (len(headers) * 15), flush=True)

        # Iterate through final merged aggregate results
        # final_result['data'] is a dict: { group_key: [agg1, agg2, ...] }
        for group_key, agg_vals in final_result['data'].items():

            row_parts = []

            # If GROUP BY exists, print the group value first
            if plan.group_by:
                row_parts.append(f"{group_key:<12}")

            # Print each aggregate result
            for val in agg_vals:

                # Pretty formatting for floats (2 decimals)
                if isinstance(val, float):
                    val_str = f"{val:.2f}"
                else:
                    # None → NULL
                    val_str = str(val) if val is not None else "NULL"

                # Align text to 12 characters
                row_parts.append(f"{val_str:<12}")

            # Print the row `"col1 | col2 | col3"`
            print(" | ".join(row_parts), flush=True)