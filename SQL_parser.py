import re
from query_plan import QueryPlan, FilterPredicate, Aggregate

class SQLParser:
    def parse(self, query_str):
        """
        Parses a subset of SQL into a QueryPlan object.
        Supported: SELECT, FROM, WHERE (simple), GROUP BY
        """
        q = query_str.strip()

        # Extract Clauses using Regex
        # This pattern looks for keywords and captures everything in between
        # flags=re.IGNORECASE allows 'select' or 'SELECT'
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', q, re.IGNORECASE)
        from_match = re.search(r'FROM\s+(.*?)(?:\s+WHERE|\s+GROUP BY|$)', q, re.IGNORECASE)
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP BY|$)', q, re.IGNORECASE)
        group_match = re.search(r'GROUP BY\s+(.*)', q, re.IGNORECASE)

        if not select_match or not from_match:
            raise ValueError("Invalid SQL: Must have SELECT and FROM")
        
        # Parse table name
        table_name = from_match.group(1).strip()

        # Parse Select Columns & Aggregates
        # Example: "region, SUM(amount), COUNT(id)"
        raw_selects = [s.strip() for s in select_match.group(1).split(',')]

        select_cols = []
        aggregates = []

        for token in raw_selects:
            # Check for Aggregates like SUM(col)
            agg_match = re.match(r'(SUM|COUNT|MIN|MAX|AVG)\((.*?)\)', token, re.IGNORECASE)
            if agg_match:
                func = agg_match.group(1).upper()
                col = agg_match.group(2).strip()
                aggregates.append(Aggregate(func,col))
            else:
                select_cols.append(token)

        # Parse WHERE Clause (Single Predicate for now)
        # Example: "amount > 100" or "id BETWEEN 1 AND 5"
        filter_pred = None
        if where_match:
            where_str = where_match.group(1).strip()

            # Check for BETWEEN
            between_match = re.search(r'(\w+)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)', where_str, re.IGNORECASE)
            
            if between_match:
                col = between_match.group(1)
                low = float(between_match.group(2))
                high = float(between_match.group(3))
                filter_pred = FilterPredicate(col, 'BETWEEN', [low,high])
            else:
                # Check for standard operators: >=, <=, >, <, =
                # We accept simple "col op val"
                op_match = re.search(r'(\w+)\s*(>=|<=|>|<|=)\s*(.*)', where_str)
                if op_match:
                    col = op_match.group(1)
                    op = op_match.group(2)
                    val_str = op_match.group(3).strip().strip("'").strip('"')

                    # Try to convert to number
                    try:
                        val = float(val_str)
                    except ValueError:
                        val = val_str
                    
                    filter_pred = FilterPredicate(col,op,val)

        # Parse Group By
        group_col = None
        if group_match:
            group_col = group_match.group(1).strip()
        
        return QueryPlan(
            table=table_name,
            select_columns=select_cols,
            aggregates=aggregates,
            filter=filter_pred,
            group_by=group_col                       
        )





