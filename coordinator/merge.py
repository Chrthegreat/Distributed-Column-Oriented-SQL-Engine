def merge_results(partial_results, plan):
    """
    Merge partial query results returned by multiple workers.
    Two types of results are supported:
        • scan       → SELECT without GROUP BY
        • aggregate  → GROUP BY with aggregate functions
        
    Workers return JSON-like dictionaries, and this function combines
    those dictionaries into a single unified result for display.
    """
    # Filter out:
    #  - None responses (worker unreachable)
    #  - responses containing "error"
    #  - malformed responses missing "type"
    #
    # This ensures the merge logic only receives valid result blocks.
    valid_results = [
        res for res in partial_results
        if res and "error" not in res and "type" in res
    ]

    # If nothing valid was received from any worker, return None
    if not valid_results:
        return None
    
    # All valid workers should return the same "type"
    first_res = valid_results[0]
    response_type = first_res.get("type")

    # CASE 1: MERGING SCAN RESULTS  (SELECT * FROM table)
    if response_type == "scan":

        merged_data = []              # combined rows from all workers
        headers = first_res.get("headers")  # column names

        # Merge data arrays: simply concatenate worker results
        for res in valid_results:
            if "data" in res:
                merged_data.extend(res["data"])

        return {
            "type": "scan",
            "headers": headers,
            "data": merged_data
        }

    # CASE 2: MERGING AGGREGATE RESULTS (GROUP BY ...)
    if response_type == "aggregate":

        final_data = {}   # { group_key → [list of merged aggregate values] }

        # First Pass: Combine worker partial aggregates
        #
        # Each worker return looks something like:
        #   {
        #       "data": {
        #           "US":  [sum, count, max],
        #           "CA":  [sum, count, max],
        #       }
        #   }
        #
        # We must merge these lists element-by-element.
        for res in valid_results:
            worker_data = res["data"]

            # Loop through each group key from this worker
            for group_key, agg_vals in worker_data.items():

                # If this is the first time seeing this key → directly insert
                if group_key not in final_data:
                    final_data[group_key] = agg_vals
                else:
                    # Already exists → merge element-wise
                    current_vals = final_data[group_key]
                    new_vals = []

                    # Loop through each aggregate function defined in the query
                    for i, agg in enumerate(plan.aggregates):

                        v1 = current_vals[i]   # merged value so far
                        v2 = agg_vals[i]       # worker's new value

                        # Handle missing data from some workers
                        if v1 is None and v2 is None:
                            new_vals.append(None)
                            continue
                        if v1 is None:
                            new_vals.append(v2)
                            continue
                        if v2 is None:
                            new_vals.append(v1)
                            continue

                        # SUM just adds values
                        if agg.func == 'SUM':
                            new_vals.append(v1 + v2)
                        # COUNT also adds values
                        elif agg.func == 'COUNT':
                            new_vals.append(v1 + v2)
                        # MAX → take maximum across workers
                        elif agg.func == 'MAX':
                            new_vals.append(max(v1, v2))
                        # MIN → take minimum across workers
                        elif agg.func == 'MIN':
                            new_vals.append(min(v1, v2))
                        # AVG merging is special:
                        # Workers return AVG as [sum, count] pairs.
                        elif agg.func == 'AVG':
                            total_sum = v1[0] + v2[0]
                            total_count = v1[1] + v2[1]
                            new_vals.append([total_sum, total_count])

                    # Store merged aggregate results
                    final_data[group_key] = new_vals

        # Second Pass: Finalize AVG values
        #
        # At this point AVG entries are stored as [sum, count].
        # To compute the actual average → divide sum / count.
        for group_key in final_data:
            row = final_data[group_key]

            for i, agg in enumerate(plan.aggregates):
                val = row[i]

                # Only AVG requires finalizing
                if agg.func == 'AVG' and val is not None:
                    total_sum, total_count = val

                    if total_count == 0:
                        row[i] = None   # no rows -> NULL
                    else:
                        row[i] = total_sum / total_count

        # Return merged aggregate dictionary
        return {
            "type": "aggregate",
            "data": final_data
        }