import os
import time
from SQL_parser import SQLParser
from worker_local import LocalExecutor

# This is only for local execution. Only one worker will run for seg-000001

# Configuration
DATA_DIR = "data"

def clear_screen():
    """Clears the terminal screen based on OS."""
    os.system('clear')

def print_help():
    print("\n" + "="*50)
    print("  MINIDIST SQL HELP MENU")
    print("="*50)
    print("Commands:")
    print("  help           : Show this message")
    print("  exit / quit    : Exit the program")
    print("  clear          : Clear the console")
    print("\nSupported SQL Syntax:")
    print("  SELECT * FROM <table>")
    print("  SELECT col1, col2 FROM <table>")
    print("  SELECT col1, AGG(col2) FROM <table> GROUP BY col1")
    print("\nExamples:")
    print("  > SELECT * FROM sales")
    print("  > SELECT region, amount FROM sales WHERE amount > 100")
    print("  > SELECT region, SUM(amount), COUNT(id) FROM sales GROUP BY region")
    print("  > SELECT * FROM sales WHERE id BETWEEN 1 AND 5")
    print("="*50 + "\n")

def run_repl():
    print("MiniDist SQL Engine (Local Model)")
    print("Type 'exit' to quit")

    parser = SQLParser()

    while True:
        try:
            # Get input
            query_str = input("\nMiniDist SQL> ")
            if query_str.strip().lower() in ['exit','quit']:
                break
            if not query_str.strip():
                continue

            if query_str.lower() == 'help':
                print_help()
                continue

            if query_str.lower() in ['clear', 'cls']:
                clear_screen()
                continue
            
            start_time = time.time()

            # Parse
            plan = parser.parse(query_str)

            # Resolve Table Path (Coordinator Logic)g.
            table_path = os.path.join(DATA_DIR, plan.table)
            segment_path = os.path.join(table_path, "seg-000001")
            schema_path = os.path.join(table_path, "_schema.ssf")

            if not os.path.exists(segment_path):
                print(f"Error: Table '{plan.table}' or segment not found.")
                continue

            # Execute
            executor = LocalExecutor(segment_path, schema_path)
            response = executor.execute(plan) # Now returns a dict with "type"

            # Print Results
            duration = (time.time() - start_time) * 1000
            print(f"\nQuery executed in {duration:.2f} ms")
            print("-" * 40)
            
            # Display logic
            if response["type"] == "scan":
                # Print Headers
                headers = response["headers"]
                print(" | ".join([f"{h:<10}" for h in headers]))
                print("-" * 40)
                # Print Rows
                for row in response["data"]:
                    # Convert all values to string for safe joining
                    row_str = " | ".join([f"{str(v):<10}" for v in row])
                    print(row_str)

            elif response["type"] == "aggregate":
                # Print Headers
                headers = []
                if plan.group_by: headers.append(plan.group_by)
                for agg in plan.aggregates: headers.append(f"{agg.func}({agg.column})")
                
                print(" | ".join([f"{h:<10}" for h in headers]))
                print("-" * 40)

                # Print Rows
                results = response["data"]
                for group_key, agg_vals in results.items():
                    row_parts = []
                    if plan.group_by: row_parts.append(f"{group_key:<10}")
                    for val in agg_vals:
                        row_parts.append(f"{str(val):<10}")
                    print(" | ".join(row_parts))

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    run_repl()
