import sys
import time
import socket
from SQL_parser import SQLParser
from serialization import plan_to_json

from .cli import parse_args, clear_screen, print_help
from .discovery import discover_workers
from .networking import send_query_to_worker
from .merge import merge_results
from .display import print_results

def run_coordinator():
    """
    Main entry point of the distributed coordinator.
    This function:
        1. Parses command-line arguments
        2. Discovers workers based on the table directory
        3. Verifies worker connectivity
        4. Enters the interactive SQL input loop
        5. Sends parsed plans to workers
        6. Merges worker results and prints them
    """
    # Parse CLI arguments
    args = parse_args()

    # Discover workers based on number of segment folders
    workers = discover_workers(args.table, args.port)

    if not workers:
        print("No workers configured. Exiting.", flush=True)
        sys.exit(1)

    print("MiniDist Distributed Coordinator", flush=True)
    print(f"Targeting Table: '{args.table}'", flush=True)
    print(
        f"Expecting {len(workers)} Workers "
        f"(Ports {args.port}-{args.port + len(workers)-1})",
        flush=True
    )
    
    # --- STARTUP CHECK ---
    print("Verifying cluster connectivity...", flush=True)
    
    missing_workers = []
    for i, (ip, port) in enumerate(workers):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5) # Fast timeout for check
            result = sock.connect_ex((ip, port))
            sock.close()
            
            if result != 0:
                missing_workers.append(port)
        except Exception:
            missing_workers.append(port)

    if missing_workers:
        print(f"FATAL ERROR: Could not connect to {len(missing_workers)} of {len(workers)} expected workers.", flush=True)
        print(f"Missing Ports: {missing_workers}", flush=True)
        print("Possible causes:", flush=True)
        print(f" 1. Wrong '--port' argument? (You provided {args.port})", flush=True)
        print(" 2. Workers are not running.", flush=True)
        sys.exit(1)
        
    print(f"Success! All {len(workers)} workers are active.", flush=True)

    # Create SQL Parser Instance
    parser = SQLParser()

    # MAIN REPL LOOP — User enters SQL queries interactively
    while True:
        try:
            # Buffer for multi-line SQL input
            lines = []
            prompt = "\nDIST-SQL> "

            # READ MULTILINE line SQL input 
            while True:
                try:
                    line = input(prompt).strip()
                except EOFError:
                    # Handle Ctrl+D gracefully
                    sys.exit(0)

                # If buffer is empty, allow fast commands
                if not lines:
                    lower = line.lower()

                    # User typed exit, quit program
                    if lower in ['exit', 'quit']:
                        print("Goodbye!")
                        sys.exit(0)

                    # Clear terminal output
                    if lower in ['clear', 'cls']:
                        clear_screen()
                        prompt = "\nDIST-SQL> "
                        continue

                    # Print help menu
                    if lower == 'help':
                        print_help()
                        continue

                # Skip empty lines in multi-line SQL
                if not line:
                    continue
                # Add the line to working buffer
                lines.append(line)
                # If the line ends in semicolon, we finalize the SQL query
                if line.endswith(';'):
                    break
                # Otherwise show continuation prompt
                prompt = "      ... "

            # Merge buffered lines and remove trailing semicolon
            query_str = " ".join(lines).rstrip(';')

            # PARSE SQL INTO EXECUTION PLAN
            try:
                plan = parser.parse(query_str)
            except ValueError as e:
                print(f"Parser Error: {e}", flush=True)
                continue

            # Convert plan to JSON to send to workers
            plan_json = plan_to_json(plan)
            print("Sending subqueries to workers...", flush=True)
            start_time = time.time()
            # SEND PLAN TO EACH WORKER AND COLLECT results
            results = []
            for ip, port in workers:
                res = send_query_to_worker(ip, port, plan_json)
                if res:
                    results.append(res)
            # MERGE PARTIAL RESULTS
            final_result = merge_results(results, plan)

            duration = (time.time() - start_time) * 1000
            print(f"Distributed execution: {duration:.2f} ms", flush=True)

            # No valid results returned
            if not final_result:
                print("No results received.", flush=True)
                continue
            print_results(final_result, plan)

        except Exception as e:
            print(f"Coordinator Error: {e}", flush=True)