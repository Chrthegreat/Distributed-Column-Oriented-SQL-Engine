import socket
import sys
import json
import os
import struct
from worker_local import LocalExecutor
from serialization import json_to_plan
from read_schema import TableSchema

BUFFER_SIZE = 4096


def get_segment_min_max(segment_path, schema_path):
    """
    Compute a simple zone map (min, max) for the primary key column of this segment.

    A "zone map" is metadata allowing fast filtering:
        If a query asks for rows outside the [min, max] range of the segment,
        we can skip reading this entire segment.

    Reads ONLY the first and last line of the PK column file, because segments
    are already sorted on disk.
    """
    try:
        # Load schema to determine PK name and type
        schema = TableSchema.from_file(schema_path)
        key_col = schema.key_column.name   # type: ignore
        key_dtype = schema.key_column.dtype   # type: ignore

        # Path to this segment’s primary key file
        file_path = os.path.join(segment_path, f"{key_col}.txt")
        if not os.path.exists(file_path):
            return None, None

        # Read all lines (should be small enough — one column only)
        with open(file_path, 'r') as f:
            lines = f.readlines()

            # Remove blank lines
            valid_lines = [l.strip() for l in lines if l.strip()]
            if not valid_lines:
                return None, None

            # First element and last element represent min & max because segment is sorted
            first_line = valid_lines[0]
            last_line = valid_lines[-1]

        # Convert PK text values into actual typed numbers/strings
        def convert(val):
            if key_dtype in ['int32', 'int64']:
                return int(val)
            if key_dtype == 'float64':
                return float(val)
            return val

        return convert(first_line), convert(last_line)

    except Exception as e:
        print(f"Warning: Zone Map Error: {e}", flush=True)
        return None, None

def start_worker(port, segment_path, schema_path):
    """
    Start a TCP worker server responsible for one segment of the table.

    Responsibilities:
        - Load schema
        - Load zone map for pruning
        - Listen for incoming coordinator requests
        - Deserialize plan
        - Optionally skip the work using zone map pruning
        - Execute local computation with LocalExecutor
        - Send back results (length-prefixed JSON)
    """
    # Worker ID for logging
    try:
        clean_path = segment_path.rstrip(os.sep)
        seg_folder = os.path.basename(clean_path)
        worker_id = int(seg_folder.split('-')[-1])
    except (ValueError, IndexError):
        # Fallback if segment folder naming is unexpected
        worker_id = port

    prefix = f"[Worker {worker_id}]"

    def log(msg):
        """Small helper to prefix messages with the worker ID."""
        print(f"{prefix} {msg}", flush=True)

    # Basic validation
    if not os.path.exists(segment_path):
        log(f"Error: Segment {segment_path} does not exist.")
        return

    abs_path = os.path.abspath(segment_path)
    # Example: /home/user/data/sales/seg-000001  → table name is "sales"
    my_table_name = os.path.basename(os.path.dirname(abs_path))
    segment_name = os.path.basename(segment_path)

    # Load zone map (min/max PK)
    min_key, max_key = get_segment_min_max(segment_path, schema_path)
    # log(f"Zone Map: [{min_key}, {max_key}]")

    # Initialize executor
    try:
        executor = LocalExecutor(segment_path, schema_path)
    except Exception as e:
        log(f"Failed to load schema: {e}")
        return
    
    # Create TCP server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Allows quick rebind after crashes
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Worker listens on 0.0.0.0:<port>
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    server_socket.settimeout(1.0)  # So Ctrl+C works without hanging

    # log(f"Ready on port {port}. Waiting...")

    # MAIN WORKER LOOP — accept() then process client request
    try:
        while True:
            # Accept client (coordinator) connection
            try:
                client_sock, addr = server_socket.accept()
            except socket.timeout:
                continue  # Loop again
            except OSError:
                break     # Server shutting down

            try:
                # Read the incoming JSON plan
                data = client_sock.recv(BUFFER_SIZE)
                if not data:
                    client_sock.close()
                    continue

                plan = json_to_plan(data.decode('utf-8'))

                # Table mismatch safeguard
                if plan.table != my_table_name:
                    log(f"Ignored query for table '{plan.table}'")
                    client_sock.close()
                    continue

                # ZONE-MAP PRUNING (Skip entire segment if useless)
                skipped = False

                # Use zone map only if:
                #   - Query has a filter
                #   - Zone map exists (min_key not None)
                if plan.filter and min_key is not None:

                    # Only prune on PK column (example: id)
                    if plan.filter.column == 'id':
                        op = plan.filter.operator
                        val = plan.filter.value

                        # Convert values to numeric if needed
                        def to_num(v):
                            try:
                                return float(v)
                            except:
                                return v

                        # BETWEEN pruning 
                        if op == 'BETWEEN' and isinstance(val, list) and len(val) >= 2:
                            low = to_num(val[0])
                            high = to_num(val[1])

                            # If [low, high] does not overlap [min_key, max_key]
                            if high < min_key or low > max_key:   # type: ignore
                                skipped = True

                        # Single-value comparisons 
                        elif not isinstance(val, list):
                            val = to_num(val)

                            # If query range is 100% before this segment
                            if op == '<' and val <= min_key:   # type: ignore
                                skipped = True
                            if op == '<=' and val < min_key:   # type: ignore
                                skipped = True

                            # If query range is 100% after this segment
                            if op == '>' and val >= max_key:   # type: ignore
                                skipped = True
                            if op == '>=' and val > max_key:   # type: ignore
                                skipped = True

                            # '=' checks for single matching value
                            if op == '=' and (val < min_key or val > max_key):  # type: ignore
                                skipped = True

                # Build either skipped-result or real execution
                if skipped:
                    log("-> Skipped query (zone map)")

                    # If output is aggregate → return empty aggregates
                    if plan.aggregates or plan.group_by:
                        result = {"type": "aggregate", "data": {}}

                    # Otherwise return an empty scan
                    else:
                        if "*" in plan.select_columns:
                            headers = [c.name for c in executor.schema.columns]
                        else:
                            headers = plan.select_columns

                        result = {"type": "scan", "headers": headers, "data": []}

                else:
                    # Execute the full plan on this segment
                    log("-> Executing Plan...")
                    result = executor.execute(plan)
                    log("<- Sent results.")

                # Encode and send length-prefixed JSON result
                resp = json.dumps(result).encode('utf-8')

                # Prefix 4 bytes: big-endian integer message length
                client_sock.sendall(struct.pack('>I', len(resp)) + resp)

            except Exception as e:
                # If something fails inside worker execution
                log(f"CRITICAL ERROR: {e}")

                # Send error back to coordinator so it can skip this worker
                try:
                    err = json.dumps({"error": str(e)}).encode('utf-8')
                    client_sock.sendall(struct.pack('>I', len(err)) + err)
                except:
                    pass

            finally:
                client_sock.close()

    except KeyboardInterrupt:
        log("Stopping worker...")
    finally:
        server_socket.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python worker_server.py <port> <segment_path> <schema_path>")
        sys.exit(1)

    start_worker(int(sys.argv[1]), sys.argv[2], sys.argv[3])


