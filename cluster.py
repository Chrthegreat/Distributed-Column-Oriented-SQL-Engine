import os
import sys
import subprocess
import time
import argparse

# Use the same Python interpreter that launched this script
# This ensures workers run in the same environment (virtualenv, etc.)
PYTHON_EXE = sys.executable

def parse_args():
    """
    Defines command-line parameters for launching the worker cluster.

    Required:
        --table  → path to the table directory (containing seg-XXXXXX folders)
        --port   → starting port for the first worker
    """
    parser = argparse.ArgumentParser(description="MiniDist Cluster Launcher")

    parser.add_argument(
        "--table", type=str, required=True,
        help="Path to table directory (e.g., data/sales)"
    )

    parser.add_argument(
        "--port", type=int, required=True,
        help="Starting port number (e.g., 9001)"
    )

    return parser.parse_args()

def run_cluster():
    """
    Launches one worker process per segment directory.

    For each folder named 'seg-000001', 'seg-000002', ...
    a worker is launched on a unique port:

        seg-000001 → port 9001
        seg-000002 → port 9002
        seg-000003 → port 9003
        ...

    The cluster stays active until the user presses Ctrl+C,
    after which all worker processes are terminated gracefully.
    """

    args = parse_args()

    # VALIDATE TABLE DIRECTORY
    if not os.path.exists(args.table):
        print(f"Error: Table directory '{args.table}' not found.", flush=True)
        return

    # Now, find segment directories
    # We detect workers by scanning for subfolders that start with "seg-".
    # Each such folder is one data segment → one worker process.
    segments = sorted([
        d for d in os.listdir(args.table)
        if d.startswith("seg-") and os.path.isdir(os.path.join(args.table, d))
    ])

    if not segments:
        print(f"No segments found in '{args.table}'. Did you run 'minidist load'?", flush=True)
        return

    schema_path = os.path.join(args.table, "_schema.ssf")
    processes = []

    print(f"--- Launching Cluster for Table: {os.path.basename(args.table)} ---", flush=True)
    print(f"Found {len(segments)} segments.", flush=True)

    # Spawn one worker per segment
    for i, seg in enumerate(segments):
        # Assign port sequentially from the user-specified starting port
        port = args.port + i
        seg_path = os.path.join(args.table, seg)

        # Worker command:
        #     python worker_server.py <port> <segment_path> <schema_path>
        cmd = [
            PYTHON_EXE,
            "worker_server.py",
            str(port),
            seg_path,
            schema_path
        ]

        # Create a background process
        p = subprocess.Popen(cmd)
        processes.append(p)
        print(
            f"  -> Launched Worker {i+1} on Port {port} "
            f"(PID: {p.pid}) | Segment: {seg}",
            flush=True
        )
    # Cluster Status
    print("-" * 50, flush=True)
    print(
        f"Cluster ACTIVE. Ports: {args.port} - {args.port + len(processes) - 1}",
        flush=True
    )
    print("Press Ctrl+C to shutdown.", flush=True)
    print("-" * 50, flush=True)

    # main loop: Run until user interrupts (Ctrl+C)
    try:
        while True:
            # We don't need to do anything — workers run independently.
            time.sleep(1)
    # Stutdown (Ctrl+C)
    except KeyboardInterrupt:
        print("\n\n--- Stopping Cluster ---", flush=True)

        for p in processes:
            # Only terminate processes that are still alive
            if p.poll() is None:
                print(f"Killing PID {p.pid}...", flush=True)
                p.terminate()

        print("All workers stopped.", flush=True)

if __name__ == "__main__":
    run_cluster()