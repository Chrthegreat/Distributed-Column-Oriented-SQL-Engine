import os

def discover_workers(table_dir, start_port):
    """
    Discover worker processes based on how many segment directories exist
    inside the table directory.

    Each worker is expected to serve exactly one segment. Therefore:
        seg-000001 → worker listening on start_port + 0
        seg-000002 → worker listening on start_port + 1
        seg-000003 → worker listening on start_port + 2
        ...
    Example:
        table_dir = "data/people"
        start_port = 5050
        If there are 3 segment folders inside table_dir, this returns:
            [
                ('127.0.0.1', 5050),
                ('127.0.0.1', 5051),
                ('127.0.0.1', 5052),
            ]
    """

    # Check that the table directory exists.
    # If user mistypes the path or the table isn't initialized,
    # we cannot proceed with worker discovery.
    if not os.path.exists(table_dir):
        print(f"Error: Directory '{table_dir}' not found.", flush=True)
        return []

    # Count segment directories inside the table folder.
    # A valid segment folder always starts with "seg-" followed by
    # a six-digit zero-padded number (e.g., seg-000001).
    #
    # We assume that each segment corresponds to a worker and 
    # workers are assigned sequential ports starting at start_port.
    segments = [d for d in os.listdir(table_dir) if d.startswith("seg-")]
    count = len(segments)

    # If no segments exist, the table is unusable for distributed execution.
    if count == 0:
        print(f"Error: No 'seg-XXXXXX' folders found in {table_dir}", flush=True)
        return []

    # Next, build a list of worker endpoints.
    # Workers are assumed to run on localhost (127.0.0.1) and each
    # listens on a consecutive port number.
    # Here is an example: start_port = 9001, count = 3
    #    → [('127.0.0.1', 9001),
    #       ('127.0.0.1', 9002),
    #       ('127.0.0.1', 9003)]
    worker_list = [
        ('127.0.0.1', start_port + i)
        for i in range(count)
    ]

    return worker_list