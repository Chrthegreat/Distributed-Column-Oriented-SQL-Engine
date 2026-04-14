import socket
import struct
import json

def send_query_to_worker(worker_ip, worker_port, plan_json):
    """
    Send a serialized query plan to one worker and receive its response.
    
    NETWORK PROTOCOL (worker_server.py implements the other side):
        1. Client sends UTF-8 JSON text immediately after connection.
        2. Worker responds with:
              [4-byte big-endian length][JSON payload]
        3. This function first reads the 4-byte length,
           then reads the full JSON payload in chunks until complete.

    Returns:
        Python object decoded from worker’s JSON response,
        or None on timeout / connection error.
    """

    try:
        # Create a TCP socket for communication with this worker
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Prevent hanging forever if a worker crashes or freezes
        sock.settimeout(2.0)

        # Connect to worker (each worker is a TCP server)
        sock.connect((worker_ip, worker_port))

        # SEND PLAN
        # The coordinator already converted the QueryPlan into JSON text.
        # We encode to bytes and send all at once (workers expect this).
        sock.sendall(plan_json.encode('utf-8'))

        # RECEIVE RESPONSE HEADER (4 bytes: message length)
        header_data = sock.recv(4)

        # Worker closed the connection prematurely
        if not header_data:
            return None

        # Interpret the 4-byte header as a big-endian unsigned integer
        msg_len = struct.unpack('>I', header_data)[0]

        # receive full message payload in chunks
        chunks = []
        bytes_recd = 0

        while bytes_recd < msg_len:
            # Read up to remaining bytes, or 4096 whichever is smaller
            chunk = sock.recv(min(msg_len - bytes_recd, 4096))

            # If chunk is empty, the worker disconnected
            if not chunk:
                break

            chunks.append(chunk)
            bytes_recd += len(chunk)

        # Combine all chunks and decode JSON
        full_data = b''.join(chunks).decode('utf-8')
        return json.loads(full_data)

    # ERROR HANDLING
    except socket.timeout:
        # Worker didn't respond in 2 seconds
        print(f"Error: Worker on port {worker_port} timed out!", flush=True)
        return None

    except ConnectionRefusedError:
        # Worker is not running, wrong port, or crashed
        print(f"Error: Worker on port {worker_port} refused connection (is it running?)", flush=True)
        return None

    except Exception as e:
        # Catch-all for unexpected socket failures
        print(f"Error: Worker {worker_port} failed: {e}", flush=True)
        return None

    finally:
        # Always ensure socket is closed, even on errors
        sock.close()