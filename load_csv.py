import csv
import os
import shutil
import math
from read_schema import TableSchema

def write_segment(base_dir, segment_id, columns_data, schema):
    """
    Writes a segment to disk. Each segment is a folder containing one text file
    per active column, where each file holds the column's values for that segment.

    Example folder layout:
        seg-000001/
            id.txt
            name.txt
            age.txt
    """
    # Construct directory path for this segment 
    seg_dir = os.path.join(base_dir, f"seg-{segment_id:06d}")

    # If the segment already exists, delete it so we cleanly overwrite
    if os.path.exists(seg_dir):
        shutil.rmtree(seg_dir)
    os.makedirs(seg_dir, exist_ok=True)

    print(f"  -> Writing Segment {segment_id}")

    for col in schema.columns:
        # FIX: We now ALWAYS write the file, even if data is missing (it will be NULLs)
        file_path = os.path.join(seg_dir, f"{col.name}.txt")
        
        data_list = columns_data.get(col.name)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # If the column was never populated, write NULLs for the length of the batch
            if data_list is None:
                # We need to know how many rows are in this batch to write enough NULLs
                # We pick the first available column to measure length
                # (There is always at least the Primary Key, so this is safe)
                sample_col = next(iter(columns_data.values()))
                for _ in range(len(sample_col)):
                    f.write("NULL\n")
            else:
                for val in data_list:
                    if val is None:
                        f.write("NULL\n")
                    else:
                        f.write(f"{str(val)}\n")


def load_table(table_dir, csv_path, target_num_segments=None):
    """
    Loads CSV data into a table directory. This function:

    1. Reads the table schema
    2. Validates CSV columns (ensures Key exists)
    3. Loads all CSV rows
    4. Sorts rows by primary key
    5. Splits data into segments
    6. Writes each segment to disk
    """
    # Load Schema File
    schema_path = os.path.join(table_dir, "_schema.ssf")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Table not initialized. Missing {schema_path}")

    schema = TableSchema.from_file(schema_path)

    # Extract primary key information
    key_col_name = schema.key_column.name  # type: ignore
    key_dtype = schema.key_column.dtype    # type: ignore

    print(f"Loading data into '{table_dir}'...")
    print(f"Schema detected. Sorting by Key: {key_col_name} ({key_dtype})")

    # Load CSV Rows
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Get CSV header list
        csv_headers = reader.fieldnames if reader.fieldnames else []

        # Validate CSV contains required Primary Key
        if key_col_name not in csv_headers:
            raise ValueError(
                f"CRITICAL: The Primary Key column '{key_col_name}' "
                f"is missing from the CSV!"
            )

        # Determine which schema columns actually appear in the CSV
        active_columns = []
        for col in schema.columns:
            if col.name in csv_headers:
                active_columns.append(col)
            else:
                print(f"Warning: Schema column '{col.name}' not found in CSV. Ignoring.")

        # Load all rows into memory as a list of dicts
        rows = list(reader)

    total_rows = len(rows)
    print(f"  Read {total_rows} rows from CSV.")

    # Duplicate key check
    seen_keys = set()
    for idx, row in enumerate(rows):
        raw_val = row.get(key_col_name)
        
        # Check for NULL/Empty Primary Key
        if raw_val is None or raw_val.strip() == "" or raw_val.upper() == "NULL":
             raise ValueError(f"Data Integrity Error: Primary Key cannot be NULL (Row {idx + 2})")
        
        # Normalize value based on type (so "04" and "4" are seen as duplicates)
        check_val = raw_val
        if check_val is not None:
            if key_dtype in ['int32', 'int64']:
                try: check_val = int(raw_val) # type: ignore
                except ValueError: pass # Keep as string if conversion fails
            elif key_dtype == 'float64':
                try: check_val = float(raw_val) # type: ignore
                except ValueError: pass

        # Check strict uniqueness
        if check_val in seen_keys:
            # Row idx + 2 because: +1 for 0-index, +1 for CSV header row
            raise ValueError(f"Data Integrity Error: Duplicate Primary Key found: '{raw_val}' (Row {idx + 2})")
        
        seen_keys.add(check_val)

    # Sort rows by primary key
    def get_sort_key(row):
        """
        Safe key extraction: converts text to correct type
        for sorting. Empty or null keys → 0 to avoid crash.
        """
        val = row[key_col_name]
        if not val:
            return 0

        # Convert to correct type based on schema
        if key_dtype in ['int32', 'int64']:
            return int(val)
        elif key_dtype == 'float64':
            return float(val)

        # Default: treat key as string
        return val

    print(f"  Sorting by Key: {key_col_name}...")
    rows.sort(key=get_sort_key)

    # Determine Segment Size
    # User explicitly set number of segments
    if target_num_segments is not None and target_num_segments > 0:
        segment_limit = math.ceil(total_rows / target_num_segments)
        print(f"  [Config] Detected {target_num_segments} segments. "
              f"Split size: {segment_limit} rows.")

    # Read from _table.txt metadata OR fallback default
    else:
        meta_path = os.path.join(table_dir, "_table.txt")

        # Default fallback if metadata missing
        segment_limit = 100000

        # Look for segment_target_rows in metadata file
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                for line in f:
                    if line.startswith("segment_target_rows"):
                        parts = line.strip().split('=')
                        if len(parts) == 2:
                            segment_limit = int(parts[1])

        print(f"  [Config] Using metadata split size: {segment_limit} rows.")

    # Partition rows into segments and write to disk
    current_segment_id = 1

    # Initialize per-column buffers only for active CSV columns
    current_batch = {col.name: [] for col in active_columns}
    row_count = 0

    for row in rows:

        # Collect values for active columns
        for col in active_columns:
            raw_val = row.get(col.name, "")

            # Interpret empty string or "NULL" → None
            val = None if (raw_val == "" or raw_val.upper() == "NULL") else raw_val

            current_batch[col.name].append(val)

        row_count += 1

        # If we reached the segment limit → write the segment
        if row_count >= segment_limit:
            write_segment(table_dir, current_segment_id, current_batch, schema)

            # Prepare for next segment
            current_segment_id += 1
            current_batch = {col.name: [] for col in active_columns}
            row_count = 0

    # Write the final partial segment, if non-empty
    if row_count > 0:
        write_segment(table_dir, current_segment_id, current_batch, schema)

    print(f"Done! Loaded {len(rows)} rows into {current_segment_id} segments.")


        


