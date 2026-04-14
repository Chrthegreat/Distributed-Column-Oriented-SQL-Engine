import os
import shutil
from read_schema import TableSchema

def init_table(table_dir, schema_source_path):
    """
    Here we implement the CLI command: minidist init <table_dir> <schema_file>
    """
    # First, create the directory
    if os.path.exists(table_dir):
        raise FileExistsError(f"Table directory '{table_dir}' already exists.")
    
    os.makedirs(table_dir)

    # Copy the Schema file to _schema.ssf inside the dir
    dest_schema_path = os.path.join(table_dir, "_schema.ssf")
    shutil.copy(schema_source_path, dest_schema_path)

    # Lets validate the schema immidiately to catch errors early
    try:
        TableSchema.from_file(dest_schema_path)
    except Exception as e:
        # Cleanup if schema is invalid
        shutil.rmtree(table_dir)
        raise ValueError(f"Invalid schema file: {e}")
    
    # Create _table.txt with all metadata
    metadata_path = os.path.join(table_dir, "_table.txt")
    with open(metadata_path, 'w') as file:
        file.write("version=final\n")
        # block_rows is how many values are loaded in memory each time.
        file.write("block_rows=65536\n")
        # How many lines each segment
        file.write("segment_target_rows=100000\n")
        file.write("endianness=little\n")
    
    print(f"Table initialized at {table_dir}")

