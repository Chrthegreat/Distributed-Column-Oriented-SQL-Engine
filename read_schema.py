import os
from dataclasses import dataclass
from typing import List

# Supported column types
# This set defines which data types are valid in .ssf schema files.
# You can extend this later to add decimals, dates, etc.
VALID_TYPES = {
    'int32', 'int64', 'float64', 'bool', 'string', 'data', 'timestamp'
}

# A Column is a lightweight container that describes one schema field.
@dataclass
class Column:
    name: str       # Name of the column (e.g., "id", "amount", "region")
    dtype: str      # Data type as declared in schema (e.g., int64, string)
    nullable: bool  # Whether NULL values are allowed in this column
    is_key: bool    # Whether this is the primary key column

# Table schema class
# Represents the structure of one table: list of columns + key column.
class TableSchema:

    def __init__(self, columns: List[Column]):
        """
        Initialize schema given a list of Column definitions.
        Ensures that:
            • columns are stored
            • exactly one key column exists
        """
        self.columns = columns

        # Determine the primary key column.
        # next(...) takes the first column where is_key = True.
        # If no such column exists, next(..., None) returns None.
        self.key_column = next((c for c in columns if c.is_key), None)

        # Enforce that every table must have a primary key
        if not self.key_column:
            raise ValueError("Schema must have a column marked as 'key'")

    # LOAD SCHEMA FROM FILE
    @staticmethod
    def from_file(file_path) -> 'TableSchema':
        """
        Parse a .ssf schema file and return a TableSchema object.

        Expected format example:

            # Example schema file
            id: int64 key
            region: string
            amount: float64 nullable

        Rules:
            • Lines starting with # are ignored
            • Blank lines are ignored
            • Format must include a colon separating name and type info
            • Extra tokens after type may include: nullable, key
        """

        columns = []

        # Read the file line by line
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                # Skip comments and blank lines
                if not line or line.startswith('#'):
                    continue

                # Every valid schema line must contain "name: type ..."
                if ':' not in line:
                    raise ValueError(f"Invalid line format: {line}")

                # split only on first colon → "name" and "type tokens..."
                column_name, column_types = line.split(':', 1)

                # Cleanup spaces
                name = column_name.strip()

                # Tokens = [dtype, [nullable], [key]]
                tokens = column_types.strip().split()
                if not tokens:
                    raise ValueError(f"Missing type for column {name}")

                # First token is dtype
                dtype = tokens[0]
                if dtype not in VALID_TYPES:
                    raise ValueError(f"Unsupported data type: {dtype}")

                # Check optional flags
                is_nullable = ('nullable' in tokens)
                is_key = ('key' in tokens)

                # Add parsed column definition
                columns.append(Column(name, dtype, is_nullable, is_key))

        # Construct and return final schema object
        return TableSchema(columns)
            
                
