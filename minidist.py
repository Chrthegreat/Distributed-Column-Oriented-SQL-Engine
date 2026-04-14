import argparse            
import sys                
from create_table import init_table  
from load_csv import load_table       

def main():
    """
    Entry point of the MiniDist CLI tool.

    This function sets up the argument parser, defines subcommands,
    parses user input, and calls the appropriate handler functions
    depending on which command is executed ("init" or "load").
    """
    # CLI Setup — create the main parser
    parser = argparse.ArgumentParser(
        prog="minidist",   # Program name shown in the help message
        description="MiniDist, A Distributed, Column-Oriented SQL Engine",
    )

    # Create a container for subcommands, e.g., `minidist init ...` or `minidist load ...`
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Require that the user *must* choose a subcommand (Python < 3.7 behavior workaround)
    subparsers.required = True

    # SUBCOMMAND: INIT
    # Create a parser specifically for the "init" command
    parser_init = subparsers.add_parser("init", help="Initialize a new table")

    # Positional argument: directory where the new table will be created
    parser_init.add_argument(
        "table_dir",
        type=str,
        help="The directory where the table will be stored"
    )

    # Positional argument: path to the schema file used to define table structure
    parser_init.add_argument(
        "schema_file",
        type=str,
        help="Path to the .ssf schema file"
    )

    # SUBCOMMAND: LOAD
    # Create a parser for the "load" command
    parser_load = subparsers.add_parser("load", help="Load data into a table")

    # Positional argument: directory where the existing table already lives
    parser_load.add_argument("table_dir", type=str)

    # Required flag: path to CSV file that will be imported
    parser_load.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to input CSV"
    )

    # Optional flag: specify number of segments for parallel loading
    parser_load.add_argument(
        "--segments",
        type=int,
        default=None,
        help="Split data into N segments"
    )
    
    # Handle case: user typed no arguments at all
    # Print help message and exit cleanly.
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # Parse all command-line arguments into a namespace object
    args = parser.parse_args()

    # COMMAND EXECUTION SECTION
    # Depending on which subcommand the user chose, run the correct logic
    try:
        # INIT TABLE COMMAND
        if args.command == "init":
            print("Initializing new table...")
            print(f"  Directory: {args.table_dir}")
            print(f"  Schema:    {args.schema_file}")

            # Call the external function that actually creates the table on disk
            init_table(args.table_dir, args.schema_file)

            print("Table successfully initialized!")

        # LOAD DATA COMMAND
        elif args.command == "load":
            print("Loading CSV data into table...")
            print(f"  Table directory:    {args.table_dir}")
            print(f"  CSV file:           {args.csv}")
            print(f"  Number of segments: {args.segments}")

            # Call the external loader function
            load_table(args.table_dir, args.csv, args.segments)

            print("Data successfully loaded!")

    # ERROR HANDLING
    # Catch common exceptions and print clean error messages
    except FileNotFoundError as e:
        # A required file (schema, csv, etc.) was not found
        print(f"File not found: {e}")
        sys.exit(1)

    except Exception as e:
        # Any other unexpected error
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


