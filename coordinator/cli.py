import argparse
import os
import platform

def parse_args():
    parser = argparse.ArgumentParser(description="MiniDist SQL Coordinator")
    parser.add_argument("--table", type=str, required=True, 
                        help="Path to table directory (e.g., data/sales)")
    parser.add_argument("--port", type=int, required=True, 
                        help="Starting port number (e.g., 9001)")
    return parser.parse_args()

def clear_screen():
    system = platform.system()
    if system == 'Windows': os.system('cls')
    else: os.system('clear')

def print_help():
    print("\n" + "="*50)
    print("  MINIDIST SQL HELP MENU")
    print("="*50)
    print("  ; (Semicolon)  : Required at the end of every SQL query")
    print("  help           : Show this message")
    print("  clear / cls    : Clear the terminal screen")
    print("  exit / quit    : Exit the program")
    print("\nSupported SQL Syntax:")
    print("  SELECT * FROM sales;")
    print("  SELECT region,\n         SUM(amount)\n  FROM sales\n  GROUP BY region;")
    print("="*50 + "\n")