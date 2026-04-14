import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from coordinator.cli import parse_args
from coordinator.repl import run_coordinator

def main():
    args = parse_args()
    try:
        run_coordinator()
    except KeyboardInterrupt:
        print("\nExiting coordinator...")
        sys.exit(0)

if __name__ == "__main__":
    main()