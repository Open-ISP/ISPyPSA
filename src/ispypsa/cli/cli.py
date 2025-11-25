#!/usr/bin/env python3
"""
ISPyPSA Command Line Interface

This module provides a command-line interface that runs ISPyPSA workflows
with config=value parameter support.

Usage:
    ispypsa config=path/to/config.yaml TASK [OPTIONS]
    ispypsa list
"""

import sys
from pathlib import Path

from doit.doit_cmd import DoitMain

# Get the path to the dodo.py file in the ISPyPSA package
DODO_FILE = Path(__file__).parent / "dodo.py"


def main():
    """Main entry point for the ispypsa CLI."""
    # Insert the -f flag to specify the dodo.py location
    # and -d flag to stay in current directory
    args = ["-f", str(DODO_FILE), "-d", "."] + sys.argv[1:]

    # Use doit directly - it natively supports config=value syntax
    sys.exit(DoitMain().run(args))


if __name__ == "__main__":
    main()
