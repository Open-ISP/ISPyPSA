#!/usr/bin/env python3
"""
ISPyPSA Command Line Interface

This module provides a command-line interface that wraps the dodo task runner,
allowing users to run ISPyPSA workflows using the `ispypsa` command instead of `dodo`.

Usage:
    ispypsa --config path/to/config.yaml run_capacity_expansion
    ispypsa --config path/to/config.yaml list
    ispypsa --config path/to/config.yaml clean
"""

import os
import subprocess
import sys
from pathlib import Path


def find_dodo_file():
    """Find the dodo.py file in the current working directory or installed package location."""
    # First check if dodo.py exists in current directory
    current_dir_dodo = Path.cwd() / "dodo.py"
    if current_dir_dodo.exists():
        return current_dir_dodo

    # If not found, look for the installed package dodo.py
    # This would be in the same directory as this cli.py file
    package_dir = Path(__file__).parent.parent.parent
    package_dodo = package_dir / "dodo.py"
    if package_dodo.exists():
        return package_dodo

    # If still not found, raise an error
    raise FileNotFoundError(
        "Could not find dodo.py. Please ensure you are in the ISPyPSA project directory "
        "or that ISPyPSA is properly installed."
    )


def main():
    """Main entry point for the ispypsa CLI."""
    try:
        # Parse our own arguments first, before passing to doit
        import argparse

        parser = argparse.ArgumentParser(
            description="ISPyPSA workflow runner",
            add_help=False,  # Don't add help to avoid conflicts with doit
        )
        parser.add_argument(
            "--config", type=str, required=True, help="Path to config file (required)"
        )
        parser.add_argument("--debug", action="store_true", help="Enable debug output")

        # Parse known args, leaving the rest for doit
        args, remaining_args = parser.parse_known_args()

        debug = args.debug
        config_path = args.config

        # Store the original working directory (where user ran the command)
        original_cwd = Path.cwd()

        # Find the dodo.py file
        dodo_path = find_dodo_file()

        if debug:
            print(f"Found dodo.py at: {dodo_path}", file=sys.stderr)

        # Get the directory containing dodo.py to set as working directory
        dodo_dir = dodo_path.parent

        if debug:
            print(f"Original working directory: {original_cwd}", file=sys.stderr)
            print(f"Dodo directory: {dodo_dir}", file=sys.stderr)
            if config_path:
                print(f"Config path: {config_path}", file=sys.stderr)

        # Set up environment variables
        env = os.environ.copy()

        # Always set original working directory for path resolution
        if original_cwd != dodo_dir:
            env["ISPYPSA_ORIGINAL_CWD"] = str(original_cwd)

        # Pass config path via environment variable
        # Resolve config path relative to original working directory
        if not Path(config_path).is_absolute():
            resolved_config = original_cwd / config_path
        else:
            resolved_config = Path(config_path)
        env["ISPYPSA_CONFIG_PATH"] = str(resolved_config)

        # Construct the doit command with remaining arguments
        # doit reads dodo.py automatically from the current working directory
        cmd = ["doit"] + remaining_args

        if debug:
            print(f"Command: {' '.join(cmd)}", file=sys.stderr)
            if "ISPYPSA_ORIGINAL_CWD" in env:
                print(
                    f"Setting ISPYPSA_ORIGINAL_CWD={env['ISPYPSA_ORIGINAL_CWD']}",
                    file=sys.stderr,
                )
            if "ISPYPSA_CONFIG_PATH" in env:
                print(
                    f"Setting ISPYPSA_CONFIG_PATH={env['ISPYPSA_CONFIG_PATH']}",
                    file=sys.stderr,
                )

        # Run doit with the remaining arguments, in the directory containing dodo.py
        result = subprocess.run(
            cmd,
            cwd=dodo_dir,
            env=env,
            capture_output=False,  # Allow direct output to terminal
            text=True,
        )

        # Exit with the same code as doit
        sys.exit(result.returncode)

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
