"""Builds tests/test_workbook_table_cache/7.5 from the IASR 7.5 workbook.

Run with the new-format flag set, so the new-format required tables are cached:

    ISPYPSA_USE_NEW_TABLE_FORMAT=true uv run python scripts/build_75_test_cache.py

The workbook is read from data/workbooks/7.5/ under the repo root and downloaded
there first if it is missing. To use a workbook file kept somewhere else, pass its
path:

    ISPYPSA_USE_NEW_TABLE_FORMAT=true uv run python scripts/build_75_test_cache.py path/to/7.5.xlsx

Files already in the cache are overwritten, never deleted: a table dropped from the
required list keeps its old CSV until it is removed by hand.
"""

import sys
from pathlib import Path

from isp_workbook_parser import Parser

from ispypsa.data_fetch import fetch_workbook
from ispypsa.iasr_table_caching import build_local_cache

# Tables the templater doesn't need, but the PLEXOS-vs-workbook custom constraint
# cross-check (tests/test_templater/custom_constraints_validation.py) reads.
_TABLES_ONLY_TESTS_READ = [
    "rez_group_constraint_summary",
    "new_entrant_electrolysers_summary",
]


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    workbook_path = (
        repo_root
        / "data"
        / "workbooks"
        / "7.5"
        / "Draft 2026 ISP Inputs and Assumptions workbook.xlsx"
    )
    if len(sys.argv) > 1:
        workbook_path = Path(sys.argv[1])
    cache_path = repo_root / "tests" / "test_workbook_table_cache" / "7.5"

    if not workbook_path.exists():
        fetch_workbook("7.5", workbook_path)

    build_local_cache(cache_path, workbook_path, "7.5")
    Parser(workbook_path).save_tables(cache_path, tables=_TABLES_ONLY_TESTS_READ)
    print(f"Wrote cache CSVs to {cache_path}")


if __name__ == "__main__":
    main()
