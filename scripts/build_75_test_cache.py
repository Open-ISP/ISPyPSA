"""One-off: build tests/test_workbook_table_cache/7.5 from the local 7.5 workbook.

Run with the new-format flag set:

    ISPYPSA_USE_NEW_TABLE_FORMAT=true uv run python scripts/build_75_test_cache.py
"""

from pathlib import Path

from ispypsa.iasr_table_caching import build_local_cache


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    workbook_path = (
        repo_root
        / "data"
        / "workbooks"
        / "7.5"
        / "Draft 2026 ISP Inputs and Assumptions workbook.xlsx"
    )
    cache_path = repo_root / "tests" / "test_workbook_table_cache" / "7.5"

    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found at: {workbook_path}")

    cache_path.mkdir(parents=True, exist_ok=True)
    build_local_cache(cache_path, workbook_path, "7.5")
    print(f"Wrote cache CSVs to {cache_path}")


if __name__ == "__main__":
    main()
