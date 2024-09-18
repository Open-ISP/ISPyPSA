from pathlib import Path

from ispypsa.data_fetch.local_cache import REQUIRED_TABLES, build_local_cache


def build_parsed_workbook_cache(cache_location: Path):
    workbook_path = list(
        Path(Path.cwd().parent, "isp-workbook-parser", "workbooks", "6.0").glob(
            "*.xlsx"
        )
    ).pop()
    build_local_cache(cache_location, workbook_path)


def task_cache_required_tables():
    cache_location = Path("model_inputs", "workbook_table_cache")
    return {
        "actions": [(build_parsed_workbook_cache, [cache_location])],
        "targets": [Path(cache_location, table + ".csv") for table in REQUIRED_TABLES],
        # force doit to always mark the task as up-to-date (unless target removed)
        # N.B. this will NOT run if target exists but has been modified
        "uptodate": [True],
    }
