from pathlib import Path
from typing import Literal

import pandas as pd
import yaml

_SCHEMAS_DIRECTORY = Path(__file__).parent.parent / "validation" / "schemas"


def read_csvs(
    directory: Path | str,
    schemas: Literal["pypsa_friendly_tables"] | None = None,
) -> dict[str : pd.DataFrame]:
    """Read all the CSVs in a directory into a dictionary with filenames (without csv
    extension) as keys.

    By default the type of each column is inferred from the file's contents,
    which goes wrong for two shapes of table: a CSV with a header and no rows
    comes back with every column as text, and a text column that is blank in
    every row comes back as numbers. Passing `schemas` reads each table that
    has a schema with the column types its schema declares, so the table has
    the same column types whatever rows it holds.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs

        Read CSVs from a directory containing parsed workbook tables.
        >>> iasr_tables = read_csvs(Path("parsed_workbook_cache"))

        Read CSVs from a directory containing ISPyPSA input tables.
        >>> ispypsa_tables = read_csvs(Path("ispypsa_inputs"))

        Read CSVs from a directory containing PyPSA-friendly tables, with the
        column types their schemas declare.
        >>> pypsa_friendly_tables = read_csvs(
        ...     Path("pypsa_friendly"), schemas="pypsa_friendly_tables"
        ... )

        Access a specific table from the dictionary.
        >>> generators = iasr_tables["existing_generators_summary"]

    Args:
        directory: Path to directory to read CSVs from.
        schemas: The set of table schemas to take column types from. Only
            "pypsa_friendly_tables" is supported so far. A table with no
            schema in the set keeps its inferred column types. If None (the
            default), every column type is inferred.

    Returns:
        dict[str, pd.DataFrame]: Dictionary with filenames (without .csv extension)
        as keys and DataFrames as values.
    """
    files = Path(directory).glob("*.csv")
    if schemas is None:
        return {file.name[:-4]: pd.read_csv(file) for file in files}
    schema_directory = _SCHEMAS_DIRECTORY / schemas
    return {
        file.name[:-4]: _read_csv_with_schema_types(file, schema_directory)
        for file in files
    }


def _read_csv_with_schema_types(file: Path, schema_directory: Path) -> pd.DataFrame:
    """Reads a CSV, giving each column the type its table schema declares.

    The schema is the YAML file in schema_directory named after the CSV. Each
    declared column is read as text and then converted (see
    _convert_text_to_schema_type), so its type doesn't depend on which rows
    the file holds. Columns the schema doesn't declare, and tables with no
    schema, keep the types pandas infers. A declared column missing from the
    file is not an error here: that is for schema validation to report.

    I/O Example:
        timeslice_snapshots.csv (header only):
            timeslice,investment_periods,snapshots

        columns of timeslice_snapshots.yaml:
            column              type
            timeslice           string
            investment_periods  int
            snapshots           datetime

        returns an empty table with the column types:
            timeslice  investment_periods  snapshots
            object     int64               datetime64[ns]

        Inferred, all three columns would be object.
    """
    schema_file = schema_directory / f"{file.stem}.yaml"
    if not schema_file.exists():
        return pd.read_csv(file)
    columns = yaml.safe_load(schema_file.read_text())["columns"]
    table = pd.read_csv(file, dtype=dict.fromkeys(columns, str))
    for name in table.columns.intersection(list(columns)):
        table[name] = _convert_text_to_schema_type(table[name], columns[name])
    return table


def _convert_text_to_schema_type(text: pd.Series, column_schema: dict) -> pd.Series:
    """Converts a column read as text to the type its schema declares.

    Covers the types the PyPSA-friendly schemas use: string, float, int and
    datetime. An int column with a blank cell becomes float64, because int64
    cannot hold a blank.

    I/O Example:
        type      text                     returns
        string    ["qld_peak", NaN]        ["qld_peak", NaN]      (object)
        string    [NaN, NaN]               [NaN, NaN]             (object)
        float     ["0.5", NaN]             [0.5, NaN]             (float64)
        int       ["2025", "2030"]         [2025, 2030]           (int64)
        int       ["2025", NaN]            [2025.0, NaN]          (float64)
        datetime  ["2025-01-01 00:00:00"]  [2025-01-01 00:00:00]  (datetime64[ns])
    """
    if column_schema["type"] == "datetime":
        return pd.to_datetime(text, format=column_schema["format"])
    if column_schema["type"] == "float":
        return text.astype("float64")
    if column_schema["type"] == "int" and text.notna().all():
        return text.astype("float64").astype("int64")
    if column_schema["type"] == "int":
        return text.astype("float64")
    return text


def write_csvs(data_dict: dict[str : pd.DataFrame], directory: Path | str):
    """Write all pd.DataFrames in a dictionary with filenames as keys (without csv extension)
    to CSVs.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import write_csvs

        Write ISPyPSA input tables to a directory.
        >>> write_csvs(ispypsa_tables, Path("ispypsa_inputs"))

        Write PyPSA-friendly tables to a directory.
        >>> write_csvs(pypsa_friendly_tables, Path("pypsa_friendly_inputs"))

        Write model results to a directory.
        >>> write_csvs(results, Path("outputs/results"))

    Args:
        data_dict: Dictionary of pd.DataFrames to write to csv files.
        directory: Path to directory to save CSVs to.

    Returns:
        None
    """
    for file_name, data in data_dict.items():
        save_path = Path(directory) / Path(f"{file_name}.csv")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        # set index=False to avoid adding "Unnamed" cols if/when reading from these csvs later
        data.to_csv(save_path, index=False)
