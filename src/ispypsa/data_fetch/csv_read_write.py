from pathlib import Path

import pandas as pd


def read_csvs(directory: Path | str) -> dict[str : pd.DataFrame]:
    """Read all the CSVs in a directory into a dictionary with filenames (without csv
    extension) as keys.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs

        Read CSVs from a directory containing parsed workbook tables.
        >>> iasr_tables = read_csvs(Path("parsed_workbook_cache"))

        Read CSVs from a directory containing ISPyPSA input tables.
        >>> ispypsa_tables = read_csvs(Path("ispypsa_inputs"))

        Access a specific table from the dictionary.
        >>> generators = iasr_tables["existing_generators_summary"]

    Args:
        directory: Path to directory to read CSVs from.

    Returns:
        dict[str, pd.DataFrame]: Dictionary with filenames (without .csv extension)
        as keys and DataFrames as values.
    """
    files = Path(directory).glob("*.csv")
    return {file.name[:-4]: pd.read_csv(file) for file in files}


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
