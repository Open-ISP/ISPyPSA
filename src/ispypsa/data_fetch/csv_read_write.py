from pathlib import Path

import pandas as pd


def read_csvs(directory: Path | str) -> dict[str : pd.DataFrame]:
    """Read all the CSVs in a directory into a dictionary with filenames (without csv
    extension) as keys.

    Args:
        directory: Path to directory to read CSVs from.

    Returns:
        `pd.DataFrame`: Cleaned generator summary DataFrame
    """
    files = Path(directory).glob("*.csv")
    return {file.name[:-4]: pd.read_csv(file) for file in files}


def write_csvs(data_dict: dict[str : pd.DataFrame], directory: Path | str):
    """Write all pd.DataFrames in a dictionary with filenames as keys (without csv extension)
    to CSVs.

    Args:
        data_dict: Dictionary of pd.DataFrames to write to csv files.
        directory: Path to directory to save CSVs to.

    """
    for file_name, data in data_dict.items():
        save_path = Path(directory) / Path(f"{file_name}.csv")
        # set index=False to avoid adding "Unnamed" cols if/when reading from these csvs later
        data.to_csv(save_path, index=False)
