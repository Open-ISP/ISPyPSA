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
    """Read all the CSVs in a directory into a dictionary with filenames (without csv
    extension) as keys.

    Args:
        data_dict: Dictionary of pd.DatatFrames to write to csv files.
        directory: Path to directory to save CSVs to.

    Returns:
        `pd.DataFrame`: Cleaned generator summary DataFrame
    """
    for file_name, data in data_dict.items():
        save_path = Path(directory) / Path(f"{file_name}.csv")
        data.to_csv(save_path)
