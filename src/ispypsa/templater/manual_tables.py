from pathlib import Path

import pandas as pd


def load_manually_extracted_tables(
    iasr_workbook_version: str,
) -> dict[str : pd.DataFrame]:
    """Retrieves the manually extracted template files for the IASR workbook version.

    Some tables can't be handled by `isp-workbook-parser` so ISPyPSA ships with the
    missing data pre-extracted for each supported workbook version.

    Examples:
        Perform required imports.
        >>> from ispypsa.templater import load_manually_extracted_tables

        Load the manually extracted tables for the workbook version.
        >>> manually_extracted_tables = load_manually_extracted_tables("6.0")

        Access a specific table from the dictionary.
        >>> custom_constraints_rhs = manually_extracted_tables["custom_constraints_rhs"]

    Args:
        iasr_workbook_version: str specifying which version of the workbook is being
            used to create the template.

    Returns:
        dict[str: `pd.DataFrame`]
    """
    path_to_tables = (
        Path(__file__).parent
        / Path("manually_extracted_template_tables")
        / Path(iasr_workbook_version)
    )
    csv_files = path_to_tables.glob("*.csv")
    df_files = {}
    for file in csv_files:
        df_files[file.name.replace(".csv", "")] = pd.read_csv(file)
    return df_files
