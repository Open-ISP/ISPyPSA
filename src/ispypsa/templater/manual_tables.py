from pathlib import Path

import pandas as pd


def template_manually_extracted_tables(iasr_workbook_version: str):
    """Retrieves the manually extracted template files for the IASR workbook version.

    Args:
        iasr_workbook_version: str specifying which version of the workbook is being
            used to create the template.

    Returns:
        list[`pd.DataFrame`]
    """
    path_to_tables = (
        Path(__file__).parent
        / Path("manually_extracted_template_tables")
        / Path(iasr_workbook_version)
    )
    files_not_to_move = ["transmission_expansion_costs.csv"]
    csv_files = path_to_tables.glob("*.csv")
    df_files = {}
    for file in csv_files:
        if file.name not in files_not_to_move:
            df_files[file.name] = pd.read_csv(file)
    return df_files
