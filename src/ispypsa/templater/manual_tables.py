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

    csv_files = path_to_tables.glob("*.csv")
    return {file.name: pd.read_csv(file) for file in csv_files}
