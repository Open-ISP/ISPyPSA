from pathlib import Path

import pandas as pd


def _translate_ecaa_generators(
    template_path: Path | str,
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) generators
    into a format aligned with PyPSA inputs.

    Args:
        template_path: Path to directory containing modelling input template CSVs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    ecaa_generators_template = (
        pd.read_csv(template_path / Path("ecaa_generators_template.csv")))
