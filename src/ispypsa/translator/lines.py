from pathlib import Path

import pandas as pd

from ispypsa.translator.mappings import _LINE_ATTRIBUTES


def translate_flow_paths_to_lines(ispypsa_inputs_path: Path | str) -> pd.DataFrame:
    """Process network line data into a format aligned with PyPSA inputs.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    lines = pd.read_csv(ispypsa_inputs_path / Path("flow_paths.csv"))
    lines = lines.loc[:, _LINE_ATTRIBUTES.keys()]
    lines = lines.rename(columns=_LINE_ATTRIBUTES)
    lines = lines.set_index("name", drop=True)
    return lines
