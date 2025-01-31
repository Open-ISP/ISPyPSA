from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import annuitised_investment_costs
from ispypsa.translator.mappings import _LINE_ATTRIBUTES


def translate_flow_paths_to_lines(
    ispypsa_inputs_path: Path | str,
    expansion_on: bool,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Process network line data into a format aligned with PyPSA inputs.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.
        expansion_on: bool indicating if transmission line expansion is considered.
        wacc: float, as fraction, indicating the weighted average coast of capital for
            transmission line investment, for the purposes of annuitising capital
            costs.
        asset_lifetime: int specifying the nominal asset lifetime in years or the
            purposes of annuitising capital costs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    lines = pd.read_csv(ispypsa_inputs_path / Path("flow_paths.csv"))
    costs = pd.read_csv(ispypsa_inputs_path / Path("transmission_expansion_costs.csv"))
    lines = pd.merge(lines, costs, how="left", on="flow_path_name")

    lines = lines.loc[:, _LINE_ATTRIBUTES.keys()]
    lines = lines.rename(columns=_LINE_ATTRIBUTES)
    lines = lines.set_index("name", drop=True)

    lines["capital_cost"] = lines["capital_cost"].apply(
        lambda x: annuitised_investment_costs(x, wacc, asset_lifetime)
    )

    # not extendable by default
    lines["s_nom_extendable"] = False
    # If a non-nan capital_cost is given then set to extendable
    lines.loc[~lines["capital_cost"].isna(), "s_nom_extendable"] = expansion_on

    return lines
