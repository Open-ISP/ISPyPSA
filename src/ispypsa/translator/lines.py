from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import _LINE_ATTRIBUTES


def _translate_flow_paths_to_lines(
    flow_paths: pd.DataFrame,
    expansion_on: bool,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Process network line data into a format aligned with PyPSA inputs.

    Args:
        flow_paths: `ISPyPSA` formatted pd.DataFrame detailing flow path capabilities
            between regions or sub regions depending on granularity.
        expansion_on: bool indicating if transmission line expansion is considered.
        wacc: float, as fraction, indicating the weighted average coast of capital for
            transmission line investment, for the purposes of annuitising capital
            costs.
        asset_lifetime: int specifying the nominal asset lifetime in years or the
            purposes of annuitising capital costs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    lines = flow_paths.loc[:, _LINE_ATTRIBUTES.keys()]
    lines = lines.rename(columns=_LINE_ATTRIBUTES)

    lines["capital_cost"] = lines["capital_cost"].apply(
        lambda x: _annuitised_investment_costs(x, wacc, asset_lifetime)
    )

    # not extendable by default
    lines["s_nom_extendable"] = False
    # If a non-nan capital_cost is given then set to extendable
    lines.loc[~lines["capital_cost"].isna(), "s_nom_extendable"] = expansion_on

    return lines
