from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import _REZ_LINE_ATTRIBUTES


def _translate_renewable_energy_zone_build_limits_to_flow_paths(
    renewable_energy_zone_build_limits: pd.DataFrame,
    expansion_on: bool,
    wacc: float,
    asset_lifetime: int,
    rez_to_sub_region_transmission_default_limit: float,
) -> pd.DataFrame:
    """Process renewable energy zone build limit data to format aligned with PyPSA
    inputs.

    Args:
        renewable_energy_zone_build_limits: `ISPyPSA` formatted pd.DataFrame detailing
            Renewable Energy Zone transmission limits.
        wacc: float, as fraction, indicating the weighted average cost of capital for
            transmission line investment, for the purposes of annuitising capital
            costs.
        asset_lifetime: int specifying the nominal asset lifetime in years or the
            purposes of annuitising capital costs.
        rez_to_sub_region_transmission_default_limit: float specifying the transmission
            limit to use for rez to subregion connections when an explicit limit
            is not given in the inputs.

    Returns:
        `pd.DataFrame`: PyPSA style line attributes in tabular format.
    """
    lines = renewable_energy_zone_build_limits.loc[:, _REZ_LINE_ATTRIBUTES.keys()]
    lines = lines.rename(columns=_REZ_LINE_ATTRIBUTES)
    lines["name"] = lines["bus0"] + "-" + lines["bus1"]

    # Lines without an explicit limit because their limits are modelled through
    # custom constraints are given a very large capacity because using inf causes
    # infeasibility
    lines["s_nom"] = lines["s_nom"].fillna(rez_to_sub_region_transmission_default_limit)

    lines["capital_cost"] = lines["capital_cost"].apply(
        lambda x: _annuitised_investment_costs(x, wacc, asset_lifetime)
    )

    # not extendable by default
    lines["s_nom_extendable"] = False
    # If a non-nan capital_cost is given then set to extendable
    lines.loc[~lines["capital_cost"].isna(), "s_nom_extendable"] = expansion_on

    return lines
