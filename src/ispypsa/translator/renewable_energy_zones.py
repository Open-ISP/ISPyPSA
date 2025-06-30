import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.links import _translate_expansion_costs_to_links
from ispypsa.translator.mappings import _REZ_LINK_ATTRIBUTES


def _translate_renewable_energy_zone_build_limits_to_links(
    renewable_energy_zone_build_limits: pd.DataFrame,
    rez_expansion_costs: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    """Process renewable energy zone build limit data to format aligned with PyPSA
    inputs, incorporating time-varying expansion costs.

    Args:
        renewable_energy_zone_build_limits: `ISPyPSA` formatted pd.DataFrame detailing
            Renewable Energy Zone transmission limits.
        rez_expansion_costs: `ISPyPSA` formatted pd.DataFrame detailing Renewable Energy
            Zone expansion costs by year.
        config: ModelConfig object containing wacc, investment periods, etc.

    Returns:
        `pd.DataFrame`: PyPSA style link attributes in tabular format.
    """
    # Create existing links from renewable energy zone build limits
    existing_links = _translate_existing_rez_connections_to_links(
        renewable_energy_zone_build_limits,
        config.network.rez_to_sub_region_transmission_default_limit,
    )

    # Create expansion links from rez expansion costs if expansion is enabled
    if config.network.rez_transmission_expansion and not rez_expansion_costs.empty:
        expansion_links = _translate_expansion_costs_to_links(
            expansion_costs=rez_expansion_costs,
            existing_links_df=existing_links.copy(),
            investment_periods=config.temporal.capacity_expansion.investment_periods,
            year_type=config.temporal.year_type,
            wacc=config.wacc,
            asset_lifetime=config.network.annuitisation_lifetime,
            id_column="rez_constraint_id",
            match_column="bus0",
        )
        # Combine existing and expansion links
        all_links = pd.concat(
            [existing_links, expansion_links], ignore_index=True, sort=False
        )
    else:
        all_links = existing_links

    return all_links


def _translate_existing_rez_connections_to_links(
    renewable_energy_zone_build_limits: pd.DataFrame,
    rez_to_sub_region_transmission_default_limit: float,
) -> pd.DataFrame:
    """Process existing REZ connection limits to PyPSA links.

    Args:
        renewable_energy_zone_build_limits: `ISPyPSA` formatted pd.DataFrame detailing
            Renewable Energy Zone transmission limits.
        rez_to_sub_region_transmission_default_limit: float specifying the transmission
            limit to use for rez to subregion connections when an explicit limit
            is not given in the inputs.

    Returns:
        `pd.DataFrame`: PyPSA style link attributes in tabular format.
    """
    links = renewable_energy_zone_build_limits.loc[:, _REZ_LINK_ATTRIBUTES.keys()]
    links = links.rename(columns=_REZ_LINK_ATTRIBUTES)
    links["isp_name"] = links["bus0"] + "-" + links["bus1"]
    links["name"] = links["isp_name"] + "_existing"

    # Links without an explicit limit because their limits are modelled through
    # custom constraints are given a very large capacity
    links["p_nom"] = links["p_nom"].fillna(rez_to_sub_region_transmission_default_limit)

    links["p_min_pu"] = -1.0
    links["build_year"] = 0
    links["lifetime"] = np.inf
    links["capital_cost"] = np.nan

    # Not extendable for existing links
    links["p_nom_extendable"] = False

    return links
