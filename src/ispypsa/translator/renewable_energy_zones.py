import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.lines import _translate_expansion_costs_to_lines
from ispypsa.translator.mappings import _REZ_LINE_ATTRIBUTES


def _translate_renewable_energy_zone_build_limits_lines(
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
        `pd.DataFrame`: PyPSA style line attributes in tabular format.
    """
    # Create existing lines from renewable energy zone build limits
    existing_lines = _translate_existing_rez_connections_to_lines(
        renewable_energy_zone_build_limits,
        config.network.rez_to_sub_region_transmission_default_limit,
    )

    # Create expansion lines from rez expansion costs if expansion is enabled
    if config.network.rez_transmission_expansion and not rez_expansion_costs.empty:
        expansion_lines = _translate_expansion_costs_to_lines(
            expansion_costs=rez_expansion_costs,
            existing_lines_df=existing_lines.copy(),
            investment_periods=config.temporal.capacity_expansion.investment_periods,
            year_type=config.temporal.year_type,
            wacc=config.wacc,
            asset_lifetime=config.network.annuitisation_lifetime,
            id_column="rez_constraint_id",
            match_column="name",
        )
        # Combine existing and expansion lines
        all_lines = pd.concat(
            [existing_lines, expansion_lines], ignore_index=True, sort=False
        )
    else:
        all_lines = existing_lines

    return all_lines


def _translate_existing_rez_connections_to_lines(
    renewable_energy_zone_build_limits: pd.DataFrame,
    rez_to_sub_region_transmission_default_limit: float,
) -> pd.DataFrame:
    """Process existing REZ connection limits to PyPSA lines.

    Args:
        renewable_energy_zone_build_limits: `ISPyPSA` formatted pd.DataFrame detailing
            Renewable Energy Zone transmission limits.
        rez_to_sub_region_transmission_default_limit: float specifying the transmission
            limit to use for rez to subregion connections when an explicit limit
            is not given in the inputs.

    Returns:
        `pd.DataFrame`: PyPSA style line attributes in tabular format.
    """
    lines = renewable_energy_zone_build_limits.loc[:, _REZ_LINE_ATTRIBUTES.keys()]
    lines = lines.rename(columns=_REZ_LINE_ATTRIBUTES)
    lines["name"] = lines["bus0"] + "-" + lines["bus1"] + "_existing"

    # Lines without an explicit limit because their limits are modelled through
    # custom constraints are given a very large capacity
    lines["s_nom"] = lines["s_nom"].fillna(rez_to_sub_region_transmission_default_limit)

    # Not extendable for existing lines
    lines["s_nom_extendable"] = False

    return lines
