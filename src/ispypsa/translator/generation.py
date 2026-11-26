"""Translate the new-format generator tables into one PyPSA generators table.

This module sits in the translator stage, between the templater, which
produces generators_existing_planned and generators_new_entrant, and the PyPSA
model build. Existing and planned units keep their templated capacity, and
each new-entrant option becomes one extendable generator per investment
period.
"""

# TODO: expand this docstring to the CLAUDE.md module layout once the stubs
# are filled in.

import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.build_years_and_costs import (
    _add_new_entrant_capital_costs,
    _build_years_from_commissioning_dates,
    _buses_for_geo_ids,
    _drop_units_closed_before_first_investment_period,
    _lifetimes_from_closure_years,
)
from ispypsa.translator.helpers import _add_investment_periods_as_build_years

_GENERATOR_COLUMNS = [
    "isp_name",
    "name",
    "carrier",
    "bus",
    "p_nom",
    "p_nom_extendable",
    "build_year",
    "lifetime",
    "capital_cost",
    "marginal_cost",
    "isp_technology_type",
    "emissions_intensity",
]


def _translate_generators(
    ispypsa_tables: dict[str, pd.DataFrame], config: ModelConfig
) -> pd.DataFrame:
    """Translates the existing/planned and new entrant generator tables into one
    PyPSA-friendly generators table."""
    existing = _build_existing_generators(
        ispypsa_tables["generators_existing_planned"],
        ispypsa_tables["network_geography"],
        config,
    )
    new_entrants = _build_new_entrant_generators(
        ispypsa_tables["generators_new_entrant"],
        ispypsa_tables["costs_new_entrant_build"],
        ispypsa_tables["costs_connection"],
        ispypsa_tables["network_geography"],
        config,
    )
    generators = pd.concat([existing, new_entrants], ignore_index=True)
    return generators.loc[:, _GENERATOR_COLUMNS]


def _build_existing_generators(
    generators: pd.DataFrame, network_geography: pd.DataFrame, config: ModelConfig
) -> pd.DataFrame:
    """Translates generators_existing_planned into fixed-capacity generators,
    with build years and lifetimes from their commissioning dates and closure
    years."""
    # A copy, so the new columns don't change the caller's ispypsa_tables.
    generators = generators.copy()
    generators["build_year"] = _build_years_from_commissioning_dates(
        generators["commissioning_date"],
        config.temporal.year_type,
        config.temporal.range.start_year,
    )
    generators["lifetime"] = _lifetimes_from_closure_years(
        generators["closure_year"], generators["build_year"]
    )
    generators = _drop_units_closed_before_first_investment_period(
        generators, config.temporal.capacity_expansion.investment_periods[0]
    )
    generators["bus"] = _buses_for_geo_ids(
        generators["geo_id"], network_geography, config.network.nodes.rezs
    )
    generators["marginal_cost"] = _marginal_cost_keys(generators["name"])
    return _map_existing_generator_columns(generators)


def _build_new_entrant_generators(
    generators: pd.DataFrame,
    build_costs: pd.DataFrame,
    connection_costs: pd.DataFrame,
    network_geography: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    """Translates generators_new_entrant into extendable generators, one per
    option and investment period, with annuitised capital costs."""
    # _add_investment_periods_as_build_years adds its column to the table it's
    # given, so it gets a copy to leave the caller's ispypsa_tables alone.
    generators = _add_investment_periods_as_build_years(
        generators.copy(), config.temporal.capacity_expansion.investment_periods
    )
    generators = _add_new_entrant_capital_costs(
        generators, build_costs, connection_costs, config.wacc
    )
    generators["bus"] = _buses_for_geo_ids(
        generators["geo_id"], network_geography, config.network.nodes.rezs
    )
    generators["marginal_cost"] = _marginal_cost_keys(generators["name"])
    return _map_new_entrant_generator_columns(generators)


def _marginal_cost_keys(names: pd.Series) -> pd.Series:
    """The key that each generator's marginal cost timeseries file is saved
    under: its name, made safe as a file name (some names contain "/")."""
    raise NotImplementedError


def _map_existing_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns for existing units: isp_name and name from name,
    carrier from fuel_type, p_nom from capacity, isp_technology_type from
    technology, p_nom_extendable False and capital_cost 0."""
    raise NotImplementedError


def _map_new_entrant_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns for new entrants: isp_name from name, name as
    <isp_name>_<build_year>, carrier from fuel_type, lifetime from
    lifetime_technical, isp_technology_type from technology, p_nom 0 and
    p_nom_extendable True."""
    raise NotImplementedError
