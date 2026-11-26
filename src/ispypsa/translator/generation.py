"""Translate the new-format generator tables into one PyPSA generators table.

This module sits in the translator stage, between the templater, which
produces generators_existing_planned and generators_new_entrant, and the PyPSA
model build. Existing and planned units keep their templated capacity, and
each new-entrant option becomes one extendable generator per investment
period.
"""

import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.build_years_and_costs import (
    _add_new_entrant_capital_costs,
    _build_years_from_commissioning_dates,
    _buses_for_geo_ids,
    _drop_units_built_after_last_investment_period,
    _drop_units_closed_before_first_investment_period,
    _lifetimes_from_closure_years,
    _resolve_build_costs,
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
    """Translates generators_existing_planned and generators_new_entrant into
    one PyPSA generators table, with the columns in _GENERATOR_COLUMNS.

    Existing and planned units become generators with a fixed capacity, which
    run from their build year until they close (see _build_existing_generators).
    Each new-entrant option becomes one extendable generator per investment
    period, so that the PyPSA model can choose how much of it to build in each
    investment period (see _build_new_entrant_generators). The two tables are then
    stacked into one.

    I/O Example (columns abridged):
        Inputs:
            ispypsa_tables["generators_existing_planned"] (capacity in MW):
                name  fuel_type   capacity  geo_id  commissioning_date  closure_year
                BW01  Black Coal  660.0     CNSW                        2033

            ispypsa_tables["generators_new_entrant"] (fom in $/MW/year, lcf_build and lcf_om in %, lifetimes in years):
                name       technology  fuel_type  geo_id  fom    lcf_build  lcf_om  lifetime_technical  lifetime_economic
                CNSW OCGT  OCGT        Gas        CNSW    20000  110        105     30                  25

            ispypsa_tables["costs_new_entrant_build"] ($/MW):
                technology  year  cost
                OCGT        2026  1000000
                OCGT        2030  900000

            ispypsa_tables["costs_connection"] ($/MW):
                geo_id  technology  year  connection_cost  system_strength_cost
                CNSW    OCGT        2026  100000           50000
                CNSW    OCGT        2030  100000           50000

            ispypsa_tables["network_geography"]:
                geo_id  geo_type   region_id  subregion_id
                CNSW    subregion  NSW        CNSW

            config.temporal.year_type: "fy"
            config.temporal.range.start_year: 2026
            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.network.nodes.rezs: "discrete_nodes"
            config.network.nodes.regional_granularity: "sub_regions"
            config.wacc: 0.07    # 7%

        returns (p_nom in MW, lifetime in years, capital_cost in $/MW/year):
            isp_name   name            carrier     bus   p_nom  p_nom_extendable  build_year  lifetime  capital_cost  marginal_cost
            BW01       BW01            Black Coal  CNSW  660.0  False             2025        9.0       0.0           BW01        # already built when the model starts
            CNSW OCGT  CNSW OCGT_2026  Gas         CNSW  0.0    True              2026        30.0      129121.25     CNSW OCGT
            CNSW OCGT  CNSW OCGT_2030  Gas         CNSW  0.0    True              2030        30.0      119682.09     CNSW OCGT   # the 2030 build cost
    """
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
    """Translates generators_existing_planned into generators with a fixed
    capacity, which PyPSA keeps in the model from their build year until they
    close.

    The build year is the model year of the unit's commissioning date. A unit
    with a blank commissioning date, or one before the model starts, is already
    built, so its build year is start_year - 1.

    The lifetime is closure_year + 1 - build_year, so the unit runs through the
    model year labelled with its closure year. This is an opinionated rule (see
    _lifetimes_from_closure_years). A blank closure_year gives a NaN lifetime,
    which PyPSA reads as a unit that never closes.

    Units that are built after the final investment period or close before the first
    are dropped and logged at INFO.

    I/O Example:
        Inputs:
            generators (capacity in MW):
                name      technology          fuel_type   capacity  geo_id  commissioning_date  closure_year
                BW01      Steam Sub Critical  Black Coal  660.0     CNSW                        2033          # already built
                LIDDELL1  Steam Sub Critical  Black Coal  500.0     CNSW                        2023          # closed before 2026: dropped
                PLANNED1  Wind                Wind        100.0     N3      01/08/2027                        # built in FY2028, no closure
                PLANNED2  Wind                Wind        200.0     N3      01/08/2030                        # built in FY2031: dropped

            network_geography:
                geo_id  geo_type   region_id  subregion_id
                CNSW    subregion  NSW        CNSW
                N3      rez        NSW        CNSW

            config.temporal.year_type: "fy"
            config.temporal.range.start_year: 2026
            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.network.nodes.rezs: "attached_to_parent_node"
            config.network.nodes.regional_granularity: "sub_regions"

        returns (p_nom in MW, lifetime in years, capital_cost in $/MW/year; the input columns that pass through aren't shown):
            isp_name  name      isp_technology_type  carrier     bus   p_nom  p_nom_extendable  build_year  lifetime  capital_cost  marginal_cost
            BW01      BW01      Steam Sub Critical   Black Coal  CNSW  660.0  False             2025        9.0       0.0           BW01
            PLANNED1  PLANNED1  Wind                 Wind        CNSW  100.0  False             2028        NaN       0.0           PLANNED1       # N3's parent node
    """
    generators = generators.copy()
    generators["build_year"] = _build_years_from_commissioning_dates(
        generators["commissioning_date"],
        config.temporal.year_type,
        config.temporal.range.start_year,
    )
    generators["lifetime"] = _lifetimes_from_closure_years(
        generators["closure_year"], generators["build_year"]
    )
    investment_periods = config.temporal.capacity_expansion.investment_periods
    generators = _drop_units_closed_before_first_investment_period(
        generators, investment_periods[0], "generators_existing_planned"
    )
    generators = _drop_units_built_after_last_investment_period(
        generators, investment_periods[-1], "generators_existing_planned"
    )
    generators["bus"] = _buses_for_geo_ids(
        generators["geo_id"],
        network_geography,
        config.network.nodes.rezs,
        config.network.nodes.regional_granularity,
    )
    return _map_existing_generator_columns(generators)


def _build_new_entrant_generators(
    generators: pd.DataFrame,
    build_costs: pd.DataFrame,
    connection_costs: pd.DataFrame,
    network_geography: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    """Translates generators_new_entrant into extendable generators: one per
    option and investment period, each with an annuitised capital cost.

    Blank years in costs_new_entrant_build are resolved first, so that each
    technology has a build cost for every investment period (see
    _resolve_build_costs).

    PyPSA only builds capacity for a generator in its build_year, so each
    option is repeated once per investment period, with that investment period
    as its build year. This lets PyPSA build the option in any investment
    period. Each copy gets the capital cost for its own build year (see
    _add_new_entrant_capital_costs) and the name <name>_<build_year>. It starts
    with no capacity, and PyPSA chooses how much to build. Its lifetime is the
    technical lifetime; the economic lifetime is only used in the annuity.

    I/O Example:
        Inputs:
            generators (fom in $/MW/year, lcf_build and lcf_om in %, lifetimes in years):
                name       technology  fuel_type  geo_id  fom    lcf_build  lcf_om  lifetime_technical  lifetime_economic
                CNSW OCGT  OCGT        Gas        CNSW    20000  110        105     30                  25
                N3 Wind    Wind        Wind       N3      30000  100        100     30                  30

            build_costs ($/MW):
                technology  year  cost
                OCGT        2026  1000000
                OCGT              900000     # blank year: OCGT's cost for 2030
                Wind        2026  3000000
                Wind        2030  2800000

            connection_costs ($/MW):
                geo_id  technology  year  connection_cost  system_strength_cost
                CNSW    OCGT        2026  100000           50000
                CNSW    OCGT        2030  100000           50000
                N3      Wind        2026  150000           60000
                N3      Wind        2030  140000           55000

            network_geography:
                geo_id  geo_type   region_id  subregion_id
                CNSW    subregion  NSW        CNSW
                N3      rez        NSW        CNSW

            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.wacc: 0.07    # 7%
            config.network.nodes.rezs: "attached_to_parent_node"
            config.network.nodes.regional_granularity: "sub_regions"

        returns (p_nom in MW, lifetime in years, capital_cost in $/MW/year; the input and cost columns that pass through aren't shown):
            isp_name   name            isp_technology_type  carrier  bus   p_nom  p_nom_extendable  build_year  lifetime  capital_cost  marginal_cost
            CNSW OCGT  CNSW OCGT_2026  OCGT                 Gas      CNSW  0.0    True              2026        30.0      129121.25     CNSW OCGT
            CNSW OCGT  CNSW OCGT_2030  OCGT                 Gas      CNSW  0.0    True              2030        30.0      119682.09     CNSW OCGT    # the blank-year build cost
            N3 Wind    N3 Wind_2026    Wind                 Wind     CNSW  0.0    True              2026        30.0      288682.36     N3 Wind      # N3's costs, at N3's parent node
            N3 Wind    N3 Wind_2030    Wind                 Wind     CNSW  0.0    True              2030        30.0      271356.28     N3 Wind
    """
    investment_periods = config.temporal.capacity_expansion.investment_periods
    build_costs = _resolve_build_costs(build_costs, investment_periods)
    generators = _add_investment_periods_as_build_years(
        generators.copy(), investment_periods
    )
    generators = _add_new_entrant_capital_costs(
        generators, build_costs, connection_costs, config.wacc
    )
    generators["bus"] = _buses_for_geo_ids(
        generators["geo_id"],
        network_geography,
        config.network.nodes.rezs,
        config.network.nodes.regional_granularity,
    )
    return _map_new_entrant_generator_columns(generators)


def _map_existing_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns for existing units: isp_name and name from name,
    carrier from fuel_type, p_nom from capacity, isp_technology_type from
    technology, p_nom_extendable False, capital_cost 0 and marginal_cost from
    isp_name.

    I/O Example (columns abridged):
        generators (capacity in MW):
            name  fuel_type   capacity
            BW01  Black Coal  660.0

        returns (p_nom in MW, capital_cost in $/MW/year):
            isp_name  name  carrier     p_nom  p_nom_extendable  capital_cost  marginal_cost
            BW01      BW01  Black Coal  660.0  False             0.0           BW01
    """
    generators = generators.rename(
        columns={
            "fuel_type": "carrier",
            "capacity": "p_nom",
            "technology": "isp_technology_type",
        }
    )
    generators["isp_name"] = generators["name"]
    generators["p_nom_extendable"] = False
    # Existing units are already built, so there's no capital cost to recover.
    generators["capital_cost"] = 0.0
    # A text marginal_cost is the name of the unit's marginal cost timeseries
    # file, which pypsa_build reads in place of a fixed cost.
    generators["marginal_cost"] = generators["isp_name"]
    return generators


def _map_new_entrant_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns for new entrants: isp_name from name, name as
    <isp_name>_<build_year>, carrier from fuel_type, lifetime from
    lifetime_technical, isp_technology_type from technology, p_nom 0,
    p_nom_extendable True and marginal_cost from isp_name (so all the build
    years of one option share its marginal cost timeseries file).

    I/O Example (columns abridged):
        generators (lifetime_technical in years):
            name       fuel_type  build_year  lifetime_technical
            CNSW OCGT  Gas        2026        25.0
            CNSW OCGT  Gas        2030        25.0

        returns (lifetime in years, p_nom in MW):
            isp_name   name            carrier  build_year  lifetime  p_nom  p_nom_extendable  marginal_cost
            CNSW OCGT  CNSW OCGT_2026  Gas      2026        25.0      0.0    True              CNSW OCGT
            CNSW OCGT  CNSW OCGT_2030  Gas      2030        25.0      0.0    True              CNSW OCGT
    """
    generators = generators.rename(
        columns={
            "fuel_type": "carrier",
            "lifetime_technical": "lifetime",
            "technology": "isp_technology_type",
        }
    )
    generators["isp_name"] = generators["name"]
    generators["name"] = (
        generators["isp_name"] + "_" + generators["build_year"].astype(str)
    )
    # New entrants start with no capacity, and are 'extendable' by the PyPSA model (CEM)
    generators["p_nom"] = 0.0
    generators["p_nom_extendable"] = True
    generators["marginal_cost"] = generators["isp_name"]
    return generators
