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
    _EXISTING_UNIT_COLUMN_RENAMES,
    _NEW_ENTRANT_COLUMN_RENAMES,
    _build_existing_units,
    _build_new_entrant_units,
)

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
    run from their build year until they close (see _build_existing_units).
    Each new-entrant option becomes one extendable generator per investment
    period, so that the PyPSA model can choose how much of it to build in each
    investment period (see _build_new_entrant_units). The storage translator
    shares both of these steps. The columns of each table are then renamed to
    their PyPSA names and given a marginal cost, and the two tables are stacked
    into one.

    Each generator's name is unique, and its isp_name is the group it belongs
    to: an existing unit's power station, or a new entrant's option name.
    Custom constraint terms can name either (see
    ispypsa.translator.constraints), and marginal_cost is the isp_name, so
    all the generators in one group share a marginal cost timeseries file.

    I/O Example (columns abridged):
        Inputs:
            ispypsa_tables["generators_existing_planned"] (capacity in MW):
                name  power_station  fuel_type   capacity  geo_id  commissioning_date  closure_year
                BW01  Bayswater      Black Coal  660.0     CNSW                        2033

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
            Bayswater  BW01            Black Coal  CNSW  660.0  False             2025        9.0       0.0           Bayswater   # already built when the model starts
            CNSW OCGT  CNSW OCGT_2026  Gas         CNSW  0.0    True              2026        30.0      129121.25     CNSW OCGT
            CNSW OCGT  CNSW OCGT_2030  Gas         CNSW  0.0    True              2030        30.0      119682.09     CNSW OCGT   # the 2030 build cost
    """
    existing = _build_existing_units(
        ispypsa_tables["generators_existing_planned"],
        ispypsa_tables["network_geography"],
        config,
        "generators_existing_planned",
    )
    existing = _map_existing_generator_columns(existing)
    new_entrants = _build_new_entrant_units(
        ispypsa_tables["generators_new_entrant"],
        ispypsa_tables["costs_new_entrant_build"],
        ispypsa_tables["costs_connection"],
        ispypsa_tables["network_geography"],
        config,
    )
    new_entrants = _map_new_entrant_generator_columns(new_entrants)
    generators = pd.concat([existing, new_entrants], ignore_index=True)
    return generators.loc[:, _GENERATOR_COLUMNS]


def _map_existing_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Renames the columns of existing units to their PyPSA names (see
    _EXISTING_UNIT_COLUMN_RENAMES), and sets marginal_cost from isp_name, with
    any "/" replaced by "-" so that it works as a file name.

    I/O Example:
        generators (capacity in MW):
            name     power_station    technology          fuel_type   capacity
            BW01     Bayswater        Steam Sub Critical  Black Coal  660.0
            BW02     Bayswater        Steam Sub Critical  Black Coal  685.0
            BOGONG1  Bogong / Mackay  Hydro               Water       70.0

        returns (p_nom in MW):
            name     isp_name         isp_technology_type  carrier     p_nom  marginal_cost
            BW01     Bayswater        Steam Sub Critical   Black Coal  660.0  Bayswater
            BW02     Bayswater        Steam Sub Critical   Black Coal  685.0  Bayswater
            BOGONG1  Bogong / Mackay  Hydro                Water       70.0   Bogong - Mackay
    """
    generators = generators.rename(
        columns=_EXISTING_UNIT_COLUMN_RENAMES, errors="raise"
    )
    # A text marginal_cost is the name of the marginal cost timeseries file
    # that pypsa_build reads in place of a fixed cost. Three 7.5 station names
    # contain "/" (e.g. "Bogong / Mackay"), which a file name can't.
    generators["marginal_cost"] = generators["isp_name"].str.replace("/", "-")
    return generators


def _map_new_entrant_generator_columns(generators: pd.DataFrame) -> pd.DataFrame:
    """Renames the columns of new entrants to their PyPSA names (see
    _NEW_ENTRANT_COLUMN_RENAMES), and sets marginal_cost from isp_name, so that
    all the build years of one option share its marginal cost timeseries file.

    I/O Example:
        generators (lifetime_technical in years):
            isp_name   name            technology  fuel_type  lifetime_technical
            CNSW OCGT  CNSW OCGT_2026  OCGT        Gas        25.0
            CNSW OCGT  CNSW OCGT_2030  OCGT        Gas        25.0

        returns (lifetime in years):
            isp_name   name            isp_technology_type  carrier  lifetime  marginal_cost
            CNSW OCGT  CNSW OCGT_2026  OCGT                 Gas      25.0      CNSW OCGT
            CNSW OCGT  CNSW OCGT_2030  OCGT                 Gas      25.0      CNSW OCGT
    """
    generators = generators.rename(columns=_NEW_ENTRANT_COLUMN_RENAMES, errors="raise")
    generators["marginal_cost"] = generators["isp_name"]
    return generators
