"""Translate the new-format storage tables into one PyPSA storage table.

This module takes templated or user-defined input tables (storage_existing_planned
and storage_new_entrant), and translates them into one PyPSA-friendly table with
each row to become one PyPSA StorageUnit. Batteries and pumped hydro (PHES)
both go in this table. Existing and planned units keep their templated
capacity, and each new-entrant option becomes one extendable storage unit per
investment period.
"""

import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.build_years_and_costs import (
    _EXISTING_UNIT_COLUMN_RENAMES,
    _NEW_ENTRANT_COLUMN_RENAMES,
    _build_existing_units,
    _build_new_entrant_units,
)

_STORAGE_COLUMNS = [
    "isp_name",
    "name",
    "carrier",
    "bus",
    "p_nom",
    "p_nom_extendable",
    "max_hours",
    "build_year",
    "lifetime",
    "capital_cost",
    "efficiency_store",
    "efficiency_dispatch",
    "isp_technology_type",
]

# A new-entrant storage option's duration is its storage_hours, which PyPSA
# calls max_hours.
_NEW_ENTRANT_STORAGE_COLUMN_RENAMES = {
    **_NEW_ENTRANT_COLUMN_RENAMES,
    "storage_hours": "max_hours",
}


def _translate_storage(
    ispypsa_tables: dict[str, pd.DataFrame], config: ModelConfig
) -> pd.DataFrame:
    """Translates storage_existing_planned and storage_new_entrant into one
    PyPSA storage table, with the columns in _STORAGE_COLUMNS.

    Existing and planned units become storage units with a fixed capacity,
    which run from their build year until they close (see
    _build_existing_units). Each existing unit's max_hours is its own energy
    capacity divided by its power capacity (see
    _map_existing_storage_columns). Each new-entrant option becomes one
    extendable storage unit per investment period, so that the PyPSA model can
    choose how much of it to build in each investment period (see
    _build_new_entrant_units), and its max_hours is the option's
    storage_hours. The generators translator shares both of these steps. The
    two tables are then stacked into one, and every unit gets its charge and
    discharge efficiencies as fractions (see
    _add_store_and_dispatch_efficiencies).

    Each storage unit's name is unique, and its isp_name is the group it
    belongs to: an existing unit's power station, or a new entrant's option
    name. Custom constraint terms can name either (see
    ispypsa.translator.constraints).

    I/O Example (columns abridged):
        Inputs:
            ispypsa_tables["storage_existing_planned"] (capacity in MW, storage_capacity in MWh, efficiencies in %):
                name  power_station          technology                      fuel_type  capacity  storage_capacity  efficiency_charge  efficiency_discharge  geo_id  commissioning_date  closure_year
                VBB1  Victorian Big Battery  Battery storage (2hrs storage)  Battery    300.0     470.0             92.0               92.0                  MEL                         2041

            ispypsa_tables["storage_new_entrant"] (storage_hours in hours, fom in $/MW/year, efficiencies, lcf_build and lcf_om in %, lifetimes in years):
                name               technology                      fuel_type  geo_id  storage_hours  fom    efficiency_charge  efficiency_discharge  lcf_build  lcf_om  lifetime_technical  lifetime_economic
                CNSW Battery - 2h  Battery storage (2hrs storage)  Battery    CNSW    2.0            13500  92.0               92.0                  100        100     20                  20

            ispypsa_tables["costs_new_entrant_build"] ($/MW):
                technology                      year  cost
                Battery storage (2hrs storage)  2026  1200000
                Battery storage (2hrs storage)  2030  1000000

            ispypsa_tables["costs_connection"] ($/MW):
                geo_id  technology                      year  connection_cost  system_strength_cost
                CNSW    Battery storage (2hrs storage)  2026  85000            160000
                CNSW    Battery storage (2hrs storage)  2030  85000            160000

            ispypsa_tables["network_geography"]:
                geo_id  geo_type   region_id  subregion_id
                MEL     subregion  VIC        MEL
                CNSW    subregion  NSW        CNSW

            config.temporal.year_type: "fy"
            config.temporal.range.start_year: 2026
            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.network.nodes.rezs: "discrete_nodes"
            config.network.nodes.regional_granularity: "sub_regions"
            config.wacc: 0.07    # 7%

        returns (p_nom in MW, max_hours in hours, lifetime in years, capital_cost in $/MW/year, efficiencies as fractions):
            isp_name               name                    carrier  bus   p_nom  p_nom_extendable  max_hours  build_year  lifetime  capital_cost  efficiency_store  efficiency_dispatch
            Victorian Big Battery  VBB1                    Battery  MEL   300.0  False             1.567      2025        17.0      0.0           0.92              0.92                 # already built when the model starts
            CNSW Battery - 2h      CNSW Battery - 2h_2026  Battery  CNSW  0.0    True              2.0        2026        20.0      149897.78     0.92              0.92
            CNSW Battery - 2h      CNSW Battery - 2h_2030  Battery  CNSW  0.0    True              2.0        2030        20.0      131019.19     0.92              0.92                 # the 2030 build cost
    """
    existing = _build_existing_units(
        ispypsa_tables["storage_existing_planned"],
        ispypsa_tables["network_geography"],
        config,
        "storage_existing_planned",
    )
    existing = _map_existing_storage_columns(existing)
    new_entrants = _build_new_entrant_units(
        ispypsa_tables["storage_new_entrant"],
        ispypsa_tables["costs_new_entrant_build"],
        ispypsa_tables["costs_connection"],
        ispypsa_tables["network_geography"],
        config,
    )
    new_entrants = new_entrants.rename(
        columns=_NEW_ENTRANT_STORAGE_COLUMN_RENAMES, errors="raise"
    )
    storage = pd.concat([existing, new_entrants], ignore_index=True)
    storage = _add_store_and_dispatch_efficiencies(storage)
    return storage.loc[:, _STORAGE_COLUMNS]


def _map_existing_storage_columns(storage: pd.DataFrame) -> pd.DataFrame:
    """Renames the columns of existing storage units to their PyPSA names (see
    _EXISTING_UNIT_COLUMN_RENAMES), and sets max_hours from
    storage_capacity / capacity.

    I/O Example:
        storage (capacity in MW, storage_capacity in MWh):
            name      power_station          technology                      fuel_type  capacity  storage_capacity
            VBB1      Victorian Big Battery  Battery storage (2hrs storage)  Battery    300.0     470.0
            DALNTH1   Dalrymple BESS         Battery storage (1hr storage)   Battery    30.0      9.0
            SNO2PH01  Snowy 2.0              Pumped Hydro (1wk storage)      Water      366.0     58330.0

        returns (p_nom in MW, storage_capacity in MWh, max_hours in hours):
            name      isp_name               isp_technology_type             carrier  p_nom  storage_capacity  max_hours
            VBB1      Victorian Big Battery  Battery storage (2hrs storage)  Battery  300.0  470.0             1.567
            DALNTH1   Dalrymple BESS         Battery storage (1hr storage)   Battery  30.0   9.0               0.3
            SNO2PH01  Snowy 2.0              Pumped Hydro (1wk storage)      Water    366.0  58330.0           159.372
    """
    storage = storage.rename(columns=_EXISTING_UNIT_COLUMN_RENAMES, errors="raise")
    # Each unit's own duration. The hours in its technology name are a size
    # class, and often differ from it: Eraring Big Battery is 460 MW and
    # 1,773 MWh (3.9 hours), but is named "Battery storage (2hrs storage)".
    storage["max_hours"] = storage["storage_capacity"] / storage["p_nom"]
    return storage


def _add_store_and_dispatch_efficiencies(storage: pd.DataFrame) -> pd.DataFrame:
    """Adds efficiency_store and efficiency_dispatch, PyPSA's names for the
    charge and discharge efficiencies, from efficiency_charge and
    efficiency_discharge, converted from % to fractions.

    I/O Example (columns abridged):
        storage (efficiencies in %):
            name      efficiency_charge  efficiency_discharge
            VBB1      92.0               92.0
            SNO2PH01  87.178             87.178

        returns (efficiencies as fractions):
            name      efficiency_charge  efficiency_discharge  efficiency_store  efficiency_dispatch
            VBB1      92.0               92.0                  0.92              0.92
            SNO2PH01  87.178             87.178                0.87178           0.87178
    """
    storage = storage.copy()
    storage["efficiency_store"] = storage["efficiency_charge"] / 100
    storage["efficiency_dispatch"] = storage["efficiency_discharge"] / 100
    return storage
