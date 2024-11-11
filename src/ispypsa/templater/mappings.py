import pandas as pd

from .lists import (
    _ALL_GENERATOR_STORAGE_TYPES,
    _CONDENSED_GENERATOR_TYPES,
    _ECAA_GENERATOR_TYPES,
)

_NEM_REGION_IDS = pd.Series(
    {
        "Queensland": "QLD",
        "New South Wales": "NSW",
        "Victoria": "VIC",
        "South Australia": "SA",
        "Tasmania": "TAS",
    },
    name="nem_region_id_mapping",
)

_HVDC_FLOW_PATHS = pd.DataFrame(
    {
        "node_from": ["NNSW", "VIC", "TAS"],
        "node_to": ["SQ", "CSA", "VIC"],
        "flow_path_name": ["Terranora", "Murraylink", "Basslink"],
    }
)

_GENERATOR_PROPERTIES = {
    "maximum_capacity": _ALL_GENERATOR_STORAGE_TYPES,
    "seasonal_ratings": _ALL_GENERATOR_STORAGE_TYPES,
    "maintenance": ["existing_generators"],
    "fixed_opex": _CONDENSED_GENERATOR_TYPES,
    "variable_opex": _CONDENSED_GENERATOR_TYPES,
    "marginal_loss_factors": _ALL_GENERATOR_STORAGE_TYPES,
    "auxiliary_load": _CONDENSED_GENERATOR_TYPES,
    "heat_rates": _CONDENSED_GENERATOR_TYPES,
    "outages_2023-2024": ["existing_generators"],
    "long_duration_outages": ["existing_generators"],
    "outages": ["new_entrants"],
    "full_outages_forecast": ["existing_generators"],
    "partial_outages_forecast": ["existing_generators"],
    "gpg_min_stable_level": ["existing_generators", "new_entrants"],
}

_ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "maximum_capacity_mw": dict(
        csv=[f"maximum_capacity_{gen_type}" for gen_type in _ECAA_GENERATOR_TYPES],
        csv_lookup="Generator",
        alternative_lookups=["Project"],
        csv_value="Installed capacity (MW)",
    ),
    "maintenance_duration_%": dict(
        csv="maintenance_existing_generators",
        csv_lookup="Generator type",
        csv_value="Proportion of time out (%)",
    ),
    "minimum_load_mw": dict(
        csv="coal_minimum_stable_level",
        csv_lookup="Generating unit",
        csv_value="Minimum Stable Level (MW)",
    ),
    "fom_$/kw/annum": dict(
        csv="fixed_opex_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_value="Fixed OPEX ($/kW/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        csv="variable_opex_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_value="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        csv="heat_rates_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_value="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        csv=[f"marginal_loss_factors_{gen_type}" for gen_type in _ECAA_GENERATOR_TYPES],
        csv_lookup="Generator",
        alternative_lookups=["Project"],
        csv_value="MLF",
        alternative_values=["MLF - Generation"],
    ),
    "auxiliary_load_%": dict(
        csv="auxiliary_load_existing_committed_anticipated_additional_generators",
        csv_lookup="Fuel/Technology type",
        csv_value="Auxiliary load (% of nameplate capacity)",
    ),
    "partial_outage_derating_factor_%": dict(
        csv="outages_2023-2024_existing_generators",
        csv_lookup="Fuel type",
        csv_value="Partial Outage Derating Factor (%)",
        generator_status="Existing",
    ),
    "mean_time_to_repair_full_outage": dict(
        csv="outages_2023-2024_existing_generators",
        csv_lookup="Fuel type",
        csv_value="Mean time to repair (hrs)_Full outage",
        generator_status="Existing",
    ),
    "mean_time_to_repair_partial_outage": dict(
        csv="outages_2023-2024_existing_generators",
        csv_lookup="Fuel type",
        csv_value="Mean time to repair (hrs)_Partial outage",
        generator_status="Existing",
    ),
}
"""
Existing, committed, anticipated and additional summary table columns mapped to
corresponding data CSV and lookup information that can be used to retrieve values.

    `csv`: A single CSV file name (excluding file extension) or a list of CSV file names
    `csv_lookup`: Column in the CSV that acts as a key for merging into the summary
    `alternative_lookups`: A list of alternative key columns, e.g. "Project" as an
        alternative to  "Generator" in the additional projects table. If a lookup value
        is NA in the `csv_lookup` column, it will be replaced by a lookup value from this
        list in the order specified.
    `csv_value`: Column in the CSV that corresponds to the data to be merged in
    `alternative_values`: As for `alternative_lookups`, but for the data values in the
        table, e.g. "MLF - Generation" instead of "MLF" in the additional projects table
    `new_col_name`: The name that will be used to rename the column in the summary table
"""
