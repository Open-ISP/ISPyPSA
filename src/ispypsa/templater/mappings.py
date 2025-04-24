import pandas as pd

from .helpers import _snakecase_string
from .lists import (
    _ALL_GENERATOR_STORAGE_TYPES,
    _CONDENSED_GENERATOR_TYPES,
    _ECAA_GENERATOR_TYPES,
    _ISP_SCENARIOS,
    _NEW_GENERATOR_TYPES,
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

_NEM_SUB_REGION_IDS = pd.Series(
    {
        "Northern Queensland": "NQ",
        "Central Queensland": "CQ",
        "Gladstone Grid": "GG",
        "Southern Queensland": "SQ",
        "Northern New South Wales": "NNSW",
        "Central New South Wales": "CNSW",
        "Southern New South Wales": "SNSW",
        "Sydney, Newcastle, Wollongong": "SNW",
        "Victoria": "VIC",
        "Central South Australia": "CSA",
        "South East South Australia": "SESA",
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
    "maintenance": ["existing_generators", "new_entrants"],
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
    "coal_prices": list(map(_snakecase_string, _ISP_SCENARIOS)),
    "gas_prices": list(map(_snakecase_string, _ISP_SCENARIOS)),
}

_ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "maximum_capacity_mw": dict(
        table=[f"maximum_capacity_{gen_type}" for gen_type in _ECAA_GENERATOR_TYPES],
        table_lookup="Generator",
        alternative_lookups=["Project"],
        table_value="Installed capacity (MW)",
    ),
    "maintenance_duration_%": dict(
        table="maintenance_existing_generators",
        table_lookup="Generator type",
        table_value="Proportion of time out (%)",
    ),
    "minimum_load_mw": dict(
        table="coal_minimum_stable_level",
        table_lookup="Generating unit",
        table_value="Minimum Stable Level (MW)",
    ),
    "fom_$/kw/annum": dict(
        table="fixed_opex_existing_committed_anticipated_additional_generators",
        table_lookup="Generator",
        table_value="Fixed OPEX ($/kW/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        table="variable_opex_existing_committed_anticipated_additional_generators",
        table_lookup="Generator",
        table_value="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        table="heat_rates_existing_committed_anticipated_additional_generators",
        table_lookup="Generator",
        table_value="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        table=[
            f"marginal_loss_factors_{gen_type}" for gen_type in _ECAA_GENERATOR_TYPES
        ],
        table_lookup="Generator",
        alternative_lookups=["Project"],
        table_value="MLF",
        alternative_values=["MLF - Generation"],
    ),
    "auxiliary_load_%": dict(
        table="auxiliary_load_existing_committed_anticipated_additional_generators",
        table_lookup="Fuel/Technology type",
        table_value="Auxiliary load (% of nameplate capacity)",
    ),
    "partial_outage_derating_factor_%": dict(
        table="outages_2023-2024_existing_generators",
        table_lookup="Fuel type",
        table_value="Partial Outage Derating Factor (%)",
        generator_status="Existing",
    ),
    "mean_time_to_repair_full_outage": dict(
        table="outages_2023-2024_existing_generators",
        table_lookup="Fuel type",
        table_value="Mean time to repair (hrs)_Full outage",
        generator_status="Existing",
    ),
    "mean_time_to_repair_partial_outage": dict(
        table="outages_2023-2024_existing_generators",
        table_lookup="Fuel type",
        table_value="Mean time to repair (hrs)_Partial outage",
        generator_status="Existing",
    ),
}
"""
Existing, committed, anticipated and additional summary table columns mapped to
corresponding IASR tables and lookup information that can be used to retrieve values.

    `table`: IASR table name or a list of table names.
    `table_lookup`: Column in the table that acts as a key for merging into the summary
    `alternative_lookups`: A list of alternative key columns, e.g. "Project" as an
        alternative to  "Generator" in the additional projects table. If a lookup value
        is NA in the `table_lookup` column, it will be replaced by a lookup value from
        this list in the order specified.
    `table_value`: Column in the table that corresponds to the data to be merged in
    `alternative_values`: As for `alternative_lookups`, but for the data values in the
        table, e.g. "MLF - Generation" instead of "MLF" in the additional projects table
    `new_col_name`: The name that will be used to rename the column in the summary table
"""

_NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "summer_peak_rating_%": dict(
        table="seasonal_ratings_new_entrants",
        table_lookup="Generator type",
        table_value="Summer Peak (% of nameplate)",
    ),
    "summer_rating_mw": dict(
        table="seasonal_ratings_new_entrants",
        table_lookup="Generator type",
        table_value="Summer Typical (% of nameplate)",
        new_col_name="summer_typical_rating_%",
    ),
    "winter_rating_mw": dict(
        table="seasonal_ratings_new_entrants",
        table_lookup="Generator type",
        table_value="Winter (% of nameplate)",
        new_col_name="winter_rating_%",
    ),
    "maximum_capacity_mw": dict(
        table="maximum_capacity_new_entrants",
        table_lookup="Generator type",
        table_value="Total plant size (MW)",
    ),
    "unit_capacity_mw": dict(
        table="maximum_capacity_new_entrants",
        table_lookup="Generator type",
        table_value="Unit size (MW)",
    ),
    "maintenance_duration_%": dict(
        table="maintenance_new_entrants",
        table_lookup="Generator type",
        table_value="Proportion of time out (%)",
    ),
    "fom_$/kw/annum": dict(
        table="fixed_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Fixed OPEX ($/kW sent out/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        table="variable_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        table="heat_rates_new_entrants",
        table_lookup="Technology",
        table_value="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        table="marginal_loss_factors_new_entrants",
        table_lookup="Generator",
        table_value="MLF",
    ),
    "auxiliary_load_%": dict(
        table="auxiliary_load_new_entrants",
        table_lookup="Generator",
        table_value="Auxiliary load (% of nameplate capacity)",
    ),
    "partial_outage_derating_factor_%": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        table_value="Partial Outage Derating Factor (%)",
    ),
    "mean_time_to_repair_full_outage": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        table_value="Mean time to repair (hrs)_Full outage",
    ),
    "mean_time_to_repair_partial_outage": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        table_value="Mean time to repair (hrs)_Partial outage",
    ),
    "lifetime": dict(
        table="lead_time_and_project_life",
        table_lookup="Technology",
        table_value="Technical life (years) 6",
    ),
    "total_lead_time": dict(
        table="lead_time_and_project_life",
        table_lookup="Technology",
        table_value="Total lead time (years)",
    ),
}
"""
New entrant generators summary table columns mapped to corresponding IASR table and
lookup information that can be used to retrieve values.

    `table`: IASR table name or a list of table names.
    `table_lookup`: Column in the table that acts as a key for merging into the summary
    `alternative_lookups`: A list of alternative key columns, e.g. "Project" as an
        alternative to  "Generator" in the additional projects table. If a lookup value
        is NA in the `table_lookup` column, it will be replaced by a lookup value from
        this list in the order specified.
    `table_value`: Column in the table that corresponds to the data to be merged in
    `alternative_values`: As for `alternative_lookups`, but for the data values in the
        table
    `new_col_name`: The name that will be used to rename the column in the summary table
    `table_col_prefix`: The string that is present at the start of each column name
        in the table as a result of row merging in isp-workbook-parser, to be used
        for opex mapping to rename columns in the table.
"""

"""
 _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP is a dictionary that maps template functions to
 lists of dictionaries containing the CSV file name, region_id and policy_id for each
 parsed table.
     `csv`: A single CSV file name (excluding file extension)
     `region_id`: region corresponding to that parsed table, to be inputted
         into templated table
     `policy_id`: policy corresponding to that parsed table, to be inputted
         into templated table links with the manually_extracted_table
         `policy_generator_types`
 """
_TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP = {
    "template_renewable_share_targets": [
        {
            "csv": "vic_renewable_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vret",
        },
        {
            "csv": "qld_renewable_target_trajectory",
            "region_id": "QLD",
            "policy_id": "qret",
        },
    ],
    "template_powering_australia_plan": [
        {
            "csv": "powering_australia_plan_trajectory",
            "region_id": "NEM",
            "policy_id": "power_aus",
        },
    ],
    "template_technology_capacity_targets": [
        {
            "csv": "capacity_investment_scheme_renewable_trajectory",
            "region_id": "NEM",
            "policy_id": "cis_generator",
        },
        {
            "csv": "capacity_investment_scheme_storage_trajectory",
            "region_id": "NEM",
            "policy_id": "cis_storage",
        },
        {
            "csv": "nsw_roadmap_storage_trajectory",
            "region_id": "NSW",
            "policy_id": "nsw_eir_sto",
        },
        {
            "csv": "vic_storage_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vic_storage",
        },
        {
            "csv": "vic_offshore_wind_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vic_offshore_wind",
        },
    ],
    "template_renewable_generation_targets": [
        {
            "csv": "nsw_roadmap_renewable_trajectory",
            "region_id": "NSW",
            "policy_id": "nsw_eir_gen",
        },
        {
            "csv": "tas_renewable_target_trajectory",
            "region_id": "TAS",
            "policy_id": "tret",
        },
    ],
}


_VRE_RESOURCE_QUALITY_AND_TECH_CODES = {
    "Wind": ["WH", "WM"],
    "Wind - offshore (fixed)": "WFX",
    "Wind - offshore (floating)": "WFL",
    "Large scale Solar PV": "SAT",
    "Solar Thermal (15hrs storage)": "CST",
}
