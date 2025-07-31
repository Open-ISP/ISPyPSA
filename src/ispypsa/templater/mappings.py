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
        "flow_path": ["Terranora", "Murraylink", "Basslink"],
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


# Subregion flow paths
_SUBREGION_FLOW_PATHS = [
    "CQ-NQ",
    "CQ-GG",
    "SQ-CQ",
    "NNSW-SQ",
    "CNSW-NNSW",
    "CNSW-SNW",
    "SNSW-CNSW",
    "VIC-SNSW",
    "TAS-VIC",
    "VIC-SESA",
    "SESA-CSA",
]

_FLOW_PATH_AGUMENTATION_TABLES = [
    "flow_path_augmentation_options_" + fp for fp in _SUBREGION_FLOW_PATHS
]

_REZ_CONNECTION_AGUMENTATION_TABLES = [
    "rez_augmentation_options_" + region for region in list(_NEM_REGION_IDS)
]

_FLOW_PATH_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE = [
    "flow_path_augmentation_costs_progressive_change_" + fp
    for fp in _SUBREGION_FLOW_PATHS
]

_FLOW_PATH_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_" + fp
    for fp in _SUBREGION_FLOW_PATHS
]

_FLOW_PATH_AUGMENTATION_COST_TABLES = (
    _FLOW_PATH_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE
    + _FLOW_PATH_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS
)

_REZ_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE = [
    "rez_augmentation_costs_progressive_change_" + region
    for region in list(_NEM_REGION_IDS)
]

_REZ_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS = [
    "rez_augmentation_costs_step_change_and_green_energy_exports_" + region
    for region in list(_NEM_REGION_IDS)
]

_REZ_AUGMENTATION_COST_TABLES = (
    _REZ_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE
    + _REZ_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS
)

_FLOW_PATH_AGUMENTATION_NAME_ADJUSTMENTS = {
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Forward direction": "transfer_increase_forward_direction_MW",
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Reverse direction": "transfer_increase_reverse_direction_MW",
}

_PREPATORY_ACTIVITIES_TABLES = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
    "flow_path_augmentation_costs_progressive_change_preparatory_activities",
]

_REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES = [
    "rez_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
    "rez_augmentation_costs_progressive_change_preparatory_activities",
]

_ACTIONABLE_ISP_PROJECTS_TABLES = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_actionable_isp_projects",
    "flow_path_augmentation_costs_progressive_change_actionable_isp_projects",
]

_PREPATORY_ACTIVITIES_NAME_TO_OPTION_NAME = {
    "500kV QNI Connect (NSW works)": "NNSW–SQ Option 5",
    "500kV QNI Connect (QLD works)": "NNSW–SQ Option 5",
    "330kV QNI single circuit (NSW works)": "NNSW–SQ Option 1",
    "330kV QNI single circuit (QLD works)": "NNSW–SQ Option 1",
    "330kV QNI double circuit (NSW works)": "NNSW–SQ Option 2",
    "330kV QNI double circuit (QLD works)": "NNSW–SQ Option 2",
    "CQ-GG": "CQ-GG Option 1",
    "Sydney Southern Ring": "CNSW-SNW Option 2",
}

_REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME = {
    "Darling Downs REZ Expansion(Stage 1)": ["SWQLD1", "Option 1"],
    "South East SA REZ": ["S1-TBMO", "Option 1"],
    "South West Victoria REZ Option 1": ["SWV1", "Option 1"],
    "South West Victoria REZ Option 1A": ["SWV1", "Option 1A"],
    "South West Victoria REZ Option 1B": ["SWV1", "Option 1B"],
    "South West Victoria REZ Option 1C": ["SWV1", "Option 1C"],
    "South West Victoria REZ Option 2A": ["SWV1", "Option 2A"],
    "South West Victoria REZ Option 2B": ["SWV1", "Option 2B"],
    "South West Victoria REZ Option 3A": ["SWV1", "Option 3A"],
    "South West Victoria REZ Option 3B": ["SWV1", "Option 3B"],
}

_PREPATORY_ACTIVITIES_OPTION_NAME_TO_FLOW_PATH = {
    "NNSW–SQ Option 5": "NNSW-SQ",
    "NNSW–SQ Option 1": "NNSW-SQ",
    "NNSW–SQ Option 2": "NNSW-SQ",
    "CNSW-SNW Option 2": "CNSW-SNW",
    "CQ-GG Option 1": "CQ-GG",
}

_ACTIONABLE_ISP_PROJECTS_NAME_TO_OPTION_NAME = {
    "Humelink": "SNSW-CNSW Option 1 (HumeLink)",
    "VNI West": "VIC-SNSW Option 1 - VNI West (Kerang)",
    "Project Marinus Stage 1": "TAS-VIC Option 1 (Project Marinus Stage 1)",
    "Project Marinus Stage 2": "TAS-VIC Option 2 (Project Marinus Stage 2)",
}

_ACTIONABLE_ISP_PROJECTS_OPTION_NAME_TO_FLOW_PATH = {
    "SNSW-CNSW Option 1 (HumeLink)": "SNSW-CNSW",
    "VIC-SNSW Option 1 - VNI West (Kerang)": "VIC-SNSW",
    "TAS-VIC Option 1 (Project Marinus Stage 1)": "TAS-VIC",
    "TAS-VIC Option 2 (Project Marinus Stage 2)": "TAS-VIC",
}

# Transmission cost processing configurations
_FLOW_PATH_CONFIG = {
    "transmission_type": "flow_path",
    "in_coming_column_mappings": {
        "Flow path": "id",
        "Flow Path": "id",
        "Option Name": "option",
        "Option": "option",
        "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Forward direction": "forward_capacity_increase",
        "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Reverse direction": "reverse_capacity_increase",
    },
    "out_going_column_mappings": {
        "id": "flow_path",
        "nominal_capacity_increase": "additional_network_capacity_mw",
    },
    "table_names": {
        "augmentation": _FLOW_PATH_AGUMENTATION_TABLES,
        "cost": {
            "progressive_change": _FLOW_PATH_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE,
            "step_change_and_green_energy_exports": _FLOW_PATH_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS,
        },
        "prep_activities": _PREPATORY_ACTIVITIES_TABLES,
        "actionable_projects": _ACTIONABLE_ISP_PROJECTS_TABLES,
    },
    "mappings": {
        "prep_activities_name_to_option": _PREPATORY_ACTIVITIES_NAME_TO_OPTION_NAME,
        "option_to_id": _PREPATORY_ACTIVITIES_OPTION_NAME_TO_FLOW_PATH,
        "actionable_name_to_option": _ACTIONABLE_ISP_PROJECTS_NAME_TO_OPTION_NAME,
        "actionable_option_to_id": _ACTIONABLE_ISP_PROJECTS_OPTION_NAME_TO_FLOW_PATH,
    },
}

_REZ_CONFIG = {
    "transmission_type": "rez",
    "in_coming_column_mappings": {
        "REZ constraint ID": "id",
        "REZ / Constraint ID": "id",
        "Option": "option",
        "REZ": "rez",
        "REZ Name": "rez",
        "Additional network capacity (MW)": "nominal_capacity_increase",
    },
    "out_going_column_mappings": {
        "id": "rez_constraint_id",
        "nominal_capacity_increase": "additional_network_capacity_mw",
    },
    "table_names": {
        "augmentation": _REZ_CONNECTION_AGUMENTATION_TABLES,
        "cost": {
            "progressive_change": _REZ_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE,
            "step_change_and_green_energy_exports": _REZ_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS,
        },
        "prep_activities": _REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES,
    },
    "prep_activities_mapping": _REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME,
}
