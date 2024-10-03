import pandas as pd

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

_EXISTING_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "maximum_capacity_mw": dict(
        csv="maximum_capacity_existing_generators",
        csv_lookup="Generator",
        csv_values="Installed capacity (MW)",
    ),
    "maintenance_duration_%": dict(
        csv="maintenance_existing_generators",
        csv_lookup="Generator type",
        csv_values="Proportion of time out (%)",
    ),
    "minimum_load_mw": dict(
        csv="coal_minimum_stable_level",
        csv_lookup="Generating unit",
        csv_values="Minimum Stable Level (MW)",
    ),
    "fom_$/kw/annum": dict(
        csv="fixed_opex_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_values="Fixed OPEX ($/kW/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        csv="variable_opex_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_values="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        csv="heat_rates_existing_committed_anticipated_additional_generators",
        csv_lookup="Generator",
        csv_values="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        csv="marginal_loss_factors_existing_generators",
        csv_lookup="Generator",
        csv_values="MLF",
    ),
    "auxiliary_load_%": dict(
        csv="auxiliary_load_existing_committed_anticipated_additional_generators",
        csv_lookup="Fuel/Technology type",
        csv_values="Auxiliary load (% of nameplate capacity)",
    ),
    "partial_outage_derating_factor_%": dict(
        csv="outages_2023-2024_existing_generators",
        csv_lookup="Fuel type",
        csv_values="Partial Outage Derating Factor (%)",
    ),
}
"""
Existing generators summary table columns mapped to corresponding data CSV and
lookup information that can be used to retrieve values
"""
