_GENERATOR_ATTRIBUTES = {
    "generator": "name",
    "maximum_capacity_mw": "p_nom",
    "fuel_type": "carrier",
}

_BUS_ATTRIBUTES = {"isp_sub_region_id": "name"}

_LINE_ATTRIBUTES = {
    "flow_path_name": "name",
    "node_from": "bus0",
    "node_to": "bus1",
    "forward_direction_mw_summer_typical": "s_nom",
    "indicative_transmission_expansion_cost_$/mw": "capital_cost",
    # TODO: implement reverse direction limit
    # "reverse_direction_mw_summer_typical": ""
}

_REZ_LINE_ATTRIBUTES = {
    "rez_id": "bus0",
    "isp_sub_region_id": "bus1",
    "rez_transmission_network_limit_summer_typical": "s_nom",
    "indicative_transmission_expansion_cost_$/mw": "capital_cost",
}

_CUSTOM_CONSTRAINT_ATTRIBUTES = {
    "term_id": "variable_name",
    "indicative_transmission_expansion_cost_$/mw": "capital_cost",
    "constraint_id": "constraint_name",
    "summer_typical": "rhs",
    "term_type": "term_type",
    "coefficient": "coefficient",
}

_CUSTOM_CONSTRAINT_EXPANSION_COSTS = [
    "rez_group_constraints_expansion_costs",
    "rez_transmission_limit_constraints_expansion_costs",
]

_CUSTOM_CONSTRAINT_RHS_TABLES = [
    "rez_group_constraints_rhs",
    "rez_transmission_limit_constraints_rhs",
]

_CUSTOM_CONSTRAINT_LHS_TABLES = [
    "rez_group_constraints_lhs",
    "rez_transmission_limit_constraints_lhs",
]

_CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE = {
    "line_flow": "Line",
    "generator_capacity": "Generator",
    "generator_output": "Generator",
    "load_consumption": "Load",
    "storage_output": "Storage",
}

_CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE = {
    "line_flow": "s",
    "generator_capacity": "p_nom",
    "generator_output": "p",
    "load_consumption": "p",
    "storage_output": "p",
}

_POLICY_CONSTRAINT_TABLES = [
    "powering_australia_plan",
    "renewable_generation_targets",
    "renewable_share_targets",
    "technology_capacity_targets",
]

_POLICY_CONSTRAINT_ID_TO_METRIC = {
    "power_aus": "pct",
    "nsw_eir_gen": "mwh",
    "tret": "mwh",
    "vret": "pct",
    "qret": "pct",
    "cis_generator": "mw",
    "nsw_eir_sto": "mw",
    "cis_storage": "mw",
    "vic_offshore_wind": "mw",
    "vic_storage": "mw",
}

_POLICY_CONSTRAINT_ID_TO_ATTRIBUTE_TYPE = {
    "power_aus": "p",
    "nsw_eir_gen": "p",
    "tret": "p",
    "vret": "p",
    "qret": "p",
    "cis_generator": "p_nom",
    "nsw_eir_sto": "p_nom",
    "cis_storage": "p_nom",
    "vic_offshore_wind": "p_nom",
    "vic_storage": "p_nom",
}

_REGION_TO_BUS = {
    "VIC": ["VIC", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8"],
    "NSW": [
        "NSW",
        "NNSW",
        "CNSW",
        "SNSW",
        "SNW",
        "N1",
        "N2",
        "N3",
        "N4",
        "N5",
        "N6",
        "N7",
        "N8",
        "N9",
        "N10",
        "N11",
        "N12",
    ],
    "QLD": [
        "QLD",
        "NQ",
        "CQ",
        "SQ",
        "GG",
        "Q1",
        "Q2",
        "Q3",
        "Q4",
        "Q5",
        "Q6",
        "Q7",
        "Q8",
        "Q9",
    ],
    "SA": [
        "SA",
        "CSA",
        "SESA",
        "S1",
        "S2",
        "S3",
        "S4",
        "S5",
        "S6",
        "S7",
        "S8",
        "S9",
        "S10",
    ],
    "TAS": ["TAS", "T1", "T2", "T3", "T4"],
    "NEM": [
        "VIC",
        "V1",
        "V2",
        "V3",
        "V4",
        "V5",
        "V6",
        "V7",
        "V8",
        "NSW",
        "NNSW",
        "CNSW",
        "SNSW",
        "SNW",
        "N1",
        "N2",
        "N3",
        "N4",
        "N5",
        "N6",
        "N7",
        "N8",
        "N9",
        "N10",
        "N11",
        "N12",
        "QLD",
        "NQ",
        "CQ",
        "SQ",
        "GG",
        "Q1",
        "Q2",
        "Q3",
        "Q4",
        "Q5",
        "Q6",
        "Q7",
        "Q8",
        "Q9",
        "SA",
        "CSA",
        "SESA",
        "S1",
        "S2",
        "S3",
        "S4",
        "S5",
        "S6",
        "S7",
        "S8",
        "S9",
        "S10",
        "TAS",
        "T1",
        "T2",
        "T3",
        "T4",
    ],
}
