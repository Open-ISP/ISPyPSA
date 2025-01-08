_GENERATOR_ATTRIBUTES = {
    "generator": "name",
    "maximum_capacity_mw": "p_nom",
    "fuel_type": "carrier",
}

_BUS_ATTRIBUTES = {"node_id": "name"}

_LINE_ATTRIBUTES = {
    "flow_path_name": "name",
    "node_from": "bus0",
    "node_to": "bus1",
    "forward_direction_mw_summer_typical": "s_nom",
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
    "term_type": "component_type",
    "coefficient": "coefficient",
}

_CUSTOM_CONSTRAINT_ADDITIONAL_LINES = [
    "rez_group_constraints_expansion_costs",
    "rez_transmission_limit_constraints_expansion_costs",
]

_CUSTOM_CONSTRAINT_RHS_FILES = [
    "rez_group_constraints_rhs",
    "rez_transmission_limit_constraints_rhs",
]

_CUSTOM_CONSTRAINT_LHS_FILES = [
    "rez_group_constraints_lhs",
    "rez_transmission_limit_constraints_lhs",
]
