_GENERATOR_ATTRIBUTES = {
    "generator": "name",
    "maximum_capacity_mw": "p_nom",
    "fuel_type": "carrier",
}

_BUS_ATTRIBUTES = {"isp_sub_region_id": "name"}

_LINK_ATTRIBUTES = {
    "flow_path": "name",
    "carrier": "carrier",
    "node_from": "bus0",
    "node_to": "bus1",
    "forward_direction_mw_summer_typical": "p_nom",
    "reverse_direction_mw_summer_typical": "p_nom_reverse",
}

_REZ_LINK_ATTRIBUTES = {
    "rez_id": "bus0",
    "isp_sub_region_id": "bus1",
    "carrier": "carrier",
    "rez_transmission_network_limit_summer_typical": "p_nom",
}

_CUSTOM_CONSTRAINT_ATTRIBUTES = {
    "term_id": "variable_name",
    "name": "variable_name",
    "isp_name": "constraint_name",
    "flow_path": "constraint_name",
    "additional_network_capacity_mw": "rhs",
    "constraint_id": "constraint_name",
    "rez_constraint_id": "constraint_name",
    "summer_typical": "rhs",
    "term_type": "term_type",
    "coefficient": "coefficient",
}

_CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE = {
    "link_flow": "Link",
    "generator_capacity": "Generator",
    "generator_output": "Generator",
    "load_consumption": "Load",
    "storage_output": "Storage",
}

_CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE = {
    "link_flow": "p",
    "generator_capacity": "p_nom",
    "generator_output": "p",
    "load_consumption": "p",
    "storage_output": "p",
}
