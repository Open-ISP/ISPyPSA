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
