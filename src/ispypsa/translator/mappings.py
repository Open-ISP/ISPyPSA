_ECAA_GENERATOR_ATTRIBUTES = {
    "generator": "name",
    "maximum_capacity_mw": "p_nom",
    "p_nom_extendable": "p_nom_extendable",
    "minimum_load_mw": "p_min_pu",
    "fuel_type": "carrier",
    "marginal_cost": "marginal_cost",
    "commissioning_date": "build_year",
    "lifetime": "lifetime",
    "capital_cost": "capital_cost",
    # attributes used for marginal cost calculations:
    "fuel_cost_mapping": "isp_fuel_cost_mapping",
    "vom_$/mwh_sent_out": "isp_vom_$/mwh_sent_out",
    "heat_rate_gj/mwh": "isp_heat_rate_gj/mwh",
    "rez_id": "isp_rez_id",
    # keeping technology_type because it's not defined anywhere else for the ECAA generators:
    "technology_type": "isp_technology_type",
}

_NEW_ENTRANT_GENERATOR_ATTRIBUTES = {
    # attributes used by the PyPSA network model:
    "generator": "name",
    "p_nom": "p_nom",
    "p_nom_extendable": "p_nom_extendable",
    "minimum_stable_level_%": "p_min_pu",
    "fuel_type": "carrier",
    "marginal_cost": "marginal_cost",
    "build_year": "build_year",
    "lifetime": "lifetime",
    "capital_cost": "capital_cost",
    # attributes used for marginal cost calculations:
    "fuel_cost_mapping": "isp_fuel_cost_mapping",
    "vom_$/mwh_sent_out": "isp_vom_$/mwh_sent_out",
    "heat_rate_gj/mwh": "isp_heat_rate_gj/mwh",
    # attributes used to filter/apply custom constraints:
    "isp_resource_type": "isp_resource_type",
    "rez_id": "isp_rez_id",
    # keeping technology_type because it's not defined anywhere else for the ECAA generators and could be useful for plotting/labelling?
    "technology_type": "isp_technology_type",
}

_GENERATOR_ATTRIBUTE_ORDER = [
    "name",
    "bus",
    "p_nom",
    "p_nom_mod",
    "p_nom_extendable",
    "p_nom_max",
    "p_min_pu",
    "carrier",
    "marginal_cost",
    "build_year",
    "lifetime",
    "capital_cost",
    "isp_technology_type",
    "isp_fuel_cost_mapping",
    "isp_vom_$/mwh_sent_out",
    "isp_heat_rate_gj/mwh",
    "isp_resource_type",
    "isp_rez_id",
]

# _GENERATOR_ATTRIBUTES dictionaries:
# Fields that have "isp_" at the beginning of the value string indicate columns
# that are used in calculating PyPSA input values for generators, but aren't
# attributes of Generator objects and aren't passed to the network.

_ECAA_BATTERY_ATTRIBUTES = {
    "storage_name": "name",
    "maximum_capacity_mw": "p_nom",
    "storage_duration_hours": "max_hours",
    "p_nom_extendable": "p_nom_extendable",
    "fuel_type": "carrier",
    "commissioning_date": "build_year",
    "lifetime": "lifetime",
    "capital_cost": "capital_cost",
    "charging_efficiency_%": "efficiency_store",
    "discharging_efficiency_%": "efficiency_dispatch",
    "rez_id": "isp_rez_id",
    # isp_resource_type has a clear mapping of technology type and storage duration
    "isp_resource_type": "isp_resource_type",
}

_NEW_ENTRANT_BATTERY_ATTRIBUTES = {  # attributes used by the PyPSA network model:
    "storage_name": "name",
    "p_nom": "p_nom",
    "storage_duration_hours": "max_hours",
    "p_nom_extendable": "p_nom_extendable",
    "fuel_type": "carrier",
    "build_year": "build_year",
    "lifetime": "lifetime",
    "capital_cost": "capital_cost",
    "charging_efficiency_%": "efficiency_store",
    "discharging_efficiency_%": "efficiency_dispatch",
    # attributes used to filter/apply custom constraints:
    "rez_id": "isp_rez_id",
    "isp_resource_type": "isp_resource_type",
}

_BATTERY_ATTRIBUTE_ORDER = [
    "name",
    "bus",
    "p_nom",
    "p_nom_extendable",
    "carrier",
    "max_hours",
    "capital_cost",
    "build_year",
    "lifetime",
    "cyclic_state_of_charge",
    "efficiency_store",
    "efficiency_dispatch",
    "isp_resource_type",
    "isp_rez_id",
]

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
    "resource_limit_mw": "rhs",
    "build_limit_mw": "rhs",
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

_VRE_BUILD_LIMIT_CUSTOM_CONSTRAINT_GROUPS = {
    "offshore_wind_build_limits": dict(
        column_mapping={
            "rez_id": "rez_id",
            "wind_generation_total_limits_mw_offshore_floating": "WFL",
            "wind_generation_total_limits_mw_offshore_fixed": "WFX",
        },
        constraint_name_suffix="build_limit",
        constraint_filter_col="isp_resource_type",
        constraint_type="build_limit_mw",
        can_be_relaxed=False,
    ),
    "onshore_wind_resource_limits": dict(
        column_mapping={
            "rez_id": "rez_id",
            "wind_generation_total_limits_mw_high": "WH",
            "wind_generation_total_limits_mw_medium": "WM",
            "rez_resource_limit_violation_penalty_factor_$/mw": "penalty_$/mw",
        },
        constraint_name_suffix="resource_limit",
        constraint_filter_col="isp_resource_type",
        constraint_type="resource_limit_mw",
        can_be_relaxed=True,
    ),
    "solar_resource_limits": dict(
        column_mapping={
            "rez_id": "rez_id",
            "solar_pv_plus_solar_thermal_limits_mw_solar": "Solar",
            "rez_resource_limit_violation_penalty_factor_$/mw": "penalty_$/mw",
        },
        constraint_name_suffix="resource_limit",
        constraint_filter_col="carrier",
        constraint_type="resource_limit_mw",
        can_be_relaxed=True,
    ),
    "wind_and_solar_land_use_limits": dict(
        column_mapping={
            "rez_id": "rez_id",
            "land_use_limits_mw_wind": "Wind",
            "land_use_limits_mw_solar": "Solar",
        },
        constraint_name_suffix="build_limit",
        constraint_filter_col="carrier",
        constraint_type="build_limit_mw",
        can_be_relaxed=False,
    ),
}
""" _VRE_BUILD_LIMIT_CUSTOM_CONSTRAINT_GROUPS
Defines configuration for variable renewable energy (VRE) build and resource limit constraint groups.
Each constraint group specifies how to create custom constraints for different types of new entrant
renewable energy resources in Renewable Energy Zones (REZs).

Dictionary structure:
- Keys: Names of constraint groups (used for logging - TODO: add logging)
- Values: Dictionaries with the following keys:
  - column_mapping: Maps columns from the renewable_energy_zones table to values used in constraints
  - constraint_name_suffix: Descriptor string to append to constraint names (e.g., "build_limit", "resource_limit")
  - constraint_filter_col: Column used to filter for generators to be subject to the constraint
  - constraint_type: Type of constraint ("build_limit_mw" or "resource_limit_mw"), also used to identify
        the values to set as the RHS of the constraint (column name in build_or_resource_limits df)
  - can_be_relaxed: Boolean indicating whether the constraint can be relaxed with penalty variables or not.
        Also defines whether or not to create dummy generators to relax the constraint.

Constraint groups:
- offshore_wind_build_limits: Hard build limit constraints on offshore wind (floating and fixed)
- onshore_wind_resource_limits: Soft constraints on onshore wind resources (high and medium quality)
- solar_resource_limits: Soft constraints on solar resources (including both solar thermal and PV)
- wind_and_solar_land_use_limits: Hard build limit constraints for onshore wind and solar defined by
        land use limits in each REZ.

Used by _create_vre_build_limit_constraints() to generate the left-hand side, right-hand side,
and dummy generators (for relaxable constraints) for the PyPSA custom constraints.
"""

_CARRIER_TO_FUEL_COST_TABLES = {
    "Gas": dict(
        base_table="gas_prices",
        blend_table="biomethane_prices",
        blend_percent_table="gpg_emissions_reduction_biomethane",
        fuel_cost_mapping_col="generator",
    ),
    "Black Coal": dict(base_table="coal_prices", fuel_cost_mapping_col="generator"),
    "Brown Coal": dict(base_table="coal_prices", fuel_cost_mapping_col="generator"),
    "Liquid Fuel": dict(
        base_table="liquid_fuel_prices",
    ),
    "Hyblend": dict(
        base_table="gas_prices",
        blend_table="hydrogen_prices",
        blend_percent_table="gpg_emissions_reduction_h2",
        fuel_cost_mapping_col="generator",
    ),
    "Biomass": dict(
        base_table="biomass_prices",
    ),
    "Hydrogen": dict(
        base_table="hydrogen_prices",
    ),
}
