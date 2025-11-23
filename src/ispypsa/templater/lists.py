_ECAA_GENERATOR_TYPES = [
    "existing_generators",
    "committed_generators",
    "anticipated_projects",
    "additional_projects",
]

_NEW_GENERATOR_TYPES = ["new_entrants"]

_ALL_GENERATOR_TYPES = _ECAA_GENERATOR_TYPES + _NEW_GENERATOR_TYPES

_ALL_GENERATOR_STORAGE_TYPES = _ALL_GENERATOR_TYPES + [
    "existing_committed_and_anticipated_batteries"
]

_CONDENSED_GENERATOR_TYPES = [
    "existing_committed_anticipated_additional_generators",
    "new_entrants",
]

_ISP_SCENARIOS = ["Progressive Change", "Step Change", "Green Energy Exports"]

_MINIMUM_REQUIRED_GENERATOR_COLUMNS = [
    # naming
    "generator",
    "generator_name",
    "isp_resource_type",
    "technology_type",
    "status",
    # region
    "region_id",
    "sub_region_id",
    "rez_id",
    # fuel/marginal cost related
    "fuel_type",
    "fuel_cost_mapping",
    "fom_$/kw/annum",
    "vom_$/mwh_sent_out",
    "heat_rate_gj/mwh",
    # connection/build cost & limits
    "connection_cost_technology",
    "connection_cost_rez/_region_id",
    "build_limit_technology",
    "build_limit_region_id",
    "technology_specific_lcf_%",
    # capacity
    "maximum_capacity_mw",
    "unit_capacity_mw",
    # generator timing
    "commissioning_date",
    "closure_year",
    "lifetime",
    # operating limit
    "minimum_stable_level_%",
    "minimum_load_mw",
]
