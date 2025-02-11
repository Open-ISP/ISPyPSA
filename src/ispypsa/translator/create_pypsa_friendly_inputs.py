from pathlib import Path

import pandas as pd

from ispypsa.translator.buses import (
    _create_single_region_bus,
    _translate_isp_sub_regions_to_buses,
    _translate_nem_regions_to_buses,
    _translate_rezs_to_buses,
)
from ispypsa.translator.custom_constraints import (
    _translate_custom_constraint_lhs,
    _translate_custom_constraint_rhs,
    _translate_custom_constraints_generators,
)
from ispypsa.translator.generators import (
    _translate_ecaa_generators,
)
from ispypsa.translator.lines import _translate_flow_paths_to_lines
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_EXPANSION_COSTS,
    _CUSTOM_CONSTRAINT_LHS_TABLES,
    _CUSTOM_CONSTRAINT_RHS_TABLES,
)
from ispypsa.translator.renewable_energy_zones import (
    _translate_renewable_energy_zone_build_limits_to_flow_paths,
)
from ispypsa.translator.snapshot import _create_complete_snapshots_index
from ispypsa.translator.temporal_filters import _filter_snapshots

_BASE_TRANSLATOR_OUPUTS = [
    "snapshots",
    "buses",
    "lines",
    "generators",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
    "custom_constraints_generators",
]


def create_pypsa_friendly_inputs(config, ispypsa_tables):
    pypsa_inputs = {}

    snapshots = _create_complete_snapshots_index(
        start_year=config.temporal.start_year,
        end_year=config.temporal.end_year,
        operational_temporal_resolution_min=config.temporal.operational_temporal_resolution_min,
        year_type=config.temporal.year_type,
    )

    pypsa_inputs["snapshots"] = _filter_snapshots(
        config=config.temporal, snapshots=snapshots
    )

    pypsa_inputs["generators"] = _translate_ecaa_generators(
        ispypsa_tables["ecaa_generators"], config.network.nodes.regional_granularity
    )

    buses = []
    lines = []

    if config.network.nodes.regional_granularity == "sub_regions":
        buses.append(_translate_isp_sub_regions_to_buses(ispypsa_tables["sub_regions"]))
    elif config.network.nodes.regional_granularity == "nem_regions":
        buses.append(_translate_nem_regions_to_buses(ispypsa_tables["regions"]))
    elif config.regional_granularity == "single_region":
        buses.append(_create_single_region_bus())

    if config.network.nodes.rezs == "discrete_nodes":
        buses.append(_translate_rezs_to_buses(ispypsa_tables["renewable_energy_zones"]))
        lines.append(
            _translate_renewable_energy_zone_build_limits_to_flow_paths(
                ispypsa_tables["renewable_energy_zones"],
                config.network.rez_transmission_expansion,
                config.wacc,
                config.network.annuitisation_lifetime,
                config.network.rez_to_sub_region_transmission_default_limit,
            )
        )

    lines.append(
        _translate_flow_paths_to_lines(
            ispypsa_tables["flow_paths"],
            config.network.transmission_expansion,
            config.wacc,
            config.network.annuitisation_lifetime,
        )
    )

    pypsa_inputs["buses"] = pd.concat(buses)
    pypsa_inputs["lines"] = pd.concat(lines)

    custom_constraint_lhs_tables = [
        ispypsa_tables[table] for table in _CUSTOM_CONSTRAINT_LHS_TABLES
    ]
    pypsa_inputs["custom_constraints_lhs"] = _translate_custom_constraint_lhs(
        custom_constraint_lhs_tables
    )
    custom_constraint_rhs_tables = [
        ispypsa_tables[table] for table in _CUSTOM_CONSTRAINT_RHS_TABLES
    ]
    pypsa_inputs["custom_constraints_rhs"] = _translate_custom_constraint_rhs(
        custom_constraint_rhs_tables
    )
    custom_constraint_generators = [
        ispypsa_tables[table] for table in _CUSTOM_CONSTRAINT_EXPANSION_COSTS
    ]
    pypsa_inputs["custom_constraints_generators"] = (
        _translate_custom_constraints_generators(
            custom_constraint_generators,
            config.network.rez_transmission_expansion,
            config.wacc,
            config.network.annuitisation_lifetime,
        )
    )

    return pypsa_inputs


def list_translator_output_files(output_path):
    files = _BASE_TRANSLATOR_OUPUTS
    files = [output_path / Path(file + ".csv") for file in files]
    return files
