from pathlib import Path

import pandas as pd

from ispypsa.templater.dynamic_generator_properties import (
    _template_generator_dynamic_properties,
)
from ispypsa.templater.flow_paths import (
    _template_regional_interconnectors,
    _template_sub_regional_flow_paths,
)
from ispypsa.templater.nodes import (
    _template_regions,
    _template_sub_regions,
)
from ispypsa.templater.renewable_energy_zones import (
    _template_rez_build_limits,
)
from ispypsa.templater.static_ecaa_generator_properties import (
    _template_ecaa_generators_static_properties,
)
from ispypsa.templater.static_new_generator_properties import (
    _template_new_generators_static_properties,
)

_BASE_TEMPLATE_OUTPUTS = [
    "sub_regions",
    "nem_regions",
    "renewable_energy_zones",
    "flow_paths",
    "ecaa_generators",
    "new_entrant_generators",
    "coal_prices",
    "gas_prices",
    "liquid_fuel_prices",
    "full_outage_forecasts",
    "partial_outage_forecasts",
    "seasonal_ratings",
    "closure_years",
    "rez_group_constraints_expansion_costs",
    "rez_group_constraints_lhs",
    "rez_group_constraints_rhs",
    "rez_transmission_limit_constraints_expansion_costs",
    "rez_transmission_limit_constraints_lhs",
    "rez_transmission_limit_constraints_rhs",
]


def create_ispypsa_inputs_template(
    scenario: str,
    regional_granularity: str,
    iasr_tables: dict[str : pd.DataFrame],
    manually_extracted_tables: dict[str : pd.DataFrame],
) -> dict[str : pd.DataFrame]:
    """"""

    template = {}

    transmission_expansion_costs = manually_extracted_tables.pop(
        "transmission_expansion_costs"
    )
    template.update(manually_extracted_tables)

    if regional_granularity == "sub_regions":
        template["sub_regions"] = _template_sub_regions(
            iasr_tables["sub_regional_reference_nodes"], mapping_only=False
        )

        template["flow_paths"] = _template_sub_regional_flow_paths(
            iasr_tables["flow_path_transfer_capability"], transmission_expansion_costs
        )

    elif regional_granularity == "nem_regions":
        template["sub_regions"] = _template_sub_regions(
            iasr_tables["sub_regional_reference_nodes"], mapping_only=True
        )

        template["nem_regions"] = _template_regions(
            iasr_tables["regional_reference_nodes"]
        )

        template["flow_paths"] = _template_regional_interconnectors(
            iasr_tables["interconnector_transfer_capability"]
        )

    else:
        template["sub_regions"] = _template_sub_regions(
            iasr_tables["sub_regional_reference_nodes"], mapping_only=True
        )

    template["renewable_energy_zones"] = _template_rez_build_limits(
        iasr_tables["initial_build_limits"]
    )

    template["ecaa_generators"] = _template_ecaa_generators_static_properties(
        iasr_tables
    )

    template["new_entrant_generators"] = _template_new_generators_static_properties(
        iasr_tables
    )

    dynamic_generator_property_templates = _template_generator_dynamic_properties(
        iasr_tables, scenario
    )

    template.update(dynamic_generator_property_templates)

    return template


def list_templater_output_files(regional_granularity, output_path=None):
    files = _BASE_TEMPLATE_OUTPUTS.copy()
    if regional_granularity in ["sub_regions", "single_region"]:
        files.remove("nem_regions")
    if regional_granularity == "single_region":
        files.remove("flow_paths")
    if output_path is not None:
        files = [output_path / Path(file + ".csv") for file in files]
    return files
