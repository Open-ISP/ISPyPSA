import pandas as pd

from ispypsa.templater.dynamic_generator_properties import (
    template_generator_dynamic_properties,
)
from ispypsa.templater.flow_paths import (
    _template_regional_interconnectors,
    _template_sub_regional_flow_paths,
)
from ispypsa.templater.nodes import (
    _template_regions,
    _template_sub_regions,
    get_reference_node_locations,
)
from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zones,
)
from ispypsa.templater.static_ecaa_generator_properties import (
    template_ecaa_generators_static_properties,
)
from ispypsa.templater.static_new_generator_properties import (
    template_new_generators_static_properties,
)


def create_template(
    scenario: str, regional_granularity: str, iasr_tables: dict[str : pd.DataFrame]
) -> pd.DataFrame:
    """"""

    template = {}

    if regional_granularity == "sub_regions":
        sub_regions = _template_sub_regions(iasr_tables["sub_regional_reference_nodes"])
        template["sub_regions"] = get_reference_node_locations(sub_regions)
        template["flow_paths"] = _template_sub_regional_flow_paths(
            iasr_tables["flow_path_transfer_capability"]
        )
    elif regional_granularity == "nem_regions":
        regions = _template_regions(iasr_tables["regional_reference_nodes"])
        template["regions"] = get_reference_node_locations(regions)
        template["flow_paths"] = _template_regional_interconnectors(
            iasr_tables["interconnector_transfer_capability"]
        )

    template["renewable_energy_zones"] = template_renewable_energy_zones(
        iasr_tables["renewable_energy_zones"]
    )

    template["ecca_generators"] = template_ecaa_generators_static_properties(
        iasr_tables
    )

    template["new_entrant_generators"] = template_new_generators_static_properties(
        iasr_tables
    )

    dynamic_generator_property_templates = template_generator_dynamic_properties(
        iasr_tables, scenario
    )
    template.update(dynamic_generator_property_templates)

    return template
