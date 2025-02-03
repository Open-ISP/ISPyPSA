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
from ispypsa.templater.static_generator_properties import (
    _template_ecaa_generators_static_properties,
)


def run(config, iasr_tables):
    template = {}

    if config.regional_granularity == "sub_regions":
        sub_regions = _template_sub_regions(iasr_tables["sub_regional_reference_nodes"])
        template["sub_regions"] = get_reference_node_locations(sub_regions)
        template["flow_paths"] = _template_sub_regional_flow_paths(
            iasr_tables["flow_path_transfer_capability"]
        )
    elif config.regional_granularity == "nem_regions":
        regions = _template_regions(iasr_tables["regional_reference_nodes"])
        template["regions"] = get_reference_node_locations(regions)
        template["flow_paths"] = _template_regional_interconnectors(
            iasr_tables["interconnector_transfer_capability"]
        )

    template["renewable_energy_zones"] = template_renewable_energy_zones(
        iasr_tables["renewable_energy_zones"]
    )

    ecaa_generators_template = _template_ecaa_generators_static_properties(
        workbook_cache_location
    )
    dynamic_generator_property_templates = template_generator_dynamic_properties(
        workbook_cache_location, config.scenario
    )
    if node_template is not None:
        node_template.to_csv(Path(template_location, "nodes.csv"))
    if renewable_energy_zone_location_mapping is not None:
        renewable_energy_zone_location_mapping.to_csv(
            Path(template_location, "mapping_renewable_energy_zone_locations.csv")
        )
    if sub_regions_to_nem_regions_mapping is not None:
        sub_regions_to_nem_regions_mapping.to_csv(
            Path(template_location, "mapping_sub_regions_to_nem_regions.csv")
        )
    if nem_region_to_single_sub_region_mapping is not None:
        nem_region_to_single_sub_region_mapping.to_csv(
            Path(template_location, "mapping_nem_region_to_single_sub_region.csv")
        )
    if flow_path_template is not None:
        flow_path_template.to_csv(Path(template_location, "flow_paths.csv"))
    if ecaa_generators_template is not None:
        ecaa_generators_template.to_csv(Path(template_location, "ecaa_generators.csv"))
    if dynamic_generator_property_templates is not None:
        for gen_property in dynamic_generator_property_templates.keys():
            dynamic_generator_property_templates[gen_property].to_csv(
                Path(template_location, f"{gen_property}.csv")
            )
