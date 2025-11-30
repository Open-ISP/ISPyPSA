import logging
from pathlib import Path

import pandas as pd

from ispypsa.templater.dynamic_generator_properties import (
    _template_generator_dynamic_properties,
)
from ispypsa.templater.energy_policy_targets import (
    _template_energy_policy_targets,
)
from ispypsa.templater.filter_template import _filter_template
from ispypsa.templater.flow_paths import (
    _template_regional_interconnectors,
    _template_rez_transmission_costs,
    _template_sub_regional_flow_path_costs,
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
from ispypsa.templater.storage import _template_battery_properties

_BASE_TEMPLATE_OUTPUTS = [
    "sub_regions",
    "nem_regions",
    "renewable_energy_zones",
    "flow_paths",
    "ecaa_generators",
    "ecaa_batteries",
    "new_entrant_generators",
    "new_entrant_batteries",
    "coal_prices",
    "gas_prices",
    "liquid_fuel_prices",
    "biomass_prices",
    "biomethane_prices",
    "hydrogen_prices",
    "new_entrant_build_costs",
    "gpg_emissions_reduction_h2",
    "gpg_emissions_reduction_biomethane",
    "full_outage_forecasts",
    "partial_outage_forecasts",
    "seasonal_ratings",
    "new_entrant_wind_and_solar_connection_costs",
    "new_entrant_non_vre_connection_costs",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
]


def create_ispypsa_inputs_template(
    scenario: str,
    regional_granularity: str,
    iasr_tables: dict[str : pd.DataFrame],
    manually_extracted_tables: dict[str : pd.DataFrame],
    filter_to_nem_regions: list[str] = None,
    filter_to_isp_sub_regions: list[str] = None,
) -> dict[str : pd.DataFrame]:
    """Creates a template set of [`ISPyPSA` input tables](tables/ispypsa.md).

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.config import load_config
        >>> from ispypsa.data_fetch import read_csvs, write_csvs
        >>> from ispypsa.templater import load_manually_extracted_tables
        >>> from ispypsa.templater import create_ispypsa_inputs_template

        Tables previously extracted from IASR workbook using isp_workbook_parser are
        loaded.
        >>> iasr_tables = read_csvs(Path("iasr_directory"))

        Some tables can't be handled by isp_workbook_parser so ISPyPSA ships with the
        missing data.
        >>> manually_extracted_tables = load_manually_extracted_tables("6.0")

        Now a template can be created by specifying the ISP scenario to use and the
        spacial granularity of model.
        >>> ispypsa_inputs_template = create_ispypsa_inputs_template(
        ... scenario="Step Change",
        ... regional_granularity="sub_regions",
        ... iasr_tables=iasr_tables,
        ... manually_extracted_tables=manually_extracted_tables
        ... )

        Write the template tables to a directory as CSVs.
        >>> write_csvs(ispypsa_inputs_template, Path("ispypsa_inputs"))

    Args:
        scenario: ISP scenario to generate template inputs based on.
        regional_granularity: the spatial granularity of the model template,
            "sub_regions", "nem_regions", or "single_region".
        iasr_tables: dictionary of dataframes providing the IASR input tables
            extracted using the `isp_workbook_parser`.
        manually_extracted_tables: dictionary of dataframes providing additional
            IASR tables that can't be parsed using `isp_workbook_parser`
        filter_to_nem_regions: Optional list of NEM region IDs (e.g., ['NSW', 'VIC'])
            to filter the template to. Cannot be specified together with
            filter_to_isp_sub_regions.
        filter_to_isp_sub_regions: Optional list of ISP sub-region IDs
            (e.g., ['CNSW', 'VIC', 'TAS']) to filter the template to. Cannot be
            specified together with filter_to_nem_regions.

    Returns:
        dictionary of dataframes in the [`ISPyPSA` format](tables/ispypsa.md)

    Raises:
        ValueError: If both filter_to_nem_regions and filter_to_isp_sub_regions are provided
    """
    # Validate filtering parameters
    if filter_to_nem_regions is not None and filter_to_isp_sub_regions is not None:
        raise ValueError(
            "Cannot specify both filter_to_nem_regions and filter_to_isp_sub_regions"
        )

    template = {}

    template.update(manually_extracted_tables)

    if regional_granularity == "sub_regions":
        template["sub_regions"] = _template_sub_regions(
            iasr_tables["sub_regional_reference_nodes"], mapping_only=False
        )

        template["flow_paths"] = _template_sub_regional_flow_paths(
            iasr_tables["flow_path_transfer_capability"]
        )

        template["flow_path_expansion_costs"] = _template_sub_regional_flow_path_costs(
            iasr_tables,
            scenario,
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
        iasr_tables["initial_build_limits"], scenario
    )

    possible_rez_or_constraint_names = list(
        set(
            list(template["renewable_energy_zones"]["rez_id"])
            + list(template["custom_constraints_rhs"]["constraint_id"])
        )
    )

    template["rez_transmission_expansion_costs"] = _template_rez_transmission_costs(
        iasr_tables,
        scenario,
        possible_rez_or_constraint_names,
    )

    template["ecaa_generators"] = _template_ecaa_generators_static_properties(
        iasr_tables
    )

    template["new_entrant_generators"] = _template_new_generators_static_properties(
        iasr_tables
    )

    ecaa_batteries, new_entrant_batteries = _template_battery_properties(iasr_tables)
    template["ecaa_batteries"] = ecaa_batteries
    template["new_entrant_batteries"] = new_entrant_batteries

    dynamic_generator_property_templates = _template_generator_dynamic_properties(
        iasr_tables, scenario
    )

    template.update(dynamic_generator_property_templates)

    energy_policy_targets = _template_energy_policy_targets(iasr_tables, scenario)

    template.update(energy_policy_targets)

    # Apply regional filtering if requested
    if filter_to_nem_regions or filter_to_isp_sub_regions:
        template = _filter_template(
            template,
            nem_regions=filter_to_nem_regions,
            isp_sub_regions=filter_to_isp_sub_regions,
        )

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
