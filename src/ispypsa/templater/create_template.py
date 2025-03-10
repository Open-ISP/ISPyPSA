from pathlib import Path

import pandas as pd

from ispypsa.templater.dynamic_generator_properties import (
    _template_generator_dynamic_properties,
)
from ispypsa.templater.energy_policy_targets import (
    _template_energy_policy_targets,
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
    """Creates a template set of `ISPyPSA` input tables based on IASR tables.

    Examples:

    # Peform required imports.
    >>> from pathlib import Path
    >>> from ispypsa.config import load_config
    >>> from ispypsa.data_fetch import read_csvs, write_csvs
    >>> from ispypsa.templater import load_manually_extracted_tables
    >>> from ispypsa.templater import create_ispypsa_inputs_template

    # Tables previously extracted from IASR workbook using isp_workbook_parser are
    # loaded.
    >>> iasr_tables = read_csvs(Path("iasr_directory"))

    # Some tables can't be handled by isp_workbook_parser so ISPyPSA ships with the
    # missing data.
    >>> manually_extracted_tables = load_manually_extracted_tables("6.0")

    # Now a template can be created by specifying the ISP scenario to use and the
    # spacial granularity of model.
    >>> ispypsa_inputs_template = create_ispypsa_inputs_template(
    ... scenario="Step Change",
    ... regional_granularity="sub_regions",
    ... iasr_tables=iasr_tables,
    ... manually_extracted_tables=manually_extracted_tables
    ... )

    # Write the template tables to a directory as CSVs.
    >>> write_csvs(ispypsa_inputs_template)

    Args:
        scenario: ISP scenario to generate template inputs based on.
        regional_granularity: the spatial granularity of the model template,
            "sub_regions", "nem_regions", or "single_region".
        iasr_tables: dictionary of dataframes providing the IASR input tables
            extracted using the `isp_workbook_parser`.
        manually_extracted_tables: dictionary of dataframes providing additional
            IASR tables that can't be parsed using `isp_workbook_parser`

    Returns: dictionary of dataframes in the `ISPyPSA` format. (add link to ispypsa
        table docs)
    """

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

    energy_policy_targets = _template_energy_policy_targets(iasr_tables, scenario)

    template.update(energy_policy_targets)

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
