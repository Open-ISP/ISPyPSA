import logging
from pathlib import Path

import pandas as pd

from ispypsa.feature_flags import FEATURE_FLAGS
from ispypsa.templater.connection_and_build_costs import _template_connection_costs
from ispypsa.templater.custom_constraints_from_plexos import (
    template_custom_constraints_from_plexos,
)
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
from ispypsa.templater.geography import _template_network_geography
from ispypsa.templater.network_expansion import (
    _extract_flow_path_costs_from_iasr,
    _extract_flow_path_options_from_iasr,
    _extract_rez_costs_from_iasr,
    _extract_rez_options_from_iasr,
    _filter_flow_path_augmentations_to_granularity,
    _template_network_expansion,
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
from ispypsa.templater.transmission import _template_network_transmission

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

# Outputs from the new-format templater branch. Granularity-invariant: the
# same five tables are emitted for sub_regions, nem_regions, and single_region
# (only their contents differ).
# FEATURE_FLAG_CLEANUP[use_new_table_format]: rename to _TEMPLATE_OUTPUTS and
# delete _BASE_TEMPLATE_OUTPUTS above.
_NEW_FORMAT_TEMPLATE_OUTPUTS = [
    "network_geography",
    "network_transmission_paths",
    "network_transmission_path_limits",
    "network_expansion_options",
    "network_transmission_path_expansion_costs",
]

# Custom constraints are templated only at sub_regions granularity (see the gate
# in create_ispypsa_inputs_template) — coarser granularities collapse the
# sub-region nodes, sub-regional flow paths and REZ-located units the constraints
# reference, leaving nothing to constrain. Listed as outputs only at that
# granularity so the create_ispypsa_inputs task tracks them where they are
# written and does not expect them where they never are.
_CUSTOM_CONSTRAINT_OUTPUTS = [
    "custom_constraints",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
]


def create_ispypsa_inputs_template(
    scenario: str,
    regional_granularity: str,
    iasr_tables: dict[str, pd.DataFrame],
    manually_extracted_tables: dict[str, pd.DataFrame],
    iasr_workbook_version: str,
    filter_to_nem_regions: list[str] | None = None,
    filter_to_isp_sub_regions: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
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
        ... manually_extracted_tables=manually_extracted_tables,
        ... iasr_workbook_version="7.5",
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
        iasr_workbook_version: the IASR workbook version (e.g. `"7.5"`), used
            to select the PLEXOS extract directory for the custom-constraints
            templater.
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

    # FEATURE_FLAG_CLEANUP[use_new_table_format]: drop the else-branch (legacy
    # templater path) and inline this branch.
    if FEATURE_FLAGS["use_new_table_format"]:
        template = {}
        sub_regional_geography = _template_network_geography(
            iasr_tables["sub_regional_reference_nodes"],
            iasr_tables["renewable_energy_zones"],
            "sub_regions",
        )
        if regional_granularity == "sub_regions":
            template["network_geography"] = sub_regional_geography
        else:
            template["network_geography"] = _template_network_geography(
                iasr_tables["sub_regional_reference_nodes"],
                iasr_tables["renewable_energy_zones"],
                regional_granularity,
            )
        region_lookup = dict(
            zip(sub_regional_geography["geo_id"], sub_regional_geography["region_id"])
        )
        flow_path_options = _filter_flow_path_augmentations_to_granularity(
            _extract_flow_path_options_from_iasr(iasr_tables),
            regional_granularity,
            region_lookup,
        )
        flow_path_costs = _filter_flow_path_augmentations_to_granularity(
            _extract_flow_path_costs_from_iasr(iasr_tables, scenario),
            regional_granularity,
            region_lookup,
        )
        paths, limits = _template_network_transmission(
            iasr_tables["flow_path_transfer_capability"],
            iasr_tables["initial_transmission_limits"],
            iasr_tables["renewable_energy_zones"],
            sub_regional_geography,
            regional_granularity,
            flow_path_options,
        )
        template["network_transmission_paths"] = paths
        template["network_transmission_path_limits"] = limits
        expansion_options, expansion_costs = _template_network_expansion(
            flow_path_options=flow_path_options,
            flow_path_costs=flow_path_costs,
            rez_options=_extract_rez_options_from_iasr(iasr_tables),
            rez_costs=_extract_rez_costs_from_iasr(iasr_tables, scenario),
            network_transmission_paths=paths,
            rez_ids=set(iasr_tables["renewable_energy_zones"]["ID"]),
        )
        template["network_expansion_options"] = expansion_options
        template["network_transmission_path_expansion_costs"] = expansion_costs

        # todo: replace with actual generators_new_entrant once that templating
        # function is written — passing empty placeholder for now so costs_connection
        # is wired up but produces no VRE rows until generators are templated.
        generators_new_entrant = pd.DataFrame(columns=["geo_id", "technology"])
        template["costs_connection"] = _template_connection_costs(
            iasr_tables["connection_cost_forecast_wind_and_solar"],
            iasr_tables["connection_costs_for_wind_and_solar"],
            iasr_tables["efficient_level_of_system_strength_cost"],
            scenario,
            generators_new_entrant,
        )
        # Custom constraints from PLEXOS are sub-regional export-group limits:
        # their LHS references sub-region nodes, sub-regional flow paths, and
        # REZ-located units that only exist as distinct entities at sub_regions
        # granularity. Once sub-regions are collapsed (nem_regions /
        # single_region) they have no meaningful representation, so only emit
        # them for sub_regions.
        if regional_granularity == "sub_regions":
            template.update(
                template_custom_constraints_from_plexos(
                    iasr_tables, iasr_workbook_version=iasr_workbook_version
                )
            )
        return template

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
    # FEATURE_FLAG_CLEANUP[use_new_table_format]: drop the else-branch and the
    # granularity-specific file removals.
    if FEATURE_FLAGS["use_new_table_format"]:
        files = _NEW_FORMAT_TEMPLATE_OUTPUTS.copy()
        if regional_granularity == "sub_regions":
            files += _CUSTOM_CONSTRAINT_OUTPUTS
    else:
        files = _BASE_TEMPLATE_OUTPUTS.copy()
        if regional_granularity in ["sub_regions", "single_region"]:
            files.remove("nem_regions")
        if regional_granularity == "single_region":
            files.remove("flow_paths")
    if output_path is not None:
        files = [output_path / Path(file + ".csv") for file in files]
    return files
