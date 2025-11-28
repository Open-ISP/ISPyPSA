from pathlib import Path

from isp_workbook_parser import Parser

from ..templater.mappings import (
    _ACTIONABLE_ISP_PROJECTS_TABLES,
    _FLOW_PATH_AGUMENTATION_TABLES,
    _FLOW_PATH_AUGMENTATION_COST_TABLES,
    _GENERATOR_PROPERTIES,
    _PREPATORY_ACTIVITIES_TABLES,
    _REZ_AUGMENTATION_COST_TABLES,
    _REZ_CONNECTION_AGUMENTATION_TABLES,
    _REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES,
)

_GENERATOR_PROPERTY_TABLES = [
    table_name
    for key, val in _GENERATOR_PROPERTIES.items()
    for table_name in [key + "_" + gen_type for gen_type in val]
]

_NEW_ENTRANTS_COST_TABLES = [
    "coal_and_biomass_price_consultant_scenario_mapping",
    "biomass_prices",
    "build_costs_scenario_mapping",
    "build_costs_current_policies",
    "build_costs_global_nze_by_2050",
    "build_costs_global_nze_post_2050",
    "build_costs_pumped_hydro",
    "connection_costs_for_wind_and_solar",
    "connection_costs_other",
    "connection_cost_forecast_wind_and_solar_progressive_change",
    "connection_cost_forecast_wind_and_solar_step_change&green_energy_exports",
    "connection_cost_forecast_non_rez_progressive_change",
    "connection_cost_forecast_non_rez_step_change&green_energy_exports",
]

_NETWORK_REQUIRED_TABLES = [
    "sub_regional_reference_nodes",
    "regional_topology_representation",
    "regional_reference_nodes",
    "renewable_energy_zones",
    "flow_path_transfer_capability",
    "interconnector_transfer_capability",
    "initial_build_limits",
]

_NETWORK_REQUIRED_TABLES = (
    _NETWORK_REQUIRED_TABLES
    + _FLOW_PATH_AGUMENTATION_TABLES
    + _FLOW_PATH_AUGMENTATION_COST_TABLES
    + _PREPATORY_ACTIVITIES_TABLES
    + _ACTIONABLE_ISP_PROJECTS_TABLES
    + _REZ_CONNECTION_AGUMENTATION_TABLES
    + _REZ_AUGMENTATION_COST_TABLES
    + _REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES
)

_GENERATORS_STORAGE_REQUIRED_SUMMARY_TABLES = [
    "existing_generators_summary",
    "committed_generators_summary",
    "anticipated_projects_summary",
    "batteries_summary",
    "additional_projects_summary",
    "new_entrants_summary",
]

_GENERATORS_REQUIRED_PROPERTY_TABLES = [
    "expected_closure_years",
    "coal_minimum_stable_level",
    "liquid_fuel_prices",
    "hydrogen_prices",
    "biomethane_prices",
    "gpg_emissions_reduction_h2_kogan",
    "gpg_emissions_reduction_h2_sa_turbine",
    "gpg_emissions_reduction_biomethane",
    "locational_cost_factors",
    "technology_cost_breakdown_ratios",
    "lead_time_and_project_life",
    "technology_specific_lcfs",
] + _GENERATOR_PROPERTY_TABLES

_POLICY_REQUIRED_TABLES = [
    "vic_renewable_target_trajectory",
    "qld_renewable_target_trajectory",
    "powering_australia_plan_trajectory",
    "capacity_investment_scheme_renewable_trajectory",
    "capacity_investment_scheme_storage_trajectory",
    "nsw_roadmap_storage_trajectory",
    "vic_storage_target_trajectory",
    "vic_offshore_wind_target_trajectory",
    "nsw_roadmap_renewable_trajectory",
    "tas_renewable_target_trajectory",
]

REQUIRED_TABLES = (
    _NETWORK_REQUIRED_TABLES
    + _GENERATORS_STORAGE_REQUIRED_SUMMARY_TABLES
    + _GENERATORS_REQUIRED_PROPERTY_TABLES
    + _NEW_ENTRANTS_COST_TABLES
    + _POLICY_REQUIRED_TABLES
)


def build_local_cache(
    cache_path: Path | str, workbook_path: Path | str, iasr_workbook_version: str
) -> None:
    """Uses `isp-workbook-parser` to build a local cache of parsed workbook CSVs

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.iasr_table_caching import build_local_cache

        Build the local cache of parsed workbook CSVs.
        >>> build_local_cache(
        ...     cache_path=Path("parsed_workbook_cache"),
        ...     workbook_path=Path("path/to/ISP_Workbook.xlsx"),
        ...     iasr_workbook_version="6.0"
        ... )

    Args:
        cache_path: Path that should be created for the local cache
        workbook_path: Path to an ISP Assumptions Workbook that is supported by
            `isp-workbook-parser`
        iasr_workbook_version: str specifying the version of the work being used.

    Returns:
        None
    """
    workbook = Parser(Path(workbook_path))
    if workbook.workbook_version != iasr_workbook_version:
        raise ValueError(
            "The IASR workbook provided does not match the version "
            "specified in the config."
        )
    tables_to_get = REQUIRED_TABLES
    workbook.save_tables(cache_path, tables=tables_to_get)
    return None


def list_cache_files(cache_path):
    files = REQUIRED_TABLES
    files = [cache_path / Path(file + ".csv") for file in files]
    return files
