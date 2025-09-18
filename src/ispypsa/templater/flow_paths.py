import logging
import re

import pandas as pd

from .helpers import (
    _fuzzy_match_names,
    _snakecase_string,
    _strip_all_text_after_numeric_value,
)
from .mappings import (
    _FLOW_PATH_CONFIG,
    _HVDC_FLOW_PATHS,
    _REZ_CONFIG,
)


def _template_sub_regional_flow_paths(
    flow_path_capabilities: pd.DataFrame,
) -> pd.DataFrame:
    """Processes the 'Flow path transfer capability' table into an ISPyPSA template
    format.

    Args:
        flow_path_capabilities: pd.DataFrame IASR table specifying the flow path
            transfer capabilities between subregions

    Returns:
        `pd.DataFrame`: ISPyPSA sub-regional flow path template
    """
    from_to_carrier = _get_flow_path_name_from_to_carrier(
        flow_path_capabilities.iloc[:, 0], regional_granularity="sub_regions"
    )
    capability_columns = _clean_capability_column_names(flow_path_capabilities)
    sub_regional_capabilities = pd.concat([from_to_carrier, capability_columns], axis=1)
    cols = [
        "flow_path",
        "node_from",
        "node_to",
        "carrier",
        "forward_direction_mw_summer_typical",
        "reverse_direction_mw_summer_typical",
    ]
    sub_regional_capabilities = sub_regional_capabilities.loc[:, cols]
    # Combine flow paths which connect the same two regions.
    sub_regional_capabilities = sub_regional_capabilities.groupby(
        ["flow_path", "node_from", "node_to", "carrier"], as_index=False
    ).sum()
    return sub_regional_capabilities


def _template_regional_interconnectors(
    interconnector_capabilities: pd.DataFrame,
) -> pd.DataFrame:
    """Processes the IASR table 'Interconnector transfer capability' into an
    ISPyPSA template format

    Args:
        interconnector_capabilities: pd.DataFrame IASR table specifying the
            interconnector transfer capabilities between nem regions

    Returns:
        `pd.DataFrame`: ISPyPSA regional flow path template
    """
    from_to_carrier = _get_flow_path_name_from_to_carrier(
        interconnector_capabilities.iloc[:, 0], regional_granularity="nem_regions"
    )
    capability_columns = _clean_capability_column_names(interconnector_capabilities)
    regional_capabilities = pd.concat([from_to_carrier, capability_columns], axis=1)
    regional_capabilities["forward_direction_mw_summer_typical"] = (
        _strip_all_text_after_numeric_value(
            regional_capabilities["forward_direction_mw_summer_typical"]
        )
    )
    regional_capabilities["forward_direction_mw_summer_typical"] = pd.to_numeric(
        regional_capabilities["forward_direction_mw_summer_typical"].str.replace(
            ",", ""
        )
    )
    # Only keep forward_direction_mw_summer_typical limit col as that all that's
    # being used for now.
    cols = [
        "flow_path",
        "node_from",
        "node_to",
        "carrier",
        "forward_direction_mw_summer_typical",
    ]
    regional_capabilities = regional_capabilities.loc[:, cols]
    return regional_capabilities


def _get_flow_path_name_from_to_carrier(
    flow_path_name_series: pd.Series, regional_granularity: str
) -> pd.DataFrame:
    """
    Capture the name, from-node ID, the to-node ID and determines a name
    for a flow path using regular expressions on a string `pandas.Series`
    that contains the flow path name in the forward power flow direction.

    A carrier ('AC' or 'DC') is determined based on whether the flow path descriptor
    is in _HVDC_FLOW_PATHS or goes from TAS to VIC.
    """

    from_to_desc = flow_path_name_series.str.strip().str.extract(
        # capture 2-4 capital letter code that is the from-node
        r"^(?P<node_from>[A-Z]{2,4})"
        # match em or en dashes, or hyphens and soft hyphens surrounded by spaces
        + r"\s*[\u2014\u2013\-\u00ad]+\s*"
        # capture 2-4 captial letter code that is the to-node
        + r"(?P<node_to>[A-Z]{2,4})"
        # capture optional descriptor (e.g. '("Heywood")')
        + r"\s*(?P<descriptor>.*)"
    )
    from_to_desc["flow_path"] = (
        from_to_desc["node_from"] + "-" + from_to_desc["node_to"]
    )
    from_to_desc["carrier"] = "AC"
    return from_to_desc.drop(columns=["descriptor"])


def _determine_flow_path_name(
    node_from: str,
    node_to: str,
    descriptor: str,
    carrier: str,
    regional_granularity: str,
) -> str:
    """
    Constructs flow path name
        - If the carrier is `DC`, looks for the name in `ispypsa.templater.mappings._HVDC_FLOW_PATHS`
        - Else if there is a descriptor, uses a regular expression to extract the name
        - Else constructs a name using typical NEM naming conventing based on `regional_granularity`
            - First letter of `node_from`, first of `node_to` followed by "I" (interconnector)
                if `regional_granularity` is `nem_regions`
            - `<node_from>-<node_to> if `regional_granularity` is `sub_regions`
    """
    if carrier == "DC":
        name = _HVDC_FLOW_PATHS.loc[
            (_HVDC_FLOW_PATHS.node_from == node_from)
            & (_HVDC_FLOW_PATHS.node_to == node_to),
            "flow_path",
        ].iat[0]
    elif descriptor and (
        match := re.search(
            # unicode characters here refer to quotation mark and left/right
            # quotation marks
            r"\(([\w\u0022\u201c\u201d]+)\)",
            descriptor,
        )
    ):
        name = match.group(1).strip('"').lstrip("\u201c").rstrip("\u201d")
    else:
        if regional_granularity == "nem_regions":
            name = node_from[0] + node_to[0] + "I"
        elif regional_granularity == "sub_regions":
            name = node_from + "-" + node_to
    return name


def _clean_capability_column_names(capability_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and simplifies flow path capability column names (e.g. drops references to
    notes)
    """
    capability_columns = []
    for direction in ("Forward direction", "Reverse direction"):
        direction_cols = [
            col for col in capability_df.columns if direction in col and "(MW)" in col
        ]
        for col in direction_cols:
            qualifier = re.search(r".*_([A-Za-z\s]+)$", col).group(1)
            col_name = _snakecase_string(direction + " (MW) " + qualifier)
            capability_columns.append(capability_df[col].rename(col_name))
    return pd.concat(capability_columns, axis=1)


def _template_sub_regional_flow_path_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
) -> pd.DataFrame:
    """
    Process flow path augmentation options and cost forecasts to find the least cost
    options for each flow path, return results in `ISPyPSA` format.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables. Relevant DataFrames
            include:
                - Augmentation tables: columns include 'flow_path', 'option_name',
                 'transfer_increase_forward_direction_mw', and
                 'transfer_increase_reverse_direction_mw'
                - Cost tables: columns include 'flow_path', 'option_name', and
                  financial year columns
                - Preparatory activities: columns include 'flow_path', and financial
                  year columns
                - Actionable projects: columns include 'flow_path', and financial year
                  columns
        scenario: str specifying the scenario name (e.g., "Step Change",
            "Progressive Change").

    Returns:
        pd.DataFrame containing the least cost option for each flow path. Columns:
            - flow_path
            - option_name
            - nominal_flow_limit_increase_mw
            - <financial year>_$/mw (one column per year, e.g., '2024_25_$/mw')
    """
    return process_transmission_costs(
        iasr_tables=iasr_tables,
        scenario=scenario,
        config=_FLOW_PATH_CONFIG,
    )


def _template_rez_transmission_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    possible_rez_or_constraint_names,
) -> pd.DataFrame:
    """
    Process REZ augmentation options and cost forecasts to find least cost options for
    each REZ, return results in `ISPyPSA` format.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables. Relevant DataFrames
            include:
                - Augmentation tables: columns include 'rez_constraint_id', 'option',
                  and 'additional_network_capacity_mw'
                - Cost tables: columns include 'rez_constraint_id', 'option', and
                  columns for each financial year (e.g., '2024-25', '2025-26', ...)
        scenario: str specifying the scenario name (e.g., "Step Change",
            "Progressive Change").
        possible_rez_or_constraint_names: list of possible names that cost data should
            map to. The cost data is known to contain typos so the names in the cost
            data are fuzzy match to the names provided in this input variable.

    Returns:
        pd.DataFrame containing the least cost option for each REZ. Columns:
            - rez_constraint_id
            - option
            - additional_network_capacity_mw
            - <financial year>_$/mw (cost per MW for each year, e.g., '2024_25_$/mw')
    """
    rez_costs = process_transmission_costs(
        iasr_tables=iasr_tables,
        scenario=scenario,
        config=_REZ_CONFIG,
    )

    rez_costs["rez_constraint_id"] = _fuzzy_match_names(
        rez_costs["rez_constraint_id"],
        possible_rez_or_constraint_names,
        task_desc="Processing rez transmission costs",
    )

    return rez_costs


def process_transmission_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    config: dict,
) -> pd.DataFrame:
    """
    Generic function to process transmission costs (flow path or REZ).

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables
        scenario: str specifying the ISP scenario name
        config: dict with processing configuration containing:
            - transmission_type: str, either "flow_path" or "rez"
            - in_coming_column_mappings: dict mapping standard column names to
              rez or flow path specific names
            - table_names: dict with augmentation and cost table lists
            - mappings: dict with mappings for preparatory activities and other data

    Returns:
        pd.DataFrame containing the least cost options with standardized column
        structure
    """
    cost_scenario = _determine_cost_scenario(scenario)

    # Get and process augmentation table
    aug_table = _get_augmentation_table(iasr_tables=iasr_tables, config=config)

    # Get and process cost table
    cost_table = _get_cost_table(
        iasr_tables=iasr_tables, cost_scenario=cost_scenario, config=config
    )

    # Find the least cost options
    final_costs = _get_least_cost_options(
        aug_table=aug_table, cost_table=cost_table, config=config
    )

    return final_costs


def _determine_cost_scenario(scenario: str) -> str:
    """
    Map ISP scenario to flow path/rez cost scenario.

    Args:
        scenario: str specifying the scenario name. Must be one of "Step Change",
        "Green Energy Exports", or "Progressive Change".

    Returns:
        str specifying the internal scenario key (e.g.,
        "step_change_and_green_energy_exports" or "progressive_change").
    """
    if scenario in ["Step Change", "Green Energy Exports"]:
        return "step_change_and_green_energy_exports"
    elif scenario == "Progressive Change":
        return "progressive_change"
    else:
        raise ValueError(f"scenario: {scenario} not recognised.")


def _get_augmentation_table(
    iasr_tables: dict[str, pd.DataFrame], config: dict
) -> pd.DataFrame:
    """
    Concatenate and clean all augmentation tables for a given transmission type.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables. Relevant tables
            must contain columns:
                - id (flow_path or rez_constraint_id)
                - option (option_name or option)
                - capacity (nominal_flow_limit_increase_mw or
                  additional_network_capacity_mw)
        config: dict with processing configuration containing:
            - in_coming_column_mappings: dict mapping standard column names to type-specific names
            - table_names: dict with augmentation table lists

    Returns:
        pd.DataFrame containing the concatenated augmentation table. Columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - capacity (nominal_flow_limit_increase_mw or additional_network_capacity_mw)
    """
    table_names = config["table_names"]["augmentation"]
    missing = [t for t in table_names if t not in iasr_tables]
    if missing:
        logging.warning(f"Missing augmentation tables: {missing}")
    aug_tables = [
        iasr_tables[table_name]
        for table_name in table_names
        if table_name in iasr_tables
    ]
    if not aug_tables:
        raise ValueError(
            f"No {config['transmission_type']} augmentation tables found in iasr_tables."
        )
    aug_table = pd.concat(aug_tables, ignore_index=True)
    aug_table = _clean_augmentation_table_column_names(aug_table, config)
    aug_table = _clean_augmentation_table_column_values(aug_table, config)
    return aug_table


def _get_cost_table(
    iasr_tables: dict[str, pd.DataFrame], cost_scenario: str, config: dict
) -> pd.DataFrame:
    """
    Combine all cost tables, preparatory activities, and actionable projects for a given
    scenario into a single DataFrame.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables. Cost tables must
            have columns:
                - id (flow_path or rez_constraint_id)
                - option (Option Name or Option)
                - <financial year> (e.g., '2024-25', ...)
        flow_path_scenario: str specifying the cost scenario name.
        config: dict with processing configuration containing:
            - transmission_type: str, either "flow_path" or "rez"
            - column_mappings: dict mapping standard column names to rez/flow path names
            - table_names: dict with cost table lists
            - mappings: dict with option name mappings for preparatory activities and
                actionable isp data

    Returns:
        pd.DataFrame containing the combined cost table. Columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - <financial year> (e.g., '2024_25', ...)
    """
    cost_table_names = config["table_names"]["cost"][cost_scenario]
    cost_table = _get_cleaned_cost_tables(iasr_tables, cost_table_names, config)
    prep_activities = _get_prep_activities_table(iasr_tables, cost_scenario, config)
    actionable_projects = _get_actionable_projects_table(
        iasr_tables, cost_scenario, config
    )
    return _combine_cost_tables(cost_table, prep_activities, actionable_projects)


def _get_least_cost_options(
    aug_table: pd.DataFrame, cost_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    For each transmission, select the augmentation option with the lowest cost per MW of
    increased capacity, using the first year with complete costs for all options. The
    selected option and its costs per MW are used for all years.

    Args:
        aug_table: pd.DataFrame containing columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - capacity (nominal_flow_limit_increase_mw or additional_network_capacity_mw)
        cost_table: pd.DataFrame containing columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - <financial year> (e.g., '2024_25', ...)
        config: dict with processing configuration containing:
            - transmission_type: str, either "flow_path" or "rez"
            - in_coming_column_mappings: dict mapping standard column names to
            type-specific names

    Returns:
        pd.DataFrame containing columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - additional_network_capacity_mw
            - <financial year>_$/mw (cost per MW for each year, e.g., '2024_25_$/mw')
    """
    year_cols = _get_year_columns(cost_table)
    valid_costs_df = _find_first_year_with_complete_costs(cost_table, year_cols)
    valid_costs_df["option"] = _fuzzy_match_names(
        valid_costs_df["option"],
        aug_table["option"],
        "matching transmission augmentation options and costs",
        not_match="existing",
        threshold=80,
    )
    transmission_analysis = pd.merge(
        aug_table, valid_costs_df, on=["id", "option"], how="inner"
    )
    _log_unmatched_transmission_options(
        aug_table, valid_costs_df, transmission_analysis
    )
    transmission_analysis["cost_per_mw"] = (
        transmission_analysis["cost"]
        / transmission_analysis["nominal_capacity_increase"]
    )
    least_cost_options = transmission_analysis.loc[
        transmission_analysis.groupby("id")["cost_per_mw"].idxmin()
    ]
    final_costs = pd.merge(
        cost_table,
        least_cost_options[["id", "option", "nominal_capacity_increase"]],
        on=["id", "option"],
        how="inner",
    )
    # Divide each financial year column by capacity and rename with _$/mw suffix
    for year_col in year_cols:
        new_col = f"{year_col}_$/mw"
        final_costs[new_col] = (
            final_costs[year_col] / final_costs["nominal_capacity_increase"]
        )
        final_costs.drop(columns=year_col, inplace=True)
    final_costs = final_costs.rename(columns=config["out_going_column_mappings"])
    return final_costs


def _clean_augmentation_table_column_names(
    aug_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    Clean and rename columns in the augmentation table.

    Args:
        aug_table: pd.DataFrame specifying the augmentation table.
        config: dict with processing configuration containing:
            - in_coming_column_mappings: dict mapping standard column names to type-specific names

    Returns:
        pd.DataFrame containing the cleaned and renamed augmentation table.
    """
    # Map specific columns to standardized names
    # Reverse the in_coming_column_mappings dict to go from specific -> generic
    aug_table = aug_table.rename(columns=config["in_coming_column_mappings"])
    cols_to_keep = list(
        set(
            [
                col
                for col in config["in_coming_column_mappings"].values()
                if col in aug_table.columns
            ]
        )
    )
    return aug_table.loc[:, cols_to_keep]


def _clean_augmentation_table_column_values(
    aug_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    Prepare and typecast augmentation table columns for analysis.

    Args:
        aug_table: pd.DataFrame containing transmission-specific columns
        config: dict with processing configuration containing:
            - transmission_type: str specifying the type of transmission
            - in_coming_column_mappings: dict mapping standard column names to
              flow path/rez names

    Returns:
        pd.DataFrame containing standardized columns:
            - id
            - option
            - nominal_capacity_increase
    """
    transmission_type = config["transmission_type"]

    # Handle flow path special case: calculate capacity as max of forward and reverse
    if transmission_type == "flow_path":
        aug_table["forward_capacity_increase"] = pd.to_numeric(
            _strip_all_text_after_numeric_value(aug_table["forward_capacity_increase"]),
            errors="coerce",
        )
        aug_table["reverse_capacity_increase"] = pd.to_numeric(
            _strip_all_text_after_numeric_value(aug_table["reverse_capacity_increase"]),
            errors="coerce",
        )
        aug_table["nominal_capacity_increase"] = aug_table[
            ["forward_capacity_increase", "reverse_capacity_increase"]
        ].max(axis=1)
    else:
        aug_table["nominal_capacity_increase"] = pd.to_numeric(
            _strip_all_text_after_numeric_value(aug_table["nominal_capacity_increase"]),
            errors="coerce",
        )
    return aug_table


def _get_cleaned_cost_tables(
    iasr_tables: dict[str, pd.DataFrame], cost_table_names: list, config: dict
) -> pd.DataFrame:
    """
    Retrieve, clean, concatenate, and filter all cost tables for a scenario and
    transmission type.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables.
        cost_table_names: list of str specifying the names of cost tables to extract
            and clean.
        config: dict with processing configuration containing:
            - in_coming_column_mappings: dict mapping standard column names to
              flow path / rez names

    Returns:
        pd.DataFrame containing the concatenated and filtered cost tables. Columns:
            - id
            - option
            - <financial year> (e.g., '2024_25', ...)
    """
    missing = [t for t in cost_table_names if t not in iasr_tables]
    if missing:
        logging.warning(f"Missing cost tables: {missing}")
    cost_tables = []
    for table_name in cost_table_names:
        if table_name not in iasr_tables:
            continue
        table = iasr_tables[table_name].copy()
        table = table.rename(columns=config["in_coming_column_mappings"])
        cost_tables.append(table)
    if not cost_tables:
        raise ValueError("No cost tables found in iasr_tables.")
    cost_table = pd.concat(cost_tables, ignore_index=True)
    cost_table.columns = [_snakecase_string(col) for col in cost_table.columns]
    forecast_year_cols = [
        col for col in cost_table.columns if re.match(r"^\d{4}_\d{2}$", col)
    ]
    if not forecast_year_cols:
        raise ValueError("No financial year columns found in cost table")
    cost_table[forecast_year_cols[0]] = pd.to_numeric(
        cost_table[forecast_year_cols[0]], errors="coerce"
    )
    cost_table = cost_table.dropna(subset=forecast_year_cols, how="all")
    return cost_table


def _get_prep_activities_table(
    iasr_tables: dict[str, pd.DataFrame], cost_scenario: str, config: dict
) -> pd.DataFrame:
    """
    Process the preparatory activities table for a given transmission type.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables.
        cost_scenario: str specifying the internal scenario key.
        config: dict with processing configuration containing:
            - mappings: dict with mappings for preparatory activities and other data

    Returns:
        pd.DataFrame containing the aggregated preparatory activities. Columns:
            - id
            - option
            - <financial year> (e.g., '2024_25', '2025_26', ...)
    """
    transmission_type = config["transmission_type"]
    if transmission_type == "flow_path":
        prep_activities_table_name = (
            f"flow_path_augmentation_costs_{cost_scenario}_preparatory_activities"
        )
    else:
        prep_activities_table_name = (
            f"rez_augmentation_costs_{cost_scenario}_preparatory_activities"
        )

    if prep_activities_table_name not in iasr_tables:
        logging.warning(
            f"Missing preparatory activities table: {prep_activities_table_name}"
        )
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["id", "option"])

    prep_activities = iasr_tables[prep_activities_table_name].copy()
    prep_activities = prep_activities.rename(
        columns=config["in_coming_column_mappings"]
    )
    prep_activities.columns = [
        _snakecase_string(col) for col in prep_activities.columns
    ]
    prep_activities = prep_activities.drop(
        columns=[col for col in prep_activities.columns if "unnamed" in col]
    )

    if transmission_type == "flow_path":
        # Flow path preparatory activities processing
        # Validate 'flow_path' values
        invalid_flow_paths = set(prep_activities["id"]) - set(
            config["mappings"]["prep_activities_name_to_option"].keys()
        )
        if invalid_flow_paths:
            raise ValueError(
                f"Missing mapping values for the flow paths provided: {sorted(invalid_flow_paths)}. "
                f"Please ensure these are present in templater/mappings.py."
            )
        prep_activities["option"] = prep_activities["id"].map(
            config["mappings"]["prep_activities_name_to_option"]
        )

        # Validate 'option_name' values
        invalid_option_names = set(prep_activities["option"]) - set(
            config["mappings"]["option_to_id"].keys()
        )
        if invalid_option_names:
            raise ValueError(
                f"Missing mapping values for the option names provided: {sorted(invalid_option_names)}. "
                f"Please ensure these are present in templater/mappings.py."
            )
        prep_activities = prep_activities.groupby("option").sum().reset_index()
        prep_activities["id"] = prep_activities["option"].map(
            config["mappings"]["option_to_id"]
        )

    elif transmission_type == "rez":
        # Validate REZ names/IDs
        invalid_rez_names = set(prep_activities["rez"]) - set(
            config["prep_activities_mapping"].keys()
        )
        if invalid_rez_names:
            raise ValueError(
                f"Missing mapping values for the REZ names provided: {sorted(invalid_rez_names)}. "
                f"Please ensure these are present in templater/mappings.py."
            )

        prep_activities["option"] = prep_activities["rez"].apply(
            lambda x: config["prep_activities_mapping"][x][1]
        )
        prep_activities["id"] = prep_activities["rez"].apply(
            lambda x: config["prep_activities_mapping"][x][0]
        )
    return _sort_cols(prep_activities, ["id", "option"])


def _get_actionable_projects_table(
    iasr_tables: dict[str, pd.DataFrame], cost_scenario: str, config: dict
) -> pd.DataFrame:
    """
    Process the actionable ISP projects table for flow paths.

    Args:
        iasr_tables: dict[str, pd.DataFrame] specifying IASR tables. Table must have
            columns:
                - id (flow_path)
                - <financial year> (e.g., '2024-25', ...)
        cost_scenario: str specifying the internal scenario key.
        config: dict with processing configuration containing:
            - mappings: dict with mappings for actionable projects

    Returns:
        pd.DataFrame containing the actionable projects table. Columns:
            - id (flow_path)
            - option (option_name)
            - <financial year> (e.g., '2024_25', '2025_26', ...)
    """
    transmission_type = config["transmission_type"]

    # REZ has no actionable projects, return empty DataFrame
    if transmission_type == "rez":
        return pd.DataFrame(columns=["id", "option"])

    # Process flow path actionable projects
    actionable_projects_table_name = (
        f"flow_path_augmentation_costs_{cost_scenario}_actionable_isp_projects"
    )

    if actionable_projects_table_name not in iasr_tables:
        logging.warning(
            f"Missing actionable ISP projects table: {actionable_projects_table_name}"
        )
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["id", "option"])

    actionable_projects = iasr_tables[actionable_projects_table_name].copy()
    actionable_projects = actionable_projects.rename(
        columns=config["in_coming_column_mappings"]
    )
    actionable_projects.columns = [
        _snakecase_string(col) for col in actionable_projects.columns
    ]
    actionable_projects = actionable_projects.drop(
        columns=[col for col in actionable_projects.columns if "unnamed" in col]
    )

    # Validate 'flow_path' values
    invalid_flow_paths = set(actionable_projects["id"]) - set(
        config["mappings"]["actionable_name_to_option"].keys()
    )
    if invalid_flow_paths:
        raise ValueError(
            f"Missing mapping values for the flow paths provided: {sorted(invalid_flow_paths)}. "
            f"Please ensure these are present in {config['mappings']['actionable_name_to_option']}."
        )
    actionable_projects["option"] = actionable_projects["id"].map(
        config["mappings"]["actionable_name_to_option"]
    )

    # Validate 'option_name' values
    invalid_option_names = set(actionable_projects["option"]) - set(
        config["mappings"]["actionable_option_to_id"].keys()
    )
    if invalid_option_names:
        raise ValueError(
            f"Missing mapping values for the option names provided: {sorted(invalid_option_names)}. "
            f"Please ensure these are present in {config['mappings']['actionable_option_to_id']}."
        )
    actionable_projects["id"] = actionable_projects["option"].map(
        config["mappings"]["actionable_option_to_id"]
    )

    return _sort_cols(actionable_projects, ["id", "option"])


def _combine_cost_tables(
    cost_table: pd.DataFrame,
    prep_activities: pd.DataFrame,
    actionable_projects: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine the cost table, preparatory activities table, and actionable projects table
    into a single DataFrame.

    Args:
        cost_table: pd.DataFrame specifying the cost table.
        prep_activities: pd.DataFrame specifying the preparatory activities table.
        actionable_projects: pd.DataFrame specifying the actionable projects table.

    Returns:
        pd.DataFrame containing the combined cost table.
    """
    tables = [cost_table, prep_activities]

    # Only include actionable_projects if it's not empty
    if not actionable_projects.empty:
        tables.append(actionable_projects)

    return pd.concat(tables, ignore_index=True)


def _get_year_columns(cost_table: pd.DataFrame) -> list:
    """
    Get the financial year columns from the cost table.

    Args:
        cost_table: pd.DataFrame specifying the cost table.

    Returns:
        list of str specifying the financial year columns.
    """
    year_cols = [col for col in cost_table.columns if re.match(r"\d{4}_\d{2}", col)]
    return year_cols


def _find_first_year_with_complete_costs(
    cost_table: pd.DataFrame, year_cols: list
) -> pd.DataFrame:
    """
    Find the first year with complete costs for each transmission.

    Args:
        cost_table: pd.DataFrame specifying the cost table with columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - <financial year> (e.g., '2024_25', ...)
        year_cols: list of str specifying the financial year column names.

    Returns:
        pd.DataFrame containing columns:
            - id (flow_path or rez_constraint_id)
            - option (option_name or option)
            - cost
            - first_valid_year_col
    """
    valid_cost_rows = []
    missing_full_year_transmissions = []
    for transmission, group in cost_table.groupby("id"):
        found = False
        # Iterate through years (sort years based of first int in year string)
        for year in sorted(year_cols, key=lambda y: int(y.split("_")[0])):
            costs = pd.to_numeric(group[year], errors="coerce")
            if not costs.isna().any():
                for idx, row in group.iterrows():
                    entry = row[["id", "option"]].to_dict()
                    entry["cost"] = costs.loc[idx]
                    entry["first_valid_year_col"] = year
                    valid_cost_rows.append(entry)
                found = True
                break
        if not found:
            missing_full_year_transmissions.append(transmission)
    if missing_full_year_transmissions:
        raise ValueError(
            f"No year found with all non-NA costs for transmissions: {missing_full_year_transmissions}"
        )
    return pd.DataFrame(valid_cost_rows)


def _log_unmatched_transmission_options(
    aug_table: pd.DataFrame, valid_costs_df: pd.DataFrame, merged_df: pd.DataFrame
):
    """
    Logs (id, option) pairs that were dropped from each side during the merge.
    """
    left_keys = set(tuple(x) for x in aug_table[["id", "option"]].values)
    right_keys = set(tuple(x) for x in valid_costs_df[["id", "option"]].values)
    merged_keys = set(tuple(x) for x in merged_df[["id", "option"]].values)

    dropped_from_left = left_keys - merged_keys
    dropped_from_right = right_keys - merged_keys

    if dropped_from_left:
        logging.info(
            f"Dropped options from augmentation table: {sorted(dropped_from_left)}"
        )
    if dropped_from_right:
        logging.info(f"Dropped options from cost table: {sorted(dropped_from_right)}")


def _sort_cols(table: pd.DataFrame, start_cols: list[str]) -> pd.DataFrame:
    """
    Reorder a pd.DataFrame's column using the fixed order provided in start_cols and
    then sorting the remaining columns alphabetically.
    """
    remaining_cols = list(set(table.columns) - set(start_cols))
    sorted_remaining_columns = sorted(remaining_cols)
    return table.loc[:, start_cols + sorted_remaining_columns]
