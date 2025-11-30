# load in all the necessary summary tables:
# - batteries_summary
# - new_entrants_summary
# - additional_projects_summary

# concat them together, filter for just battery only rows (simple)
# cut out the columns we don't need
# add necesary new columns
# merge in the info

# key data to merge in:
# - efficiencies
# - cost related data?
# - lifetime
# - rez_id
# - max_hours (storage duration)


import logging
import re

import pandas as pd

from .helpers import (
    _fuzzy_match_names,
    _one_to_one_priority_based_fuzzy_matching,
    _rez_name_to_id_mapping,
    _snakecase_string,
    _standardise_storage_capitalisation,
    _where_any_substring_appears,
)
from .lists import _ALL_GENERATOR_STORAGE_SUMMARIES, _MINIMUM_REQUIRED_BATTERY_COLUMNS
from .mappings import (
    _ECAA_STORAGE_NEW_COLUMN_MAPPING,
    _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP,
    _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP,
    _NEW_STORAGE_NEW_COLUMN_MAPPING,
)


def _template_battery_properties(
    iasr_tables: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    storage_summaries = []
    for gen_storage_type in _ALL_GENERATOR_STORAGE_SUMMARIES:
        summary_df = iasr_tables[_snakecase_string(gen_storage_type) + "_summary"]
        summary_df.columns = ["Storage Name", *summary_df.columns[1:]]
        storage_summaries.append(summary_df)

    storage_summaries = pd.concat(storage_summaries, axis=0).reset_index(drop=True)

    cleaned_storage_summaries = _clean_storage_summary(storage_summaries)

    # filter to just return "battery" rows for now
    battery_storage_rows = cleaned_storage_summaries["technology_type"].str.contains(
        r"battery", case=False
    )
    # need to add pumped hydro properties to isp-workbook-parser to handle PHES
    pumped_hydro_rows = cleaned_storage_summaries["technology_type"].str.contains(
        r"pumped hydro", case=False
    )

    cleaned_battery_summaries = cleaned_storage_summaries[
        battery_storage_rows
    ].reset_index(drop=True)

    # restructure the battery properties table for easier merging:
    battery_properties = _restructure_battery_property_table(
        iasr_tables["battery_properties"]
    )
    iasr_tables["battery_properties"] = battery_properties

    # Separate out ECAA and new entrants again for some of the merging in of static properties
    ecaa_battery_summary = cleaned_battery_summaries[
        cleaned_battery_summaries["status"].isin(
            ["Existing", "Committed", "Anticipated", "Additional projects"]
        )
    ].copy()
    merged_cleaned_ecaa_battery_summaries = (
        _merge_and_set_ecaa_battery_static_properties(ecaa_battery_summary, iasr_tables)
    )
    ecaa_required_cols = [
        col
        for col in _MINIMUM_REQUIRED_BATTERY_COLUMNS
        if col in merged_cleaned_ecaa_battery_summaries.columns
    ]

    new_entrant_battery_summary = cleaned_battery_summaries[
        cleaned_battery_summaries["status"].isin(["New Entrant"])
    ].copy()
    merged_cleaned_new_entrant_battery_summaries = (
        _merge_and_set_new_battery_static_properties(
            new_entrant_battery_summary, iasr_tables
        )
    )
    new_entrant_required_cols = [
        col
        for col in _MINIMUM_REQUIRED_BATTERY_COLUMNS
        if col in merged_cleaned_new_entrant_battery_summaries.columns
    ]

    return (
        merged_cleaned_ecaa_battery_summaries[ecaa_required_cols],
        merged_cleaned_new_entrant_battery_summaries[new_entrant_required_cols],
    )


def _clean_storage_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the storage summary table

    1. Converts column names to snakecase
    2. Adds "_id" to the end of region/sub-region ID columns
    3. Removes redundant outage columns
    4. Enforces consistent formatting of "storage" str instances

    Args:
        df: Storage summary `pd.DataFrame`

    Returns:
        `pd.DataFrame`: Cleaned storage summary DataFrame
    """

    def _fix_forced_outage_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Removes until/post 2022 distinction in columns if it exists"""
        if (
            any(until_cols := [col for col in df.columns if "until" in col])
            and any(post_cols := [col for col in df.columns if "post" in col])
            and len(until_cols) == len(post_cols)
        ):
            df = df.rename(
                columns={col: col.replace("_until_2022", "") for col in until_cols}
            )
            df = df.drop(columns=post_cols)
        return df

    # clean up column naming
    df.columns = [_snakecase_string(col_name) for col_name in df.columns]
    df = df.rename(
        columns={col: (col + "_id") for col in df.columns if re.search(r"region$", col)}
    )
    df = _fix_forced_outage_columns(df)

    # standardise storage names
    df["storage_name"] = _standardise_storage_capitalisation(df["storage_name"])
    return df


def _merge_and_set_ecaa_battery_static_properties(
    df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
):
    """Merges into and sets static (i.e. not time-varying) storage unit properties in the
    existing storage units template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: Existing storage summary DataFrame
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: Existing storage template with static properties filled in
    """

    # add new columns with existing column mapping:
    for new_column, existing_column_mapping in _ECAA_STORAGE_NEW_COLUMN_MAPPING.items():
        df[new_column] = df[existing_column_mapping]

    merged_static_cols = []
    for col, table_attrs in _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP.items():
        if type(table_attrs["table"]) is list:
            data = [iasr_tables[table] for table in table_attrs["table"]]
            data = pd.concat(data, axis=0)
        else:
            data = iasr_tables[table_attrs["table"]]
        df, col = _merge_table_data(df, col, data, table_attrs)
        merged_static_cols.append(col)

    df = _add_closure_year_column(df, iasr_tables["expected_closure_years"])
    df = _calculate_storage_duration_hours(df)
    df = _add_and_clean_rez_ids(df, "rez_id", iasr_tables["renewable_energy_zones"])
    df = _add_isp_resource_type_column(df)

    # replace remaining string values in static property columns
    df = df.infer_objects()
    for col in [
        col
        for col in merged_static_cols
        if df[col].dtype == "object"
        and "date" not in col  # keep instances of date/datetime strings as strings
    ]:
        df[col] = df[col].apply(lambda x: pd.NA if isinstance(x, str) else x)

    return df


def _merge_and_set_new_battery_static_properties(
    df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges into and sets static (i.e. not time-varying) storage unit properties for new
    entrant storage units template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: New entrant storage summary DataFrame
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: New entrant storage template with static properties filled in
    """
    # add new columns with existing column mapping:
    for new_column, existing_column_mapping in _NEW_STORAGE_NEW_COLUMN_MAPPING.items():
        df[new_column] = df[existing_column_mapping]

    # merge in static properties using the static property mapping
    merged_static_cols = []
    for col, table_attrs in _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP.items():
        # if col is an opex column, use separate function to handle merging in:
        if re.search("^[fv]om_", col):
            data = iasr_tables[table_attrs["table"]]
            df, col = _process_and_merge_opex(df, data, col, table_attrs)
        else:
            if type(table_attrs["table"]) is list:
                data = [iasr_tables[table] for table in table_attrs["table"]]
                data = pd.concat(data, axis=0)
            else:
                data = iasr_tables[table_attrs["table"]]
            df, col = _merge_table_data(df, col, data, table_attrs)
        merged_static_cols.append(col)

    df = _calculate_and_merge_tech_specific_lcfs(
        df, iasr_tables, "technology_specific_lcf_%"
    )
    df = _process_and_merge_connection_cost(df, iasr_tables["connection_costs_other"])
    df = _add_and_clean_rez_ids(df, "rez_id", iasr_tables["renewable_energy_zones"])
    df = _add_isp_resource_type_column(df)
    df = _add_unique_new_entrant_storage_name_column(df)

    # replace remaining string values in static property columns
    df = df.infer_objects()
    for col in [col for col in merged_static_cols if df[col].dtype == "object"]:
        df[col] = df[col].apply(lambda x: pd.NA if isinstance(x, str) else x)
    return df


def _merge_table_data(
    df: pd.DataFrame, col: str, table_data: pd.DataFrame, table_attrs: dict
) -> tuple[pd.DataFrame, str]:
    """Replace values in the provided column of the summary mapping with those
    in the table data using the provided attributes in
    `_STORAGE_STATIC_PROPERTY_TABLE_MAP`
    """
    # handle alternative lookup and value columns
    for alt_attr in ("lookup", "value"):
        if f"alternative_{alt_attr}s" in table_attrs.keys():
            table_col = table_attrs[f"table_{alt_attr}"]
            for alt_col in table_attrs[f"alternative_{alt_attr}s"]:
                table_data[table_col] = table_data[table_col].where(
                    pd.notna, table_data[alt_col]
                )
    replacement_dict = (
        table_data.loc[:, [table_attrs["table_lookup"], table_attrs["table_value"]]]
        .set_index(table_attrs["table_lookup"])
        .squeeze()
        .to_dict()
    )
    # handles slight difference in capitalisation
    where_str = df[col].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col] = _fuzzy_match_names(
        df.loc[where_str, col],
        replacement_dict.keys(),
        f"merging in the storage static property {col}",
        not_match="existing",
        threshold=90,
    )
    df[col] = df[col].replace(replacement_dict)
    if "new_col_name" in table_attrs.keys():
        df = df.rename(columns={col: table_attrs["new_col_name"]})
        col = table_attrs["new_col_name"]
    return df, col


def _process_and_merge_opex(
    df: pd.DataFrame,
    table_data: pd.DataFrame,
    col_name: str,
    table_attrs: dict,
) -> tuple[pd.DataFrame, str]:
    """Processes and merges in fixed or variable OPEX values for new entrant storage.

    In v6.0 of the IASR workbook the base values for all OPEX are found in
    the column "NSW Low" or the relevant table, all other values are calculated
    from this base value multiplied by the O&M locational cost factor. This function
    merges in the post-LCF calculated values provided in the IASR workbook.
    """
    # update the mapping in this column to include storage name and the cost region initially given
    df[col_name] = df["storage_name"] + " " + df[col_name]
    table_data = table_data.rename(
        columns={
            col: col.replace(f"{table_attrs['table_col_prefix']}_", "")
            for col in table_data.columns
        }
    )
    opex_table = table_data.melt(
        id_vars=[table_attrs["table_lookup"]],
        var_name="cost_region",
        value_name="opex_value",
    )
    # add column with same storage + cost_region mapping as df[col_name]:
    opex_table["mapping"] = (
        opex_table[table_attrs["table_lookup"]] + " " + opex_table["cost_region"]
    )
    opex_replacement_dict = (
        opex_table[["mapping", "opex_value"]].set_index("mapping").squeeze().to_dict()
    )
    # use fuzzy matching in case of slight differences in storage names:
    where_str = df[col_name].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col_name] = _fuzzy_match_names(
        df.loc[where_str, col_name],
        opex_replacement_dict.keys(),
        f"merging in the new entrant storage static property {col_name}",
        not_match="existing",
        threshold=90,
    )
    df[col_name] = df[col_name].replace(opex_replacement_dict)
    return df, col_name


def _add_closure_year_column(
    df: pd.DataFrame, closure_years: pd.DataFrame
) -> pd.DataFrame:
    """Adds a column containing the expected closure year (calendar year) for ECAA storage units.

    Note: the IASR table specifies the expected closure years as calendar years, without
    giving more detail about the expected closure month elsewhere. For now, this function
    also makes the OPINIONATED choice to just return the year given by the table as the
    closure year.

    Args:
        df: `ISPyPSA` formatted pd.DataFrame detailing the ECAA storage units.
        closure_years: pd.Dataframe containing the IASR table `expected_generator_closure_years`
            for the ECAA storage units, by unit. Expects that closure years are given as integers.

    Returns:
        `pd.DataFrame`: ECAA storage attributes table with additional 'closure_year' column.
    """

    default_closure_year = -1

    # reformat closure_years table for clearer mapping:
    closure_years.columns = [_snakecase_string(col) for col in closure_years.columns]
    closure_years = closure_years.rename(
        columns={
            "generator_name": "storage_name",
            "expected_closure_year_calendar_year": "closure_year",
        }
    )

    # process closure_years to get the earliest expected closure year for each storage:
    closure_years = (
        closure_years.sort_values("closure_year", ascending=True)
        .drop_duplicates(subset="storage_name", keep="first")
        .dropna(subset="closure_year")
    )
    closure_years_dict = closure_years.set_index("storage_name")[
        "closure_year"
    ].to_dict()

    where_str = df["closure_year"].apply(lambda x: isinstance(x, str))
    df.loc[where_str, "closure_year"] = _fuzzy_match_names(
        df.loc[where_str, "closure_year"],
        closure_years_dict.keys(),
        f"adding closure_year column to ecaa_storage_summary table",
        not_match="existing",
        threshold=85,
    )
    # map rather than replace to pass default value for undefined closure years:
    df["closure_year"] = df["closure_year"].map(
        lambda closure_year: closure_years_dict.get(closure_year, default_closure_year)
    )

    return df


def _process_and_merge_connection_cost(
    df: pd.DataFrame, connection_costs_table: pd.DataFrame
) -> pd.DataFrame:
    """Process and merge connection cost data from IASR tables to new entrant storage template.

    The function processes the connection cost data by creating a new column
    "connection_cost_$/mw" and mapping the connection cost technology to the
    corresponding connection cost values in $/MW from the IASR tables.

    Args:
        df (pd.DataFrame): dataframe containing `ISPyPSA` formatted new entrant storage summary
            table.
        connection_costs_table (pd.DataFrame): parsed IASR table containing non-VRE connection costs.

    Returns:
        pd.DataFrame: dataframe containing `ISPyPSA` formatted new entrant storage summary table
            with connection costs in $/MW merged in as new column named "connection_cost_$/mw".
    """
    df["connection_cost_$/mw"] = (
        df["connection_cost_rez/_region_id"] + "_" + df["connection_cost_technology"]
    )
    for col in connection_costs_table.columns[1:]:
        connection_costs_table[col] *= 1000  # convert to $/mw

    connection_costs = (
        connection_costs_table.melt(
            id_vars=["Region"],
            var_name="connection_cost_technology",
            value_name="connection_cost_$/mw",
        )
        .fillna(0.0)
        .reset_index()
    )

    battery_connection_costs = connection_costs[
        connection_costs["connection_cost_technology"].str.contains("Battery")
    ].copy()

    battery_connection_costs["region_technology_mapping"] = (
        battery_connection_costs["Region"]
        + "_"
        + battery_connection_costs["connection_cost_technology"]
    )
    battery_connection_costs_mapping = (
        battery_connection_costs[["region_technology_mapping", "connection_cost_$/mw"]]
        .set_index("region_technology_mapping")
        .squeeze()
        .to_dict()
    )

    where_str = df["connection_cost_$/mw"].apply(lambda x: isinstance(x, str))
    df.loc[where_str, "connection_cost_$/mw"] = _fuzzy_match_names(
        df.loc[where_str, "connection_cost_$/mw"],
        battery_connection_costs_mapping.keys(),
        "merging in the new entrant battery static property 'connection_cost_$/mw'",
        not_match="existing",
        threshold=90,
    )

    df["connection_cost_$/mw"] = df["connection_cost_$/mw"].replace(
        battery_connection_costs_mapping
    )
    missing_connection_costs = df["connection_cost_$/mw"].map(
        lambda x: not isinstance(x, (float, int))
    )
    batteries_missing_connection_costs = df.loc[missing_connection_costs]
    if not batteries_missing_connection_costs.empty:
        raise ValueError(
            f"Missing connection costs for the following batteries: {batteries_missing_connection_costs['storage_name'].unique()}"
        )

    return df


def _calculate_and_merge_tech_specific_lcfs(
    df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame], tech_lcf_col: str
) -> pd.DataFrame:
    """Calculates the technology-specific locational cost factor as a percentage
    for each new entrant storage unit and merges into summary mapping table.
    """
    # loads in the three tables needed
    breakdown_ratios = iasr_tables["technology_cost_breakdown_ratios"].reset_index()
    breakdown_ratios = breakdown_ratios.loc[
        _where_any_substring_appears(breakdown_ratios["Technology"], ["battery"])
    ].copy()
    technology_specific_lcfs = iasr_tables["technology_specific_lcfs"]
    # loads all cols unless the str "O&M" is in col name
    locational_cost_factors = iasr_tables["locational_cost_factors"]
    locational_cost_factors = locational_cost_factors.set_index(
        locational_cost_factors.columns[0]
    )
    cols = [col for col in locational_cost_factors.columns if "O&M" not in col]
    locational_cost_factors = locational_cost_factors.loc[:, cols]

    # reshape technology_specific_lcfs and name columns manually:
    technology_specific_lcfs = (
        technology_specific_lcfs.melt(
            id_vars="Cost zones / Sub-region", value_name="LCF", var_name="Technology"
        )
        .dropna(axis=0, how="any")
        .reset_index(drop=True)
    )
    technology_specific_lcfs = technology_specific_lcfs.loc[
        _where_any_substring_appears(
            technology_specific_lcfs["Technology"], ["battery"]
        )
    ].copy()
    technology_specific_lcfs.rename(
        columns={"Cost zones / Sub-region": "Location"}, inplace=True
    )
    # ensures storage names in LCF tables match those in the summary table
    for df_to_match_gen_names in [technology_specific_lcfs, breakdown_ratios]:
        df_to_match_gen_names["Technology"] = _fuzzy_match_names(
            df_to_match_gen_names["Technology"],
            df["storage_name"].unique(),
            "calculating and merging in LCFs to static new entrant storage summary",
            not_match="existing",
            threshold=90,
        )
        df_to_match_gen_names.set_index("Technology", inplace=True)
    # use fuzzy matching to ensure that col names in tables to combine match up:
    fuzzy_column_renaming = _one_to_one_priority_based_fuzzy_matching(
        set(locational_cost_factors.columns.to_list()),
        set(breakdown_ratios.columns.to_list()),
        not_match="existing",
        threshold=90,
    )
    locational_cost_factors.rename(columns=fuzzy_column_renaming, inplace=True)
    # loops over rows to calculate LCF for batteries:
    for tech, row in technology_specific_lcfs.iterrows():
        calculated_lcf = breakdown_ratios.loc[tech, :].dot(
            locational_cost_factors.loc[row["Location"], :]
        )
        calculated_lcf /= 100
        df.loc[
            ((df["storage_name"] == tech) & (df[tech_lcf_col] == row["Location"])),
            tech_lcf_col,
        ] = calculated_lcf
    # fills rows with no LCF with pd.NA
    df[tech_lcf_col] = df[tech_lcf_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _calculate_storage_duration_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the storage duration (in hours) for each storage unit in the ECAA
    storage attributes table.

    This function uses the `maximum_capacity_mw` and `energy_capacity_mwh` columns
    in the summary table to calculate the storage duration in hours as
    `energy_capacity_mwh`/`maximum_capacity_mw`.

    Requires that the columns `maximum_capacity_mw` and `energy_capacity_mwh` are
    present and not NaN. If either of these columns is NaNfor a particular storage unit,
    a warning will be logged and that storage unit will be dropped from the table.

    Returns:
        pd.DataFrame: ECAA storage attributes table with the additional `storage_duration_hours` column.
    """

    def _safe_calculate_storage_duration_hours(row):
        if pd.isna(row["maximum_capacity_mw"]) or pd.isna(row["energy_capacity_mwh"]):
            logging.warning(
                f"Could not calculate storage_duration_hours for {row['storage_name']} "
                + "due to missing maximum_capacity_mw or energy_capacity_mwh value."
                + "This storage unit will not be considered in the model."
            )
            return pd.NA
        elif row["maximum_capacity_mw"] == 0:
            return 0
        else:
            return row["energy_capacity_mwh"] / row["maximum_capacity_mw"]

    df["energy_capacity_mwh"] = pd.to_numeric(
        df["energy_capacity_mwh"], errors="coerce"
    )
    df["maximum_capacity_mw"] = pd.to_numeric(
        df["maximum_capacity_mw"], errors="coerce"
    )

    df["storage_duration_hours"] = df.apply(
        _safe_calculate_storage_duration_hours, axis=1
    )
    # drop rows with missing storage_duration_hours - these have been logged.
    df = df.dropna(subset=["storage_duration_hours"], ignore_index=True)

    return df


def _restructure_battery_property_table(
    battery_property_table: pd.DataFrame,
) -> pd.DataFrame:
    """Restructures the IASR battery property table into a more usable format.

    The output table will have columns "storage_name" and the battery property names as
    columns, with the values of those properties as the values in the table (converted to
    numeric values where possible). Rows match storage names/mappings in the summary tables.

    Args:
        battery_property_table: pd.DataFrame, `battery_properties` table from the IASR workbook.

    Returns:
        pd.DataFrame, restructured battery property table.
    """
    battery_properties = battery_property_table.set_index("Property")
    battery_properties = battery_properties.T.reset_index(names="storage_name")

    battery_properties.columns.name = None

    columns_to_make_numeric = [
        col for col in battery_properties.columns if col != "storage_name"
    ]
    for col in columns_to_make_numeric:
        battery_properties[col] = pd.to_numeric(
            battery_properties[col], errors="coerce"
        )

    return battery_properties


def _add_and_clean_rez_ids(
    df: pd.DataFrame, rez_id_col_name: str, renewable_energy_zones: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges REZ IDs into the new entrant storage table and cleans up REZ names.

    REZ IDs are unique letter/digit identifiers used in the IASR workbook. This function
    also handles the Non-REZ IDs for Victoria (V0) and New South Wales (N0). There are
    also some manual mappings to correct REZ names that have been updated/changed
    across tables (currently in the IASR workbook v6.0): 'North [East/West] Tasmania Coast'
    becomes 'North Tasmania Coast', 'Portland Coast' becomes 'Southern Ocean'.

    Args:
        df: new entrant storage DataFrame
        rez_id_col_name: str, name of the new column to be added.
        renewable_energy_zones: a pd.Dataframe of the IASR table `renewable_energy_zones`
            containing columns "ID" and "Name" used to map the REZ IDs.

    Returns:
        pd.DataFrame: new entrant storage DataFrame with REZ ID column added.
    """

    # add a new column to hold the REZ IDs that maps to the current rez_location:
    df[rez_id_col_name] = df["rez_location"]

    # update references to "North [East|West] Tasmania Coast" to "North Tasmania Coast"
    # update references to "Portland Coast" to "Southern Ocean"
    rez_or_region_cols = [col for col in df.columns if re.search(r"rez|region_id", col)]

    for col in rez_or_region_cols:
        df[col] = _rez_name_to_id_mapping(df[col], col, renewable_energy_zones)

    return df


def _add_isp_resource_type_column(df: pd.DataFrame):
    """Maps the 'isp_resource_type' column in the combined storage units template table
    to a new column that holds a string describing the resource type.

    Uses a regular expression to extract the storage duration in hours from the
    'isp_resource_type' string, at the moment only battery storage is handled here so
    the resulting string becomes 'Battery Storage {duration}h'.

    Args:
        df: pd.DataFrame, storage units template table

    Returns:
        pd.DataFrame: storage units template table with a new column
            'isp_resource_type' that holds the descriptive string.
    """

    def _get_storage_duration_for_battery_type(name: str) -> str | None:
        duration_pattern = r"(?P<duration>\d+h)rs* storage"
        duration_string = re.search(duration_pattern, name, re.IGNORECASE)

        if duration_string:
            return "Battery Storage " + duration_string.group("duration")
        else:
            return None

    df["isp_resource_type"] = df["isp_resource_type"].map(
        _get_storage_duration_for_battery_type
    )

    return df


def _add_unique_new_entrant_storage_name_column(df: pd.DataFrame):
    """Adds a new column to the new entrant storage units template table to hold a unique
    identifier for each storage unit.

    New entrant storage are not defined for each REZ, with sub-regions being the most
    granular regional grouping as of IASR workbook v6.0.

    Args:
        df: pd.DataFrame, new entrant storage units template table

    Returns:
        pd.DataFrame: new entrant storage units template table with the "storage_name" column
            filled by a unique identifier string for each row.
    """

    df["storage_name"] = df["isp_resource_type"] + "_" + df["sub_region_id"]
    df["storage_name"] = df["storage_name"].map(_snakecase_string)

    return df
