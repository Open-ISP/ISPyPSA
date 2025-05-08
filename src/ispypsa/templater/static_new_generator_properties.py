import logging
import re
from pathlib import Path

import pandas as pd

from .helpers import (
    _fuzzy_match_names,
    _one_to_one_priority_based_fuzzy_matching,
    _snakecase_string,
    _where_any_substring_appears,
)
from .lists import _NEW_GENERATOR_TYPES
from .mappings import (
    _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP,
    _VRE_RESOURCE_QUALITY_AND_TECH_CODES,
)

_OBSOLETE_COLUMNS = [
    "Maximum capacity factor (%)",
]


def _template_new_generators_static_properties(
    iasr_tables: dict[pd.DataFrame],
) -> pd.DataFrame:
    """Processes the new entrant generators summary tables into an ISPyPSA
    template format

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA new entrant generators template
    """
    logging.info("Creating a new entrant generators template")

    new_generator_summaries = []
    for gen_type in _NEW_GENERATOR_TYPES:
        df = iasr_tables[_snakecase_string(gen_type) + "_summary"]
        df.columns = ["Generator Name", *df.columns[1:]]
        new_generator_summaries.append(df)
    new_generator_summaries = pd.concat(new_generator_summaries, axis=0).reset_index(
        drop=True
    )
    cleaned_new_generator_summaries = _clean_generator_summary(new_generator_summaries)
    # drop any battery storage
    cleaned_new_generator_summaries = cleaned_new_generator_summaries.loc[
        ~cleaned_new_generator_summaries["technology_type"].str.contains("Battery"),
        :,
    ].reset_index(drop=True)
    merged_cleaned_new_generator_summaries = (
        _merge_and_set_new_generators_static_properties(
            cleaned_new_generator_summaries, iasr_tables
        )
    )
    return merged_cleaned_new_generator_summaries


def _clean_generator_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans generator summary tables

    1. Converts column names to snakecase
    2. Adds "_id" to the end of region/sub-region ID columns
    3. Removes redundant outage columns
    4. Enforces consistent formatting of "storage" str instances
    4. Adds the following columns with appropriate mappings:
            - `partial_outage_derating_factor_%`
            - `maximum_capacity_mw`
            - `lifetime`
            - `summer_peak_rating_%`
            - `technology_specific_lcf_%`
            - `minimum_stable_level_%`

    Args:
        df: Generator summary `pd.DataFrame`

    Returns:
        `pd.DataFrame`: Cleaned generator summary DataFrame
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

    df = df.drop(columns=_OBSOLETE_COLUMNS)
    df.columns = [_snakecase_string(col_name) for col_name in df.columns]
    df = df.rename(
        columns={col: (col + "_id") for col in df.columns if re.search(r"region$", col)}
    )
    # enforces capitalisation structure for instances of str "storage" in generator_name col
    df["generator_name"] = df["generator_name"].replace(
        [r"s[a-z]{6}\s", r"S[a-z]{6}\)"], [r"Storage ", r"storage)"], regex=True
    )
    df = _fix_forced_outage_columns(df)

    # Drop rows that contain new entrants for the Illawarra REZ (N12) - these
    # don't have any trace data from AEMO currently and "No VRE is projected for this REZ."
    # (AEMO 2024 | Appendix 3. Renewable Energy Zones, p.38)
    df = df.loc[~(df["rez_location"] == "Illawarra"), :]

    # adds extra necessary columns taking appropriate mapping values
    # NOTE: this could be done more efficiently in future if needed, potentially
    # adding a `new_mapping` field to relevant table map dicts?
    df["partial_outage_derating_factor_%"] = df[
        "forced_outage_rate_partial_outage_%_of_time"
    ]
    df["maximum_capacity_mw"] = df["generator_name"]
    df["unit_capacity_mw"] = df["generator_name"]
    df["lifetime"] = df["generator_name"]
    df["minimum_stable_level_%"] = df["technology_type"]
    df["summer_peak_rating_%"] = df["summer_rating_mw"]
    df["technology_specific_lcf_%"] = df["regional_build_cost_zone"]
    return df


def _merge_and_set_new_generators_static_properties(
    df: pd.DataFrame, iasr_tables: dict[str : pd.DataFrame]
) -> pd.DataFrame:
    """Merges into and sets static (i.e. not time-varying) generator properties in the
    "New entrants summary" template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: Existing generator summary DataFrame
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: Existing generator template with static properties filled in
    """
    # merge in static properties using the static property mapping
    merged_static_cols = []
    for col, table_attrs in _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP.items():
        # if col is an opex column, use separate function to handle merging in:
        if re.search("^[fv]om_", col):
            data = iasr_tables[table_attrs["table"]]
            df, col = _process_and_merge_opex(df, data, col, table_attrs)
        else:
            if type(table_attrs["table"]) is list:
                data = [
                    iasr_tables[table_attrs["table"]] for table in table_attrs["table"]
                ]
                data = pd.concat(data, axis=0)
            else:
                data = iasr_tables[table_attrs["table"]]
            df, col = _merge_table_data(df, col, data, table_attrs)
        merged_static_cols.append(col)

    gpg_min_stable_level_new_entrants = iasr_tables["gpg_min_stable_level_new_entrants"]
    df = _process_and_merge_new_gpg_min_stable_lvl(
        df, gpg_min_stable_level_new_entrants, "minimum_stable_level_%"
    )
    df = _calculate_and_merge_tech_specific_lcfs(
        df, iasr_tables, "technology_specific_lcf_%"
    )
    df = _zero_renewable_heat_rates(df, "heat_rate_gj/mwh")
    df = _zero_solar_wind_battery_partial_outage_derating_factor(
        df, "partial_outage_derating_factor_%"
    )
    df = _add_technology_rez_subregion_column(df, iasr_tables, "generator")
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
    `_NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
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
    # handles slight difference in capitalisation e.g. Bogong/Mackay vs Bogong/MacKay
    where_str = df[col].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col] = _fuzzy_match_names(
        df.loc[where_str, col],
        replacement_dict.keys(),
        f"merging in the new entrant generator static property {col}",
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
    """Processes and merges in fixed or variable OPEX values for new entrant generators.

    In v6.0 of the IASR workbook the base values for all OPEX are found in
    the column "NSW Low" or the relevant table, all other values are calculated
    from this base value multiplied by the O&M locational cost factor. This function
    merges in the post-LCF calculated values provided in the IASR workbook.
    """
    # update the mapping in this column to include generator name and the
    # cost region initially given
    df[col_name] = df["generator_name"] + " " + df[col_name]
    # renames columns by removing the specified table_col_prefix (the string present
    # at the start of all variable col names due to row merging from isp-workbook-parser)
    table_data = table_data.rename(
        columns={
            col: col.replace(f"{table_attrs['table_col_prefix']}_", "")
            for col in table_data.columns
        }
    )
    opex_table = table_data.melt(
        id_vars=[table_attrs["table_lookup"]],
        var_name="Cost region",
        value_name="OPEX value",
    )
    # add column with same generator + cost region mapping as df[col_name]:
    opex_table["Mapping"] = (
        opex_table[table_attrs["table_lookup"]] + " " + opex_table["Cost region"]
    )
    opex_replacement_dict = (
        opex_table[["Mapping", "OPEX value"]].set_index("Mapping").squeeze().to_dict()
    )
    # use fuzzy matching in case of slight differences in generator names:
    where_str = df[col_name].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col_name] = _fuzzy_match_names(
        df.loc[where_str, col_name],
        opex_replacement_dict.keys(),
        f"merging in the new entrant generator static property {col_name}",
        not_match="existing",
        threshold=90,
    )
    df[col_name] = df[col_name].replace(opex_replacement_dict)
    return df, col_name


def _calculate_and_merge_tech_specific_lcfs(
    df: pd.DataFrame, iasr_tables: dict[str : pd.DataFrame], tech_lcf_col: str
) -> pd.DataFrame:
    """Calculates the technology-specific locational cost factor as a percentage
    for each new entrant generator and merges into summary mapping table.
    """
    # loads in the three tables needed
    breakdown_ratios = iasr_tables["technology_cost_breakdown_ratios"]
    technology_specific_lcfs = iasr_tables["technology_specific_lcfs"]
    # loads all cols unless the str "O&M" is in col name
    locational_cost_factors = iasr_tables["locational_cost_factors"]
    locational_cost_factors = locational_cost_factors.set_index(
        locational_cost_factors.columns[0]
    )
    cols = [col for col in locational_cost_factors.columns if "O&M" not in col]
    locational_cost_factors = locational_cost_factors.loc[:, cols]

    # reshape technology_specific_lcfs and name columns manually:
    technology_specific_lcfs = technology_specific_lcfs.melt(
        id_vars="Cost zones / Sub-region", value_name="LCF", var_name="Technology"
    ).dropna(axis=0, how="any")
    technology_specific_lcfs.rename(
        columns={"Cost zones / Sub-region": "Location"}, inplace=True
    )
    # ensures generator names in LCF tables match those in the summary table
    for df_to_match_gen_names in [technology_specific_lcfs, breakdown_ratios]:
        df_to_match_gen_names["Technology"] = _fuzzy_match_names(
            df_to_match_gen_names["Technology"],
            df["generator_name"].unique(),
            "calculating and merging in LCFs to static new entrant gen summary",
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
    # loops over rows and use existing LCF for all pumped hydro gens, calculates for others
    # values are all converted to a percentage as needed
    for tech, row in technology_specific_lcfs.iterrows():
        if re.search(r"^(Pump|BOTN)", tech):
            calculated_or_given_lcf = row["LCF"] * 100
        else:
            calculated_or_given_lcf = breakdown_ratios.loc[tech, :].dot(
                locational_cost_factors.loc[row["Location"], :]
            )
            calculated_or_given_lcf /= 100
        df.loc[
            ((df["generator_name"] == tech) & (df[tech_lcf_col] == row["Location"])),
            tech_lcf_col,
        ] = calculated_or_given_lcf
    # fills rows with no LCF (some PHES REZs) with pd.NA
    df[tech_lcf_col] = df[tech_lcf_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _process_and_merge_new_gpg_min_stable_lvl(
    df: pd.DataFrame, new_gpg_min_stable_lvls: pd.DataFrame, min_level_col: str
) -> pd.DataFrame:
    """Processes and merges in gas-fired generation minimum stable level data (%)

    Minimum stable level is given as a percentage of nameplate capacity, and set
    to zero for renewable generators (wind, solar, hydro), storage, OCGT, and
    hydrogen reciprocating engines.

    NOTE: v6 IASR workbook does not specify a minimum stable level for hydrogen
    reciprocating engines.
    """
    new_gpg_min_stable_lvls = new_gpg_min_stable_lvls.set_index("Technology")
    # manually maps percentages to the new min stable level column
    for tech, row in new_gpg_min_stable_lvls.iterrows():
        df.loc[df["technology_type"] == tech, min_level_col] = row[
            "Min Stable Level (% of nameplate)"
        ]
    # fills renewable generators, storage, hydrogen reciprocating engines and OCGT with 0.0
    df.loc[
        _where_any_substring_appears(
            df[min_level_col],
            ["solar", "wind", "pumped hydro", "battery", "ocgt", "hydrogen"],
        ),
        min_level_col,
    ] = 0.0
    # replace any remaining cells containing str (tech type) with pd.NA
    df[min_level_col] = df[min_level_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _zero_renewable_heat_rates(df: pd.DataFrame, heat_rate_col: str) -> pd.DataFrame:
    """
    Fill any empty heat rate values with the technology type, and then set
    renewable energy (solar, solar thermal, wind, hydro) and battery storage
    heat rates to 0.0. Ensure "pumped hydro" used (not just "hydro") to avoid
    including hydrogen reciprocating engines.
    """
    df[heat_rate_col] = df[heat_rate_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(
            df[heat_rate_col], ["solar", "wind", "pumped hydro", "battery"]
        ),
        heat_rate_col,
    ] = 0.0
    return df


def _zero_solar_wind_battery_partial_outage_derating_factor(
    df: pd.DataFrame, po_derating_col: str
) -> pd.DataFrame:
    """
    Fill any empty partial outage derating factor values with the technology type, and
    then set values for solar, wind and batteries to 0
    """
    df[po_derating_col] = df[po_derating_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(df[po_derating_col], ["solar", "wind", "battery"]),
        po_derating_col,
    ] = 0.0
    return df


def _add_technology_rez_subregion_column(
    df: pd.DataFrame, iasr_tables: dict[str : pd.DataFrame], new_col_name: str
) -> pd.DataFrame:
    """Adds an extra column holding the technology type and either REZ or ISP
    subregion ID."""

    # update references to "North [East|West] Tasmania Coast" to "North Tasmania Coast"
    # update references to "Portland Coast" to "Southern Ocean"
    rez_or_region_cols = [col for col in df.columns if "rez" in col or "region" in col]
    df[rez_or_region_cols] = df[rez_or_region_cols].replace(
        {
            r".+Tasmania Coast": "North Tasmania Coast",
            r"Portland Coast": "Southern Ocean",
        },
        regex=True,
    )
    # adds new column filled with REZ names (inc. updated references)
    df[new_col_name] = df["rez_location"]

    rez_id_table = iasr_tables["renewable_energy_zones"]
    replacement_dict = (
        rez_id_table.loc[:, ["Name", "ID"]].set_index("Name").squeeze().to_dict()
    )
    # add Non-REZ option IDs for Victoria Non-REZ (V0) and New South Wales Non-REZ (N0)
    non_rez_ids = {"New South Wales Non-REZ": "N0", "Victoria Non-REZ": "V0"}
    replacement_dict.update(non_rez_ids)

    where_str = df[new_col_name].apply(lambda x: isinstance(x, str))
    df.loc[where_str, new_col_name] = _fuzzy_match_names(
        df.loc[where_str, new_col_name],
        replacement_dict.keys(),
        f"adding unique id column for new entrant generators called {new_col_name}",
        not_match="existing",
        threshold=90,
    )
    df[new_col_name] = df[new_col_name].replace(replacement_dict)
    # drop rows with REZ N12 (Build limit for VRE == 0.0MW)
    # TODO: incorporate this into build limit filtering?
    df = df.loc[df[new_col_name] != "N12"]

    # add a col with resource quality/technology code:
    df["temporary_quality_col"] = df["generator_name"]
    df["temporary_quality_col"] = df["temporary_quality_col"].map(
        _VRE_RESOURCE_QUALITY_AND_TECH_CODES
    )
    # expand to account for fixed and floating offshore wind resources:
    df = df.explode("temporary_quality_col").reset_index(drop=True)
    # apply different naming convention for rez and non-rez new entrants:
    df.loc[
        _where_any_substring_appears(df["technology_type"], ["solar", "wind"]),
        new_col_name,
    ] = (
        df["technology_type"]
        + "_"
        + df[new_col_name]
        + "_"
        + df["temporary_quality_col"]
    )

    df[new_col_name] = df[new_col_name].where(
        df[new_col_name].notna(), df["generator_name"] + "_" + df["sub_region_id"]
    )
    return df.drop(columns=["temporary_quality_col"]).drop_duplicates(
        subset=[new_col_name], ignore_index=True
    )
