import logging
import re
from pathlib import Path

import ipdb  ### delete this after testing and finalising this script!
import pandas as pd

from .helpers import (
    _fuzzy_match_names,
    _snakecase_string,
    _where_any_substring_appears,
)
from .lists import _NEW_GENERATOR_TYPES
from .mappings import _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP

_OBSOLETE_COLUMNS = [
    "Maximum capacity factor (%)",
]


def template_new_generators_static_properties(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the new entrant generators summary tables into an ISPyPSA
    template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: ISPyPSA new entrant generators template
    """
    logging.info("Creating a new entrant generators template")

    new_generator_summaries = []
    for gen_type in _NEW_GENERATOR_TYPES:
        df = pd.read_csv(
            Path(parsed_workbook_path, _snakecase_string(gen_type) + "_summary.csv")
        )
        df.columns = ["Generator", *df.columns[1:]]
        new_generator_summaries.append(df)
    new_generator_summaries = pd.concat(new_generator_summaries, axis=0).reset_index(
        drop=True
    )
    cleaned_new_generator_summaries = _clean_generator_summary(new_generator_summaries)
    # drop any energy storage
    # NOTE should this inclue solar thermal (new entrants)?
    cleaned_new_generator_summaries = cleaned_new_generator_summaries.loc[
        ~cleaned_new_generator_summaries["technology_type"].str.contains("Battery"),
        :,
    ].reset_index(drop=True)
    merged_cleaned_new_generator_summaries = (
        _merge_and_set_new_generators_static_properties(
            cleaned_new_generator_summaries, parsed_workbook_path
        )
    )
    return merged_cleaned_new_generator_summaries.set_index("generator")


def _clean_generator_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans generator summary tables

    1. Converts column names to snakecase
    2. Adds "_id" to the end of region/sub-region ID columns
    3. Removes redundant outage columns
    4. Adds partial outage derating factor column

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
    df = _fix_forced_outage_columns(df)
    # adds a partial derating factor column that takes partial outage rate mappings
    df["partial_outage_derating_factor_%"] = df[
        "forced_outage_rate_partial_outage_%_of_time"
    ]
    return df


def _merge_and_set_new_generators_static_properties(
    df: pd.DataFrame, parsed_workbook_path: Path | str
) -> pd.DataFrame:
    """Merges into and sets static (i.e. not time-varying) generator properties in the
    "New entrants summary" template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: Existing generator summary DataFrame
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: Existing generator template with static properties filled in
    """
    # adds a max capacity column that takes the existing generator name mapping
    df["maximum_capacity_mw"] = df["generator"]
    df["generator_build_cost_zone"] = df["generator"].str.cat(
        df["regional_build_cost_zone"], sep=": "
    )
    # merge in static properties using the static property mapping
    merged_static_cols = []
    for col, csv_attrs in _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP.items():
        if type(csv_attrs["csv"]) is list:
            data = [
                pd.read_csv(Path(parsed_workbook_path, csv + ".csv"))
                for csv in csv_attrs["csv"]
            ]
            data = pd.concat(data, axis=0)
        else:
            data = pd.read_csv(Path(parsed_workbook_path, csv_attrs["csv"] + ".csv"))
        df, col = _merge_csv_data(df, col, data, csv_attrs)
        merged_static_cols.append(col)
    df = _process_and_merge_new_gpg_min_stable_lvl(df, parsed_workbook_path)
    df = _zero_renewable_heat_rates(df, "heat_rate_gj/mwh")
    df = _zero_renewable_minimum_load(df, "minimum_load_mw")
    df = _zero_ocgt_recip_minimum_load(df, "minimum_load_mw")
    df = _zero_solar_wind_h2gt_partial_outage_derating_factor(
        df, "partial_outage_derating_factor_%"
    )
    # replace remaining string values in static property columns
    df = df.infer_objects()
    for col in [col for col in merged_static_cols if df[col].dtype == "object"]:
        df[col] = df[col].apply(lambda x: pd.NA if isinstance(x, str) else x)
    return df


def _merge_csv_data(
    df: pd.DataFrame, col: str, csv_data: pd.DataFrame, csv_attrs: dict
) -> tuple[pd.DataFrame, str]:
    """Replace values in the provided column of the summary mapping with those
    in the CSV data using the provided attributes in
    `_NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    """
    # handle alternative table structures for locational OPEX values
    if re.search(r"^[f,v]om_\$", col):
        df[col] = df["generator_build_cost_zone"]
        csv_data = _process_locational_opex_table(csv_data, csv_attrs)
    # handle alternative lookup and value columns
    for alt_attr in ("lookup", "value"):
        if f"alternative_{alt_attr}s" in csv_attrs.keys():
            csv_col = csv_attrs[f"csv_{alt_attr}"]
            for alt_col in csv_attrs[f"alternative_{alt_attr}s"]:
                csv_data[csv_col] = csv_data[csv_col].where(pd.notna, csv_data[alt_col])
    replacement_dict = (
        csv_data.loc[:, [csv_attrs["csv_lookup"], csv_attrs["csv_value"]]]
        .set_index(csv_attrs["csv_lookup"])
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
    if "generator_status" in csv_attrs.keys():
        row_filter = df["status"] == csv_attrs["generator_status"]
        df.loc[row_filter, col] = df.loc[row_filter, col].replace(replacement_dict)
    else:
        df[col] = df[col].replace(replacement_dict)
    if "new_col_name" in csv_attrs.keys():
        df = df.rename(columns={col: csv_attrs["new_col_name"]})
        col = csv_attrs["new_col_name"]
    return df, col


def _process_locational_opex_table(
    opex_table_data: pd.DataFrame, csv_attrs: dict
) -> pd.DataFrame:
    """
    Restructure workbook table for new entrant fixed and variable OPEX to work with mapping system.
    """
    ipdb.set_trace()
    # sets column names to the regional build cost zone
    opex_table_data = opex_table_data.rename(
        columns={col: col.split("_")[-1] for col in opex_table_data.columns}
    )
    # reshapes the dataframe to long form with columns containing generator type, regional build cost zone, and corresponding opex value.
    opex_table_data_reformatted = opex_table_data.melt(
        id_vars=[csv_attrs["csv_lookup"]],
        var_name="Regional build cost zone",
        value_name=csv_attrs["csv_value"],
    )
    # create a column named by the lookup_value, containing the generator:cost_region mapping
    opex_table_data_reformatted[csv_attrs["csv_lookup"]] = opex_table_data_reformatted[
        csv_attrs["csv_lookup"]
    ].str.cat(opex_table_data_reformatted["Regional build cost zone"], sep=": ")
    return opex_table_data_reformatted


def _process_and_merge_new_gpg_min_stable_lvl(
    df: pd.DataFrame, parsed_workbook_path: Path | str
) -> pd.DataFrame:
    """Processes and merges in gas-fired generation minimum stable level data

    Minimum stable level is given as a percentage of nameplate capacity.
    """
    # adds a min stable level column that takes the existing generator name mapping
    df["min_stable_level_%"] = df["generator"]
    new_gpg_min_stable_lvls = pd.read_csv(
        Path(parsed_workbook_path, "gpg_min_stable_level_new_entrants.csv")
    )
    new_gpg_min_stable_lvls = new_gpg_min_stable_lvls.set_index("Technology")
    # manually mapps percentages to the new min stable level column
    for gen, row in new_gpg_min_stable_lvls.iterrows():
        df.loc[df["generator"] == gen, "min_stable_level_%"] = row[
            "Min Stable Level (% of nameplate)"
        ]
    return df


def _zero_renewable_heat_rates(df: pd.DataFrame, heat_rate_col: str) -> pd.DataFrame:
    """
    Fill any empty heat rate values with the technology type, and then set
    renewable energy (solar, wind, hydro) and battery storage heat rates to 0
    """
    # NOTE battery storage dropped before this function called - battery not
    # actually included in this function
    df[heat_rate_col] = df[heat_rate_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(df[heat_rate_col], ["solar", "wind", "hydro"]),
        heat_rate_col,
    ] = 0.0
    return df


def _zero_renewable_minimum_load(
    df: pd.DataFrame, minimum_load_col: str
) -> pd.DataFrame:
    """
    Fill any empty minimum load values with the technology type, and then set values for
    renewable energy (solar, wind, hydro) and battery storage minimum loads to 0
    """
    # NOTE battery storage dropped before this function called - battery not
    # actually included in this function
    df[minimum_load_col] = df[minimum_load_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(df[minimum_load_col], ["solar", "wind", "hydro"]),
        minimum_load_col,
    ] = 0.0
    return df


def _zero_ocgt_recip_minimum_load(
    df: pd.DataFrame, minimum_load_col: str
) -> pd.DataFrame:
    """
    Set values for OCGT and Reciprocating Engine minimum loads to 0
    """
    # NOTE "Reciprocating Engine" currently applies to "Hydrogen reciprocating
    # engines" -> is this expected?
    df.loc[
        _where_any_substring_appears(
            df[minimum_load_col], ["OCGT", "Reciprocating Engine"]
        ),
        minimum_load_col,
    ] = 0.0
    return df


def _zero_solar_wind_h2gt_partial_outage_derating_factor(
    df: pd.DataFrame, po_derating_col: str
) -> pd.DataFrame:
    """
    Fill any empty partial outage derating factor values with the technology type, and
    then set values for solar and wind to 0 (including solar thermal)
    """
    # NOTE currently this will include solar thermal and set to 0.
    # Need to either: set derating factor for ST before this point or exclude?
    df[po_derating_col] = df[po_derating_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(
            df[po_derating_col], ["solar", "wind", "hydrogen-based gas turbine"]
        ),
        po_derating_col,
    ] = 0.0
    return df


template_new_generators_static_properties(
    Path("ispypsa_runs") / Path("workbook_table_cache")
)
