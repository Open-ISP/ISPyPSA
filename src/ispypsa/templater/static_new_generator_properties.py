import logging
import re
from pathlib import Path

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
    4. Enforces consistent formatting of "storage" str instances
    4. Adds the following columns with appropriate mappings:
            - `partial_outage_derating_factor_%`
            - `maximum_capacity_mw`
            - `lifetime`
            - `summer_peak_rating_%`
            - `om_locational_cost_factor_%`
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
    # enforces capitalisation structure for instances of str "storage" in generator col
    df["generator"] = df["generator"].replace(
        [r"s[a-z]{6}\s", r"S[a-z]{6}\)"], [r"Storage ", r"storage)"], regex=True
    )
    df = _fix_forced_outage_columns(df)
    # adds a partial derating factor column that takes partial outage rate mappings
    df["partial_outage_derating_factor_%"] = df[
        "forced_outage_rate_partial_outage_%_of_time"
    ]
    # adds extra necessary columns taking appropriate mapping values
    df["maximum_capacity_mw"] = df["generator"]
    df["lifetime"] = df["generator"]
    df["minimum_stable_level_%"] = df["generator"]
    df["summer_peak_rating_%"] = df["summer_rating_mw"]
    df["om_locational_cost_factor_%"] = df["fom_$/kw/annum"]
    df["technology_specific_lcf_%"] = df["regional_build_cost_zone"]
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
    df = _process_and_merge_new_gpg_min_stable_lvl(
        df, parsed_workbook_path, "minimum_stable_level_%"
    )
    df = _calculate_and_merge_tech_specific_lcfs(
        df, parsed_workbook_path, "technology_specific_lcf_%"
    )
    df = _zero_renewable_heat_rates(df, "heat_rate_gj/mwh")
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
    if re.search(r"[vf]om", col):
        # change mapping from regional cost zone to generator
        df[col] = df["generator"]
        # fill BOTN - Cethana row with pd.NA where currently str:
        csv_data[csv_attrs["csv_value"]] = csv_data[csv_attrs["csv_value"]].apply(
            lambda x: pd.NA if re.search(r"BOTN", str(x)) else float(x)
        )
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
    df[col] = df[col].replace(replacement_dict)
    if "new_col_name" in csv_attrs.keys():
        df = df.rename(columns={col: csv_attrs["new_col_name"]})
        col = csv_attrs["new_col_name"]
    return df, col


def _calculate_and_merge_tech_specific_lcfs(
    df: pd.DataFrame, parsed_workbook_path: Path | str, tech_lcf_col: str
) -> pd.DataFrame:
    """Calculates the technology-specific locational cost factor as a percentage
    for each new entrant generator and merges into summary mapping table.
    """
    # loads in the three tables needed
    breakdown_ratios = pd.read_csv(
        Path(parsed_workbook_path, "technology_cost_breakdown_ratios.csv")
    )
    technology_specific_lcfs = pd.read_csv(
        Path(parsed_workbook_path, "technology_specific_lcfs.csv")
    )
    # load all cols unless the str "O&M" is in col name
    locational_cost_factors = pd.read_csv(
        Path(parsed_workbook_path, "locational_cost_factors.csv"),
        index_col=0,
        usecols=lambda x: "O&M" not in x,
    )
    technology_specific_lcfs = technology_specific_lcfs.melt(
        id_vars="Cost zones / Sub-region",
    ).dropna(axis=0, how="any")
    technology_specific_lcfs.columns = ["Location", "Technology", "LCF"]
    # ensures generator names in LCF tables match those in the summary table
    for df_to_match_gen_names in [technology_specific_lcfs, breakdown_ratios]:
        df_to_match_gen_names["Technology"] = _fuzzy_match_names(
            df_to_match_gen_names["Technology"],
            df["generator"].unique(),
            "calculating and merging in LCFs to static new entrant gen summary",
            not_match="existing",
            threshold=90,
        )
        df_to_match_gen_names.set_index("Technology", inplace=True)
    # ensures that col names in tables to combine are the same
    locational_cost_factors.columns = breakdown_ratios.columns
    # loops over rows and use existing LCF for all pumped hydro gens, calculate for others
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
            ((df["generator"] == tech) & (df[tech_lcf_col] == row["Location"])),
            tech_lcf_col,
        ] = calculated_or_given_lcf
    # fills rows with no LCF (some PHES REZs) with pd.NA
    df[tech_lcf_col] = df[tech_lcf_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _process_and_merge_new_gpg_min_stable_lvl(
    df: pd.DataFrame, parsed_workbook_path: Path | str, min_level_col: str
) -> pd.DataFrame:
    """Processes and merges in gas-fired generation minimum stable level data (%)

    Minimum stable level is given as a percentage of nameplate capacity, and set
    to zero for renewable generators (wind, solar, hydro), storage, and OCGT.
    """
    new_gpg_min_stable_lvls = pd.read_csv(
        Path(parsed_workbook_path, "gpg_min_stable_level_new_entrants.csv")
    )
    new_gpg_min_stable_lvls = new_gpg_min_stable_lvls.set_index("Technology")
    # manually maps percentages to the new min stable level column
    for tech, row in new_gpg_min_stable_lvls.iterrows():
        df.loc[df["generator"] == tech, min_level_col] = row[
            "Min Stable Level (% of nameplate)"
        ]
    df.loc[
        _where_any_substring_appears(
            df[min_level_col], ["solar", "wind", "hydro", "battery", "ocgt"]
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
    renewable energy (solar, solar thermal,  wind, hydro) and battery storage
    heat rates to 0.0
    """
    df[heat_rate_col] = df[heat_rate_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(
            df[heat_rate_col], ["solar", "wind", "hydro", "battery"]
        ),
        heat_rate_col,
    ] = 0.0
    return df


def _zero_solar_wind_h2gt_partial_outage_derating_factor(
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
