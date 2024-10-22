import re
from pathlib import Path

import pandas as pd

from .helpers import (
    _fuzzy_match_names_above_threshold,
    _snakecase_string,
    _where_any_substring_appears,
)
from .mappings import _EXISTING_GENERATOR_STATIC_PROPERTY_TABLE_MAP

_OBSOLETE_COLUMNS = [
    "Maximum capacity factor (%)",
]


def _template_existing_generators(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Existing generator summary' table into an ISPyPSA template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: ISPyPSA existing generators template
    """
    existing_generators = pd.read_csv(
        Path(parsed_workbook_path, "existing_generator_summary.csv")
    )
    cleaned_existing_generators = _clean_generator_summary(existing_generators)
    merged_cleaned_existing_generators = (
        _merge_and_set_existing_generators_static_properties(
            cleaned_existing_generators, parsed_workbook_path
        )
    )
    return merged_cleaned_existing_generators


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


def _merge_and_set_existing_generators_static_properties(
    df: pd.DataFrame, parsed_workbook_path: Path | str
) -> pd.DataFrame:
    """Merges into and sets static (i.e. not time-varying) generator properties in the
    "Existing generator summary" template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._EXISTING_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: Existing generator summary DataFrame
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: Existing generator template with static properties filled in
    """

    def _merge_csv_data(
        df: pd.DataFrame, col: str, csv_data: pd.DataFrame, csv_attrs: dict[str, str]
    ) -> pd.DataFrame:
        """Replace values in the provided column of the summary mapping with those
        in the CSV data using the provided attributes in
        `_EXISTING_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
        """
        replacement_dict = (
            csv_data.loc[:, [csv_attrs["csv_lookup"], csv_attrs["csv_values"]]]
            .set_index(csv_attrs["csv_lookup"])
            .squeeze()
            .to_dict()
        )
        # handles differences of mapping values between summmary and outage tables
        if re.search("outage", col):
            df[col] = _rename_summary_outage_mappings(df[col])
        # handles slight difference in capitalisation e.g. Bongong/Mackay vs Bogong/MacKay
        df[col] = _fuzzy_match_names_above_threshold(
            df[col], replacement_dict.keys(), 99
        )
        df[col] = df[col].replace(replacement_dict).infer_objects(copy=False)
        if "new_col_name" in csv_attrs.keys():
            df = df.rename(columns={col: csv_attrs["new_col_name"]})
        return df

    # adds a max capacity column that takes the existing generator name mapping
    df["maximum_capacity_mw"] = df["existing_generator"]
    for col, csv_attrs in _EXISTING_GENERATOR_STATIC_PROPERTY_TABLE_MAP.items():
        data = pd.read_csv(Path(parsed_workbook_path, csv_attrs["csv"] + ".csv"))
        df = _merge_csv_data(df, col, data, csv_attrs)
    df = _process_and_merge_existing_gpg_min_load(df, parsed_workbook_path)
    df = _zero_renewable_heat_rates(df, "heat_rate_gj/mwh")
    df = _zero_renewable_minimum_load(df, "minimum_load_mw")
    df = _zero_ocgt_recip_minimum_load(df, "minimum_load_mw")
    for outage_col in [col for col in df.columns if re.search("outage", col)]:
        df = _zero_wind_solar_outages(df, outage_col)
        # correct remaining outage mapping differences
        df[outage_col] = _rename_summary_outage_mappings(df[outage_col])
    return df


def _zero_renewable_heat_rates(df: pd.DataFrame, heat_rate_col: str) -> pd.DataFrame:
    """Set renewable energy (solar, wind, hydro) heat rates to 0"""
    df.loc[
        _where_any_substring_appears(df[heat_rate_col], ["solar", "wind", "hydro"]),
        heat_rate_col,
    ] = 0.0
    return df


def _zero_renewable_minimum_load(
    df: pd.DataFrame, minimum_load_col: str
) -> pd.DataFrame:
    """Set values for renewable energy (solar, wind, hydro) minimum loads to 0"""
    df.loc[
        _where_any_substring_appears(df[minimum_load_col], ["solar", "wind", "hydro"]),
        minimum_load_col,
    ] = 0.0
    return df


def _zero_ocgt_recip_minimum_load(
    df: pd.DataFrame, minimum_load_col: str
) -> pd.DataFrame:
    """Set values for OCGT and Reciprocating =Engine minimum loads to 0"""
    df.loc[
        _where_any_substring_appears(
            df[minimum_load_col], ["OCGT", "Reciprocating Engine"]
        ),
        minimum_load_col,
    ] = 0.0
    return df


def _zero_wind_solar_outages(df: pd.DataFrame, outage_col: str) -> pd.DataFrame:
    """Set values for wind and solar in the outage column to 0"""
    df.loc[
        _where_any_substring_appears(df[outage_col], ["solar", "wind"]),
        outage_col,
    ] = 0.0
    return df


def _rename_summary_outage_mappings(outage_series: pd.Series) -> pd.Series:
    """Renames values in the outage summary column to match those in the outages
    workbook tables
    """
    return outage_series.replace(
        {
            "Steam Turbine & CCGT": "CCGT + Steam Turbine",
            "OCGT Small": "Small peaking plants",
        }
    )


def _process_and_merge_existing_gpg_min_load(
    df: pd.DataFrame, parsed_workbook_path: Path | str
) -> pd.DataFrame:
    """Processes and merges in gas-fired generation minimum load data

    Only retains first Gas Turbine min load if there are multiple turbines (OPINIONATED).
    """
    existing_gpg_min_loads = pd.read_csv(
        Path(parsed_workbook_path, "gpg_min_stable_level_existing_generators.csv")
    )
    to_merge = []
    for station in existing_gpg_min_loads["Generator Station"].drop_duplicates():
        station_rows = existing_gpg_min_loads[
            existing_gpg_min_loads["Generator Station"] == station
        ]
        if len(station_rows) > 1:
            # CCGTs with ST and GTs
            if all(
                [re.search("CCGT", tt) for tt in set(station_rows["Technology Type"])]
            ):
                gt_rows = station_rows.loc[
                    station_rows["Technology Type"].str.contains("Gas Turbine")
                ]
                to_merge.append(gt_rows.iloc[0, :].squeeze())
            # Handles cases like TIPSB
            else:
                to_merge.append(station_rows.iloc[0, :].squeeze())
        else:
            to_merge.append(station_rows.squeeze())
    processed_gpg_min_loads = pd.concat(to_merge, axis=1).T
    # manual corrections
    processed_gpg_min_loads["Generator Station"] = processed_gpg_min_loads[
        "Generator Station"
    ].replace(
        {"Tamar Valley": "Tamar Valley Combined Cycle", "Condamine": "Condamine A"}
    )
    processed_gpg_min_loads = processed_gpg_min_loads.set_index("Generator Station")
    for gen, row in processed_gpg_min_loads.iterrows():
        df.loc[df["existing_generator"] == gen, "minimum_load_mw"] = row[
            "Min Stable Level (MW)"
        ]
    return df