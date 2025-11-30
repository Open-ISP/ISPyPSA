import logging
import re
from typing import Iterable

import numpy as np
import pandas as pd
from thefuzz import fuzz


def _fuzzy_match_names(
    name_series: pd.Series,
    choices: Iterable[str],
    task_desc: str,
    not_match: str = "existing",
    threshold: int = 0,
) -> pd.Series:
    """
    Fuzzy matches values in `name_series` with values in `choices`.
    Fuzzy matching is used where typos or minor differences in names in raw data
    may cause issues with exact mappings (e.g. using a dictionary mapping).
    This function is only suitable for use where name_series does not have
    repeated values since matching is done without replacement

    Args:
        name_series: :class:`pandas.Series` with names to be matched with values in
            `choices`
        choices: Iterable of `choices` that are replacement values
        task_desc: Task description to include in logging information
        not_match: optional. Defaults to "existing". If "existing", wherever a match
            that exceeds the threshold does not exist the existing value is retained.
            If any other string, this will be used to replace the existing value
            where a match that exceeds the threshold does not exist.
        threshold: match quality threshold to exceed for replacement. Between 0 and 100

    Returns:
        :class:`pandas.Series` with values from `choices` that correspond to the closest
            match to the original values in `name_series`
    """
    match_dict = _one_to_one_priority_based_fuzzy_matching(
        set(name_series), set(choices), not_match, threshold
    )
    matched_series = name_series.apply(lambda x: match_dict[x])
    _log_fuzzy_match(name_series, matched_series, task_desc)
    return matched_series


def _one_to_one_priority_based_fuzzy_matching(
    strings_to_match: set, choices: set, not_match: str, threshold: int
):
    """
    Find matches between two sets of strings, assuming that strings_to_match and choices
    contain unique values (e.g. from the index column of a table) that must be matched one
    to one. This is done by:

        1. Identifying exact matches
        2. Matching remaining strings by finding the highest similarity pair and then
           recording the best match (iteratively).

    Args:
        strings_to_match: set of strings to find a match for in the set of choices.
        choices: set of strings to choose from when finding matches.
        not_match: optional. Defaults to "existing". If "existing", wherever a match
            that exceeds the threshold does not exist, the existing value is retained.
            If any other string, this will be used to replace the existing value
            where a match that exceeds the threshold does not exist.
        threshold: match quality threshold to exceed for replacement. Between 0 and 100

    Returns:
        dict: dict matching strings to the choice they matched with.
    """

    matches = []

    remaining_strings_to_match = strings_to_match
    remaining_choices = choices

    # Find and remove exact matches
    exact_matches = remaining_strings_to_match.intersection(remaining_choices)
    for s in exact_matches:
        matches.append((s, s))
        remaining_strings_to_match.remove(s)
        remaining_choices.remove(s)

    # Convert remaining sets to lists for index access
    remaining_strings_to_match_list = list(remaining_strings_to_match)
    remaining_choices_list = list(remaining_choices)

    # For remaining strings, use greedy approach with fuzzy matching
    while remaining_strings_to_match_list and remaining_choices_list:
        best_score = -1
        best_pair = None

        # Find the highest similarity score among remaining pairs
        for i, str_a in enumerate(remaining_strings_to_match_list):
            for j, str_b in enumerate(remaining_choices_list):
                score = fuzz.ratio(str_a, str_b)
                if score > best_score and score >= threshold:
                    best_score = score
                    best_pair = (i, j, str_a, str_b, score)

        if best_pair:
            i, j, str_a, str_b, score = best_pair
            matches.append((str_a, str_b))

            # Remove matched strings
            remaining_strings_to_match_list.pop(i)
            remaining_choices_list.pop(j)
        else:
            # If none of the remaining string comparisons is greater
            # than the threshold provided break and resort to the
            # no_match strategy.
            break

    for str_to_match in remaining_strings_to_match_list:
        if not_match == "existing":
            matches.append((str_to_match, str_to_match))
        else:
            matches.append((str_to_match, not_match))

    return dict(matches)


def _log_fuzzy_match(
    original_series: pd.Series, matched_series: pd.Series, task_desc: str
) -> None:
    """Log any fuzzy matches at the INFO level"""
    if any(diff := matched_series != original_series):
        originals = original_series[diff]
        matches = matched_series[diff]
        for original, match in zip(originals, matches):
            logging.info(f"'{original}' matched to '{match}' whilst {task_desc}")


def _snakecase_string(string: str) -> str:
    """Returns the input string in snakecase

    Steps:
        1. Strip leading and tailing spaces
        2. Catch units that are not properly handled by following steps (e.g. "MWh")
        3. Replaces words starting with an uppercase character (and not otherwise
            containing capitals) that are not at the start of the string or preceded
            by an underscore, with the same word preceded by an underscore
        4. Replaces groups of numbers (2+ digits) that are not at the start of the string
            or preceded by an underscore, with the same group of numbers preceded
            by an underscore
        5. Replaces hyphens with underscores
        6. Replaces commas with underscores
        7. Replaces spaces not followed by an underscore with an underscore, and any
            remaining spaces with nothing
        8. Replaces parentheses with nothing
        9. Removese duplicated underscores
        10. Makes all characters lowercase

    Args:
        string: String to be snakecased
    """
    string = string.strip().replace("MWh", "mwh")
    precede_words_with_capital_with_underscore = re.sub(
        r"(?<!^)(?<!_)([A-Z][a-z0-9]+)", r"_\1", string
    )
    precede_number_groups_with_underscore = re.sub(
        r"(?<!^)(?<!_)(?<![0-9])([0-9]{2,}+)(?![a-zA-Z]+)",
        r"_\1",
        precede_words_with_capital_with_underscore,
    )
    replace_hyphens = re.sub(r"-", "_", precede_number_groups_with_underscore)
    replace_commas = re.sub(r",", "_", replace_hyphens)
    replace_spaces = re.sub(r"\s(?!_)", "_", replace_commas).replace(" ", "")
    replace_parentheses = re.sub(r"\(|\)|", "", replace_spaces)
    replace_duplicated_underscores = re.sub(r"_+", "_", replace_parentheses)
    snaked = replace_duplicated_underscores.lower()
    return snaked


def _where_any_substring_appears(
    series: pd.Series, substrings: Iterable[str]
) -> pd.Series:
    """Returns string elements of a series that contain any of the provided
    substrings (not case sensitive).

    Args:
        series: :class:`pd.Series`
        substrings: Iterable containing substrings to use for selection

    Returns:
        Boolean :class:`pd.Series` with `True` where a substring appears in a string
    """
    series_where_str = series.apply(lambda x: isinstance(x, str))
    false_series = pd.Series(False, index=series_where_str.index)
    if not any(series_where_str):
        return false_series
    substrings = list(substrings)
    wheres = []
    for string in substrings:
        wheres.append(
            false_series.where(
                ~series_where_str,
                series.str.contains(string, case=False, na=False),
            )
        )
    if len(wheres) < 2:
        boolean = wheres.pop()
    else:
        boolean = np.logical_or(wheres[0], wheres[1])
        for i in range(2, len(wheres)):
            boolean = np.logical_or(boolean, wheres[i])
    return boolean


def _add_units_to_financial_year_columns(
    columns: pd.Index, units_str: str
) -> list[str]:
    """Adds '_{units_str}' to the financial year columns"""
    cols = [
        _snakecase_string(col + f"_{units_str}")
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
        else _snakecase_string(col)
        for col in columns
    ]
    return cols


def _convert_financial_year_columns_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Forcefully converts FY columns to float columns"""
    cols = [
        df[col].astype(float) if re.match(r"[0-9]{4}_[0-9]{2}", col) else df[col]
        for col in df.columns
    ]
    return pd.concat(cols, axis=1)


def _strip_all_text_after_numeric_value(
    series: pd.Series,
) -> pd.Series:
    """
    Extracts numeric values from the start of strings and removes any trailing text.

    This function is designed to clean data from IASR workbook tables where numeric
    values may be followed by units or descriptive text (e.g., "1,500 MW" → "1,500").

    Args:
        series: A pandas Series with object dtype containing strings to process.
                Non-object dtype Series are returned unchanged.

    Returns:
        A pandas Series with numeric values extracted and trailing text removed.

    Supported numeric formats:
        - Unsigned integers: "123", "1234"
        - Signed integers: "+123", "-123"
        - Numbers with commas: "1,234", "12,345,678"
        - Decimal numbers: "123.45", "1,234.56"
        - Numbers without proper comma formatting: "1500" (not "1,500")

    Behavior:
        - Extracts only from the beginning of the string
        - Stops at the first valid number found
        - Requires zero or more whitespace between number and text
        - Returns the original string if no valid number is found at the start
        - Only processes object dtype Series

    Examples:
        "100 MW capacity" → "100"
        "1,500 units" → "1,500"
        "-123.45 deficit" → "-123.45"
        "100MW" → "100" (no space required)
        "Text 100" → "Text 100" (number not at start)
        "++100" → "++100" (invalid format)
        "1.2.3" → "1.2" (extracts first valid number)
    """
    if series.dtypes == "object":
        # This regex matches:
        # - Optional plus or minus sign at start
        # - Either properly formatted numbers with commas (1,234) or simple numbers (1234)
        # - Optional decimal part with one period
        # - Followed by optional whitespace and any other text
        series = series.astype(str).str.replace(
            r"^([+-]?(?:[0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)(?:\.[0-9]+)?)\s*.*",
            r"\1",
            regex=True,
        )
    return series


def _standardise_storage_capitalisation(series: pd.Series) -> pd.Series:
    """
    Standardises capitalisation of "storage" in a pandas Series.

    In the context of the new entrant generator summary table, this function is used to
    enforce a consistent naming convention for instances of "storage" in the
    "New entrants" column (renamed by the templator to "generator_name").

    The convention is as follows:
        - "Battery Storage" for instances where "storage" is part of a battery name,
          not a descriptor for duration.
        - "storage" (lowercase) for instances where "storage" is a descriptor for duration
          (e.g., "2hrs storage", "1hr storage").
        - All other cases are left unchanged.
    """

    battery_name_pattern = r"Battery [s|S]torage"
    battery_name_standard = r"Battery Storage"

    series = series.str.replace(battery_name_pattern, battery_name_standard, regex=True)

    # 'duration_string' instances of storage are preceeded by a number and "hr"/"hrs"
    duration_string_pattern = r"(?P<duration>\d+\s*hrs*) [S|s]torage"

    # make sure all duration-related instances use lowercase "storage"
    series = series.str.replace(
        duration_string_pattern, r"\g<duration> storage", regex=True
    )

    return series


def _manual_remove_footnotes_from_generator_names(df: pd.DataFrame) -> pd.DataFrame:
    """Manually handles specific cases where footnote numbers have remained in generator names."""

    strings_with_footnotes = {
        "Small OCGT2": "Small OCGT",
        "Pumped Hydro3 (8 hrs storage)": "Pumped Hydro (8 hrs storage)",
    }
    # rename columns AND replace values across the df to cover all potential cases:
    df_cols_renamed = df.rename(columns=strings_with_footnotes)
    df_all_replaced = df_cols_renamed.replace(
        list(strings_with_footnotes.keys()), list(strings_with_footnotes.values())
    )
    return df_all_replaced


def _rez_name_to_id_mapping(
    series: pd.Series, series_name: str, renewable_energy_zones: pd.DataFrame
) -> pd.Series:
    """Maps REZ names to REZ IDs."""

    if series.empty or series is None or all(series.isna()):
        return series

    # add non-REZs to the REZ table and set up mapping:
    non_rez_ids = pd.DataFrame(
        {
            "ID": ["V0", "N0"],
            "Name": ["Victoria Non-REZ", "New South Wales Non-REZ"],
        }
    )
    renewable_energy_zones = pd.concat(
        [renewable_energy_zones, non_rez_ids], ignore_index=True
    )
    rez_name_to_id = dict(
        zip(renewable_energy_zones["Name"], renewable_energy_zones["ID"])
    )

    # ------ clean up the series in case of old/unsupported REZ names
    # update references to "North [East|West] Tasmania Coast" to "North Tasmania Coast"
    # update references to "Portland Coast" to "Southern Ocean"
    series_fixed_rez_names = series.replace(
        {
            r".+Tasmania Coast": "North Tasmania Coast",
            r"Portland Coast": "Southern Ocean",
        },
        regex=True,
    )
    # fuzzy match series to REZ names to make sure they are consistent - but only
    # for not-exact matches that already exist, to avoid skipping necessary fixes:
    where_not_existing_match_str = series_fixed_rez_names.apply(
        lambda x: x not in rez_name_to_id.keys()
    )

    series_fixed_rez_names.loc[where_not_existing_match_str] = _fuzzy_match_names(
        series_fixed_rez_names.loc[where_not_existing_match_str],
        rez_name_to_id.keys(),
        f"mapping REZ names to REZ IDs for property '{series_name}'",
        threshold=90,
    )

    return series_fixed_rez_names.replace(rez_name_to_id)
