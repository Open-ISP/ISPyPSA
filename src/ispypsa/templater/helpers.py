import re
from typing import Iterable

import numpy as np
import pandas as pd
from thefuzz import process


def _fuzzy_match_names(name_series: pd.Series, choices: Iterable[str]) -> pd.Series:
    """
    Fuzzy matches values in `name_series` with values in `choices`.
    Fuzzy matching is used where typos or minor differences in names in raw data
    may cause issues with exact mappings (e.g. using a dictionary mapping)

    Args:
        name_series: :class:`pandas.Series` with names to be matched with values in
            `choices`

    Returns:
        :class:`pandas.Series` with values from `choices` that correspond to the closest
            match to the original values in `name_series`
    """
    matched_series = name_series.apply(lambda x: process.extractOne(x, choices)[0])
    return matched_series


def _fuzzy_match_names_above_threshold(
    name_series: pd.Series, choices: Iterable[str], threshold: int
) -> pd.Series:
    """
    Fuzzy matches values in `name_series` with values in `choices` and applies the match
    only if the Levenshtein distance exceeds `threshold`.

    Args:
        name_series: :class:`pandas.Series` with names to be matched with values in
            `choices`

    Returns:
        :class:`pandas.Series` with selective fuzzy matching
    """
    matched_series = name_series.apply(
        lambda x: process.extractOne(x, choices)[0]
        if process.extractOne(x, choices)[1] > threshold
        else x
    )
    return matched_series


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
        4. Replaces spaces not followed by an underscore with an underscore, and any
            remaining spaces with nothing
        6. Replaces parentheses with nothing
        7. Removese duplicated underscores
        8. Makes all characters lowercase

    Args:
        string: String to be snakecased
    """
    string = string.strip().replace("MWh", "mwh")
    precede_words_with_capital_with_underscore = re.sub(
        r"(?<!^)(?<!_)([A-Z][a-z0-9]+)", r"_\1", string
    )
    precede_number_groups_with_underscore = re.sub(
        r"(?<!^)(?<!_)([0-9]{2,}+)(?![a-zA-Z]+)",
        r"_\1",
        precede_words_with_capital_with_underscore,
    )
    replace_hyphens = re.sub(r"-", "_", precede_number_groups_with_underscore)
    replace_spaces = re.sub(r"\s(?!_)", "_", replace_hyphens).replace(" ", "")
    replace_parentheses = re.sub(r"\(|\)|", "", replace_spaces)
    replace_duplicated_underscores = re.sub(r"_+", "_", replace_parentheses)
    snaked = replace_duplicated_underscores.lower()
    return snaked


def _where_any_substring_appears(
    series: pd.Series, substrings: Iterable[str]
) -> pd.Series:
    """Returns elements of a series that contain any of the substrings (not case sensitive)

    Args:
        series: :class:`pd.Series`
        substrings: Iterable containing substrings to use for selection

    Returns:
        Boolean :class:`pd.Series` with `True` where a substring appears
    """
    substrings = list(substrings)
    wheres = []
    for string in substrings:
        wheres.append(series.str.contains(string, case=False, na=False))
    if len(wheres) < 2:
        boolean = wheres.pop()
    else:
        boolean = np.logical_or(wheres[0], wheres[1])
        for i in range(2, len(wheres)):
            boolean = np.logical_or(boolean, wheres[i])
    return boolean
