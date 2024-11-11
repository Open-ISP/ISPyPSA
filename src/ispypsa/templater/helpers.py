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
    false_series = pd.Series(np.repeat(False, len(series)))
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
