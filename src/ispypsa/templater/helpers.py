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
    """Log any fuzzy matches at the INFO level — one line per distinct mapping.

    Callers may pass series with the same (original, match) pair repeated across
    rows (e.g. one cost row per year for the same option name). Dedup so each
    name-matching decision appears exactly once in the log, regardless of how many
    rows shared it. Sorted for stable order across runs.
    """
    if any(diff := matched_series != original_series):
        pairs = sorted(set(zip(original_series[diff], matched_series[diff])))
        for original, match in pairs:
            logging.info(f"'{original}' matched to '{match}' whilst {task_desc}")


def _best_fuzzy_match(value: str, choices: Iterable[str], threshold: int) -> str | None:
    """Returns the highest-scoring match from choices if score >= threshold, else None.

    I/O Example:
        _best_fuzzy_match("Step Chaneg", ["Step Change", "Slower Growth"], 85)
        → "Step Change"   # fuzz.ratio ~89, above threshold

        _best_fuzzy_match("Hmm", ["Step Change", "Slower Growth"], 85)
        → None            # best score well below threshold
    """
    # NOTE https://github.com/Open-ISP/ISPyPSA/issues/106 (short strings)
    best_choice, best_score = max(
        ((c, fuzz.ratio(value, c)) for c in choices),
        key=lambda x: x[1],
    )
    return best_choice if best_score >= threshold else None


def _fuzzy_map_to_allowed_values(
    name_series: pd.Series,
    choices: Iterable[str],
    task_desc: str,
    threshold: int = 85,
) -> pd.Series:
    """Maps each value in name_series to the closest match in choices (many-to-one).

    Unlike _fuzzy_match_names, choices are not consumed — multiple input values can
    map to the same allowed value. Successful fuzzy corrections are logged at INFO
    via _log_fuzzy_match. Any remaining values scoring below ``threshold`` after
    matching raises a ValueError.

    Args:
        name_series: Series of names to map.
        choices: Allowed values to match against.
        task_desc: Description included in log messages.
        threshold: Minimum fuzz.ratio score (0–100) to accept a replacement. Default 85.

    Returns:
        Series with values replaced by the closest match where score >= threshold.

    Raises:
        ValueError: if any values in ``name_series`` do not have a match scoring
            above ``threshold`` in ``choices``.

    I/O Examples:
        name_series:    ["Step Change", "Step Chaneg", "Step Change"]
        choices:        ["Step Change", "Slower Growth"]
        task_desc:      "canonicalising scenarios"
        threshold:      85

        returns:      ["Step Change", "Step Change", "Step Change"]
        # "Step Chaneg" corrected → INFO logged

        name_series:    ["Wind", "Wind", "Solar PV", "Solar pv", "the sun"]
        choices:        ["Wind", "Solar PV"]
        task_desc:      "fixing technologies"
        threshold:      85

        raises:
            ValueError: "Could not fuzzy match to a canonical value
                        whilst fixing technologies: ['the sun']"
    """
    canonical = set(choices)
    match_dict = {
        name: _best_fuzzy_match(name, canonical, threshold)
        for name in name_series.unique()
    }
    matched = name_series.map(
        lambda name: match_dict[name] if match_dict[name] is not None else name
    )
    _log_fuzzy_match(name_series, matched, task_desc)
    unmatched = sorted(name for name, match in match_dict.items() if match is None)
    if unmatched:
        msg = (
            f"Could not fuzzy match to an allowed value whilst {task_desc}: {unmatched}"
        )
        raise ValueError(msg)
    return matched


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


def _looks_like_financial_year(col: str) -> bool:
    """True if column name matches a financial year pattern like '2024-25'"""
    return bool(re.match(r"^\d{4}-\d{2}$", str(col)))


def _financial_year_string_to_end_year_int(fy_string: str) -> int:
    """Converts an IASR financial-year string like '2024-25' to its ending year (2025).

    Adds 1 to the start year (rather than parsing the two-digit end) to avoid
    century-crossover ambiguity, mirroring
    :func:`ispypsa.translator.helpers._get_financial_year_int_from_string`.

    I/O Example:
        "2024-25" -> 2025
        "2099-00" -> 2100
    """
    return int(fy_string.split("-")[0]) + 1


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


def _pick_location(row: pd.Series) -> str:
    """Return a technology's REZ ID when populated, otherwise Sub-region.

    I/O Example:
        {"REZ ID": "Q8",             "Sub-region": "SQ"}  -> "Q8"
        {"REZ ID": "Not Applicable", "Sub-region": "SQ"}  -> "SQ"
    """
    rez_id = row["REZ ID"]
    if pd.notna(rez_id) and rez_id != "Not Applicable":
        return rez_id
    return row["Sub-region"]


def _set_geo_id(df: pd.DataFrame) -> pd.DataFrame:
    """Adds 'geo_id' column: REZ ID with Sub-region fallback (see ``_pick_location``)."""
    df = df.copy()
    df["geo_id"] = df.apply(_pick_location, axis=1)
    return df


def _build_geo_region_lookup(sub_regional_geography: pd.DataFrame) -> dict[str, str]:
    """Maps every geo (sub-region, REZ, or NEM region) to its NEM region id.

    ``sub_regional_geography`` already lists both sub-regions and REZs against their
    ``region_id``. NEM regions are added as identities so that geos which are
    already regions — after granularity aggregation, or on new parallel corridors —
    resolve to themselves.

    I/O Example:
        sub_regional_geography:
            geo_id  geo_type   region_id
            NQ      subregion  QLD
            CNSW    subregion  NSW
            Q1      rez        QLD

        returns:
            {"NQ": "QLD", "CNSW": "NSW", "Q1": "QLD", "QLD": "QLD", "NSW": "NSW"}
    """
    lookup = dict(
        zip(sub_regional_geography["geo_id"], sub_regional_geography["region_id"])
    )
    for region in set(sub_regional_geography["region_id"]):
        lookup[region] = region
    return lookup


def _map_geo_id_to_granularity(
    geo_id: pd.Series, regional_granularity: str, sub_regional_geography: pd.DataFrame
) -> pd.Series:
    """Maps sub-region geo_ids to their region_id ("nem_regions"), "NEM" ("single_region"),
    or returns untouched; REZ geo_ids always return untouched.

    I/O Example:
        geo_id: pd.Series(["CNSW", "SNW", "Q1"])

        sub_regional_geography:
            geo_id  geo_type    region_id
            CNSW    subregion   NSW
            SNW     subregion   NSW
            Q1      rez         QLD

        returns:
            regional_granularity = "sub_regions":
                pd.Series(["CNSW", "SNW", "Q1"])

            regional_granularity = "nem_regions":
                pd.Series(["NSW", "NSW", "Q1"])

            regional_granularity = "single_region":
                pd.Series(["NEM", "NEM", "Q1"])
    """
    # Deferred import to avoid a circular import: mappings.py imports from this
    # module (_snakecase_string, old-format section) and this function needs
    # mappings.py's _SINGLE_REGION_ID.
    # TODO: if/when we move to fully new-format templater - move import
    from ispypsa.templater.mappings import _SINGLE_REGION_ID

    geo_id = geo_id.copy()
    is_subregion = _is_subregion_geo_id(geo_id, sub_regional_geography)

    if regional_granularity == "sub_regions":
        return geo_id
    if regional_granularity == "single_region":
        return geo_id.where(~is_subregion, _SINGLE_REGION_ID)
    if regional_granularity == "nem_regions":
        # only map subregion geo_ids to their respective regions; REZs untouched
        geo_id.loc[is_subregion] = geo_id[is_subregion].map(
            _build_geo_region_lookup(sub_regional_geography)
        )
        return geo_id
    raise ValueError(f"Unknown regional_granularity: {regional_granularity!r}")


def _is_subregion_geo_id(
    geo_id: pd.Series, sub_regional_geography: pd.DataFrame
) -> pd.Series:
    """Boolean mask of ``geo_id`` values that are sub-region-located (not REZ).

    I/O Example:
        geo_id: pd.Series(["CNSW", "Q1"])
        sub_regional_geography:
            geo_id  geo_type
            CNSW    subregion
            Q1      rez

        returns: pd.Series([True, False])
    """
    geo_type_by_geo_id = sub_regional_geography.set_index("geo_id")["geo_type"]
    return geo_id.map(geo_type_by_geo_id) == "subregion"


def _assert_table_valid(
    table: pd.DataFrame, table_name: str, required_cols: set[str], merge_desc: str
) -> None:
    """Asserts a source table has every required column and isn't empty.

    Shared precondition check for tables merged in the new-format templater modules —
    guards against two silent-failure modes: a missing column producing a KeyError, and
    an empty table merging to an all-NaN column with no warning.

    Args:
        table: the source table to validate, e.g. ``iasr_tables["battery_properties"]``.
        table_name: ``table``'s IASR table name, used to name it in error messages.
        required_cols: every column the downstream merge reads from ``table``.
        merge_desc: short description of what would be merged, named in the
            empty-table error, e.g. ``"properties '['fom']'"`` or ``"'lcf_build'"``.

    Raises:
        ValueError: if any of ``required_cols`` is missing from ``table``, or if
            ``table`` has no rows.

    I/O Example:
        table:
            Technology  Base value  Extra Column
            Wind        2.0         unused_info

        table_name: "fixed_opex_new_entrants"
        required_cols: {"Technology", "Base value"}
        merge_desc: "properties '['fom']'"

        # No ValueError raised: table has rows, both required columns present.
    """
    missing_cols = required_cols - set(table.columns)
    if missing_cols:
        raise ValueError(
            f"'{table_name}' table missing required columns: {sorted(missing_cols)}"
        )
    if table.empty:
        raise ValueError(f"'{table_name}' table is empty - cannot merge {merge_desc}")


def _apply_iasr_table_replacements(
    iasr_tables: dict[str, pd.DataFrame], corrections: list[dict]
) -> dict[str, pd.DataFrame]:
    """Returns ``iasr_tables`` with 'corrections' applied to input IASR tables.

    Shared shape for small, explicitly declared fixes (a documented typo or naming
    mismatch) to specified locations. ``corrections`` can carry multiple fixes,
    each with 'fix' specifics (``table_name``, ``column``, ``replacements``) bundled.
    Returns a shallow copy of ``iasr_tables`` with only listed tables replaced.

    Note: while fuzzy-matching is used to standardise names or other ID strings,
    some typos/diffs are too 'big' to pass any safe fuzzy-match threshold (see
    example below - fuzz.ratio("KiataWF1", "KIATAWF1") == 50). This function
    explicitly handles those known instances where this is the case.

    I/O Example:
        iasr_tables["maximum_capacity_..."]:
            IASR ID   Power Station    Installed capacity (MW)
            KiataWF1  Kiata Wind Farm  31.05
            BW01      Bayswater        660.0

        corrections (as a single dict element in list):
            table_name:   "maximum_capacity_..."
            column:       "IASR ID"
            replacements: {"KiataWF1": "KIATAWF1"}

        returns copy of iasr_tables with only listed tables edited:
            iasr_tables["maximum_capacity_..."]:
                IASR ID   Power Station    Installed capacity (MW)
                KIATAWF1  Kiata Wind Farm  31.05
                BW01      Bayswater        660.0
    """
    corrected_tables = iasr_tables
    for correction in corrections:
        table_name = correction["table_name"]
        col_to_fix = correction["column"]
        replacements = correction["replacements"]
        corrected = iasr_tables[table_name].replace({col_to_fix: replacements})
        corrected_tables = {**corrected_tables, table_name: corrected}
    return corrected_tables


def _group_properties_by_source(
    property_map: dict[str, dict],
) -> dict[tuple[str, str], dict]:
    """Groups a property map's entries by their source (table, key_col).

    Shared by ``new_entrants._merge_properties`` and
    ``existing_planned._merge_unit_keyed_properties`` so a table contributing several
    properties (e.g. ``battery_properties`` feeds six) is validated and key-resolved
    once per source, not once per property.

    I/O Example (abbreviated property_map entries):
        property_map: {
            "storage_hours":      dict(table="battery_properties",
                                       key_col="Technology", ...),
            "efficiency_charge":  dict(table="battery_properties",
                                       key_col="Technology", ...),
            "lifetime_technical": dict(table="lead_time_and_project_life",
                                       key_col="Technology", ...),
        }

        returns: {
            ("battery_properties", "Technology"): {
                "storage_hours":     {...},   # each property's attrs, unchanged
                "efficiency_charge": {...},
            },
            ("lead_time_and_project_life", "Technology"): {
                "lifetime_technical": {...},
            },
        }
    """
    groups = {}
    for property_name, attrs in property_map.items():
        source_key = (attrs["table"], attrs["key_col"])
        groups.setdefault(source_key, {})[property_name] = attrs
    return groups


def _required_property_columns(props: dict[str, dict]) -> set[str]:
    """Returns every ``value_col``/``key_col`` named across a source's properties.

    The returned set of strings is primarily used to pass to ``_assert_table_valid``
    as the ``required_cols`` argument.

    I/O Example:
        props:
            fom: {table: fixed_opex_new_entrants, key_col: Technology, value_col: Base value}
            vom: {table: variable_opex_new_entrants, key_col: Generator, value_col: Base value}

        returns:
            {"Technology", "Generator", "Base value"}
    """
    return {d[col] for col in ["value_col", "key_col"] for d in props.values()}


def _get_property_value_map(
    table: pd.DataFrame, attrs: dict[str, str | float | bool]
) -> pd.Series:
    """Returns one property's value, keyed by ``key_col`` and scaled.

    Numeric columns (the default) are coerced with ``pd.to_numeric`` before ``scale``
    is applied, raising on anything unparseable — a stray typo in the IASR table. A
    map entry with ``numeric=False`` (e.g. a date string) skips both the coercion and
    the scale.

    I/O Example:
        table:
            Technology  Base value
            Wind        2.0
            CCGT        5.0

        attrs: {key_col: Technology, value_col: Base value, scale: 1000.0}

        returns (indexed by Technology):
            Wind    2000.0
            CCGT    5000.0
    """
    value_map = table.set_index(attrs["key_col"])[attrs["value_col"]]
    if attrs.get("numeric", True):
        value_map = pd.to_numeric(value_map, errors="raise") * float(
            attrs.get("scale", 1.0)
        )
        # TODO: 'year' type cols become floats from this transform - leave for
        # validator to type-correct or edit handling here? See Open-ISP/ISPyPSA#145
    return value_map


def _is_battery_row(
    df: pd.DataFrame, col_to_check: str = "Technology Type"
) -> pd.Series:
    """Boolean mask selecting battery technology rows in ``df``.

    Matches any ``col_to_check`` row that contains the literal substring
    "Batter" -- covers both "Battery Storage (Xhrs storage)" (singular)
    and "Distributed Resources Batteries" (plural). Other storage
    technologies (pumped hydro, solar thermal) intentionally do not match.
    """
    return df[col_to_check].str.contains("Batter", na=False)


def _is_pumped_hydro_row(
    df: pd.DataFrame, col_to_check: str = "Technology Type"
) -> pd.Series:
    """Boolean mask selecting pumped hydro technology rows in ``df``.

    Matches any ``col_to_check`` row that contains the literal substring
    "Pumped Hydro" -- covering all durations. Other storage technologies
    (batteries, solar thermal) intentionally do not match.
    """
    return df[col_to_check].str.contains("Pumped Hydro", na=False)


def _is_storage_row(
    df: pd.DataFrame, col_to_check: str = "Technology Type"
) -> pd.Series:
    """Wrapper that returns union of ``_is_battery_row`` and ``_is_pumped_hydro_row``."""
    return _is_battery_row(df, col_to_check) | _is_pumped_hydro_row(df, col_to_check)


def _derive_phes_symmetric_efficiency(phes: pd.DataFrame) -> pd.DataFrame:
    """Splits the round-trip 'round_trip_efficiency' (%) into charge and discharge legs.

    The IASR PHES tables give only a single round-trip efficiency. Assuming symmetric
    legs, each one-way efficiency is its square root, so e.g. a 76% round trip becomes
    ~87.2% charge and ~87.2% discharge (sqrt(0.76) ≈ 0.872). The function returns
    the input `phes` df with two new columns (efficiency_charge and efficiency_discharge),
    dropping the intermediate round_trip_efficiency column.

    I/O Example:
        phes:
            name                 round_trip_efficiency
            NQ Pumped Hydro-10h  76.0

        returns (adds the two efficiency columns):
            name                 efficiency_charge  efficiency_discharge
            NQ Pumped Hydro-10h  87.18              87.18
    """
    phes = phes.copy()
    one_way_efficiency = (phes["round_trip_efficiency"] / 100) ** 0.5 * 100
    phes["efficiency_charge"] = one_way_efficiency
    phes["efficiency_discharge"] = one_way_efficiency
    return phes.drop(columns=["round_trip_efficiency"])


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
