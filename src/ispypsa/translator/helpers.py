import logging
import re

import pandas as pd


def _get_iteration_start_and_end_time(year_type: str, start_year: int, end_year: int):
    """Get the model start year, end year, and start/end month for iteration, which depend on
    financial vs calendar year.
    """
    if year_type == "fy":
        start_year = start_year - 1
        end_year = end_year
        month = 7
    else:
        start_year = start_year
        end_year = end_year + 1
        month = 1
    return start_year, end_year, month


def _annuitised_investment_costs(
    capital_cost: float, wacc: float, asset_lifetime: int
) -> float:
    """Calculate the cost of capital cost spread over the asset lifetime.

    Args:
        capital_cost: as float, typically in $/MW
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.
        asset_lifetime: as int, asset lifetime in years.

    Returns: float specifying the annuitised cost in $/MW/yr
    """
    return (capital_cost * wacc) / (1 - (1 + wacc) ** (-1.0 * asset_lifetime))


def _get_commissioning_or_build_year_as_int(
    commissioning_date_str: str, default_build_year: int, year_type: str = "fy"
) -> int:
    """Return build year of CAA generator as an int, or default_build_year if no build year given.

    Build years are related to investment periods, so the year type (financial or
    calendar) is used to determine the correct integer year to return.

    If the commissioning date results in a year earlier than default_build_year,
    default_build_year is returned instead to align existing generators with the
    model's first investment period.

    Args:
        commissioning_date_str: string describing commissioning date of committed, anticipated
            or additional generator. Expects a date string in the format "%Y-%m-%d".
        default_build_year: integer to return if no build year is given or if the
            commissioning year is earlier than this value. Typically this will be
            one year before the first investment period year.
        year_type: str which should be "fy" or "calendar". If "fy" then investment
            periods are interpreted as specifying financial years (according to the
            calendar year the financial year ends in).

    Returns: integer, default_build_year or year of commissioning date (whichever is later).
    """
    if not isinstance(commissioning_date_str, str):
        return default_build_year
    else:
        commissioning_date = pd.to_datetime(commissioning_date_str, format="%Y-%m-%d")
        if commissioning_date.month < 7 or year_type == "calendar":
            commissioning_year = int(commissioning_date.year)
        else:
            commissioning_year = int(commissioning_date.year) + 1
        # Cap at default_build_year to align early generators with model start
        return max(commissioning_year, default_build_year)


def _get_financial_year_int_from_string(
    input_string: str, quantity: str, year_type: str = "fy"
) -> int:
    """
    Takes a string containing a financial year represented in the format YYYY_YY
    and returns the financial year as an int.

    Financial years are referred to by the end year of the financial year.
    For example, if the input string is "2023_24" then the returned int is 2024.

    Args:
        input_string: string representing a financial year in the format YYYY_YY
        quantity: string noting what quantity is being translated when this function
            is called; used for error messaging. For example, "generator marginal costs".
        year_type: str which should be "fy" or "calendar".

    Returns:
        int representing the financial year. For example, if the input string is "2023_24"
        then the returned int is 2024.

    Raises:
        ValueError if the input string does not match the expected format.
    """
    if year_type == "fy":
        check_format = re.match(
            r"^(?P<start_year>\d{4})_(?P<end_year>\d{2})($|_)", input_string
        )
        if check_format:
            start_year_string = check_format.groupdict()["start_year"]
            # adding 1 to start year instead of just returning end year to avoid
            # any potential century crossover issues
            financial_year_int = int(start_year_string) + 1
            return financial_year_int
        raise ValueError(
            f"Invalid financial year string for {quantity}: {input_string}"
        )
    elif year_type == "calendar":
        raise NotImplementedError(
            f"Calendar years are not implemented yet for {quantity}"
        )
    else:
        raise ValueError(f"Unknown year_type: {year_type}")


def _add_investment_periods_as_build_years(
    df: pd.DataFrame, investment_periods: list[int]
):
    """
    Add investment periods as build years to a pd.DataFrame, adding duplicate rows
    for each investment period as needed.

    Args:
        df (pd.DataFrame): pd.DataFrame to add investment periods to.
        investment_periods (list[int]): list of investment periods.

    Returns:
        pd.DataFrame: pd.DataFrame with added investment periods as build years.
    """
    df["build_year"] = "investment_periods"
    df["build_year"] = df["build_year"].map({"investment_periods": investment_periods})
    df = df.explode("build_year").reset_index(drop=True)
    df["build_year"] = df["build_year"].astype("int64")

    return df


def convert_to_numeric_if_possible(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Convert only numeric values to numeric leaving strings or other types that don't parse as they are.

    Args:
        df: pd.DataFrame to convert numeric columns to.
        cols: list of column names to convert.

    Returns:
        pd.DataFrame: pd.DataFrame with converted numeric columns.
    """

    for col in cols:
        df["temp_col"] = pd.to_numeric(df[col], errors="coerce")
        # Replace values that failed to convert to numeric with original strings.
        df[col] = df["temp_col"].fillna(df[col])
        df = df.drop(columns=["temp_col"])

    return df


def _resolve_wildcards(
    table: pd.DataFrame,
    allowed_values: dict[str, list],
    value_columns: list[str],
    expected_drops: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Expand a sparse "wildcard" table into one row per concrete key combination.

    A key column may be left blank (NaN) to act as a wildcard that applies to
    every value of that column. ``allowed_values`` lists, for each wildcardable
    column, the concrete values it may take — the schema's allowed_values /
    allowed_values_from, resolved to actual values. Each blank cell in those
    columns is fanned out to every allowed value; a filled cell survives only if
    it is itself an allowed value, so a row carrying an out-of-set value drops
    out. Drops are logged at INFO unless the column is named in
    ``expected_drops`` — for those, dropping is the caller's designed selection
    (e.g. keeping only investment-period years), not data loss worth surfacing.
    Key columns absent from ``allowed_values`` (e.g. timeslice) ride along
    unchanged.

    Once the blanks are filled in, several rows can land on the same key — a
    specific row and a wildcard one. The row that used the fewest wildcards (the
    most specific) wins; callers rely on the schema's *_resolve_unambiguously
    rule to guarantee there is never a tie.

    I/O Example:
        table (a blank cell is a wildcard):
            path_id  direction  timeslice  capacity
            CQ-NQ    forward    peak       1200
                                           500       # all keys blank: a global default
            N1-CNSW                                  # blank direction and capacity

        allowed_values = {"path_id": ["CQ-NQ", "N1-CNSW"],
                          "direction": ["forward", "reverse"]}
        value_columns = ["capacity"]

        returns:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward    peak       1200      # specific row, untouched
            CQ-NQ    forward               500       # global default fills the gaps...
            CQ-NQ    reverse               500
            N1-CNSW  forward               NaN       # ...but N1-CNSW's own blank-capacity
            N1-CNSW  reverse               NaN       #   row is more specific, so it wins
    """
    key_columns = [c for c in table.columns if c not in value_columns]
    work = table.copy()
    # Count each row's wildcards now, before the blanks are filled in, so that
    # after expansion we can keep the row that used the fewest wildcards wherever
    # rows land on the same key.
    work["_wildcards"] = sum(work[c].isna().astype(int) for c in allowed_values)
    # Resolve one wildcardable column per pass; each pass fills that column in.
    for column, values in allowed_values.items():
        work = _expand_column(work, column, values, column not in expected_drops)
    # Most specific (fewest wildcards) wins where rows now share a key.
    most_specific_first = work.sort_values("_wildcards", kind="stable")
    resolved = most_specific_first.drop_duplicates(key_columns, keep="first")
    return resolved.loc[:, list(table.columns)].reset_index(drop=True)


def _expand_column(
    table: pd.DataFrame, column: str, allowed_values: list, log_drops: bool
) -> pd.DataFrame:
    """Resolve a single wildcardable column of a wildcard table.

    Splits the rows on whether ``column`` is blank, then recombines: a filled
    cell is kept only if its value is allowed, while each blank (wildcard) cell is
    cross-joined with the allowed values so it fans out to one row per value.
    Splitting first is the trick that keeps any blank out of the merge key (a NaN
    key would not match itself in a merge). Out-of-set filled cells drop out,
    logged only when ``log_drops`` is set (see _resolve_wildcards).

    I/O Example:
        table:                      column = "direction"
            path_id  direction      allowed_values = ["forward", "reverse"]
            CQ-NQ    forward
            N1-CNSW                  # blank: a wildcard

        returns:
            path_id  direction
            CQ-NQ    forward         # filled and allowed: kept as-is
            N1-CNSW  forward         # blank: fanned out to every allowed value
            N1-CNSW  reverse
    """
    is_wildcard = table[column].isna()
    if log_drops:
        _log_dropped_values(table.loc[~is_wildcard, column], allowed_values, column)
    # A filled cell survives only if its value is one of the allowed values...
    concrete = table[~is_wildcard & table[column].isin(allowed_values)]
    # ...and a blank cell fans out to one row per allowed value (a cross join).
    allowed_frame = pd.DataFrame({column: allowed_values})
    expanded = table[is_wildcard].drop(columns=column).merge(allowed_frame, how="cross")
    return pd.concat([concrete, expanded], ignore_index=True)


def _log_dropped_values(values: pd.Series, allowed_values: list, column: str) -> None:
    """Log, once, any filled values dropped for falling outside the allowed set."""
    # tolist() converts numpy scalars to native types so the message reads
    # "[2025]" rather than "[np.int64(2025)]".
    dropped = sorted(set(values.dropna().tolist()) - set(allowed_values))
    if dropped:
        logging.info(f"Dropped rows whose {column} is not an allowed value: {dropped}")
